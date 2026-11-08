package store_test

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestFullBlockWriteMaxWrite1MiBAndLocklessReadRoundTrip(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-rw",
		BranchID: "main",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	fileAttr, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("f1.bin"),
		Mode:      store.S_IFREG | store.S_ISUID | store.S_ISGID | 0o644,
	})
	require.NoError(t, err)
	ino := fileAttr.Ino

	// 预先写入 security.capability 扩展属性，验证 Write 会通过 DeleteDeferred 原子清除它并清理 S_ISUID/S_ISGID
	require.NoError(t, actor.Execute(store.WriteOp{
		Validate: func(v store.ReadView) error { return nil },
		Mutate: func(m store.MutateView) {
			store.BatchSetXattrDeferred(m.Batch(), ino, []byte("security.capability"), []byte("cap-data"))
		},
	}))

	block0 := bytes.Repeat([]byte{0x11}, store.BlobSize)
	block2 := bytes.Repeat([]byte{0x33}, store.BlobSize)

	// 1. 写入第 0 块 (offset = 0, 256 KiB)
	written, attr, err := actor.Write(ino, 0, block0)
	require.NoError(t, err)
	require.Equal(t, uint32(store.BlobSize), written)
	require.Equal(t, uint64(store.BlobSize), attr.Size)
	require.Equal(t, uint64(store.BlobSize/512), attr.Blocks)
	require.Equal(t, uint32(store.S_IFREG|0o644), attr.Mode, "Write 必须清除非目录节点的 S_ISUID/S_ISGID")

	// 验证 security.capability 已被清除
	_, closer, err := actor.DB().Get(store.XattrKey(ino, []byte("security.capability")))
	if err == nil {
		_ = closer.Close()
	}
	require.Error(t, err, "Write 必须删除 security.capability")

	// 2. 跳过第 1 块（稀疏空洞），直接写入第 2 块 (offset = 2 * 256 KiB, 256 KiB)
	written, attr, err = actor.Write(ino, 2*store.BlobSize, block2)
	require.NoError(t, err)
	require.Equal(t, uint32(store.BlobSize), written)
	require.Equal(t, uint64(3*store.BlobSize), attr.Size)

	st, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(3*store.BlobSize), st.UsedBytes)

	// 3. 单块直读（命中 hotShard）
	got0, err := actor.Read(ino, 1024, 4096)
	require.NoError(t, err)
	require.Equal(t, block0[1024:1024+4096], got0)

	// 4. 稀疏空洞直读（第 1 块未分配 DataKey，返回全零）
	gotHole, err := actor.Read(ino, store.BlobSize+512, 8192)
	require.NoError(t, err)
	require.Equal(t, make([]byte, 8192), gotHole)

	// 5. 跨块直读（跨越 block 0 尾部 -> block 1 空洞 -> block 2 头部，走 SeekPrefixGE）
	crossStart := uint64(store.BlobSize - 2048)
	crossSize := uint32(2048 + store.BlobSize + 2048)
	gotCross, err := actor.Read(ino, crossStart, crossSize)
	require.NoError(t, err)
	expectedCross := make([]byte, crossSize)
	copy(expectedCross[:2048], block0[store.BlobSize-2048:])
	// 中间 256 KiB 保持 0
	copy(expectedCross[2048+store.BlobSize:], block2[:2048])
	require.Equal(t, expectedCross, gotCross)

	// 6. 主动驱逐 hotShard 后，Read 直接走常驻 .pack 文件句柄 pread
	actor.Blobs().EvictHot(store.BlobRef{PackID: 1, Offset: 0, Length: store.BlobSize})
	actor.Blobs().EvictHot(store.BlobRef{PackID: 1, Offset: store.BlobSize, Length: store.BlobSize})
	gotCold, err := actor.Read(ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, block0, gotCold)

	// 7. 越界 EOF 读取返回空切片
	gotEOF, err := actor.Read(ino, 3*store.BlobSize, 4096)
	require.NoError(t, err)
	require.Empty(t, gotEOF)

	// 8. 单次 MaxWriteBytes = 1 MiB（跨越 4 个连续 256 KiB 逻辑块）在单轮 Tick 内原子写入
	f1M, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("one-mib.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	payload1M := make([]byte, store.MaxWriteBytes)
	for b := range 4 {
		fill := byte(0xa0 + b)
		copy(payload1M[b*store.BlobSize:(b+1)*store.BlobSize], bytes.Repeat([]byte{fill}, store.BlobSize))
	}

	ticksBefore := actor.TickCount()
	written1M, attr1M, err := actor.Write(f1M.Ino, 0, payload1M)
	require.NoError(t, err)
	require.Equal(t, uint32(store.MaxWriteBytes), written1M)
	require.Equal(t, uint64(store.MaxWriteBytes), attr1M.Size)
	require.Equal(t, ticksBefore+1, actor.TickCount(), "单次 1 MiB 跨 4 块写入必须在单个 IndexedBatch 批次中原子提交")

	for b := range uint64(4) {
		val, c, getErr := actor.DB().Get(store.DataKey(f1M.Ino, b))
		require.NoError(t, getErr)
		ref, decErr := store.DecodeBlobRef(val)
		require.NoError(t, c.Close())
		require.NoError(t, decErr)
		require.Equal(t, uint64(1), ref.PackID)
		require.Equal(t, uint32(store.BlobSize), ref.Length)
	}

	got1M, err := actor.Read(f1M.Ino, 0, store.MaxWriteBytes)
	require.NoError(t, err)
	require.Equal(t, payload1M, got1M)
}

func TestCross256KiBBoundaryPartialWriteAndTailExtension(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-partial-pack",
		BranchID: "main",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("stream.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 1. 连续 8 次 4 KiB 尾部追加写：命中活跃 .pack 最末尾同块 ExtendTail，原地顺延写指针，0 写放大
	const step = 4096
	expected := make([]byte, 0, step*8)
	for i := range 8 {
		chunk := bytes.Repeat([]byte{byte(0x10 + i)}, step)
		n, attr, wErr := actor.Write(f.Ino, uint64(len(expected)), chunk)
		require.NoError(t, wErr)
		require.Equal(t, uint32(step), n)
		expected = append(expected, chunk...)
		require.Equal(t, uint64(len(expected)), attr.Size)
	}
	// 8 次追加均在 Pack 1 Offset 0 原地顺延，ActiveOffset 恰好等于 32 KiB
	require.Equal(t, uint64(1), actor.Blobs().ActivePackID())
	require.Equal(t, uint32(len(expected)), actor.Blobs().ActiveOffset())

	val0, c0, err := actor.DB().Get(store.DataKey(f.Ino, 0))
	require.NoError(t, err)
	ref0, err := store.DecodeBlobRef(val0)
	require.NoError(t, c0.Close())
	require.NoError(t, err)
	require.Equal(t, store.BlobRef{PackID: 1, Offset: 0, Length: uint32(len(expected))}, ref0)

	// 2. 块内非尾部局部覆写：读取旧块合并后通过 Append 追加至活跃 .pack 新偏移，原偏移 [0, 32KiB) 不被原地篡改
	patch := bytes.Repeat([]byte{0xee}, 2048)
	_, _, err = actor.Write(f.Ino, 4096, patch)
	require.NoError(t, err)
	copy(expected[4096:4096+2048], patch)

	val0After, c0After, err := actor.DB().Get(store.DataKey(f.Ino, 0))
	require.NoError(t, err)
	ref0After, err := store.DecodeBlobRef(val0After)
	require.NoError(t, c0After.Close())
	require.NoError(t, err)
	require.Equal(t, store.BlobRef{
		PackID: 1,
		Offset: uint32(len(expected)),
		Length: uint32(len(expected)),
	}, ref0After)

	// 3. 任意跨 256 KiB 边界的非对齐写入：从 128 KiB 处一次写入 600 KiB（跨越 block 0 尾部、block 1 满块、block 2 头部）
	unalignedOffset := uint64(128 * 1024)
	unalignedLen := 600 * 1024
	unalignedData := make([]byte, unalignedLen)
	for i := range unalignedData {
		unalignedData[i] = byte((i*31 + 7) & 0xff)
	}
	written, attr, err := actor.Write(f.Ino, unalignedOffset, unalignedData)
	require.NoError(t, err)
	require.Equal(t, uint32(unalignedLen), written)
	require.Equal(t, unalignedOffset+uint64(unalignedLen), attr.Size)

	fullExpected := make([]byte, unalignedOffset+uint64(unalignedLen))
	copy(fullExpected, expected)
	copy(fullExpected[unalignedOffset:], unalignedData)

	// 先验证命中热缓存读取
	gotHot, err := actor.Read(f.Ino, 0, uint32(len(fullExpected)))
	require.NoError(t, err)
	require.Equal(t, fullExpected, gotHot)

	// 驱逐全部 3 个块的热缓存后，验证从常驻 .pack 句柄跨块读取
	for b := range uint64(3) {
		v, closer, getErr := actor.DB().Get(store.DataKey(f.Ino, b))
		require.NoError(t, getErr)
		r, decErr := store.DecodeBlobRef(v)
		require.NoError(t, closer.Close())
		require.NoError(t, decErr)
		actor.Blobs().EvictHot(r)
	}
	gotCold, err := actor.Read(f.Ino, 0, uint32(len(fullExpected)))
	require.NoError(t, err)
	require.Equal(t, fullExpected, gotCold)
}

func TestFullBlockOverwriteAndPhaseAFailureIsolation(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-overwrite",
		BranchID: "main",
		MaxBytes: 2 * store.BlobSize, // 配额上限 2 块 (512 KiB)
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f1, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("f1"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	f2, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("f2"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	payloadA := bytes.Repeat([]byte{0x41}, store.BlobSize)
	payloadB := bytes.Repeat([]byte{0x42}, store.BlobSize)

	// 首次写入 f1 block 0
	_, _, err = actor.Write(f1.Ino, 0, payloadA)
	require.NoError(t, err)
	require.True(t, actor.Blobs().Exists(1))

	// 覆盖写 f1 block 0：覆盖已有块净增量为 0，不触发配额超限
	_, attr, err := actor.Write(f1.Ino, 0, payloadB)
	require.NoError(t, err)
	require.Equal(t, uint64(store.BlobSize), attr.Size)
	require.True(t, actor.Blobs().Exists(1))

	got, err := actor.Read(f1.Ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, payloadB, got)

	// 构造同批提交：
	// - f1 写入第 2 块 (offset = 2 * BlobSize，使 f1 Size 从 1 块增至 3 块，净增 2 块超出总配额 2 块 -> Phase A ErrNoSpace)
	// - f2 写入第 0 块 (offset = 0，净增 1 块，总用量变为 2 块 <= 2 块 -> 同批成功提交)
	blockCh := make(chan struct{})
	enteredCh := make(chan struct{})
	gateDone := make(chan error, 1)
	go func() {
		gateDone <- actor.Execute(store.WriteOp{
			Validate: func(v store.ReadView) error {
				close(enteredCh)
				<-blockCh
				return nil
			},
			Mutate: func(m store.MutateView) {},
		})
	}()
	<-enteredCh

	errF1Ch := make(chan error, 1)
	errF2Ch := make(chan error, 1)

	go func() {
		_, _, wErr := actor.Write(f1.Ino, 2*store.BlobSize, payloadA)
		errF1Ch <- wErr
	}()
	time.Sleep(10 * time.Millisecond)
	go func() {
		_, _, wErr := actor.Write(f2.Ino, 0, payloadA)
		errF2Ch <- wErr
	}()
	time.Sleep(15 * time.Millisecond)
	close(blockCh)

	require.NoError(t, <-gateDone)
	require.ErrorIs(t, <-errF1Ch, store.ErrNoSpace)
	require.NoError(t, <-errF2Ch)

	gotF1, err := actor.Read(f1.Ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, payloadB, gotF1)

	gotF2, err := actor.Read(f2.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, payloadA[:4096], gotF2)
}

func TestConcurrentWritesAndLocklessReadsOnPack(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-conc",
		BranchID: "main",
		MaxBytes: 512 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	const fileCount = 4
	const blocksPerFile = 4
	inos := make([]uint64, fileCount)
	for i := range fileCount {
		attr, err := actor.Create(store.CreateReq{
			ParentIno: store.RootIno,
			Name:      []byte{byte('a' + i)},
			Mode:      store.S_IFREG | 0o644,
		})
		require.NoError(t, err)
		inos[i] = attr.Ino
	}

	var wg sync.WaitGroup
	// 并发整块写、跨块局部写与覆盖写
	for fIdx, ino := range inos {
		for bIdx := range blocksPerFile {
			wg.Add(1)
			go func(fileIndex int, fileIno uint64, blockIndex int) {
				defer wg.Done()
				for round := range 3 {
					fill := byte((fileIndex+1)*16 + blockIndex*4 + round)
					data := bytes.Repeat([]byte{fill}, store.BlobSize)
					n, _, wErr := actor.Write(fileIno, uint64(blockIndex)*store.BlobSize, data)
					require.NoError(t, wErr)
					require.Equal(t, uint32(store.BlobSize), n)
				}
			}(fIdx, ino, bIdx)
		}
	}

	// 并发无锁读取（无需 ENOENT 重试，全程 0 错误）
	stopRead := make(chan struct{})
	var readWg sync.WaitGroup
	for _, ino := range inos {
		readWg.Add(1)
		go func(fileIno uint64) {
			defer readWg.Done()
			for {
				select {
				case <-stopRead:
					return
				default:
				}
				_, rErr := actor.Read(fileIno, 0, 2*store.BlobSize)
				require.NoError(t, rErr)
			}
		}(ino)
	}

	wg.Wait()
	close(stopRead)
	readWg.Wait()

	// 验证所有写入均收敛在扁平 packs/*.pack 封包文件中
	entries, err := os.ReadDir(actor.Blobs().Root())
	require.NoError(t, err)
	var packCount int
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".pack" {
			packCount++
		}
	}
	require.Equal(t, 1, packCount, "64 MiB 上限下 48 次 256 KiB 写入仅产生 1 个活跃 .pack 文件")
}

func TestWriteErrorPathsAndStage1FailureOrClosedCleanup(t *testing.T) {
	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-err",
		BranchID: "main",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)

	regFile, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("reg"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	fifoNode, err := actor.Mknod(store.MknodReq{
		ParentIno: store.RootIno,
		Name:      []byte("fifo"),
		Mode:      store.S_IFIFO | 0o644,
	})
	require.NoError(t, err)

	blockData := bytes.Repeat([]byte{0x55}, store.BlobSize)

	// 1. 阶段 1 磁盘写入失败时直接返回错误，绝不进入 Actor 队列触发 Tick
	prevTicks := actor.TickCount()
	require.NoError(t, os.WriteFile(actor.Blobs().Root(), []byte("not-a-dir"), 0o644))
	_, _, err = actor.Write(regFile.Ino, 0, blockData)
	require.Error(t, err)
	require.Equal(t, prevTicks, actor.TickCount(), "阶段 1 磁盘写入失败不得进入 Actor 队列触发 Tick")
	require.NoError(t, os.Remove(actor.Blobs().Root()))

	// 2. 读写目录节点返回 ErrIsDir，读写 FIFO 返回 ErrInvalid，读写不存在节点返回 ErrNotFound
	_, _, err = actor.Write(store.RootIno, 0, blockData)
	require.ErrorIs(t, err, store.ErrIsDir)
	_, err = actor.Read(store.RootIno, 0, 128)
	require.ErrorIs(t, err, store.ErrIsDir)

	_, _, err = actor.Write(fifoNode.Ino, 0, blockData)
	require.ErrorIs(t, err, store.ErrInvalid)
	_, err = actor.Read(fifoNode.Ino, 0, 128)
	require.ErrorIs(t, err, store.ErrInvalid)

	_, _, err = actor.Write(99999, 0, blockData)
	require.ErrorIs(t, err, store.ErrNotFound)
	_, err = actor.Read(99999, 0, 128)
	require.ErrorIs(t, err, store.ErrNotFound)

	// 3. 0 字节写入校验存活并返回原属性，不分配物理块
	n, zeroAttr, err := actor.Write(regFile.Ino, 0, nil)
	require.NoError(t, err)
	require.Equal(t, uint32(0), n)
	require.Equal(t, uint64(0), zeroAttr.Size)

	// 4. 关闭后 Write 返回 ErrClosed
	require.NoError(t, actor.Close())
	_, _, err = actor.Write(regFile.Ino, 0, blockData)
	require.ErrorIs(t, err, store.ErrClosed)
}
