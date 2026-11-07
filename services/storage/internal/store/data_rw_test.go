package store_test

import (
	"bytes"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestFullBlockWriteAndLocklessReadRoundTrip(t *testing.T) {
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

	// 1. 写入第 0 块 (offset = 0, 1 MiB)
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

	// 2. 跳过第 1 块（稀疏空洞），直接写入第 2 块 (offset = 2 MiB, 1 MiB)
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
	// 中间 1 MiB 保持 0
	copy(expectedCross[2048+store.BlobSize:], block2[:2048])
	require.Equal(t, expectedCross, gotCross)

	// 6. 主动驱逐 hotShard 后，Read 自动回退磁盘 pread
	actor.Blobs().EvictHot(1)
	actor.Blobs().EvictHot(2)
	gotCold, err := actor.Read(ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, block0, gotCold)

	// 7. 越界 EOF 读取返回空切片
	gotEOF, err := actor.Read(ino, 3*store.BlobSize, 4096)
	require.NoError(t, err)
	require.Empty(t, gotEOF)
}

func TestFullBlockOverwriteRemovesOldBlobAndPhaseAFailureCleansPreallocatedBlob(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-overwrite",
		BranchID: "main",
		MaxBytes: 2 * store.BlobSize, // 配额上限 2 MiB
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

	// 首次写入 f1 block 0 (blobID = 1)
	_, _, err = actor.Write(f1.Ino, 0, payloadA)
	require.NoError(t, err)
	require.True(t, actor.Blobs().Exists(1))

	// 覆盖写 f1 block 0 (blobID = 2)：
	// - 覆盖已有块净增量为 0，不触发配额超限
	// - 旧块 blobID=1 必须在 Stage 4 被调用方协程从磁盘物理删净且从 hotShard 摘除
	_, attr, err := actor.Write(f1.Ino, 0, payloadB)
	require.NoError(t, err)
	require.Equal(t, uint64(store.BlobSize), attr.Size)
	require.False(t, actor.Blobs().Exists(1), "被覆盖的旧 .blob 文件必须被物理删除")
	require.True(t, actor.Blobs().Exists(2))

	got, err := actor.Read(f1.Ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, payloadB, got)

	// 构造同批提交：
	// - f1 写入第 2 块 (offset = 2 MiB，使 f1 Size 从 1 MiB 增至 3 MiB，净增 2 MiB 超出总配额 2 MiB -> Phase A ErrNoSpace)
	// - f2 写入第 0 块 (offset = 0，净增 1 MiB，总用量变为 2 MiB <= 2 MiB -> 同批成功提交)
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

	// 验证 f1 失败预写的块 (blobID = 3) 已在队列外被自动物理清理，而 f2 成功的块 (blobID = 4) 正常存在
	require.False(t, actor.Blobs().Exists(3), "Phase A 配额失败的预写 .blob 文件必须被清理")
	require.True(t, actor.Blobs().Exists(4), "同批另一条合法 Write 必须正常提交")

	gotF2, err := actor.Read(f2.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, payloadA[:4096], gotF2)
}

func TestReadENOENTRetryOnConcurrentOverwrite(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-enoent",
		BranchID: "main",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("race.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	v1 := bytes.Repeat([]byte{0x11}, store.BlobSize)
	v2 := bytes.Repeat([]byte{0x22}, store.BlobSize)

	_, _, err = actor.Write(f.Ino, 0, v1)
	require.NoError(t, err)

	// 驱逐 hotShard 使 Read 必须走磁盘 os.Open
	actor.Blobs().EvictHot(1)

	// 注入测试钩子：当 Read 第一次读出旧 DataKey (blobID=1)、正准备打开 1.blob 前，
	// 并发完成一次覆盖写 (写入 v2, blobID=2) 并将旧 1.blob 物理删除，迫使第一次 os.Open 返回 ENOENT，
	// 验证 Read 自动重读最新 DataKey (blobID=2) 并返回 v2 数据。
	var once sync.Once
	actor.SetTestBeforeBlobReadHook(func(ino, blockIdx, blobID uint64, attempt int) {
		once.Do(func() {
			_, _, wErr := actor.Write(ino, 0, v2)
			require.NoError(t, wErr)
			// 驱逐新块的热缓存以同时验证重试时也能走磁盘读取
			actor.Blobs().EvictHot(2)
		})
	})
	defer actor.SetTestBeforeBlobReadHook(nil)

	got, err := actor.Read(f.Ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, v2, got)
}

func TestConcurrentFullBlockWritesAndReads(t *testing.T) {
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
	// 并发整块写与覆盖写
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

	// 并发无锁读取
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

	// 验证最终每个块只有 1 个存活物理块文件，总共 fileCount * blocksPerFile = 16 个 .blob 文件
	var liveBlobCount int
	entries, err := os.ReadDir(actor.Blobs().Root())
	require.NoError(t, err)
	for _, shard := range entries {
		if !shard.IsDir() {
			continue
		}
		files, err := os.ReadDir(actor.Blobs().Root() + "/" + shard.Name())
		require.NoError(t, err)
		liveBlobCount += len(files)
	}
	require.Equal(t, fileCount*blocksPerFile, liveBlobCount, "所有被覆盖的旧 .blob 物理文件必须已被调用方协程删净")
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

	// 1. 读写目录节点返回 ErrIsDir，读写 FIFO 返回 ErrInvalid，读写不存在节点返回 ErrNotFound
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

	// 2. 0 字节写入校验存活并返回原属性，不分配物理块
	n, zeroAttr, err := actor.Write(regFile.Ino, 0, nil)
	require.NoError(t, err)
	require.Equal(t, uint32(0), n)
	require.Equal(t, uint64(0), zeroAttr.Size)

	// 3. 阶段 1 磁盘写入失败时当场清理并返回错误，绝不进入 Actor 队列
	prevTicks := actor.TickCount()
	// 将下一个要分配的块 (id=2 -> shard "02") 的分片目录占为普通文件，触发阶段 1 OpenFile 失败
	// 先确认当前下一个 ID：前序 ErrIsDir/ErrInvalid/ErrNotFound 在阶段 1 预写了 1..3 并已在 Phase A 失败清理，下一个 ID 是 4 (shard "04")
	shard04 := actor.Blobs().Root() + "/04"
	require.NoError(t, os.WriteFile(shard04, []byte("not-a-dir"), 0o644))
	_, _, err = actor.Write(regFile.Ino, 0, blockData)
	require.Error(t, err)
	require.Equal(t, prevTicks, actor.TickCount(), "阶段 1 磁盘写入失败不得进入 Actor 队列触发 Tick")
	require.NoError(t, os.Remove(shard04))

	// 4. 关闭后 Write 返回 ErrClosed
	require.NoError(t, actor.Close())
	_, _, err = actor.Write(regFile.Ino, 0, blockData)
	require.ErrorIs(t, err, store.ErrClosed)
}
