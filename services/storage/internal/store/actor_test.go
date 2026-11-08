package store_test

import (
	"bytes"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestPebbleOptionsAndPrefixBloom(t *testing.T) {
	opts := store.NewPebbleOptions(nil, false)
	defer opts.Cache.Unref()

	require.NotNil(t, opts.Comparer)
	require.NotNil(t, opts.Comparer.Split)
	require.Equal(t, 15, opts.Comparer.Split(store.EntryKey(1, []byte("a"))))
	require.Equal(t, 15, opts.Comparer.Split(store.XattrKey(1, []byte("user.k"))))
	require.Equal(t, 14, opts.Comparer.Split(store.DataKey(1, 0)))
	require.Equal(t, 14, opts.Comparer.Split(store.InodeKey(1)))
	require.Equal(t, 15, opts.Comparer.Split(store.OrphanKey(1)))
	require.Equal(t, 17, opts.Comparer.Split(store.PackStatKey(1)))
	require.Equal(t, 8, opts.Comparer.Split(store.StatKey()))

	require.Equal(t, uint64(16<<20), opts.MemTableSize)
	require.Equal(t, 128, opts.MaxOpenFiles)
	require.Equal(t, 4<<20, opts.WALBytesPerSync)
	require.Equal(t, pebble.FormatNewest, opts.FormatMajorVersion)
	require.Equal(t, int64(64<<20), opts.Cache.MaxSize())

	require.Len(t, opts.Levels, 7)
	expectedPolicy := bloom.FilterPolicy(10)
	for i, lvl := range opts.Levels {
		require.NotNil(t, lvl.FilterPolicy, "level %d missing FilterPolicy", i)
		require.Equal(t, expectedPolicy.Name(), lvl.FilterPolicy.Name())
	}
}

func TestOpenInitializesRootAndRecoversOnReopen(t *testing.T) {
	dir := t.TempDir()
	const maxBytes = uint64(100 << 20)

	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-1",
		MaxBytes: maxBytes,
	})
	require.NoError(t, err)

	// 验证初始 41B sys:stat 已落盘 WriterEpoch=0 与 MaxBytes=maxBytes
	rawStat, statCloser, err := actor.DB().Get(store.StatKey())
	require.NoError(t, err)
	require.Len(t, rawStat, store.BranchStatSize)
	gotInitStat, err := store.DecodeBranchStat(rawStat)
	require.NoError(t, statCloser.Close())
	require.NoError(t, err)
	require.Equal(t, uint64(0), gotInitStat.WriterEpoch)
	require.Equal(t, maxBytes, gotInitStat.MaxBytes)

	// 1. StatFS 零 I/O 原子直读校验
	statFS, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, maxBytes, statFS.TotalBytes)
	require.Equal(t, uint64(0), statFS.UsedBytes)
	require.Equal(t, maxBytes, statFS.AvailBytes)
	require.Equal(t, uint64(1), statFS.Files)
	require.Equal(t, store.DefaultStatFSBlockSize, statFS.Bsize)
	require.Equal(t, uint32(store.MaxFileNameBytes), statFS.Namelen)

	// 2. GetAttr(1) 并发直读根 Inode(1)
	attr, err := actor.GetAttr(store.RootIno)
	require.NoError(t, err)
	require.Equal(t, store.RootIno, attr.Ino)
	require.Equal(t, store.RootIno, attr.ParentIno)
	require.Equal(t, store.S_IFDIR|uint32(0o755), attr.Mode)
	require.Equal(t, uint32(2), attr.Nlink)
	require.Equal(t, store.DefaultDirSize, attr.Size)
	require.Equal(t, store.DefaultBlockSize, attr.Blksize)
	require.Equal(t, store.DefaultDirSize/512, attr.Blocks)
	require.Positive(t, attr.Atime)

	// 不存在的节点返回 ErrNotFound，非法编号 0 返回 ErrInvalid
	_, err = actor.GetAttr(9999)
	require.ErrorIs(t, err, store.ErrNotFound)
	_, err = actor.GetAttr(0)
	require.ErrorIs(t, err, store.ErrInvalid)

	// 3. dirtyAtime 内存叠加与正常 Close 刷盘
	futureAtime := time.Unix(0, attr.Atime).Add(10 * time.Minute)
	actor.MarkAtimeDirty(store.RootIno, futureAtime)

	attrOverlay, err := actor.GetAttr(store.RootIno)
	require.NoError(t, err)
	require.Equal(t, futureAtime.UnixNano(), attrOverlay.Atime)

	require.NoError(t, actor.Close())

	// 4. 以只读模式重新打开且不传 Options.MaxBytes，验证从持久化 sys:stat 恢复 MaxBytes 至内存原子镜像与 StatFS
	roActor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-1",
		ReadOnly: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, roActor.Close()) }()

	require.Equal(t, maxBytes, roActor.MaxBytes())
	roStat, err := roActor.StatFS()
	require.NoError(t, err)
	require.Equal(t, maxBytes, roStat.TotalBytes)
	require.Equal(t, maxBytes, roStat.AvailBytes)
	require.Equal(t, uint64(1), roStat.Files)
	require.Equal(t, uint64(0), roStat.UsedBytes)

	roAttr, err := roActor.GetAttr(store.RootIno)
	require.NoError(t, err)
	require.Equal(t, futureAtime.UnixNano(), roAttr.Atime)
	require.Equal(t, store.S_IFDIR|uint32(0o755), roAttr.Mode)
	require.Equal(t, uint32(2), roAttr.Nlink)

	// 只读实例拦截一切写操作
	_, err = roActor.Mkdir(store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("ro-dir"),
		Mode:      0o755,
	})
	require.ErrorIs(t, err, store.ErrReadOnly)
}

func TestNoDirtyKeysWrittenAcrossWritePaths(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-no-dirty",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	d, err := actor.Mkdir(store.MkdirReq{ParentIno: store.RootIno, Name: []byte("d"), Mode: 0o755})
	require.NoError(t, err)
	f, err := actor.Create(store.CreateReq{ParentIno: d.Ino, Name: []byte("f"), Mode: 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(f.Ino, 0, []byte("hello"))
	require.NoError(t, err)
	require.NoError(t, actor.Fallocate(f.Ino, 0, 4096, 0))
	require.NoError(t, actor.SetXattr(f.Ino, []byte("user.k"), []byte("v"), 0))
	require.NoError(t, actor.RemoveXattr(f.Ino, []byte("user.k")))
	uid := uint32(1000)
	_, err = actor.SetAttr(f.Ino, store.SetAttrReq{UID: &uid})
	require.NoError(t, err)
	_, err = actor.Link(store.LinkReq{Ino: f.Ino, NewParentIno: d.Ino, NewName: []byte("f2")})
	require.NoError(t, err)
	require.NoError(t, actor.Rename(store.RenameReq{OldParentIno: d.Ino, OldName: []byte("f2"), NewParentIno: d.Ino, NewName: []byte("f3")}))
	require.NoError(t, actor.Unlink(d.Ino, []byte("f3"), false))
	require.NoError(t, actor.Unlink(d.Ino, []byte("f"), false))
	require.NoError(t, actor.Rmdir(store.RootIno, []byte("d")))

	iter, err := actor.DB().NewIter(nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()
	for valid := iter.First(); valid; valid = iter.Next() {
		require.False(t, bytes.HasPrefix(iter.Key(), []byte("dirty:")), "unexpected dirty key: %q", iter.Key())
	}
	require.NoError(t, iter.Error())
}

func TestGroupCommitPhaseAFailureIsolationAndReadYourWrites(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-1",
		MaxBytes: 16384,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// 1. 先创建 file1 (Ino=2) 并预留 4096 字节
	f1, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("file1"),
		Mode:      0o644,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), f1.Ino)
	require.NoError(t, actor.Fallocate(f1.Ino, 0, 4096, 0))

	// 2. 占住当前 Tick，使后续 5 条真实命令同时进入 mb.active 并在下一轮单次 Tick 内合并演进：
	//   Sub-Cmd A: Mkdir(RootIno, "batch-dir") -> 分配 Ino=3
	//   Sub-Cmd B: Create(RootIno, "batch-dir") -> Phase A 先 PrepareIno() 预览 4，再因同名冲突返回 ErrExist（验证不消耗 Ino=4、不污染 s.batch）
	//   Sub-Cmd C: Create(3, "inside") -> Phase A 同批写后读（Intra-Batch Read-Your-Writes）读取 Sub-Cmd A 刚写入的目录 Ino=3，并成功分配 Ino=4
	//   Sub-Cmd D: Fallocate(4, 0, 32768, 0) -> 超出 16384 配额上限，Phase A 返回 ErrNoSpace（0 污染）
	//   Sub-Cmd E: Fallocate(4, 0, 4096, 0) -> 同批成功扩容 Ino=4 至 4096 字节
	release := store.HoldTickForTest(actor)

	type attrRes struct {
		attr store.Attribute
		err  error
	}
	chA := make(chan attrRes, 1)
	chB := make(chan attrRes, 1)
	chC := make(chan attrRes, 1)
	chD := make(chan error, 1)
	chE := make(chan error, 1)

	go func() {
		a, e := actor.Mkdir(store.MkdirReq{ParentIno: store.RootIno, Name: []byte("batch-dir"), Mode: 0o755})
		chA <- attrRes{a, e}
	}()
	time.Sleep(5 * time.Millisecond)

	go func() {
		a, e := actor.Create(store.CreateReq{ParentIno: store.RootIno, Name: []byte("batch-dir"), Mode: 0o644})
		chB <- attrRes{a, e}
	}()
	time.Sleep(5 * time.Millisecond)

	go func() {
		a, e := actor.Create(store.CreateReq{ParentIno: 3, Name: []byte("inside"), Mode: 0o600})
		chC <- attrRes{a, e}
	}()
	time.Sleep(5 * time.Millisecond)

	go func() {
		chD <- actor.Fallocate(4, 0, 32768, 0)
	}()
	time.Sleep(5 * time.Millisecond)

	go func() {
		chE <- actor.Fallocate(4, 0, 4096, 0)
	}()
	time.Sleep(10 * time.Millisecond)

	release()

	resA := <-chA
	resB := <-chB
	resC := <-chC
	require.NoError(t, resA.err)
	require.Equal(t, uint64(3), resA.attr.Ino)
	require.ErrorIs(t, resB.err, store.ErrExist)
	require.NoError(t, resC.err)
	// Sub-Cmd B 失败未消耗 Inode 4，Sub-Cmd C 成功拿到 Inode 4
	require.Equal(t, uint64(4), resC.attr.Ino)
	require.ErrorIs(t, <-chD, store.ErrNoSpace)
	require.NoError(t, <-chE)

	st, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(4), st.Files)
	require.Equal(t, uint64(8192), st.UsedBytes)

	attr4, err := actor.GetAttr(4)
	require.NoError(t, err)
	require.Equal(t, uint64(4096), attr4.Size)
}

func TestBuiltinInodeAllocatorAndReopenRecovery(t *testing.T) {
	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:    dir,
		DiskID: "disk-seg",
	})
	require.NoError(t, err)

	// 连续执行 4097 次 Inode 分配（编号 2..4098）
	var firstIno, lastIno uint64
	for i := range int(store.InodeSegment) + 1 {
		var attr store.Attribute
		attr, err = actor.Create(store.CreateReq{
			ParentIno: store.RootIno,
			Name:      []byte("f-" + strconv.Itoa(i)),
			Mode:      0o644,
		})
		require.NoError(t, err)
		if i == 0 {
			firstIno = attr.Ino
		}
		lastIno = attr.Ino
	}
	require.Equal(t, uint64(2), firstIno)
	require.Equal(t, uint64(4098), lastIno)
	require.NoError(t, actor.Close())

	// 重新打开后内置分配器从已有最大 Inode + 1 (4099) 继续分配
	reopened, err := store.Open(store.Options{
		Dir:    dir,
		DiskID: "disk-seg",
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()

	nextAttr, err := reopened.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("f-after-reopen"),
		Mode:      0o644,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(4099), nextAttr.Ino)
}

func TestIndexedBatchSkiplistBloatResetGuard(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:    t.TempDir(),
		DiskID: "disk-reset",
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// 1. 普通小批次仅执行 s.batch.Reset()，不触发重建
	require.NoError(t, actor.SetXattr(store.RootIno, []byte("user.small"), []byte("val"), 0))
	require.Equal(t, uint64(0), actor.BatchReallocCount())

	// 2. 在单次 Tick 内合并写入 17 条 64 KiB 扩展属性（总计 > 1 MiB），触发跳表防膨胀重建
	release := store.HoldTickForTest(actor)
	chunk := make([]byte, store.MaxXattrValueBytes)
	errCh := make(chan error, 17)
	for i := range 17 {
		go func(idx int) {
			errCh <- actor.SetXattr(store.RootIno, []byte("user.big."+strconv.Itoa(idx)), chunk, 0)
		}(i)
	}
	time.Sleep(15 * time.Millisecond)
	release()
	for range 17 {
		require.NoError(t, <-errCh)
	}
	require.Equal(t, uint64(1), actor.BatchReallocCount())

	// 3. 重建后后续正常写入依然工作正常
	f, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("after-reset"),
		Mode:      0o644,
	})
	require.NoError(t, err)
	require.NoError(t, actor.Fallocate(f.Ino, 0, 128, 0))
	st, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(128), st.UsedBytes)
}

func TestCloseDiscardSkipsAtimeAndWALFlush(t *testing.T) {
	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:    dir,
		DiskID: "disk-discard",
	})
	require.NoError(t, err)

	origAttr, err := actor.GetAttr(store.RootIno)
	require.NoError(t, err)

	dirtyTime := time.Unix(0, origAttr.Atime).Add(2 * time.Hour)
	actor.MarkAtimeDirty(store.RootIno, dirtyTime)

	// 关闭前内存叠加可见
	memAttr, err := actor.GetAttr(store.RootIno)
	require.NoError(t, err)
	require.Equal(t, dirtyTime.UnixNano(), memAttr.Atime)

	// CloseDiscard 跳过尾部 dirtyAtime 刷盘
	require.NoError(t, actor.CloseDiscard())

	// 重新打开后应保持原始 Atime，而非未刷盘的 dirtyTime
	reopened, err := store.Open(store.Options{
		Dir:    dir,
		DiskID: "disk-discard",
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()

	afterAttr, err := reopened.GetAttr(store.RootIno)
	require.NoError(t, err)
	require.Equal(t, origAttr.Atime, afterAttr.Atime)
}

func TestConcurrentReadsWritesAndThreeStepShutdown(t *testing.T) {
	for _, discard := range []bool{false, true} {
		t.Run(map[bool]string{false: "Close", true: "CloseDiscard"}[discard], func(t *testing.T) {
			actor, err := store.Open(store.Options{
				Dir:      t.TempDir(),
				DiskID:   "disk-race",
				MaxBytes: 1 << 30,
			})
			require.NoError(t, err)

			stopCh := make(chan struct{})
			errCh := make(chan error, 64)

			// 16 个并发直读协程（GetAttr + StatFS + MarkAtimeDirty）
			for range 16 {
				go func() {
					for {
						select {
						case <-stopCh:
							return
						default:
						}
						if _, rErr := actor.GetAttr(store.RootIno); rErr != nil && !errors.Is(rErr, store.ErrClosed) {
							errCh <- rErr
							return
						}
						if _, rErr := actor.StatFS(); rErr != nil && !errors.Is(rErr, store.ErrClosed) {
							errCh <- rErr
							return
						}
						actor.MarkAtimeDirty(store.RootIno, time.Now())
					}
				}()
			}

			// 16 个并发写协程（双缓冲 MPSC 入队 + Group Commit）
			for i := range 16 {
				go func(worker int) {
					key := []byte("user.w." + strconv.Itoa(worker))
					for {
						select {
						case <-stopCh:
							return
						default:
						}
						wErr := actor.SetXattr(store.RootIno, key, []byte("v"), 0)
						if wErr != nil && !errors.Is(wErr, store.ErrClosed) {
							errCh <- wErr
							return
						}
					}
				}(i)
			}

			// 2 个并发 FlushWAL 协程
			for range 2 {
				go func() {
					for {
						select {
						case <-stopCh:
							return
						default:
						}
						if fErr := actor.FlushWAL(); fErr != nil && !errors.Is(fErr, store.ErrClosed) {
							errCh <- fErr
							return
						}
					}
				}()
			}

			time.Sleep(40 * time.Millisecond)
			if discard {
				require.NoError(t, actor.CloseDiscard())
				require.NoError(t, actor.CloseDiscard())
			} else {
				require.NoError(t, actor.Close())
				require.NoError(t, actor.Close())
			}
			close(stopCh)

			select {
			case unexpectedErr := <-errCh:
				t.Fatalf("unexpected error during concurrent close: %v", unexpectedErr)
			default:
			}

			// 关闭后所有新请求必须稳定返回 ErrClosed，且 BlobStore 常驻 .pack 句柄已关闭
			_, err = actor.GetAttr(store.RootIno)
			require.ErrorIs(t, err, store.ErrClosed)
			_, err = actor.StatFS()
			require.ErrorIs(t, err, store.ErrClosed)
			err = actor.FlushWAL()
			require.ErrorIs(t, err, store.ErrClosed)
			err = actor.SetXattr(store.RootIno, []byte("user.after"), []byte("v"), 0)
			require.ErrorIs(t, err, store.ErrClosed)
			_, err = actor.Blobs().Append(store.RootIno, []byte("after-close"))
			require.ErrorIs(t, err, store.ErrClosed)
		})
	}
}
