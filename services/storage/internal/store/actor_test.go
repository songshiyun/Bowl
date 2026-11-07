package store_test

import (
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
	require.Equal(t, 14, opts.Comparer.Split(store.DirtyKey(1)))
	require.Equal(t, 15, opts.Comparer.Split(store.OrphanKey(1)))
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
		BranchID: "main",
		MaxBytes: maxBytes,
	})
	require.NoError(t, err)

	// 1. StatFS 零 I/O 原子直读校验
	statFS, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, maxBytes, statFS.TotalBytes)
	require.Equal(t, uint64(0), statFS.UsedBytes)
	require.Equal(t, maxBytes, statFS.AvailBytes)
	require.Equal(t, uint64(1), statFS.Files)
	require.Equal(t, uint32(store.DefaultStatFSBlockSize), statFS.Bsize)
	require.Equal(t, uint32(store.MaxFileNameBytes), statFS.Namelen)

	// 2. GetAttr(1) 并发直读根 Inode(1)
	attr, err := actor.GetAttr(store.RootIno)
	require.NoError(t, err)
	require.Equal(t, store.RootIno, attr.Ino)
	require.Equal(t, store.RootIno, attr.ParentIno)
	require.Equal(t, store.S_IFDIR|uint32(0o755), attr.Mode)
	require.Equal(t, uint32(2), attr.Nlink)
	require.Equal(t, uint64(store.DefaultDirSize), attr.Size)
	require.Equal(t, uint32(store.DefaultBlockSize), attr.Blksize)
	require.Equal(t, uint64(store.DefaultDirSize/512), attr.Blocks)
	require.Greater(t, attr.Atime, int64(0))

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

	// 4. 以只读模式重新打开，验证根 Inode、统计值及 Close 刷盘的 dirtyAtime 均已持久化恢复
	roActor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-1",
		BranchID: "main",
		ReadOnly: true,
		MaxBytes: maxBytes,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, roActor.Close()) }()

	roStat, err := roActor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(1), roStat.Files)
	require.Equal(t, uint64(0), roStat.UsedBytes)

	roAttr, err := roActor.GetAttr(store.RootIno)
	require.NoError(t, err)
	require.Equal(t, futureAtime.UnixNano(), roAttr.Atime)
	require.Equal(t, store.S_IFDIR|uint32(0o755), roAttr.Mode)
	require.Equal(t, uint32(2), roAttr.Nlink)

	// 只读实例拦截一切写操作
	err = roActor.Execute(store.WriteOp{
		Validate: func(v store.ReadView) error { return nil },
		Mutate:   func(m store.MutateView) {},
	})
	require.ErrorIs(t, err, store.ErrReadOnly)
}

func TestGroupCommitPhaseAFailureIsolationAndReadYourWrites(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-1",
		BranchID: "main",
		MaxBytes: 16384,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// 通过栅栏让 3 条写命令在同一轮或者连续批次中演进：
	// Cmd 1: 正常分配 Inode(2) 并写入 4096 字节
	// Cmd 2: Phase A 先调用 PrepareIno() 预览 Inode(3)，随后因配额超限失败（验证不消耗 Inode(3)、不污染 s.batch）
	// Cmd 3: Phase A 通过 ReadView.Get 读取 Cmd 1 写入的 Inode(2)，并分配 Inode(3) 写入 4096 字节
	var (
		ino1, ino3       uint64
		err1, err2, err3 error
	)

	err1 = actor.Execute(store.WriteOp{
		Validate: func(v store.ReadView) error {
			if err := v.CheckQuota(4096, 1); err != nil {
				return err
			}
			_, err := v.PrepareIno()
			return err
		},
		Mutate: func(m store.MutateView) {
			ino1 = m.ConsumeIno()
			seq := m.NextWriteSeq()
			now := time.Now().UnixNano()
			_ = store.BatchSetInodeDeferred(m.Batch(), store.Inode{
				Ino:       ino1,
				ParentIno: store.RootIno,
				Mode:      store.S_IFREG | 0o644,
				Nlink:     1,
				Size:      4096,
				Atime:     now,
				Mtime:     now,
				Ctime:     now,
				WriteSeq:  seq,
			})
			store.BatchSetEntryDeferred(m.Batch(), store.RootIno, []byte("file1"), store.Entry{
				Ino:  ino1,
				Mode: store.S_IFREG | 0o644,
			})
			m.AddInodes(1)
			m.AddLogicalBytes(4096)
		},
	})
	require.NoError(t, err1)
	require.Equal(t, uint64(2), ino1)

	// 构造同批提交：先用 1 条阻塞在信号上的命令占住 Actor 循环，使后续命令同时进入 mb.active 队列并在单次 Tick 内合并执行
	blockCh := make(chan struct{})
	enteredCh := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- actor.Execute(store.WriteOp{
			Validate: func(v store.ReadView) error {
				close(enteredCh)
				<-blockCh
				return nil
			},
			Mutate: func(m store.MutateView) {},
		})
	}()
	<-enteredCh

	// 此时 Actor 正在当前 Tick 等待 blockCh，我们按序投递 3 条命令进入 mb.active（保证在下一轮 Tick 同批合并执行）：
	// Sub-Cmd A: 写入 Entry("batch-a") -> Ino(2)
	// Sub-Cmd B: Phase A 预览 PrepareIno() 后返回 ErrNoSpace 失败（验证 0 污染）
	// Sub-Cmd C: Phase A 通过 ReadView.Get 同批写后读验证 "batch-a" 已存在，然后分配 Inode(3)
	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	doneC := make(chan error, 1)
	enqueued := make(chan struct{}, 3)

	go func() {
		enqueued <- struct{}{}
		doneA <- actor.Execute(store.WriteOp{
			Validate: func(v store.ReadView) error { return nil },
			Mutate: func(m store.MutateView) {
				store.BatchSetEntryDeferred(m.Batch(), store.RootIno, []byte("batch-a"), store.Entry{
					Ino:  ino1,
					Mode: store.S_IFREG | 0o644,
				})
			},
		})
	}()

	time.Sleep(5 * time.Millisecond)
	var subCmdBPhaseBCalled bool
	go func() {
		enqueued <- struct{}{}
		doneB <- actor.Execute(store.WriteOp{
			Validate: func(v store.ReadView) error {
				candIno, err := v.PrepareIno()
				if err != nil {
					return err
				}
				if candIno != 3 {
					return store.ErrInvalidValue
				}
				// 超出 16384 配额上限，触发 Phase A 失败
				return v.CheckQuota(32768, 1)
			},
			Mutate: func(m store.MutateView) {
				subCmdBPhaseBCalled = true
			},
		})
	}()

	time.Sleep(5 * time.Millisecond)
	go func() {
		enqueued <- struct{}{}
		doneC <- actor.Execute(store.WriteOp{
			Validate: func(v store.ReadView) error {
				// 验证同批写后读（Intra-Batch Read-Your-Writes）：能够读到同批 Sub-Cmd A 刚写入的 "batch-a"
				val, closer, err := v.Get(store.EntryKey(store.RootIno, []byte("batch-a")))
				if err != nil {
					return err
				}
				e, decErr := store.DecodeEntry(val)
				_ = closer.Close()
				if decErr != nil {
					return decErr
				}
				if e.Ino != ino1 {
					return store.ErrInvalidValue
				}
				if err := v.CheckQuota(4096, 1); err != nil {
					return err
				}
				_, err = v.PrepareIno()
				return err
			},
			Mutate: func(m store.MutateView) {
				ino3 = m.ConsumeIno()
				seq := m.NextWriteSeq()
				now := time.Now().UnixNano()
				_ = store.BatchSetInodeDeferred(m.Batch(), store.Inode{
					Ino:       ino3,
					ParentIno: store.RootIno,
					Mode:      store.S_IFREG | 0o600,
					Nlink:     1,
					Size:      4096,
					Atime:     now,
					Mtime:     now,
					Ctime:     now,
					WriteSeq:  seq,
				})
				m.AddInodes(1)
				m.AddLogicalBytes(4096)
			},
		})
	}()

	for range 3 {
		<-enqueued
	}
	time.Sleep(10 * time.Millisecond)
	close(blockCh)

	require.NoError(t, <-firstDone)
	require.NoError(t, <-doneA)
	err2 = <-doneB
	err3 = <-doneC
	require.ErrorIs(t, err2, store.ErrNoSpace)
	require.False(t, subCmdBPhaseBCalled)
	require.NoError(t, err3)
	// Sub-Cmd B 失败未消耗 Inode 3，Sub-Cmd C 成功拿到 Inode 3
	require.Equal(t, uint64(3), ino3)

	st, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(3), st.Files)
	require.Equal(t, uint64(8192), st.UsedBytes)

	attr3, err := actor.GetAttr(3)
	require.NoError(t, err)
	require.Equal(t, uint64(4096), attr3.Size)
}

func TestInodeSegmentReservation4096AndExhaustion(t *testing.T) {
	var (
		reserveCalls int
		lastReserved uint64 = 1
		exhaust      bool
	)

	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-seg",
		BranchID: "main",
		ReserveInode: func(count uint64) (uint64, uint64, error) {
			if count != store.InodeSegment {
				return 0, 0, store.ErrInvalid
			}
			if exhaust {
				return 0, 0, store.ErrInodeExhausted
			}
			reserveCalls++
			first := lastReserved + 1
			last := lastReserved + count
			lastReserved = last
			return first, last, nil
		},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// 连续执行 4096 次 Inode 分配（编号 2..4097），仅触发 1 次底层号段预留
	var firstIno, lastInoInFirstSeg uint64
	for i := range int(store.InodeSegment) {
		var allocated uint64
		err := actor.Execute(store.WriteOp{
			Validate: func(v store.ReadView) error {
				_, err := v.PrepareIno()
				return err
			},
			Mutate: func(m store.MutateView) {
				allocated = m.ConsumeIno()
			},
		})
		require.NoError(t, err)
		if i == 0 {
			firstIno = allocated
		}
		if i == int(store.InodeSegment)-1 {
			lastInoInFirstSeg = allocated
		}
	}
	require.Equal(t, uint64(2), firstIno)
	require.Equal(t, uint64(4097), lastInoInFirstSeg)
	require.Equal(t, 1, reserveCalls)

	// 第 4097 次分配触发第 2 次 4096 步长预留，拿到 4098
	var nextSegIno uint64
	err = actor.Execute(store.WriteOp{
		Validate: func(v store.ReadView) error {
			_, err := v.PrepareIno()
			return err
		},
		Mutate: func(m store.MutateView) {
			nextSegIno = m.ConsumeIno()
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(4098), nextSegIno)
	require.Equal(t, 2, reserveCalls)

	// 耗尽测试：将剩余预留消费完后触发 ErrInodeExhausted
	for range int(store.InodeSegment) - 1 {
		require.NoError(t, actor.Execute(store.WriteOp{
			Validate: func(v store.ReadView) error {
				_, err := v.PrepareIno()
				return err
			},
			Mutate: func(m store.MutateView) {
				_ = m.ConsumeIno()
			},
		}))
	}
	exhaust = true
	var exhaustedMutateCalled bool
	err = actor.Execute(store.WriteOp{
		Validate: func(v store.ReadView) error {
			_, err := v.PrepareIno()
			return err
		},
		Mutate: func(m store.MutateView) {
			exhaustedMutateCalled = true
		},
	})
	require.ErrorIs(t, err, store.ErrInodeExhausted)
	require.False(t, exhaustedMutateCalled)
}

func TestIndexedBatchSkiplistBloatResetGuard(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-reset",
		BranchID: "main",
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// 1. 普通小批次仅执行 s.batch.Reset()，不触发重建
	err = actor.Execute(store.WriteOp{
		Validate: func(v store.ReadView) error { return nil },
		Mutate: func(m store.MutateView) {
			store.BatchSetXattrDeferred(m.Batch(), store.RootIno, []byte("user.small"), []byte("val"))
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(0), actor.BatchReallocCount())

	// 2. 写入超过 1 MiB (len(s.batch.Repr()) > 1<<20) 触发跳表防膨胀重建
	bigPayload := make([]byte, (1<<20)+1024)
	err = actor.Execute(store.WriteOp{
		Validate: func(v store.ReadView) error { return nil },
		Mutate: func(m store.MutateView) {
			store.BatchSetXattrDeferred(m.Batch(), store.RootIno, []byte("user.big"), bigPayload)
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), actor.BatchReallocCount())

	// 3. 写入超过 8192 条操作 (s.batch.Count() > 8192) 触发跳表防膨胀重建
	err = actor.Execute(store.WriteOp{
		Validate: func(v store.ReadView) error { return nil },
		Mutate: func(m store.MutateView) {
			for i := range 8193 {
				store.BatchSetDirtyDeferred(m.Batch(), uint64(i+10))
			}
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), actor.BatchReallocCount())

	// 4. 重建后后续正常写入依然工作正常
	err = actor.Execute(store.WriteOp{
		Validate: func(v store.ReadView) error { return nil },
		Mutate: func(m store.MutateView) {
			m.AddLogicalBytes(128)
		},
	})
	require.NoError(t, err)
	st, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(128), st.UsedBytes)
}

func TestCloseDiscardSkipsAtimeAndWALFlush(t *testing.T) {
	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-discard",
		BranchID: "main",
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
		Dir:      dir,
		DiskID:   "disk-discard",
		BranchID: "main",
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
				BranchID: "main",
				MaxBytes: 1 << 30,
			})
			require.NoError(t, err)

			stopCh := make(chan struct{})
			errCh := make(chan error, 64)

			// 16 个并发直读协程（GetAttr + StatFS + MarkAtimeDirty）
			for i := range 16 {
				go func(worker int) {
					for {
						select {
						case <-stopCh:
							return
						default:
						}
						if _, err := actor.GetAttr(store.RootIno); err != nil && err != store.ErrClosed {
							errCh <- err
							return
						}
						if _, err := actor.StatFS(); err != nil && err != store.ErrClosed {
							errCh <- err
							return
						}
						actor.MarkAtimeDirty(store.RootIno, time.Now())
					}
				}(i)
			}

			// 16 个并发写协程（双缓冲 MPSC 入队 + Group Commit）
			for i := range 16 {
				go func(worker int) {
					for {
						select {
						case <-stopCh:
							return
						default:
						}
						err := actor.Execute(store.WriteOp{
							Validate: func(v store.ReadView) error {
								_, err := v.PrepareIno()
								return err
							},
							Mutate: func(m store.MutateView) {
								ino := m.ConsumeIno()
								now := time.Now().UnixNano()
								_ = store.BatchSetInodeDeferred(m.Batch(), store.Inode{
									Ino:       ino,
									ParentIno: store.RootIno,
									Mode:      store.S_IFREG | 0o644,
									Nlink:     1,
									Size:      64,
									Atime:     now,
									Mtime:     now,
									Ctime:     now,
									WriteSeq:  m.NextWriteSeq(),
								})
								m.AddInodes(1)
								m.AddLogicalBytes(64)
							},
						})
						if err != nil && err != store.ErrClosed {
							errCh <- err
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
						if err := actor.FlushWAL(); err != nil && err != store.ErrClosed {
							errCh <- err
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

			// 关闭后所有新请求必须稳定返回 ErrClosed
			_, err = actor.GetAttr(store.RootIno)
			require.ErrorIs(t, err, store.ErrClosed)
			_, err = actor.StatFS()
			require.ErrorIs(t, err, store.ErrClosed)
			err = actor.FlushWAL()
			require.ErrorIs(t, err, store.ErrClosed)
			err = actor.Execute(store.WriteOp{
				Validate: func(v store.ReadView) error { return nil },
				Mutate:   func(m store.MutateView) {},
			})
			require.ErrorIs(t, err, store.ErrClosed)
		})
	}
}
