package meta_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestBranchLockTableAscendingDeduplicatedInodeLocks(t *testing.T) {
	locks := meta.NewBranchLockTable()
	require.NotNil(t, locks)

	// 先对 ino=20 加写锁，再在子协程中调用 LockInodesWrite("d1", "main", 30, 10, 20, 10, 0)。
	// 若按升序 (10 -> 20 -> 30) 去重加锁，子协程应先持有 ino=10 的写锁，并在等待 ino=20 时阻塞，且尚未触碰 ino=30。
	unlock20 := locks.LockInodeWrite("d1", "main", 20)

	started := make(chan struct{})
	acquired := make(chan func())
	go func() {
		close(started)
		unlockAll := locks.LockInodesWrite("d1", "main", 30, 10, 20, 10, 0)
		acquired <- unlockAll
	}()
	<-started

	// 等待子协程已经获取 ino=10 并阻塞在 ino=20（ino=10, 20 共 2 个活跃锁条目，ino=30 尚未获取）
	require.Eventually(t, func() bool {
		return locks.ActiveInodeLocks() == 2
	}, 2*meta.DefaultSessionLeaseTTL, meta.DefaultSessionLeaseTTL/100)

	// 此时 ino=30 尚未被锁定，主协程可以立即获取并释放 ino=30 的写锁
	unlock30 := locks.LockInodeWrite("d1", "main", 30)
	unlock30()

	// 释放 ino=20 后，子协程应顺利完成 ino=20 与 ino=30 的加锁
	unlock20()
	unlockAll := <-acquired
	require.Equal(t, 3, locks.ActiveInodeLocks())

	unlockAll()
	require.Equal(t, 0, locks.ActiveInodeLocks())
}

func TestBranchLockTableConcurrentInodeLockRefCountConservation(t *testing.T) {
	locks := meta.NewBranchLockTable()
	const workers = 32
	const iterations = 200
	inos := []uint64{1, 2, 3, 4, 5, 6, 7, 8}

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := range workers {
		go func(workerID int) {
			defer wg.Done()
			for i := range iterations {
				a := inos[(workerID+i)%len(inos)]
				b := inos[(workerID*3+i*5+1)%len(inos)]
				c := inos[(workerID*7+i*2+3)%len(inos)]
				switch (workerID + i) % 3 {
				case 0:
					unlock := locks.LockInodeRead("d1", "main", a)
					unlock()
				case 1:
					unlock := locks.LockInodeWrite("d1", "main", b)
					unlock()
				default:
					// 传入乱序且可能包含重复的 Inode 编号，验证并发交叉时无死锁且解锁后引用计数归零
					unlock := locks.LockInodesWrite("d1", "main", c, a, b, a, 0)
					unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	require.Equal(t, 0, locks.ActiveInodeLocks(), "全部并发加锁/解锁完成后 inodeLocks 内部 Map 必须归零清空")
}

func TestBranchLockTableEvictBranchAndDiskCleansLockEntries(t *testing.T) {
	locks := meta.NewBranchLockTable()

	// 在 disk-1 的 main 与 feature 分支、disk-2 的 main 分支上触发分支拓扑锁与分支元数据锁
	unlockR := locks.LockBranchRead("disk-1", "main")
	unlockR()
	unlockW := locks.LockBranchWrite("disk-1", "feature")
	unlockW()
	unlockM1 := locks.LockBranchMeta("disk-1", "main")
	unlockM1()
	unlockM2 := locks.LockBranchMeta("disk-1", "feature")
	unlockM2()
	unlockD2 := locks.LockBranchRead("disk-2", "main")
	unlockD2()
	unlockMD2 := locks.LockBranchMeta("disk-2", "main")
	unlockMD2()

	require.True(t, locks.HasBranchLocks("disk-1", "main"))
	require.True(t, locks.HasBranchLocks("disk-1", "feature"))
	require.True(t, locks.HasBranchLocks("disk-2", "main"))

	// 1. 删除单个分支时，仅清理该分支的拓扑锁与元数据锁条目
	locks.EvictBranch("disk-1", "feature")
	require.False(t, locks.HasBranchLocks("disk-1", "feature"))
	require.True(t, locks.HasBranchLocks("disk-1", "main"))
	require.True(t, locks.HasBranchLocks("disk-2", "main"))

	// 2. 删除整块磁盘时，清理该磁盘下所有剩余分支锁与元数据锁条目，不影响其他磁盘
	locks.EvictDisk("disk-1")
	require.False(t, locks.HasBranchLocks("disk-1", "main"))
	require.True(t, locks.HasBranchLocks("disk-2", "main"))
}

func TestManagerDeleteBranchAndDiskCleansBranchLockTable(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	file, err := e.mgr.Create(e.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("f.txt"), Mode: 0o644, SessionID: &e.sid})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("hello"), SessionID: &e.sid})
	require.NoError(t, err)
	commit, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)

	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "feature", SourceCommitID: &commit.CommitID})
	require.NoError(t, err)
	featSID := uint64(501)
	featBranch := "feature"
	_, err = e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: featSID, DiskID: e.disk.DiskID, BranchName: &featBranch})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("world"), SessionID: &featSID})
	require.NoError(t, err)
	require.NoError(t, e.mgr.CloseSession(e.ctx, featSID))

	locks := meta.NewBranchLockTable()
	mgr := meta.New(db.NewStore(e.pool), meta.WithStorageRoot(e.root), meta.WithPebbleManager(e.storage), meta.WithBranchLockTable(locks))

	// 通过新管理器实例在 main 与 feature 上执行写入以填充其锁表条目
	featSID2 := uint64(502)
	_, err = mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: featSID2, DiskID: e.disk.DiskID, BranchName: &featBranch})
	require.NoError(t, err)
	_, err = mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("feat2"), SessionID: &featSID2})
	require.NoError(t, err)
	require.NoError(t, mgr.CloseSession(e.ctx, featSID2))

	_, err = mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("main2"), SessionID: &e.sid})
	require.NoError(t, err)
	require.NoError(t, mgr.CloseSession(e.ctx, e.sid))

	require.True(t, locks.HasBranchLocks(e.disk.DiskID, "main"))
	require.True(t, locks.HasBranchLocks(e.disk.DiskID, "feature"))
	require.Equal(t, 0, locks.ActiveInodeLocks())

	// 删除分支后，feature 分支的锁与元数据锁条目被清理，main 保留
	require.NoError(t, mgr.DeleteBranch(e.ctx, meta.DeleteBranchParams{DiskID: e.disk.DiskID, Name: "feature"}))
	require.False(t, locks.HasBranchLocks(e.disk.DiskID, "feature"))
	require.True(t, locks.HasBranchLocks(e.disk.DiskID, "main"))

	// 删除磁盘后，该磁盘下所有剩余分支锁与元数据锁条目被清理
	require.NoError(t, mgr.DeleteDisk(e.ctx, e.disk.DiskID))
	require.False(t, locks.HasBranchLocks(e.disk.DiskID, "main"))
}
