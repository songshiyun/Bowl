package meta

import (
	"errors"
	"math"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

// BranchMutationTx 封装单个 Branch 工作区变更事务的元数据锁、BranchStat 序列与字节记账、
// Inode/Entry/Xattr 批次暂存、DirtyKey 自动登记、TreeQuota 原子预留回调、MarkBranchDirty 通知以及新旧 Blob 文件清理。
type BranchMutationTx struct {
	pebble       *pebblekv.Manager
	atimes       *PebbleAtimeTracker
	disk         string
	branch       string
	store        *pebble.DB
	blobs        *pebblekv.BlobStore
	quota        *TreeQuota
	stat         pebblekv.BranchStat
	statLoaded   bool
	advanceSeq   bool
	seq          uint64
	now          time.Time
	batch        *pebble.Batch
	dirty        []uint64
	dirtySeen    map[uint64]struct{}
	reclaimed    []uint64
	newBlobs     []uint64
	removedBlobs []uint64
	beforeCommit func() error
	committed    bool
}

// RunBranchMutationTx 在无 manager 实例的独立存储场景下执行工作区变更事务。
func RunBranchMutationTx(pebbleMgr *pebblekv.Manager, disk, branch string, store *pebble.DB, blobs *pebblekv.BlobStore, quota *TreeQuota, fn func(*BranchMutationTx) error) error {
	return runBranchMutationTx(nil, pebbleMgr, disk, branch, store, blobs, quota, true, fn)
}

// RunBranchReclaimTx 在无 manager 实例的独立存储场景下执行不推进 NextWriteSeq 的孤儿节点回收事务。
func RunBranchReclaimTx(pebbleMgr *pebblekv.Manager, disk, branch string, store *pebble.DB, blobs *pebblekv.BlobStore, quota *TreeQuota, fn func(*BranchMutationTx) error) error {
	return runBranchMutationTx(nil, pebbleMgr, disk, branch, store, blobs, quota, false, fn)
}

func (m *manager) withBranchMutationTx(disk, branch string, store *pebble.DB, blobs *pebblekv.BlobStore, quota *TreeQuota, fn func(*BranchMutationTx) error) error {
	var pebbleMgr *pebblekv.Manager
	if m != nil {
		pebbleMgr = m.pebble
	}
	return runBranchMutationTx(m, pebbleMgr, disk, branch, store, blobs, quota, true, fn)
}

func (m *manager) withBranchReclaimTx(disk, branch string, store *pebble.DB, blobs *pebblekv.BlobStore, quota *TreeQuota, fn func(*BranchMutationTx) error) error {
	var pebbleMgr *pebblekv.Manager
	if m != nil {
		pebbleMgr = m.pebble
	}
	return runBranchMutationTx(m, pebbleMgr, disk, branch, store, blobs, quota, false, fn)
}

func runBranchMutationTx(m *manager, pebbleMgr *pebblekv.Manager, disk, branch string, store *pebble.DB, blobs *pebblekv.BlobStore, quota *TreeQuota, requireSeq bool, fn func(*BranchMutationTx) error) (err error) {
	if m != nil {
		unlockMeta := m.locks.LockBranchMeta(disk, branch)
		defer unlockMeta()
	}
	var stat pebblekv.BranchStat
	var seq uint64
	if requireSeq {
		stat, err = readCreateStat(store)
		if err != nil {
			return err
		}
		if stat.NextWriteSeq == math.MaxUint64 {
			return ErrNoSpace
		}
		seq = stat.NextWriteSeq
	}
	batch := store.NewBatch()
	var atimeTracker *PebbleAtimeTracker
	if m != nil {
		atimes := m.atimes
		atimeTracker = atimes
	}
	tx := &BranchMutationTx{
		pebble:     pebbleMgr,
		atimes:     atimeTracker,
		disk:       disk,
		branch:     branch,
		store:      store,
		blobs:      blobs,
		quota:      quota,
		stat:       stat,
		statLoaded: requireSeq,
		advanceSeq: requireSeq,
		seq:        seq,
		now:        time.Now().UTC(),
		batch:      batch,
		dirtySeen:  make(map[uint64]struct{}, 4),
	}
	defer func() {
		err = errors.Join(err, batch.Close())
		if !tx.committed && tx.blobs != nil && len(tx.newBlobs) > 0 {
			err = errors.Join(err, pebbleRemoveBlobs(tx.blobs, tx.newBlobs))
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if tx.committed && tx.atimes != nil {
		for _, ino := range tx.reclaimed {
			tx.atimes.DiscardInode(disk, branch, ino)
		}
	}
	if tx.committed && tx.blobs != nil && len(tx.removedBlobs) > 0 {
		if err := pebbleRemoveBlobs(tx.blobs, tx.removedBlobs); err != nil {
			return err
		}
	}
	if tx.committed && tx.pebble != nil {
		tx.pebble.MarkBranchDirty(disk, branch)
	}
	return nil
}

// LoadStat 按需加载 BranchStat（在未预先加载序列号的孤儿回收场景下使用）。
func (tx *BranchMutationTx) LoadStat() error {
	if tx.statLoaded {
		return nil
	}
	stat, err := readCreateStat(tx.store)
	if err != nil {
		return err
	}
	tx.stat = stat
	tx.statLoaded = true
	return nil
}

// Now 返回本次变更事务统一采样的 UTC 时间戳。
func (tx *BranchMutationTx) Now() time.Time {
	return tx.now
}

// WriteSeq 返回本次变更事务分配的递增写入序列号。
func (tx *BranchMutationTx) WriteSeq() uint64 {
	return tx.seq
}

// Batch 返回底层 Pebble 批次，供复合数据键或范围操作直接暂存。
func (tx *BranchMutationTx) Batch() *pebble.Batch {
	return tx.batch
}

// Stat 返回当前事务持有的分支统计指针。
func (tx *BranchMutationTx) Stat() *pebblekv.BranchStat {
	return &tx.stat
}

// RequireNewInode 校验分支统计的节点数与逻辑字节数是否还能容纳一个大小为 size 的新节点。
func (tx *BranchMutationTx) RequireNewInode(size uint64) error {
	if tx.stat.Inodes == math.MaxUint64 || size > math.MaxUint64-tx.stat.LogicalBytes {
		return ErrNoSpace
	}
	return nil
}

// AddInodes 递增分支节点计数。
func (tx *BranchMutationTx) AddInodes(n uint64) {
	tx.stat.Inodes += n
}

// SubInodes 递减分支节点计数（下限为 0）。
func (tx *BranchMutationTx) SubInodes(n uint64) {
	tx.stat.Inodes -= min(tx.stat.Inodes, n)
}

// AddLogicalBytes 递增分支逻辑字节数。
func (tx *BranchMutationTx) AddLogicalBytes(delta uint64) {
	tx.stat.LogicalBytes += delta
}

// SubLogicalBytes 递减分支逻辑字节数（下限为 0）。
func (tx *BranchMutationTx) SubLogicalBytes(delta uint64) {
	tx.stat.LogicalBytes -= min(tx.stat.LogicalBytes, delta)
}

// AdjustLogicalBytes 按节点新旧大小增减分支逻辑字节数，并在上溢时返回 ErrNoSpace。
func (tx *BranchMutationTx) AdjustLogicalBytes(oldSize, newSize uint64) error {
	if newSize > oldSize {
		delta := newSize - oldSize
		if delta > math.MaxUint64-tx.stat.LogicalBytes {
			return ErrNoSpace
		}
		tx.stat.LogicalBytes += delta
		return nil
	}
	tx.stat.LogicalBytes -= min(tx.stat.LogicalBytes, oldSize-newSize)
	return nil
}

// MarkDirty 登记需要在提交批次中写入 DirtyKey 的节点编号。
func (tx *BranchMutationTx) MarkDirty(inos ...uint64) {
	for _, ino := range inos {
		if ino == 0 {
			continue
		}
		if _, ok := tx.dirtySeen[ino]; ok {
			continue
		}
		tx.dirtySeen[ino] = struct{}{}
		tx.dirty = append(tx.dirty, ino)
	}
}

// PutInode 将节点的 WriteSeq 更新为当前事务序列号，编码写入批次并自动登记 DirtyKey。
func (tx *BranchMutationTx) PutInode(inode *pebblekv.Inode) error {
	inode.WriteSeq = tx.seq
	if err := setCreateInode(tx.batch, inode); err != nil {
		return err
	}
	tx.MarkDirty(inode.Ino)
	return nil
}

// PutEntry 在批次中写入目录项。
func (tx *BranchMutationTx) PutEntry(parentIno uint64, name []byte, ino uint64, mode uint32) error {
	return tx.batch.Set(pebblekv.EntryKey(parentIno, name), pebblekv.EncodeEntry(pebblekv.Entry{Ino: ino, Mode: mode}), nil)
}

// DeleteEntry 在批次中删除目录项。
func (tx *BranchMutationTx) DeleteEntry(parentIno uint64, name []byte) error {
	return tx.batch.Delete(pebblekv.EntryKey(parentIno, name), nil)
}

// PutXattr 在批次中写入或删除扩展属性。
func (tx *BranchMutationTx) PutXattr(ino uint64, name, value []byte, remove bool) error {
	key := pebblekv.XattrKey(ino, name)
	if remove {
		return tx.batch.Delete(key, nil)
	}
	if value == nil {
		value = []byte{}
	}
	return tx.batch.Set(key, value, nil)
}

// TrackNewBlobs 登记本次事务新分配的块编号；若事务未提交成功，自动回滚清理这些块文件。
func (tx *BranchMutationTx) TrackNewBlobs(ids ...uint64) {
	for _, id := range ids {
		if id != 0 {
			tx.newBlobs = append(tx.newBlobs, id)
		}
	}
}

// TrackRemovedBlobs 登记本次事务替换淘汰的旧块编号；仅在事务提交成功后删除。
func (tx *BranchMutationTx) TrackRemovedBlobs(ids ...uint64) {
	for _, id := range ids {
		if id != 0 {
			tx.removedBlobs = append(tx.removedBlobs, id)
		}
	}
}

// ReclaimInode 在批次中移除指定节点的 InodeKey、OrphanKey、XattrRange 与 DataRange，
// 收集需在提交后删除的数据块文件编号，并扣减 BranchStat 节点数与逻辑字节数。
func (tx *BranchMutationTx) ReclaimInode(inode *pebblekv.Inode) error {
	if err := tx.LoadStat(); err != nil {
		return err
	}
	var removed []uint64
	if err := removeTopologyInode(tx.store, tx.batch, &tx.stat, inode, &removed); err != nil {
		return err
	}
	tx.reclaimed = append(tx.reclaimed, inode.Ino)
	tx.TrackRemovedBlobs(removed...)
	return nil
}

// ReclaimOrphanInode 按节点编号回收孤儿节点的全部物理键、数据块与配额统计；
// 若 InodeKey 已被移除但仍残留 OrphanKey/Xattr/Data，则仅清理残留键与块文件而不重复扣减 BranchStat。
func (tx *BranchMutationTx) ReclaimOrphanInode(ino uint64) error {
	value, closer, err := tx.store.Get(pebblekv.InodeKey(ino))
	if err == nil {
		inode, decodeErr := pebblekv.DecodeInode(value)
		closeErr := closer.Close()
		if joinErr := errors.Join(decodeErr, closeErr); joinErr != nil {
			return joinErr
		}
		if inode.Ino != ino {
			return errors.New("Inode 编号与键不一致")
		}
		return tx.ReclaimInode(&inode)
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	var removed []uint64
	if err := deleteTopologyInodeKeys(tx.store, tx.batch, ino, &removed); err != nil {
		return err
	}
	tx.reclaimed = append(tx.reclaimed, ino)
	tx.TrackRemovedBlobs(removed...)
	return nil
}

// SetOrphanedOrReclaim 在节点链接数大于 0 时持久化节点；链接数归零时，若客户端仍持有打开句柄则标记为孤儿，否则直接物理回收。
// 返回 removed=true 表示该节点已在本次批次中物理移除，提交时需同步通过 CommitReclaim 释放配额。
func (tx *BranchMutationTx) SetOrphanedOrReclaim(inode *pebblekv.Inode, openInClient bool) (bool, error) {
	if inode.Nlink > 0 {
		return false, tx.PutInode(inode)
	}
	if !openInClient {
		tx.MarkDirty(inode.Ino)
		return true, tx.ReclaimInode(inode)
	}
	if err := tx.PutInode(inode); err != nil {
		return false, err
	}
	return false, tx.batch.Set(pebblekv.OrphanKey(inode.Ino), []byte{1}, nil)
}

func deleteTopologyInodeKeys(store *pebble.DB, batch *pebble.Batch, ino uint64, removed *[]uint64) error {
	if err := batch.Delete(pebblekv.InodeKey(ino), nil); err != nil {
		return err
	}
	if err := batch.Delete(pebblekv.OrphanKey(ino), nil); err != nil {
		return err
	}
	lower, upper := pebblekv.XattrRange(ino)
	if err := deletePebbleRangePointwise(store, batch, lower, upper); err != nil {
		return err
	}
	return deletePebbleDataKeys(store, batch, ino, removed)
}

// removeTopologyInode 在变更批次中删除节点及其关联数据，并登记需即时回收的块编号。
func removeTopologyInode(store *pebble.DB, batch *pebble.Batch, stat *pebblekv.BranchStat, inode *pebblekv.Inode, removed *[]uint64) error {
	if err := deleteTopologyInodeKeys(store, batch, inode.Ino, removed); err != nil {
		return err
	}
	stat.Inodes -= min(stat.Inodes, 1)
	stat.LogicalBytes -= min(stat.LogicalBytes, inode.Size)
	return nil
}

func reclaimBranchOrphan(store *pebble.DB, blobs *pebblekv.BlobStore, quota *TreeQuota, branch string, ino uint64) error {
	return RunBranchReclaimTx(nil, "", branch, store, blobs, quota, func(tx *BranchMutationTx) error {
		if err := tx.ReclaimOrphanInode(ino); err != nil {
			return err
		}
		return tx.CommitReclaim(ino)
	})
}

// SetBeforeCommit 注册在配额校验通过后、Pebble 批次真正提交前执行的钩子（如窗口内脏块原地补丁）。
func (tx *BranchMutationTx) SetBeforeCommit(fn func() error) {
	tx.beforeCommit = fn
}

func (tx *BranchMutationTx) sealAndPersist() error {
	for _, ino := range tx.dirty {
		if err := tx.batch.Set(pebblekv.DirtyKey(ino), []byte{1}, nil); err != nil {
			return err
		}
	}
	nextStat := tx.stat
	if tx.statLoaded {
		if tx.advanceSeq {
			nextStat.NextWriteSeq = tx.seq + 1
		}
		if err := tx.batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(nextStat), nil); err != nil {
			return err
		}
	}
	if tx.beforeCommit != nil {
		if err := tx.beforeCommit(); err != nil {
			return err
		}
	}
	if err := tx.batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	if tx.statLoaded {
		tx.stat = nextStat
	}
	tx.committed = true
	return nil
}

// CommitCreate 通过 TreeQuota.Create 在配额锁内校验剩余容量、执行 prepare（可选）并提交新建节点批次。
func (tx *BranchMutationTx) CommitCreate(ino, size uint64, prepare ...func() error) error {
	var prep func() error
	if len(prepare) > 0 {
		prep = prepare[0]
	}
	if tx.quota == nil {
		if prep != nil {
			if err := prep(); err != nil {
				return err
			}
		}
		return tx.sealAndPersist()
	}
	return tx.quota.Create(tx.branch, ino, size, prep, tx.sealAndPersist)
}

// CommitApplyChange 通过 TreeQuota.ApplyChange 在配额锁内更新单节点逻辑字节数或存活状态并提交批次。
func (tx *BranchMutationTx) CommitApplyChange(ino, size uint64, live, enforce bool) error {
	if tx.quota == nil {
		return tx.sealAndPersist()
	}
	return tx.quota.ApplyChange(tx.branch, ino, size, live, enforce, tx.sealAndPersist)
}

// CommitReclaim 通过 TreeQuota.Reclaim 在配额锁内回收无引用的节点并提交批次。
func (tx *BranchMutationTx) CommitReclaim(ino uint64) error {
	if tx.quota == nil {
		return tx.sealAndPersist()
	}
	return tx.quota.Reclaim(tx.branch, ino, tx.sealAndPersist)
}

// CommitWithQuota 通过 TreeQuota.CommitWithQuota 在配额锁内执行不改变节点字节数的批次提交。
func (tx *BranchMutationTx) CommitWithQuota(enforce bool) error {
	if tx.quota == nil {
		return tx.sealAndPersist()
	}
	return tx.quota.CommitWithQuota(enforce, tx.sealAndPersist)
}

// CommitWithoutQuota 直接提交不涉及配额变更的批次（如扩展属性写入或移除）。
func (tx *BranchMutationTx) CommitWithoutQuota() error {
	return tx.sealAndPersist()
}
