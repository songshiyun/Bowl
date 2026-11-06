package meta

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

// defaultAtimeFlushInterval 是内存脏访问时间合并刷入 Pebble 的后台周期。
const defaultAtimeFlushInterval = time.Second

type pebbleAtimeKey struct {
	disk, branch string
	ino          uint64
}

// PebbleAtimeTracker 统一管理服务端元数据管理器的内存脏访问时间（atime）登记、
// 只读属性叠加（GetAttr / Lookup）、单节点 Fsync 刷盘、节点/分支/磁盘删除清理，
// 以及生成 Pebble Checkpoint 或检查分支脏状态前的批量刷盘屏障。
type PebbleAtimeTracker struct {
	mode        AtimeMode
	pebble      *pebblekv.Manager
	locks       *BranchLockTable
	storageRoot string

	mu    sync.Mutex
	dirty map[pebbleAtimeKey]time.Time
}

// NewPebbleAtimeTracker 创建内存脏访问时间追踪器。
func NewPebbleAtimeTracker(mode AtimeMode, pebbleMgr *pebblekv.Manager, locks *BranchLockTable, storageRoot string) *PebbleAtimeTracker {
	return &PebbleAtimeTracker{
		mode:        mode,
		pebble:      pebbleMgr,
		locks:       locks,
		storageRoot: storageRoot,
		dirty:       make(map[pebbleAtimeKey]time.Time),
	}
}

func (t *PebbleAtimeTracker) shouldTouch(atime, mtime, ctime, now time.Time) bool {
	if t == nil {
		return false
	}
	switch t.mode {
	case AtimeModeStrictAtime:
		return now.Sub(atime) >= time.Second
	case AtimeModeRelAtime:
		return atime.Before(mtime) || atime.Before(ctime) || now.Sub(atime) >= 24*time.Hour
	default:
		return false
	}
}

// MarkDirty 在读热路径（Read / Readlink）按配置的 AtimeMode 登记内存脏访问时间，避免同步提交 Pebble 批次。
func (t *PebbleAtimeTracker) MarkDirty(disk, branch string, inode pebblekv.Inode, now time.Time) {
	if t == nil || t.mode == AtimeModeNoAtime {
		return
	}
	key := pebbleAtimeKey{disk: disk, branch: branch, ino: inode.Ino}
	atime := time.Unix(inode.AtimeSec, int64(inode.AtimeNsec)).UTC()
	t.mu.Lock()
	if pending, ok := t.dirty[key]; ok && pending.After(atime) {
		atime = pending
	}
	t.mu.Unlock()
	mtime := time.Unix(inode.MtimeSec, int64(inode.MtimeNsec)).UTC()
	ctime := time.Unix(inode.CtimeSec, int64(inode.CtimeNsec)).UTC()
	if !t.shouldTouch(atime, mtime, ctime, now) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dirty == nil {
		t.dirty = make(map[pebbleAtimeKey]time.Time)
	}
	if current, ok := t.dirty[key]; !ok || now.After(current) {
		t.dirty[key] = now
	}
}

// Overlay 将内存中尚未刷盘的最新访问时间叠加到节点属性上，使 GetAttr 与 Lookup 立即可见。
func (t *PebbleAtimeTracker) Overlay(disk, branch string, inode *pebblekv.Inode) {
	if t == nil || inode == nil || t.mode == AtimeModeNoAtime {
		return
	}
	t.mu.Lock()
	value, ok := t.dirty[pebbleAtimeKey{disk: disk, branch: branch, ino: inode.Ino}]
	t.mu.Unlock()
	if !ok {
		return
	}
	if value.After(time.Unix(inode.AtimeSec, int64(inode.AtimeNsec))) {
		inode.AtimeSec, inode.AtimeNsec = value.Unix(), uint32(value.Nanosecond())
	}
}

// HasPending 报告指定分支是否仍有尚未合并到 Pebble 的内存脏访问时间。
func (t *PebbleAtimeTracker) HasPending(disk, branch string) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for key := range t.dirty {
		if key.disk == disk && key.branch == branch {
			return true
		}
	}
	return false
}

// DiscardInode 在节点被物理删除或顯式覆写 Atime 时清理其内存脏访问时间条目。
func (t *PebbleAtimeTracker) DiscardInode(disk, branch string, ino uint64) {
	if t == nil || ino == 0 {
		return
	}
	t.mu.Lock()
	delete(t.dirty, pebbleAtimeKey{disk: disk, branch: branch, ino: ino})
	t.mu.Unlock()
}

// DiscardBranch 在分支删除或回档替换时清理该分支下所有内存脏访问时间条目。
func (t *PebbleAtimeTracker) DiscardBranch(disk, branch string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for key := range t.dirty {
		if key.disk == disk && key.branch == branch {
			delete(t.dirty, key)
		}
	}
}

// DiscardDisk 在整盘删除时清理该磁盘下所有分支的内存脏访问时间条目。
func (t *PebbleAtimeTracker) DiscardDisk(disk string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for key := range t.dirty {
		if key.disk == disk {
			delete(t.dirty, key)
		}
	}
}

func (t *PebbleAtimeTracker) takeBranchEntries(disk, branch string) map[uint64]time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	var entries map[uint64]time.Time
	for key, value := range t.dirty {
		if key.disk != disk || key.branch != branch {
			continue
		}
		if entries == nil {
			entries = make(map[uint64]time.Time)
		}
		entries[key.ino] = value
		delete(t.dirty, key)
	}
	return entries
}

func (t *PebbleAtimeTracker) takeInodeEntry(disk, branch string, ino uint64) (time.Time, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := pebbleAtimeKey{disk: disk, branch: branch, ino: ino}
	value, ok := t.dirty[key]
	if ok {
		delete(t.dirty, key)
	}
	return value, ok
}

func (t *PebbleAtimeTracker) restoreEntries(disk, branch string, entries map[uint64]time.Time) {
	if len(entries) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dirty == nil {
		t.dirty = make(map[pebbleAtimeKey]time.Time)
	}
	for ino, value := range entries {
		key := pebbleAtimeKey{disk: disk, branch: branch, ino: ino}
		if current, ok := t.dirty[key]; !ok || value.After(current) {
			t.dirty[key] = value
		}
	}
}

// FlushBranchLocked 在已持有分支生命周期锁时把该分支的内存脏访问时间写入 Pebble；
// 若底层写入失败，未刷盘的脏访问时间条目将自动回填保留，供后续重试。
func (t *PebbleAtimeTracker) FlushBranchLocked(disk, branch string, store *pebble.DB) error {
	if t == nil {
		return nil
	}
	entries := t.takeBranchEntries(disk, branch)
	if len(entries) == 0 {
		return nil
	}
	if err := t.writeEntries(disk, branch, store, entries); err != nil {
		t.restoreEntries(disk, branch, entries)
		return err
	}
	return nil
}

// writeEntries 逐个节点合并访问时间；某个节点写入失败时，仅将尚未写入的剩余节点保留在 entries 中以便回填。
func (t *PebbleAtimeTracker) writeEntries(disk, branch string, store *pebble.DB, entries map[uint64]time.Time) error {
	wrote := false
	for ino, value := range entries {
		var unlock func()
		if t.locks != nil {
			unlock = t.locks.LockInodeWrite(disk, branch, ino)
		}
		inode, err := readPebbleInode(store, ino)
		if errors.Is(err, ErrNotFound) {
			if unlock != nil {
				unlock()
			}
			delete(entries, ino)
			continue
		}
		if err != nil {
			if unlock != nil {
				unlock()
			}
			return err
		}
		if !value.After(time.Unix(inode.AtimeSec, int64(inode.AtimeNsec))) {
			if unlock != nil {
				unlock()
			}
			delete(entries, ino)
			continue
		}
		inode.AtimeSec, inode.AtimeNsec = value.Unix(), uint32(value.Nanosecond())
		encoded, err := pebblekv.EncodeInode(inode)
		if err != nil {
			if unlock != nil {
				unlock()
			}
			return err
		}
		err = writePebbleAtime(store, ino, encoded)
		if unlock != nil {
			unlock()
		}
		if err != nil {
			return err
		}
		delete(entries, ino)
		wrote = true
	}
	if wrote && t.pebble != nil {
		t.pebble.MarkBranchDirty(disk, branch)
	}
	return nil
}

// FlushInodeLocked 在已持有目标 Inode 写锁时合并单个节点的访问时间；失败时自动回填保留。
func (t *PebbleAtimeTracker) FlushInodeLocked(disk, branch string, store *pebble.DB, ino uint64) error {
	if t == nil {
		return nil
	}
	value, ok := t.takeInodeEntry(disk, branch, ino)
	if !ok {
		return nil
	}
	inode, err := readPebbleInode(store, ino)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		t.restoreEntries(disk, branch, map[uint64]time.Time{ino: value})
		return err
	}
	if !value.After(time.Unix(inode.AtimeSec, int64(inode.AtimeNsec))) {
		return nil
	}
	inode.AtimeSec, inode.AtimeNsec = value.Unix(), uint32(value.Nanosecond())
	encoded, err := pebblekv.EncodeInode(inode)
	if err != nil {
		t.restoreEntries(disk, branch, map[uint64]time.Time{ino: value})
		return err
	}
	if err := writePebbleAtime(store, ino, encoded); err != nil {
		t.restoreEntries(disk, branch, map[uint64]time.Time{ino: value})
		return err
	}
	if t.pebble != nil {
		t.pebble.MarkBranchDirty(disk, branch)
	}
	return nil
}

// FlushAndCheckDirtyLocked 是分支预检屏障：先将该分支的内存脏 atime 刷入 Pebble（同时写入 DirtyKey），
// 再统一检测分支是否存在任何 `dirty:*` 记录。
func (t *PebbleAtimeTracker) FlushAndCheckDirtyLocked(disk, branch string, store *pebble.DB) (bool, error) {
	if err := t.FlushBranchLocked(disk, branch, store); err != nil {
		return false, err
	}
	return branchHasDirty(store)
}

// CheckpointBranchLocked 是分支快照生成屏障：在生成 Pebble Checkpoint 前自动刷盘该分支的内存脏 atime。
func (t *PebbleAtimeTracker) CheckpointBranchLocked(disk, branchName string, branch *pebblekv.Branch, commitID string) error {
	if err := t.FlushBranchLocked(disk, branchName, branch.DB()); err != nil {
		return err
	}
	return branch.Checkpoint(commitID)
}

// FlushDisk 合并指定 Disk 下所有分支的内存脏访问时间。
func (t *PebbleAtimeTracker) FlushDisk(ctx context.Context, disk string) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	grouped := make(map[string]map[uint64]time.Time)
	for key, value := range t.dirty {
		if key.disk != disk {
			continue
		}
		if grouped[key.branch] == nil {
			grouped[key.branch] = make(map[uint64]time.Time)
		}
		grouped[key.branch][key.ino] = value
		delete(t.dirty, key)
	}
	t.mu.Unlock()
	var errs []error
	for branch, entries := range grouped {
		if err := ctx.Err(); err != nil {
			t.restoreEntries(disk, branch, entries)
			errs = append(errs, err)
			continue
		}
		if err := t.flushBranch(disk, branch, entries); err != nil {
			t.restoreEntries(disk, branch, entries)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (t *PebbleAtimeTracker) flushBranch(disk, branch string, entries map[uint64]time.Time) (err error) {
	if t.locks != nil {
		unlock := t.locks.LockBranchRead(disk, branch)
		defer unlock()
	}
	path := filepath.Join(t.storageRoot, "disks", disk, "branches", branch)
	if _, statErr := os.Stat(path); errors.Is(statErr, os.ErrNotExist) {
		// 分支已被删除，丢弃脏项而不是重建目录。
		return nil
	} else if statErr != nil {
		return statErr
	}
	branchHandle, err := t.pebble.OpenBranch(disk, branch)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, branchHandle.Close()) }()
	return t.writeEntries(disk, branch, branchHandle.DB(), entries)
}

// FlushAll 由后台周期协程与停机屏障调用，把所有磁盘的内存脏访问时间刷入 Pebble。
func (t *PebbleAtimeTracker) FlushAll(ctx context.Context) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	disks := make(map[string]struct{})
	for key := range t.dirty {
		disks[key.disk] = struct{}{}
	}
	t.mu.Unlock()
	var errs []error
	for disk := range disks {
		errs = append(errs, t.FlushDisk(ctx, disk))
	}
	return errors.Join(errs...)
}

func (m *manager) markPebbleAtimeDirty(session pebbleReadSession, inode pebblekv.Inode, now time.Time) {
	m.gate.RecordAtime(session, inode, now)
}

func (m *manager) applyDirtyPebbleAtime(session pebbleReadSession, inode *pebblekv.Inode) {
	m.gate.OverlayAtime(session, inode)
}

// flushAndCheckBranchDirtyLocked 在持有分支独占锁下刷盘该分支的内存脏 atime 并检测 dirty:* 键。
func (m *manager) flushAndCheckBranchDirtyLocked(disk, branch string, store *pebble.DB) (bool, error) {
	return m.gate.FlushAndCheckBranchDirtyLocked(disk, branch, store)
}

// checkpointBranchLocked 在持有分支独占锁下先刷盘内存脏 atime 再生成 Pebble Checkpoint。
func (m *manager) checkpointBranchLocked(disk, branchName string, branch *pebblekv.Branch, commitID string) error {
	return m.gate.CheckpointBranchLocked(disk, branchName, branch, commitID)
}
