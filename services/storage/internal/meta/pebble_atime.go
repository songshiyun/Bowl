package meta

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

// markPebbleAtimeDirty 在读热路径只登记内存脏项，避免同步提交 Pebble 批次。
func (m *manager) markPebbleAtimeDirty(session pebbleReadSession, inode pebblekv.Inode, now time.Time) {
	if session.readOnly || m.atimeMode == AtimeModeNoAtime {
		return
	}
	key := pebbleAtimeKey{disk: session.diskID.String(), branch: session.branch, ino: inode.Ino}
	atime := time.Unix(inode.AtimeSec, int64(inode.AtimeNsec)).UTC()
	m.atimeMu.Lock()
	if pending, ok := m.dirtyAtimes[key]; ok && pending.After(atime) {
		atime = pending
	}
	m.atimeMu.Unlock()
	mtime := time.Unix(inode.MtimeSec, int64(inode.MtimeNsec)).UTC()
	ctime := time.Unix(inode.CtimeSec, int64(inode.CtimeNsec)).UTC()
	if !m.shouldTouchAtime(atime, mtime, ctime, now) {
		return
	}
	m.atimeMu.Lock()
	defer m.atimeMu.Unlock()
	if m.dirtyAtimes == nil {
		m.dirtyAtimes = make(map[pebbleAtimeKey]time.Time)
	}
	if current, ok := m.dirtyAtimes[key]; !ok || now.After(current) {
		m.dirtyAtimes[key] = now
	}
}

// applyDirtyPebbleAtime 让 GetAttr 立即观测到尚未刷盘的访问时间。
func (m *manager) applyDirtyPebbleAtime(session pebbleReadSession, inode *pebblekv.Inode) {
	if session.readOnly || m.atimeMode == AtimeModeNoAtime {
		return
	}
	m.atimeMu.Lock()
	value, ok := m.dirtyAtimes[pebbleAtimeKey{disk: session.diskID.String(), branch: session.branch, ino: inode.Ino}]
	m.atimeMu.Unlock()
	if !ok {
		return
	}
	if value.After(time.Unix(inode.AtimeSec, int64(inode.AtimeNsec))) {
		inode.AtimeSec, inode.AtimeNsec = value.Unix(), uint32(value.Nanosecond())
	}
}

// hasPendingAtime 报告分支是否仍有未合并的访问时间，供 Fork/Merge 判定需要生成快照。
func (m *manager) hasPendingAtime(disk, branch string) bool {
	m.atimeMu.Lock()
	defer m.atimeMu.Unlock()
	for key := range m.dirtyAtimes {
		if key.disk == disk && key.branch == branch {
			return true
		}
	}
	return false
}

// takeDirtyAtimes 原子取走指定分支的脏访问时间，失败时由调用方回填。
func (m *manager) takeDirtyAtimes(disk, branch string) map[uint64]time.Time {
	m.atimeMu.Lock()
	defer m.atimeMu.Unlock()
	var entries map[uint64]time.Time
	for key, value := range m.dirtyAtimes {
		if key.disk != disk || key.branch != branch {
			continue
		}
		if entries == nil {
			entries = make(map[uint64]time.Time)
		}
		entries[key.ino] = value
		delete(m.dirtyAtimes, key)
	}
	return entries
}

// takeDirtyAtime 原子取走单个节点的脏访问时间。
func (m *manager) takeDirtyAtime(disk, branch string, ino uint64) (time.Time, bool) {
	m.atimeMu.Lock()
	defer m.atimeMu.Unlock()
	key := pebbleAtimeKey{disk: disk, branch: branch, ino: ino}
	value, ok := m.dirtyAtimes[key]
	if ok {
		delete(m.dirtyAtimes, key)
	}
	return value, ok
}

// restoreDirtyAtimes 回填合并失败的访问时间，避免丢失。
func (m *manager) restoreDirtyAtimes(disk, branch string, entries map[uint64]time.Time) {
	if len(entries) == 0 {
		return
	}
	m.atimeMu.Lock()
	defer m.atimeMu.Unlock()
	if m.dirtyAtimes == nil {
		m.dirtyAtimes = make(map[pebbleAtimeKey]time.Time)
	}
	for ino, value := range entries {
		key := pebbleAtimeKey{disk: disk, branch: branch, ino: ino}
		if current, ok := m.dirtyAtimes[key]; !ok || value.After(current) {
			m.dirtyAtimes[key] = value
		}
	}
}

// flushPebbleAtimeEntriesLocked 在已持有分支生命周期锁时把脏访问时间写入 Pebble。
func (m *manager) flushPebbleAtimeEntriesLocked(disk, branch string, store *pebble.DB) error {
	entries := m.takeDirtyAtimes(disk, branch)
	if len(entries) == 0 {
		return nil
	}
	if err := m.writePebbleAtimes(disk, branch, store, entries); err != nil {
		m.restoreDirtyAtimes(disk, branch, entries)
		return err
	}
	return nil
}

// writePebbleAtimes 逐个节点合并访问时间；节点已删除时直接跳过。
func (m *manager) writePebbleAtimes(disk, branch string, store *pebble.DB, entries map[uint64]time.Time) error {
	wrote := false
	for ino, value := range entries {
		unlock := m.locks.LockInodeWrite(disk, branch, ino)
		inode, err := readPebbleInode(store, ino)
		if errors.Is(err, ErrNotFound) {
			unlock()
			continue
		}
		if err != nil {
			unlock()
			return err
		}
		if !value.After(time.Unix(inode.AtimeSec, int64(inode.AtimeNsec))) {
			unlock()
			continue
		}
		inode.AtimeSec, inode.AtimeNsec = value.Unix(), uint32(value.Nanosecond())
		encoded, err := pebblekv.EncodeInode(inode)
		if err != nil {
			unlock()
			return err
		}
		err = writePebbleAtime(store, ino, encoded)
		unlock()
		if err != nil {
			return err
		}
		wrote = true
	}
	if wrote {
		m.pebble.MarkBranchDirty(disk, branch)
	}
	return nil
}

// flushPebbleAtimeInodeLocked 在持有目标 Inode 写锁时合并单个节点的访问时间。
func (m *manager) flushPebbleAtimeInodeLocked(disk, branch string, store *pebble.DB, ino uint64) error {
	value, ok := m.takeDirtyAtime(disk, branch, ino)
	if !ok {
		return nil
	}
	inode, err := readPebbleInode(store, ino)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		m.restoreDirtyAtimes(disk, branch, map[uint64]time.Time{ino: value})
		return err
	}
	if !value.After(time.Unix(inode.AtimeSec, int64(inode.AtimeNsec))) {
		return nil
	}
	inode.AtimeSec, inode.AtimeNsec = value.Unix(), uint32(value.Nanosecond())
	encoded, err := pebblekv.EncodeInode(inode)
	if err != nil {
		m.restoreDirtyAtimes(disk, branch, map[uint64]time.Time{ino: value})
		return err
	}
	if err := writePebbleAtime(store, ino, encoded); err != nil {
		m.restoreDirtyAtimes(disk, branch, map[uint64]time.Time{ino: value})
		return err
	}
	m.pebble.MarkBranchDirty(disk, branch)
	return nil
}

// flushPebbleAtimes 合并指定 Disk 下所有分支的脏访问时间。
func (m *manager) flushPebbleAtimes(ctx context.Context, disk string) error {
	m.atimeMu.Lock()
	grouped := make(map[string]map[uint64]time.Time)
	for key, value := range m.dirtyAtimes {
		if key.disk != disk {
			continue
		}
		if grouped[key.branch] == nil {
			grouped[key.branch] = make(map[uint64]time.Time)
		}
		grouped[key.branch][key.ino] = value
		delete(m.dirtyAtimes, key)
	}
	m.atimeMu.Unlock()
	var errs []error
	for branch, entries := range grouped {
		if err := ctx.Err(); err != nil {
			m.restoreDirtyAtimes(disk, branch, entries)
			errs = append(errs, err)
			continue
		}
		if err := m.flushPebbleAtimeBranch(disk, branch, entries); err != nil {
			m.restoreDirtyAtimes(disk, branch, entries)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *manager) flushPebbleAtimeBranch(disk, branch string, entries map[uint64]time.Time) (err error) {
	unlock := m.locks.LockBranchRead(disk, branch)
	defer unlock()
	path := filepath.Join(m.storageRoot, "disks", disk, "branches", branch)
	if _, statErr := os.Stat(path); errors.Is(statErr, os.ErrNotExist) {
		// 分支已被删除，丢弃脏项而不是重建目录。
		return nil
	} else if statErr != nil {
		return statErr
	}
	branchHandle, err := m.pebble.OpenBranch(disk, branch)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, branchHandle.Close()) }()
	return m.writePebbleAtimes(disk, branch, branchHandle.DB(), entries)
}

// flushAllPebbleAtimes 由后台周期协程与停机屏障调用。
func (m *manager) flushAllPebbleAtimes(ctx context.Context) error {
	m.atimeMu.Lock()
	disks := make(map[string]struct{})
	for key := range m.dirtyAtimes {
		disks[key.disk] = struct{}{}
	}
	m.atimeMu.Unlock()
	var errs []error
	for disk := range disks {
		errs = append(errs, m.flushPebbleAtimes(ctx, disk))
	}
	return errors.Join(errs...)
}
