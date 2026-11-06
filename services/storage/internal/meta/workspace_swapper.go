package meta

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

// WorkspaceSwapper 统一协调分支工作区目录的准备、独占写锁、常驻句柄驱逐、
// .backup-* / .delete-* / .restore-* 轮转、TreeQuota 两阶段配额暂存与失败回滚。
type WorkspaceSwapper struct {
	mgr *manager
}

func newWorkspaceSwapper(mgr *manager) WorkspaceSwapper {
	return WorkspaceSwapper{mgr: mgr}
}

// WorkspaceSwap 持有单次分支目录或磁盘存储的两阶段切换状态，支持 Commit 或 Rollback。
type WorkspaceSwap struct {
	mgr                *manager
	disk               string
	branch             string
	unlock             func()
	targetPath         string
	stagePath          string
	backupPath         string
	quotaStage         *BranchQuotaStage
	evictTargetOnAbort bool
	removeDiskOnAbort  bool
	invalidateSessions bool
	evictBranchLocks   bool
	moved              bool
	installed          bool
	committed          bool
	closed             bool
}

func (s *WorkspaceSwap) attachQuota(stage *BranchQuotaStage, err error) error {
	if err != nil {
		return err
	}
	s.quotaStage = stage
	return nil
}

// StageReplace 在当前切换上绑定分支全量配额替换暂存。
func (s *WorkspaceSwap) StageReplace(quota *TreeQuota, sizes map[uint64]uint64, enforce bool) error {
	return s.attachQuota(quota.StageBranchReplace(s.branch, sizes, enforce))
}

// StagePatch 在当前切换上绑定分支增量配额补丁暂存（供三路合并等场景使用）。
func (s *WorkspaceSwap) StagePatch(quota *TreeQuota, dirty map[uint64]bool, changed map[uint64]uint64, enforce bool) error {
	return s.attachQuota(quota.StageBranchPatch(s.branch, dirty, changed, enforce))
}

// ReleaseUnlock 提前释放分支独占写锁（供外层 defer 需要按特定顺序解锁的场景使用）。
func (s *WorkspaceSwap) ReleaseUnlock() {
	if s != nil && s.unlock != nil {
		s.unlock()
		s.unlock = nil
	}
}

// Commit 确认工作区切换：提交配额暂存、清理备份与暂存目录、按需使分支会话缓存失效并释放写锁。
func (s *WorkspaceSwap) Commit() error {
	if s == nil || s.closed {
		return nil
	}
	s.committed = true
	s.closed = true
	if s.quotaStage != nil {
		s.quotaStage.Commit()
	}
	var err error
	if s.moved && s.backupPath != "" {
		err = errors.Join(err, os.RemoveAll(s.backupPath))
	}
	if s.stagePath != "" {
		err = errors.Join(err, os.RemoveAll(s.stagePath))
	}
	if s.invalidateSessions {
		s.mgr.invalidatePebbleBranchSessions(s.disk, s.branch)
	}
	s.ReleaseUnlock()
	if s.evictBranchLocks {
		s.mgr.locks.EvictBranch(s.disk, s.branch)
	}
	return err
}

// Rollback 回滚未提交的工作区切换：恢复配额、还原备份目录或删除新建目录、清理暂存目录并释放写锁。
func (s *WorkspaceSwap) Rollback() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	var err error
	if s.quotaStage != nil {
		err = errors.Join(err, s.quotaStage.Rollback())
	}
	if !s.committed {
		if s.removeDiskOnAbort {
			err = errors.Join(err, s.mgr.pebble.RemoveDisk(s.disk))
		}
		if s.evictTargetOnAbort {
			err = errors.Join(err, s.mgr.pebble.EvictBranch(s.disk, s.branch), os.RemoveAll(s.targetPath))
		}
		if s.moved {
			if s.installed {
				err = errors.Join(err, s.mgr.pebble.EvictBranch(s.disk, s.branch), os.RemoveAll(s.targetPath))
			}
			err = errors.Join(err, os.Rename(s.backupPath, s.targetPath))
		}
		if s.stagePath != "" {
			err = errors.Join(err, os.RemoveAll(s.stagePath))
		}
	}
	s.ReleaseUnlock()
	if !s.committed {
		if s.removeDiskOnAbort {
			s.mgr.locks.EvictDisk(s.disk)
		}
		if s.evictTargetOnAbort {
			s.mgr.locks.EvictBranch(s.disk, s.branch)
		}
	}
	return err
}

// Finalize 根据事务错误自动提交或回滚工作区切换，并将切换清理错误合并返回。
func (s *WorkspaceSwap) Finalize(txErr error) error {
	if s == nil {
		return txErr
	}
	if txErr != nil {
		return errors.Join(txErr, s.Rollback())
	}
	return s.Commit()
}

// StageDiskCreate 初始化新磁盘的根分支目录与初始快照，并在后续注册失败时自动移除整盘目录。
func (w WorkspaceSwapper) StageDiskCreate(disk, commitID string) (*WorkspaceSwap, error) {
	swap := &WorkspaceSwap{
		mgr:               w.mgr,
		disk:              disk,
		branch:            "main",
		removeDiskOnAbort: true,
	}
	if err := w.mgr.createDiskStorage(disk, commitID); err != nil {
		return nil, errors.Join(err, swap.Rollback())
	}
	return swap, nil
}

// StageFork 在持有目标分支写锁下从指定快照派生新分支、清理孤儿并读取分支节点大小。
func (w WorkspaceSwapper) StageFork(disk, commitID, branch string) (*WorkspaceSwap, map[uint64]uint64, error) {
	targetPath := filepath.Join(w.mgr.storageRoot, "disks", disk, "branches", branch)
	swap := &WorkspaceSwap{
		mgr:        w.mgr,
		disk:       disk,
		branch:     branch,
		unlock:     w.mgr.locks.LockBranchWrite(disk, branch),
		targetPath: targetPath,
	}
	if _, err := os.Stat(targetPath); err == nil {
		return swap, nil, ErrAlreadyExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return swap, nil, err
	}
	swap.evictTargetOnAbort = true
	if err := w.mgr.pebble.Fork(disk, commitID, branch); err != nil {
		return swap, nil, err
	}
	sizes, err := w.prepareAndReadSizes(disk, branch)
	if err != nil {
		return swap, nil, err
	}
	return swap, sizes, nil
}

// StageDelete 在持有目标分支写锁下驱逐常驻句柄并将分支目录轮转至 .delete-* 备份目录。
func (w WorkspaceSwapper) StageDelete(disk, branch string) (*WorkspaceSwap, error) {
	targetPath := filepath.Join(w.mgr.storageRoot, "disks", disk, "branches", branch)
	backupPath := filepath.Join(filepath.Dir(targetPath), ".delete-"+uuid.Must(uuid.NewV7()).String())
	swap := &WorkspaceSwap{
		mgr:                w.mgr,
		disk:               disk,
		branch:             branch,
		unlock:             w.mgr.locks.LockBranchWrite(disk, branch),
		targetPath:         targetPath,
		backupPath:         backupPath,
		invalidateSessions: true,
		evictBranchLocks:   true,
	}
	if err := w.mgr.pebble.EvictBranch(disk, branch); err != nil {
		return swap, err
	}
	if err := os.Rename(targetPath, backupPath); err != nil {
		return swap, err
	}
	swap.moved = true
	return swap, nil
}

// StageRestore 在持有目标分支写锁下驱逐常驻句柄、派生 .restore-* 暂存目录并轮转替换目标分支目录。
func (w WorkspaceSwapper) StageRestore(disk, branch, commitID string) (*WorkspaceSwap, map[uint64]uint64, error) {
	targetPath := filepath.Join(w.mgr.storageRoot, "disks", disk, "branches", branch)
	staging := ".restore-" + uuid.Must(uuid.NewV7()).String()
	stagePath := filepath.Join(filepath.Dir(targetPath), staging)
	backupPath := filepath.Join(filepath.Dir(targetPath), ".backup-"+uuid.Must(uuid.NewV7()).String())
	swap := &WorkspaceSwap{
		mgr:                w.mgr,
		disk:               disk,
		branch:             branch,
		unlock:             w.mgr.locks.LockBranchWrite(disk, branch),
		targetPath:         targetPath,
		stagePath:          stagePath,
		backupPath:         backupPath,
		invalidateSessions: true,
	}
	if err := w.mgr.pebble.EvictBranch(disk, branch); err != nil {
		return swap, nil, err
	}
	if err := w.mgr.pebble.Fork(disk, commitID, staging); err != nil {
		return swap, nil, err
	}
	sizes, err := w.prepareAndReadSizes(disk, staging)
	if err != nil {
		return swap, nil, err
	}
	if err := swap.InstallStaged(); err != nil {
		return swap, nil, err
	}
	return swap, sizes, nil
}

// PrepareStaging 为目标分支创建指定前缀（如 ".merge-"）的暂存分支工作区并完成孤儿清理。
func (w WorkspaceSwapper) PrepareStaging(disk, targetBranch, baseCommitID, prefix string, unlock func()) (*WorkspaceSwap, *pebblekv.Branch, error) {
	targetPath := filepath.Join(w.mgr.storageRoot, "disks", disk, "branches", targetBranch)
	staging := prefix + uuid.Must(uuid.NewV7()).String()
	stagePath := filepath.Join(filepath.Dir(targetPath), staging)
	backupPath := filepath.Join(filepath.Dir(targetPath), ".backup-"+uuid.Must(uuid.NewV7()).String())
	swap := &WorkspaceSwap{
		mgr:                w.mgr,
		disk:               disk,
		branch:             targetBranch,
		unlock:             unlock,
		targetPath:         targetPath,
		stagePath:          stagePath,
		backupPath:         backupPath,
		invalidateSessions: true,
	}
	if err := w.mgr.pebble.Fork(disk, baseCommitID, staging); err != nil {
		return swap, nil, err
	}
	workspace, err := w.mgr.pebble.OpenBranch(disk, staging)
	if err != nil {
		return swap, nil, err
	}
	if err := prepareBranch(workspace.DB(), workspace.Blobs(), staging); err != nil {
		return swap, nil, errors.Join(err, workspace.Close())
	}
	return swap, workspace, nil
}

// InstallStaged 驱逐目标分支常驻句柄，将旧目录移入 .backup-* 并将暂存目录原子重命名为目标分支目录。
func (s *WorkspaceSwap) InstallStaged() error {
	if err := s.mgr.pebble.EvictBranch(s.disk, s.branch); err != nil {
		return err
	}
	if err := os.Rename(s.targetPath, s.backupPath); err != nil {
		return err
	}
	s.moved = true
	if err := os.Rename(s.stagePath, s.targetPath); err != nil {
		return err
	}
	s.installed = true
	return nil
}

func (w WorkspaceSwapper) prepareAndReadSizes(disk, branchOrStaging string) (map[uint64]uint64, error) {
	workspace, err := w.mgr.pebble.OpenBranch(disk, branchOrStaging)
	if err != nil {
		return nil, err
	}
	err = prepareBranch(workspace.DB(), workspace.Blobs(), branchOrStaging)
	var sizes map[uint64]uint64
	if err == nil {
		sizes, err = readBranchSizesFromDB(workspace.DB())
	}
	err = errors.Join(err, workspace.Close())
	if err != nil {
		return nil, err
	}
	return sizes, nil
}
