package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

type storedBranch struct {
	BranchID     string          `json:"branch_id"`
	Name         string          `json:"name"`
	ParentBranch *string         `json:"parent_branch"`
	ForkCommitID *string         `json:"fork_commit_id"`
	HeadCommitID *string         `json:"head_commit_id"`
	State        string          `json:"state"`
	MergeState   json.RawMessage `json:"merge_state"`
	CreatedAt    *time.Time      `json:"created_at,omitempty"`
}

type storedMergeState struct {
	BaseCommitID   string          `json:"base_commit_id"`
	SourceBranch   string          `json:"source_branch"`
	SourceCommitID string          `json:"source_commit_id"`
	TargetCommitID string          `json:"target_commit_id"`
	SidecarPaths   []string        `json:"sidecar_paths,omitempty"`
	SidecarInodes  []uint64        `json:"sidecar_inodes,omitempty"`
	Conflicts      []MergeConflict `json:"conflicts"`
}

func (m *manager) createDiskStorage(diskID, commitID string) (err error) {
	unlock := m.lockBranchWrite(diskID, "main")
	defer unlock()
	branch, err := m.pebble.OpenBranch(diskID, "main")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, branch.Close()) }()
	now := time.Now()
	root, err := pebblekv.EncodeInode(pebblekv.Inode{
		Ino: 1, ParentIno: 1, Mode: S_IFDIR | 0o755, Nlink: 2,
		AtimeSec: now.Unix(), AtimeNsec: uint32(now.Nanosecond()),
		MtimeSec: now.Unix(), MtimeNsec: uint32(now.Nanosecond()),
		CtimeSec: now.Unix(), CtimeNsec: uint32(now.Nanosecond()),
	})
	if err != nil {
		return err
	}
	batch := branch.DB().NewBatch()
	defer func() { err = errors.Join(err, batch.Close()) }()
	if err := batch.Set(pebblekv.InodeKey(1), root, nil); err != nil {
		return err
	}
	if err := batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(pebblekv.BranchStat{Inodes: 1, NextWriteSeq: 1}), nil); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	if err := branch.Checkpoint(commitID); err != nil {
		return err
	}
	return m.pebble.InitInodeAllocator(diskID)
}

func (m *manager) registerDiskStorage(ctx context.Context, q db.Querier, row db.GetDiskRow, commitID uuid.UUID) ([]byte, error) {
	diskID := row.DiskID.String()
	storageDir := filepath.Join(m.storageRoot, "disks", diskID)
	control, err := q.SetDiskStorageDir(ctx, db.SetDiskStorageDirParams{DiskID: row.DiskID, StorageDir: storageDir})
	if err != nil {
		return nil, fmt.Errorf("登记磁盘存储目录: %w", err)
	}
	_, err = q.CreateCommit(ctx, db.CreateCommitParams{
		CommitID: pgtype.UUID{Bytes: commitID, Valid: true}, DiskID: row.DiskID,
		BranchName: "main", CheckpointDir: filepath.Join(storageDir, "commits", commitID.String()),
	})
	if err != nil {
		return nil, fmt.Errorf("登记初始快照: %w", err)
	}
	var branches []storedBranch
	if err := json.Unmarshal(control.Branches, &branches); err != nil {
		return nil, fmt.Errorf("读取分支树: %w", err)
	}
	id := commitID.String()
	found := false
	for i := range branches {
		if branches[i].Name == "main" {
			branches[i].ForkCommitID = &id
			branches[i].HeadCommitID = &id
			branches[i].CreatedAt = &row.CreatedAt.Time
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("分支树缺少 main")
	}
	raw, err := json.Marshal(branches)
	if err != nil {
		return nil, err
	}
	return q.CompareAndSetDiskBranches(ctx, db.CompareAndSetDiskBranchesParams{
		DiskID: row.DiskID, ExpectedBranches: control.Branches, Branches: raw,
	})
}

func setDiskBranches(disk *Disk, raw []byte) error {
	var branches []storedBranch
	if err := json.Unmarshal(raw, &branches); err != nil {
		return fmt.Errorf("读取分支树: %w", err)
	}
	disk.DefaultBranch = "main"
	disk.Branches = make([]Branch, 0, len(branches))
	for _, b := range branches {
		createdAt := disk.CreatedAt
		if b.CreatedAt != nil {
			createdAt = *b.CreatedAt
		}
		disk.Branches = append(disk.Branches, Branch{
			BranchID: b.BranchID, DiskID: disk.DiskID, Name: b.Name,
			ParentBranch: b.ParentBranch, ForkCommitID: b.ForkCommitID,
			HeadCommitID: b.HeadCommitID, State: b.State, CreatedAt: createdAt,
		})
	}
	return nil
}

func (m *manager) diskWithBranches(ctx context.Context, row db.GetDiskRow) (*Disk, error) {
	disk := toDomainDisk(row)
	control, err := m.store.GetDiskControlPlane(ctx, row.DiskID)
	if err != nil {
		return nil, fmt.Errorf("读取磁盘分支树: %w", err)
	}
	if err := setDiskBranches(disk, control.Branches); err != nil {
		return nil, err
	}
	quota, err := m.loadTreeQuota(ctx, row, control)
	if err != nil {
		return nil, err
	}
	usage := quota.Usage()
	disk.UsedBytes = usage.Bytes
	disk.UsedInodes = usage.Inodes
	return disk, nil
}

// TreeQuota 返回磁盘共享的配额协调器，并在首次访问时从存活分支恢复。
func (m *manager) TreeQuota(ctx context.Context, diskID string) (*TreeQuota, error) {
	id, err := parseDiskID(diskID)
	if err != nil {
		return nil, err
	}
	if quota, ok := m.lookupTreeQuota(id.String()); ok {
		return quota, nil
	}
	row, err := m.store.GetDisk(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	control, err := m.store.GetDiskControlPlane(ctx, id)
	if err != nil {
		return nil, err
	}
	return m.loadTreeQuota(ctx, row, control)
}

// lookupTreeQuota 只读取内存配额协调器，未命中时不触达控制面。
func (m *manager) lookupTreeQuota(diskID string) (*TreeQuota, bool) {
	m.treeQuotaMu.Lock()
	defer m.treeQuotaMu.Unlock()
	quota := m.treeQuotas[diskID]
	return quota, quota != nil
}

func (m *manager) loadTreeQuota(ctx context.Context, row db.GetDiskRow, control db.GetDiskControlPlaneRow) (*TreeQuota, error) {
	m.treeQuotaMu.Lock()
	defer m.treeQuotaMu.Unlock()
	if m.treeQuotas == nil {
		m.treeQuotas = make(map[string]*TreeQuota)
	}
	id := row.DiskID.String()
	quota := m.treeQuotas[id]
	if quota == nil {
		quota = NewTreeQuota(math.MaxUint64)
		if control.StorageDir != "" {
			if filepath.Clean(control.StorageDir) != filepath.Join(m.storageRoot, "disks", id) {
				return nil, fmt.Errorf("磁盘 %s 的存储目录与配置不一致", id)
			}
			var branches []storedBranch
			if err := json.Unmarshal(control.Branches, &branches); err != nil {
				return nil, fmt.Errorf("读取分支树: %w", err)
			}
			for _, branch := range branches {
				sizes, err := m.readBranchSizes(id, branch.Name)
				if err != nil {
					return nil, fmt.Errorf("恢复分支 %s 配额: %w", branch.Name, err)
				}
				if err := quota.ReplaceBranch(branch.Name, sizes); err != nil {
					return nil, err
				}
			}
		}
		quota.InitMaxBytes(uint64(row.MaxBytes))
		m.treeQuotas[id] = quota
	}
	return quota, nil
}

func (m *manager) readBranchSizes(diskID, branchName string) (sizes map[uint64]uint64, err error) {
	unlock := m.lockBranchRead(diskID, branchName)
	defer unlock()
	path := filepath.Join(m.storageRoot, "disks", diskID, "branches", branchName)
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	branch, err := m.pebble.OpenBranch(diskID, branchName)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, branch.Close()) }()
	return readBranchSizesFromDB(branch.DB())
}

func readBranchSizesFromDB(store *pebble.DB) (sizes map[uint64]uint64, err error) {
	lower, upper := pebblekv.InodeRange()
	iter, err := store.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()
	sizes = make(map[uint64]uint64)
	for iter.First(); iter.Valid(); iter.Next() {
		ino, err := pebblekv.DecodeInodeKey(iter.Key())
		if err != nil {
			return nil, err
		}
		inode, err := pebblekv.DecodeInode(iter.Value())
		if err != nil {
			return nil, err
		}
		if inode.Ino != ino {
			return nil, fmt.Errorf("Inode 编号与键不一致")
		}
		if inode.Nlink == 0 {
			_, closer, err := store.Get(pebblekv.OrphanKey(ino))
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			if err := closer.Close(); err != nil {
				return nil, err
			}
		}
		sizes[ino] = inode.Size
	}
	return sizes, iter.Error()
}
