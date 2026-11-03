package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

// ErrLegacySession 标识有效的旧版会话，同时保留无效分支会话的错误语义。
var ErrLegacySession error = legacyBranchSessionError{}

type legacyBranchSessionError struct{}

func (legacyBranchSessionError) Error() string { return "普通会话不能用于分支访问" }
func (legacyBranchSessionError) Unwrap() error { return ErrBadFileHandle }

type pebbleNodeParams struct {
	parent uint64
	name   []byte
	mode   uint32
	uid    uint32
	gid    uint32
	rdev   uint32
	target []byte
}

// createBranchSession 校验显式上下文，其他会话交由原子 SQL 判断。
func createBranchSession(ctx context.Context, paramID *uint64) (*uint64, bool, error) {
	if contextID, ok := SessionIDFromContext(ctx); ok && contextID != nil {
		if err := validateOptionalSessionID(contextID); err != nil {
			return nil, false, err
		}
		if paramID != nil && *contextID != *paramID {
			return nil, false, ErrInvalidArgument
		}
		return contextID, true, nil
	}
	return paramID, false, nil
}

func (m *manager) pebbleCreateResult(ctx context.Context, sid *uint64, params CreateParams, mode uint32) (*CreateResult, error) {
	inode, err := m.createPebbleNode(ctx, sid, pebbleNodeParams{parent: params.ParentIno, name: params.Name, mode: mode, uid: params.UID, gid: params.GID})
	if err != nil {
		return nil, err
	}
	var openFlags uint32
	if m.openKeepCache {
		openFlags = FOPEN_KEEP_CACHE
	}
	return &CreateResult{Inode: inode, Fh: 1, OpenFlags: openFlags, EntryTimeoutSeconds: inode.EntryTimeoutSeconds, AttrTimeoutSeconds: inode.AttrTimeoutSeconds}, nil
}

// withWritablePebbleBranch 在分支锁内使用已验证的可写会话。
func (m *manager) withWritablePebbleBranch(ctx context.Context, sid *uint64, fn func(string, string, *TreeQuota, *pebble.DB) error) (err error) {
	return m.withWritablePebbleBranchFor(ctx, sid, nil, fn)
}

func (m *manager) withWritablePebbleBranchFor(ctx context.Context, sid *uint64, check func(pgtype.UUID, string) error, fn func(string, string, *TreeQuota, *pebble.DB) error) (err error) {
	if sid == nil || *sid == 0 || *sid > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if !m.hasPool() {
		return ErrBadFileHandle
	}
	var diskID pgtype.UUID
	var branchName string
	var readOnly, managed, active bool
	var createdAt time.Time
	err = m.store.(poolProvider).Pool().QueryRow(ctx, `SELECT disk_id, branch_name, read_only, lease_managed, expires_at > clock_timestamp(), created_at
		FROM client_sessions WHERE session_id = $1`, int64(*sid)).Scan(&diskID, &branchName, &readOnly, &managed, &active, &createdAt)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && !active {
		return ErrBadFileHandle
	}
	if err != nil {
		return err
	}
	if !managed {
		return ErrLegacySession
	}
	if readOnly {
		return ErrPerm
	}
	if check != nil {
		if err := check(diskID, branchName); err != nil {
			return err
		}
	}
	row, err := m.store.GetDisk(ctx, diskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	control, err := m.store.GetDiskControlPlane(ctx, diskID)
	if err != nil {
		return err
	}
	quota, err := m.loadTreeQuota(ctx, row, control)
	if err != nil {
		return err
	}
	disk := diskID.String()
	unlock := m.lockBranch(disk, branchName)
	defer unlock()
	current, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return err
	}
	if current.diskID != diskID || current.branch != branchName || current.readOnly || !current.createdAt.Equal(createdAt) {
		return ErrBadFileHandle
	}
	branch, err := m.pebble.OpenBranch(disk, branchName)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, branch.Close()) }()
	return fn(disk, branchName, quota, branch.DB())
}

func readCreateInode(store *pebble.DB, ino uint64) (pebblekv.Inode, error) {
	value, closer, err := store.Get(pebblekv.InodeKey(ino))
	if errors.Is(err, pebble.ErrNotFound) {
		return pebblekv.Inode{}, ErrNotFound
	}
	if err != nil {
		return pebblekv.Inode{}, err
	}
	inode, decodeErr := pebblekv.DecodeInode(value)
	if err := errors.Join(decodeErr, closer.Close()); err != nil {
		return pebblekv.Inode{}, err
	}
	if inode.Ino != ino {
		return pebblekv.Inode{}, fmt.Errorf("节点编号与键不一致")
	}
	return inode, nil
}

func readCreateParent(store *pebble.DB, ino uint64, name []byte) (pebblekv.Inode, error) {
	parent, err := readCreateInode(store, ino)
	if err != nil {
		return parent, err
	}
	if parent.Mode&S_IFMT != S_IFDIR {
		return parent, ErrNotDir
	}
	if parent.Nlink == 0 {
		return parent, ErrNotFound
	}
	if isDotOrDotDot(name) {
		return parent, ErrAlreadyExists
	}
	return parent, nil
}

func checkCreateEntry(store *pebble.DB, parent uint64, name []byte) error {
	_, closer, err := store.Get(pebblekv.EntryKey(parent, name))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := closer.Close(); err != nil {
		return err
	}
	return ErrAlreadyExists
}

func readCreateStat(store *pebble.DB) (pebblekv.BranchStat, error) {
	value, closer, err := store.Get(pebblekv.StatKey())
	if err != nil {
		return pebblekv.BranchStat{}, err
	}
	stat, decodeErr := pebblekv.DecodeBranchStat(value)
	return stat, errors.Join(decodeErr, closer.Close())
}

func setCreateInode(batch *pebble.Batch, inode pebblekv.Inode) error {
	value, err := pebblekv.EncodeInode(inode)
	if err != nil {
		return err
	}
	return batch.Set(pebblekv.InodeKey(inode.Ino), value, nil)
}

func touchCreateParent(parent *pebblekv.Inode, now time.Time, seq uint64) {
	parent.MtimeSec, parent.MtimeNsec = now.Unix(), uint32(now.Nanosecond())
	parent.CtimeSec, parent.CtimeNsec = now.Unix(), uint32(now.Nanosecond())
	parent.WriteSeq = seq
}

func (m *manager) createdPebbleInode(inode pebblekv.Inode) *Inode {
	entryTimeout := m.entryTimeout
	size := inode.Size
	if inode.Mode&S_IFMT == S_IFDIR {
		entryTimeout = m.dirEntryTimeout
		if size == 0 {
			size = DefaultDirSize
		}
	}
	return &Inode{
		Ino: inode.Ino, Generation: inode.Generation,
		EntryTimeoutSeconds: entryTimeout, AttrTimeoutSeconds: m.attrTimeout,
		Attr: Attribute{
			Mode: inode.Mode, Size: size, Nlink: inode.Nlink, UID: inode.UID, GID: inode.GID,
			Rdev: inode.Rdev, Blksize: DefaultBlockSize, Blocks: (size + 511) / 512,
			Atime: time.Unix(inode.AtimeSec, int64(inode.AtimeNsec)).UTC(),
			Mtime: time.Unix(inode.MtimeSec, int64(inode.MtimeNsec)).UTC(),
			Ctime: time.Unix(inode.CtimeSec, int64(inode.CtimeNsec)).UTC(),
			Flags: inode.Flags, ParentIno: inode.ParentIno,
		},
	}
}

func (m *manager) createPebbleNode(ctx context.Context, sid *uint64, params pebbleNodeParams) (result *Inode, err error) {
	err = m.withWritablePebbleBranch(ctx, sid, func(disk, branch string, quota *TreeQuota, store *pebble.DB) (err error) {
		parent, err := readCreateParent(store, params.parent, params.name)
		if err != nil {
			return err
		}
		if err := checkCreateEntry(store, params.parent, params.name); err != nil {
			return err
		}
		isDir := params.mode&S_IFMT == S_IFDIR
		if isDir && parent.Nlink >= MaxNlink {
			return ErrTooManyLinks
		}
		stat, err := readCreateStat(store)
		if err != nil {
			return err
		}
		size := uint64(len(params.target))
		if stat.NextWriteSeq == math.MaxUint64 || stat.Inodes == math.MaxUint64 || size > math.MaxUint64-stat.LogicalBytes {
			return ErrNoSpace
		}

		// 分配编号和提交批次期间持有配额锁，避免其他分支同时占满磁盘。
		quota.mu.Lock()
		defer quota.mu.Unlock()
		if quota.closed {
			return ErrNotFound
		}
		if quota.usage.Bytes >= quota.maxBytes || size > quota.maxBytes-quota.usage.Bytes {
			return ErrNoSpace
		}
		ino, err := m.pebble.AllocateInode(disk)
		if err != nil {
			return err
		}
		if ino > math.MaxInt64 {
			return ErrNoSpace
		}
		gid, mode := params.gid, params.mode
		if parent.Mode&S_ISGID != 0 {
			gid = parent.GID
			if isDir {
				mode |= S_ISGID
			}
		}
		rdev := uint32(0)
		if mode&S_IFMT == S_IFCHR || mode&S_IFMT == S_IFBLK {
			rdev = params.rdev
		}
		now := time.Now().UTC()
		seq := stat.NextWriteSeq
		inode := pebblekv.Inode{
			Ino: ino, Mode: mode, Nlink: 1, UID: params.uid, GID: gid, Rdev: rdev,
			Size: size, Blksize: DefaultBlockSize, Blocks: (size + 511) / 512,
			AtimeSec: now.Unix(), AtimeNsec: uint32(now.Nanosecond()),
			MtimeSec: now.Unix(), MtimeNsec: uint32(now.Nanosecond()),
			CtimeSec: now.Unix(), CtimeNsec: uint32(now.Nanosecond()),
			WriteSeq: seq, SymlinkTarget: params.target,
		}
		if isDir {
			inode.ParentIno = params.parent
			inode.Nlink = 2
			parent.Nlink++
		}
		touchCreateParent(&parent, now, seq)
		stat.NextWriteSeq++
		stat.Inodes++
		stat.LogicalBytes += size
		batch := store.NewBatch()
		defer func() { err = errors.Join(err, batch.Close()) }()
		if err := setCreateInode(batch, inode); err != nil {
			return err
		}
		if err := setCreateInode(batch, parent); err != nil {
			return err
		}
		if err := batch.Set(pebblekv.EntryKey(params.parent, params.name), pebblekv.EncodeEntry(pebblekv.Entry{Ino: ino, Mode: mode}), nil); err != nil {
			return err
		}
		for _, dirtyIno := range []uint64{params.parent, ino} {
			if err := batch.Set(pebblekv.DirtyKey(dirtyIno), []byte{1}, nil); err != nil {
				return err
			}
		}
		if err := batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(stat), nil); err != nil {
			return err
		}
		if err := quota.applyLocked(branch, ino, size, true, func() error { return batch.Commit(pebble.Sync) }, true); err != nil {
			return err
		}
		m.onDirEntryMutated(params.parent)
		result = m.createdPebbleInode(inode)
		return nil
	})
	return result, err
}

func (m *manager) linkPebbleNode(ctx context.Context, sid *uint64, params LinkParams) (result *Inode, err error) {
	err = m.withWritablePebbleBranch(ctx, sid, func(_, _ string, quota *TreeQuota, store *pebble.DB) (err error) {
		parent, err := readCreateParent(store, params.NewParentIno, params.NewName)
		if err != nil {
			return err
		}
		inode, err := readCreateInode(store, params.Ino)
		if err != nil {
			return err
		}
		if inode.Mode&S_IFMT == S_IFDIR {
			return ErrPerm
		}
		if inode.Nlink >= MaxNlink {
			return ErrTooManyLinks
		}
		if inode.Nlink == 0 {
			return ErrNotFound
		}
		if err := checkCreateEntry(store, params.NewParentIno, params.NewName); err != nil {
			return err
		}
		stat, err := readCreateStat(store)
		if err != nil {
			return err
		}
		if stat.NextWriteSeq == math.MaxUint64 {
			return ErrNoSpace
		}
		now := time.Now().UTC()
		seq := stat.NextWriteSeq
		stat.NextWriteSeq++
		inode.Nlink++
		inode.CtimeSec, inode.CtimeNsec = now.Unix(), uint32(now.Nanosecond())
		inode.WriteSeq = seq
		touchCreateParent(&parent, now, seq)
		batch := store.NewBatch()
		defer func() { err = errors.Join(err, batch.Close()) }()
		if err := setCreateInode(batch, inode); err != nil {
			return err
		}
		if err := setCreateInode(batch, parent); err != nil {
			return err
		}
		if err := batch.Set(pebblekv.EntryKey(params.NewParentIno, params.NewName), pebblekv.EncodeEntry(pebblekv.Entry{Ino: inode.Ino, Mode: inode.Mode}), nil); err != nil {
			return err
		}
		for _, ino := range []uint64{params.NewParentIno, params.Ino} {
			if err := batch.Set(pebblekv.DirtyKey(ino), []byte{1}, nil); err != nil {
				return err
			}
		}
		if err := batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(stat), nil); err != nil {
			return err
		}
		quota.mu.Lock()
		defer quota.mu.Unlock()
		if quota.closed {
			return ErrNotFound
		}
		if quota.usage.Bytes >= quota.maxBytes {
			return ErrNoSpace
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
		m.onDirEntryMutated(params.NewParentIno)
		result = m.createdPebbleInode(inode)
		return nil
	})
	return result, err
}
