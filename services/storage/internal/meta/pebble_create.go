package meta

import (
	"bytes"
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

type pebbleNodeParams struct {
	parent uint64
	name   []byte
	mode   uint32
	uid    uint32
	gid    uint32
	rdev   uint32
	target []byte
}

func (m *manager) Mknod(ctx context.Context, params MknodParams) (*Inode, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
	}
	if params.Mode > 0o177777 {
		return nil, ErrInvalidArgument
	}

	modeType := params.Mode & S_IFMT
	switch modeType {
	case S_IFREG, S_IFCHR, S_IFBLK, S_IFIFO, S_IFSOCK:
	default:
		return nil, ErrInvalidArgument
	}

	return m.createPebbleNode(ctx, params.SessionID, pebbleNodeParams{parent: params.ParentIno, name: params.Name, mode: modeType | ((params.Mode & PermMask) &^ (params.Umask & PermMask)), uid: params.UID, gid: params.GID, rdev: params.Rdev})
}

func (m *manager) Mkdir(ctx context.Context, params MkdirParams) (*Inode, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
	}
	if params.Mode > 65535 {
		return nil, ErrInvalidArgument
	}

	modeType := params.Mode & S_IFMT
	if modeType != 0 && modeType != S_IFDIR {
		return nil, ErrInvalidArgument
	}

	return m.createPebbleNode(ctx, params.SessionID, pebbleNodeParams{parent: params.ParentIno, name: params.Name, mode: S_IFDIR | ((params.Mode & PermMask) &^ (params.Umask & PermMask)), uid: params.UID, gid: params.GID})
}

func (m *manager) Symlink(ctx context.Context, params SymlinkParams) (*Inode, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
	}
	if len(params.Target) == 0 || bytes.IndexByte(params.Target, 0) >= 0 {
		return nil, ErrInvalidArgument
	}
	if len(params.Target) > MaxSymlinkTargetBytes {
		return nil, ErrNameTooLong
	}

	return m.createPebbleNode(ctx, params.SessionID, pebbleNodeParams{parent: params.ParentIno, name: params.Name, mode: S_IFLNK | 0o777, uid: params.UID, gid: params.GID, target: params.Target})
}

func (m *manager) Link(ctx context.Context, params LinkParams) (*Inode, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if params.NewParentIno == 0 || params.NewParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateEntryName(params.NewName); err != nil {
		return nil, err
	}

	return m.linkPebbleNode(ctx, params.SessionID, params)
}

func (m *manager) Create(ctx context.Context, params CreateParams) (*CreateResult, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
	}
	if params.Mode > 0xffff {
		return nil, ErrInvalidArgument
	}
	if (params.Flags & O_TMPFILE) != 0 {
		return nil, ErrInvalidArgument
	}

	modeType := params.Mode & S_IFMT
	if modeType != 0 && modeType != S_IFREG {
		return nil, ErrInvalidArgument
	}

	effectivePerm := (params.Mode & PermMask) &^ (params.Umask & (PermMask & 0o777))
	mode := S_IFREG | effectivePerm

	return m.pebbleCreateResult(ctx, params.SessionID, params, mode)
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
func (m *manager) withWritablePebbleBranch(ctx context.Context, sid *uint64, fn func(string, string, *TreeQuota, *pebble.DB, *pebblekv.BlobStore) error) (err error) {
	return m.withWritablePebbleBranchFor(ctx, sid, nil, fn)
}

func (m *manager) withWritablePebbleBranchFor(ctx context.Context, sid *uint64, check func(pgtype.UUID, string) error, fn func(string, string, *TreeQuota, *pebble.DB, *pebblekv.BlobStore) error) (err error) {
	if sid == nil || *sid == 0 || *sid > math.MaxInt64 {
		return ErrInvalidArgument
	}
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return err
	}
	if session.readOnly {
		return ErrPerm
	}
	if check != nil {
		if err := check(session.diskID, session.branch); err != nil {
			return err
		}
	}
	disk := session.diskID.String()
	quota, err := m.sessionTreeQuota(ctx, session.diskID)
	if err != nil {
		return err
	}
	unlock := m.locks.LockBranchRead(disk, session.branch)
	defer unlock()
	// 锁内复查会话缓存：CloseSession 等控制面入口驱逐后立即拒绝后续请求。
	if current, ok := m.peekPebbleSession(session.id); !ok || !samePebbleSession(current, session) {
		return ErrBadFileHandle
	}
	branch, err := m.pebble.OpenBranch(disk, session.branch)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, branch.Close()) }()
	if err := fn(disk, session.branch, quota, branch.DB(), branch.Blobs()); err != nil {
		return err
	}
	m.pebble.MarkBranchDirty(disk, session.branch)
	return nil
}

// sessionTreeQuota 优先复用内存配额协调器，未命中时才读取控制面。
func (m *manager) sessionTreeQuota(ctx context.Context, diskID pgtype.UUID) (*TreeQuota, error) {
	if quota, ok := m.lookupTreeQuota(diskID.String()); ok {
		return quota, nil
	}
	row, err := m.store.GetDisk(ctx, diskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	control, err := m.store.GetDiskControlPlane(ctx, diskID)
	if err != nil {
		return nil, err
	}
	return m.loadTreeQuota(ctx, row, control)
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
	err = m.withWritablePebbleBranch(ctx, sid, func(disk, branch string, quota *TreeQuota, store *pebble.DB, blobs *pebblekv.BlobStore) error {
		unlockInode := m.locks.LockInodeWrite(disk, branch, params.parent)
		defer unlockInode()
		return m.withBranchMutationTx(disk, branch, store, blobs, quota, func(tx *BranchMutationTx) error {
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
			size := uint64(len(params.target))
			if err := tx.RequireNewInode(size); err != nil {
				return err
			}

			// 号段分配在配额锁之外完成，避免持久化预留阻塞其他分支的写入。
			ino, err := m.pebble.AllocateInode(disk)
			if err != nil {
				return err
			}
			if ino > math.MaxInt64 {
				return ErrNoSpace
			}

			var inode pebblekv.Inode
			if err := tx.CommitCreate(ino, size, func() error {
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
				now := tx.Now()
				inode = pebblekv.Inode{
					Ino: ino, Mode: mode, Nlink: 1, UID: params.uid, GID: gid, Rdev: rdev,
					Size: size, Blksize: DefaultBlockSize, Blocks: (size + 511) / 512,
					AtimeSec: now.Unix(), AtimeNsec: uint32(now.Nanosecond()),
					MtimeSec: now.Unix(), MtimeNsec: uint32(now.Nanosecond()),
					CtimeSec: now.Unix(), CtimeNsec: uint32(now.Nanosecond()),
					SymlinkTarget: params.target,
				}
				if isDir {
					inode.ParentIno = params.parent
					inode.Nlink = 2
					parent.Nlink++
				}
				touchCreateParent(&parent, now, tx.WriteSeq())
				tx.AddInodes(1)
				tx.AddLogicalBytes(size)
				if err := tx.PutInode(&inode); err != nil {
					return err
				}
				if err := tx.PutInode(&parent); err != nil {
					return err
				}
				return tx.PutEntry(params.parent, params.name, ino, mode)
			}); err != nil {
				return err
			}

			result = m.createdPebbleInode(inode)
			return nil
		})
	})
	return result, err
}

func (m *manager) linkPebbleNode(ctx context.Context, sid *uint64, params LinkParams) (result *Inode, err error) {
	err = m.withWritablePebbleBranch(ctx, sid, func(disk, branch string, quota *TreeQuota, store *pebble.DB, blobs *pebblekv.BlobStore) error {
		unlockInodes := m.locks.LockInodesWrite(disk, branch, params.NewParentIno, params.Ino)
		defer unlockInodes()
		return m.withBranchMutationTx(disk, branch, store, blobs, quota, func(tx *BranchMutationTx) error {
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
			now := tx.Now()
			inode.Nlink++
			inode.CtimeSec, inode.CtimeNsec = now.Unix(), uint32(now.Nanosecond())
			touchCreateParent(&parent, now, tx.WriteSeq())
			if err := tx.PutInode(&inode); err != nil {
				return err
			}
			if err := tx.PutInode(&parent); err != nil {
				return err
			}
			if err := tx.PutEntry(params.NewParentIno, params.NewName, inode.Ino, inode.Mode); err != nil {
				return err
			}
			if err := tx.CommitWithQuota(true); err != nil {
				return err
			}

			result = m.createdPebbleInode(inode)
			return nil
		})
	})
	return result, err
}
