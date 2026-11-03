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

type pebbleReadSession struct {
	id        uint64
	diskID    pgtype.UUID
	branch    string
	commitID  pgtype.UUID
	readOnly  bool
	managed   bool
	active    bool
	createdAt time.Time
}

type pebbleDirCursorKey struct {
	sessionID uint64
	createdAt time.Time
	ino       uint64
	offset    uint64
}

func (m *manager) findPebbleSession(ctx context.Context, sid *uint64) (pebbleReadSession, error) {
	if sid == nil || *sid == 0 || *sid > math.MaxInt64 {
		return pebbleReadSession{}, ErrInvalidArgument
	}
	pp, ok := m.store.(poolProvider)
	if !ok || pp == nil || pp.Pool() == nil {
		return pebbleReadSession{}, ErrBadFileHandle
	}
	var session pebbleReadSession
	session.id = *sid
	err := pp.Pool().QueryRow(ctx, `SELECT disk_id, branch_name, read_only, commit_id, lease_managed,
		expires_at > clock_timestamp(), created_at FROM client_sessions WHERE session_id = $1`, int64(*sid)).Scan(
		&session.diskID, &session.branch, &session.readOnly, &session.commitID,
		&session.managed, &session.active, &session.createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return pebbleReadSession{}, ErrBadFileHandle
	}
	return session, err
}

func (m *manager) requirePebbleSession(ctx context.Context, sid *uint64) (pebbleReadSession, error) {
	session, err := m.findPebbleSession(ctx, sid)
	if err != nil {
		return pebbleReadSession{}, err
	}
	if !session.managed || !session.active {
		return pebbleReadSession{}, ErrBadFileHandle
	}
	return session, nil
}

func (m *manager) resolveDirReadSession(ctx context.Context, fh uint64, paramID *uint64) (pebbleReadSession, bool, error) {
	contextID, contextSet := SessionIDFromContext(ctx)
	contextSet = contextSet && contextID != nil
	strict := fh == 0 || paramID == nil && contextSet
	if paramID == nil {
		paramID = contextID
	} else if contextSet && *paramID != *contextID {
		return pebbleReadSession{}, false, ErrInvalidArgument
	}
	if paramID == nil {
		if fh == 0 {
			return pebbleReadSession{}, false, ErrInvalidArgument
		}
		return pebbleReadSession{}, false, nil
	}
	if err := validateOptionalSessionID(paramID); err != nil {
		return pebbleReadSession{}, false, err
	}
	if !strict && !m.hasPool() {
		return pebbleReadSession{}, false, nil
	}
	session, err := m.findPebbleSession(ctx, paramID)
	if err != nil {
		if !strict && errors.Is(err, ErrBadFileHandle) {
			return pebbleReadSession{}, false, nil
		}
		return pebbleReadSession{}, false, err
	}
	if !session.managed {
		if !strict {
			return pebbleReadSession{}, false, nil
		}
		return pebbleReadSession{}, false, ErrBadFileHandle
	}
	if !session.active {
		return pebbleReadSession{}, false, ErrBadFileHandle
	}
	return session, true, nil
}

func (m *manager) withPebbleSession(ctx context.Context, session pebbleReadSession, fn func(*pebble.DB) error) (err error) {
	disk := session.diskID.String()
	if session.readOnly {
		commit, err := m.pebble.OpenCommit(disk, session.commitID.String())
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, commit.Close()) }()
		return fn(commit.DB())
	}
	unlock := m.lockBranch(disk, session.branch)
	defer unlock()
	current, err := m.requirePebbleSession(ctx, &session.id)
	if err != nil {
		return err
	}
	if current.diskID != session.diskID || current.branch != session.branch || current.readOnly || !current.createdAt.Equal(session.createdAt) {
		return ErrBadFileHandle
	}
	branch, err := m.pebble.OpenBranch(disk, session.branch)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, branch.Close()) }()
	return fn(branch.DB())
}

func readPebbleInode(store *pebble.DB, ino uint64) (pebblekv.Inode, error) {
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
		return pebblekv.Inode{}, fmt.Errorf("节点编号与键不一致: %d", ino)
	}
	return inode, nil
}

func checkPebbleInodeLive(store *pebble.DB, inode pebblekv.Inode) error {
	if inode.Nlink != 0 {
		return nil
	}
	_, closer, err := store.Get(pebblekv.OrphanKey(inode.Ino))
	if errors.Is(err, pebble.ErrNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return closer.Close()
}

func checkPebbleSessionInodeLive(session pebbleReadSession, store *pebble.DB, inode pebblekv.Inode) error {
	if session.readOnly && inode.Nlink == 0 {
		return ErrNotFound
	}
	return checkPebbleInodeLive(store, inode)
}

func writePebbleAtime(store *pebble.DB, ino uint64, value []byte) (err error) {
	batch := store.NewBatch()
	defer func() { err = errors.Join(err, batch.Close()) }()
	if err := batch.Set(pebblekv.InodeKey(ino), value, nil); err != nil {
		return err
	}
	if err := batch.Set(pebblekv.DirtyKey(ino), []byte{1}, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (m *manager) newPebbleInode(target pebblekv.Inode) *Inode {
	entryTimeout := m.entryTimeout
	if target.Mode&S_IFMT == S_IFDIR {
		entryTimeout = m.dirEntryTimeout
	}
	return m.newPebbleInodeWithTimeout(target, entryTimeout, m.attrTimeout)
}

func (m *manager) newPebbleInodeWithTimeout(target pebblekv.Inode, entryTimeout, attrTimeout float64) *Inode {
	var size, blocks uint64
	var rdev uint32
	switch target.Mode & S_IFMT {
	case S_IFDIR:
		size = target.Size
		if size == 0 {
			size = DefaultDirSize
		}
		blocks = size / 512
		if size%512 != 0 {
			blocks++
		}
	case S_IFREG, S_IFLNK:
		size = target.Size
		blocks = size / 512
		if size%512 != 0 {
			blocks++
		}
	case S_IFCHR, S_IFBLK:
		rdev = target.Rdev
	}
	blksize := target.Blksize
	if blksize == 0 {
		blksize = DefaultBlockSize
	}
	return &Inode{
		Ino: target.Ino, Generation: target.Generation,
		EntryTimeoutSeconds: entryTimeout, AttrTimeoutSeconds: attrTimeout,
		Attr: Attribute{
			Mode: target.Mode, Size: size, Nlink: target.Nlink, UID: target.UID, GID: target.GID,
			Rdev: rdev, Blksize: blksize, Blocks: blocks, Flags: target.Flags, ParentIno: target.ParentIno,
			Atime: time.Unix(target.AtimeSec, int64(target.AtimeNsec)).UTC(),
			Mtime: time.Unix(target.MtimeSec, int64(target.MtimeNsec)).UTC(),
			Ctime: time.Unix(target.CtimeSec, int64(target.CtimeNsec)).UTC(),
		},
	}
}

func (m *manager) pebbleLookup(ctx context.Context, sid *uint64, parentIno uint64, name []byte) (*Inode, error) {
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	var result *Inode
	err = m.withPebbleSession(ctx, session, func(store *pebble.DB) error {
		parent, err := readPebbleInode(store, parentIno)
		if err != nil {
			return err
		}
		if parent.Mode&S_IFMT != S_IFDIR {
			return ErrNotDir
		}
		if bytes.Equal(name, []byte("..")) {
			if err := checkPebbleSessionInodeLive(session, store, parent); err != nil {
				return err
			}
			if parentIno == RootIno {
				result = m.newPebbleInode(parent)
				return nil
			}
			if parent.ParentIno == 0 || parent.ParentIno == parentIno {
				return ErrNotFound
			}
			grandparent, err := readPebbleInode(store, parent.ParentIno)
			if err != nil {
				return err
			}
			if err := checkPebbleSessionInodeLive(session, store, grandparent); err != nil {
				return err
			}
			result = m.newPebbleInode(grandparent)
			return nil
		}
		if parent.Nlink == 0 {
			return ErrNotFound
		}
		if bytes.Equal(name, []byte(".")) {
			result = m.newPebbleInode(parent)
			return nil
		}
		if len(name) == 0 {
			return ErrNotFound
		}
		value, closer, err := store.Get(pebblekv.EntryKey(parentIno, name))
		if errors.Is(err, pebble.ErrNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		entry, decodeErr := pebblekv.DecodeEntry(value)
		if err := errors.Join(decodeErr, closer.Close()); err != nil {
			return err
		}
		child, err := readPebbleInode(store, entry.Ino)
		if err != nil {
			return err
		}
		if child.Nlink == 0 {
			return ErrNotFound
		}
		result = m.newPebbleInode(child)
		return nil
	})
	return result, err
}

func (m *manager) pebbleGetAttr(ctx context.Context, sid *uint64, ino uint64) (*Inode, error) {
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	var result *Inode
	err = m.withPebbleSession(ctx, session, func(store *pebble.DB) error {
		inode, err := readPebbleInode(store, ino)
		if err != nil {
			return err
		}
		if err := checkPebbleSessionInodeLive(session, store, inode); err != nil {
			return err
		}
		result = m.newPebbleInode(inode)
		return nil
	})
	return result, err
}

func (m *manager) pebbleReadlink(ctx context.Context, sid *uint64, ino uint64) ([]byte, error) {
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	var result []byte
	err = m.withPebbleSession(ctx, session, func(store *pebble.DB) error {
		inode, err := readPebbleInode(store, ino)
		if err != nil {
			return err
		}
		if err := checkPebbleSessionInodeLive(session, store, inode); err != nil {
			return err
		}
		if inode.Mode&S_IFMT != S_IFLNK {
			return ErrInvalidArgument
		}
		if len(inode.SymlinkTarget) == 0 {
			return fmt.Errorf("%w: 符号链接节点 %d 的目标为空", ErrIO, ino)
		}
		if !session.readOnly && m.atimeMode != AtimeModeNoAtime {
			now := time.Now().UTC()
			atime := time.Unix(inode.AtimeSec, int64(inode.AtimeNsec)).UTC()
			mtime := time.Unix(inode.MtimeSec, int64(inode.MtimeNsec)).UTC()
			ctime := time.Unix(inode.CtimeSec, int64(inode.CtimeNsec)).UTC()
			if m.shouldTouchAtime(atime, mtime, ctime, now) {
				inode.AtimeSec, inode.AtimeNsec = now.Unix(), uint32(now.Nanosecond())
				value, err := pebblekv.EncodeInode(inode)
				if err != nil {
					return err
				}
				if err := writePebbleAtime(store, ino, value); err != nil {
					return err
				}
			}
		}
		result = append([]byte(nil), inode.SymlinkTarget...)
		return nil
	})
	return result, err
}

func (m *manager) pebbleReadDir(ctx context.Context, session pebbleReadSession, ino, offset uint64, limit uint32) ([]DirEntry, error) {
	plus, err := m.readPebbleDir(ctx, session, ino, offset, limit)
	if err != nil {
		return nil, err
	}
	entries := make([]DirEntry, 0, len(plus))
	for _, entry := range plus {
		entries = append(entries, DirEntry{Ino: entry.Ino, Name: entry.Name, Mode: entry.Inode.Attr.Mode, NextOffset: entry.NextOffset})
	}
	return entries, nil
}

func (m *manager) pebbleReadDirPlus(ctx context.Context, session pebbleReadSession, ino, offset uint64, limit uint32) ([]DirEntryPlus, error) {
	return m.readPebbleDir(ctx, session, ino, offset, limit)
}

func (m *manager) readPebbleDir(ctx context.Context, session pebbleReadSession, ino, offset uint64, limit uint32) ([]DirEntryPlus, error) {
	entries := make([]DirEntryPlus, 0, min(int(limit), 16))
	anchors := make(map[pebbleDirCursorKey][]byte)
	err := m.withPebbleSession(ctx, session, func(store *pebble.DB) (err error) {
		dir, err := readPebbleInode(store, ino)
		if err != nil {
			return err
		}
		if dir.Mode&S_IFMT != S_IFDIR {
			return ErrNotDir
		}
		if dir.Nlink == 0 {
			return checkPebbleSessionInodeLive(session, store, dir)
		}
		appendEntry := func(name []byte, target pebblekv.Inode, next uint64, dot bool) {
			inode := m.newPebbleInode(target)
			if dot {
				inode = m.newPebbleInodeWithTimeout(target, 0, m.attrTimeout)
			}
			entries = append(entries, DirEntryPlus{Ino: target.Ino, Name: name, NextOffset: next, Inode: inode,
				EntryTimeoutSeconds: inode.EntryTimeoutSeconds, AttrTimeoutSeconds: inode.AttrTimeoutSeconds})
		}
		if offset < 1 && len(entries) < int(limit) {
			appendEntry([]byte("."), dir, 1, true)
		}
		if offset < 2 && len(entries) < int(limit) {
			parent := dir
			if ino != RootIno && dir.ParentIno > 0 {
				target, err := readPebbleInode(store, dir.ParentIno)
				if err == nil {
					parent = target
				} else if !errors.Is(err, ErrNotFound) {
					return err
				}
			}
			appendEntry([]byte(".."), parent, 2, true)
		}
		if len(entries) == int(limit) {
			return nil
		}
		lower, upper := pebblekv.EntryRange(ino)
		iter, err := store.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, iter.Close()) }()
		cursorKey := pebbleDirCursorKey{sessionID: session.id, createdAt: session.createdAt, ino: ino, offset: offset}
		m.dirCursorMu.Lock()
		anchor, anchored := m.pebbleDirCursors[cursorKey]
		m.dirCursorMu.Unlock()
		var valid bool
		var skip uint64
		if offset >= 3 && anchored {
			valid = iter.SeekGE(pebblekv.EntryKey(ino, anchor))
			if valid && bytes.Equal(iter.Key(), pebblekv.EntryKey(ino, anchor)) {
				valid = iter.Next()
			}
		} else {
			valid = iter.First()
			if offset > 2 {
				skip = offset - 2
			}
		}
		next := max(offset, uint64(2))
		for valid && len(entries) < int(limit) {
			if err := ctx.Err(); err != nil {
				return err
			}
			_, name, err := pebblekv.DecodeEntryKey(iter.Key())
			if err != nil {
				return err
			}
			entry, err := pebblekv.DecodeEntry(iter.Value())
			if err != nil {
				return err
			}
			child, err := readPebbleInode(store, entry.Ino)
			if err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
			if err == nil && child.Nlink > 0 {
				if skip > 0 {
					skip--
				} else {
					if next == math.MaxInt64 {
						return ErrInvalidArgument
					}
					next++
					appendEntry(name, child, next, false)
					key := pebbleDirCursorKey{sessionID: session.id, createdAt: session.createdAt, ino: ino, offset: next}
					anchors[key] = name
				}
			}
			valid = iter.Next()
		}
		return iter.Error()
	})
	if err != nil {
		return nil, err
	}
	m.dirCursorMu.Lock()
	if m.pebbleDirCursors == nil {
		m.pebbleDirCursors = make(map[pebbleDirCursorKey][]byte)
	}
	for key, name := range anchors {
		m.pebbleDirCursors[key] = name
	}
	m.dirCursorMu.Unlock()
	return entries, nil
}

func (m *manager) pebbleStatFS(ctx context.Context, sid *uint64, ino uint64) (*StatFSResult, error) {
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	err = m.withPebbleSession(ctx, session, func(store *pebble.DB) error {
		inode, err := readPebbleInode(store, ino)
		if err != nil {
			return err
		}
		return checkPebbleSessionInodeLive(session, store, inode)
	})
	if err != nil {
		return nil, err
	}
	disk, err := m.GetDisk(ctx, session.diskID.String())
	if err != nil {
		return nil, err
	}
	return statFSForDisk(disk), nil
}
