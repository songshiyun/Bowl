package meta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/cockroachdb/pebble/v2"
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

// pebbleReader 是只读访问 Pebble 的最小接口，*pebble.DB 与 *pebble.Snapshot 都满足。
type pebbleReader interface {
	Get(key []byte) ([]byte, io.Closer, error)
	NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error)
}

type pebbleDirCursorKey struct {
	sessionID uint64
	createdAt time.Time
	ino       uint64
	offset    uint64
}

// findPebbleSession 从控制面读取会话权威状态，调用方通常应使用带缓存的 loadPebbleSession。
func (m *manager) findPebbleSession(ctx context.Context, sid *uint64) (pebbleReadSession, error) {
	if sid == nil || *sid == 0 || *sid > math.MaxInt64 {
		return pebbleReadSession{}, ErrInvalidArgument
	}
	return newBranchControlStore(m.store).FindSession(ctx, *sid)
}

// loadPebbleSession 命中内存缓存时直接返回，避免每次数据面 RPC 查询 PostgreSQL。
func (m *manager) loadPebbleSession(ctx context.Context, sid *uint64) (pebbleReadSession, error) {
	if sid == nil || *sid == 0 || *sid > math.MaxInt64 {
		return pebbleReadSession{}, ErrInvalidArgument
	}
	m.sessionCacheMu.Lock()
	entry, cached := m.sessionCache[*sid]
	m.sessionCacheMu.Unlock()
	if cached && time.Now().Before(entry.fresh) {
		return entry.session, nil
	}
	session, err := m.findPebbleSession(ctx, sid)
	if err != nil {
		return pebbleReadSession{}, err
	}
	m.cachePebbleSession(session)
	return session, nil
}

// peekPebbleSession 只读取缓存，不触发数据库查询；用于锁内复查会话是否已被注销。
func (m *manager) peekPebbleSession(id uint64) (pebbleReadSession, bool) {
	m.sessionCacheMu.Lock()
	defer m.sessionCacheMu.Unlock()
	entry, ok := m.sessionCache[id]
	return entry.session, ok
}

func (m *manager) cachePebbleSession(session pebbleReadSession) {
	m.sessionCacheMu.Lock()
	defer m.sessionCacheMu.Unlock()
	if m.sessionCache == nil {
		m.sessionCache = make(map[uint64]pebbleSessionCacheEntry)
	}
	now := time.Now()
	if len(m.sessionCache) >= maxCachedSessions {
		for id, entry := range m.sessionCache {
			if now.After(entry.fresh) {
				delete(m.sessionCache, id)
			}
		}
	}
	m.sessionCache[session.id] = pebbleSessionCacheEntry{session: session, fresh: now.Add(m.sessionCacheTTL)}
}

func (m *manager) invalidatePebbleSession(id uint64) {
	m.sessionCacheMu.Lock()
	defer m.sessionCacheMu.Unlock()
	delete(m.sessionCache, id)
}

func (m *manager) invalidatePebbleBranchSessions(disk, branch string) {
	m.sessionCacheMu.Lock()
	defer m.sessionCacheMu.Unlock()
	for id, entry := range m.sessionCache {
		if entry.session.diskID.String() == disk && entry.session.branch == branch {
			delete(m.sessionCache, id)
		}
	}
}

func (m *manager) invalidatePebbleDiskSessions(disk string) {
	m.sessionCacheMu.Lock()
	defer m.sessionCacheMu.Unlock()
	for id, entry := range m.sessionCache {
		if entry.session.diskID.String() == disk {
			delete(m.sessionCache, id)
		}
	}
}

func samePebbleSession(a, b pebbleReadSession) bool {
	return a.id == b.id && a.diskID == b.diskID && a.branch == b.branch && a.readOnly == b.readOnly &&
		a.commitID == b.commitID && a.createdAt.Equal(b.createdAt)
}

func (m *manager) requirePebbleSession(ctx context.Context, sid *uint64) (pebbleReadSession, error) {
	session, err := m.loadPebbleSession(ctx, sid)
	if err != nil {
		return pebbleReadSession{}, err
	}
	if !session.managed || !session.active {
		return pebbleReadSession{}, ErrBadFileHandle
	}
	return session, nil
}

// withPebbleReadSession 在分支读锁内用短命快照执行多键读取，保证 Inode 与 Blob 视图一致。
func (m *manager) withPebbleReadSession(ctx context.Context, session pebbleReadSession, fn func(pebbleReader, *pebblekv.BlobStore) error) (err error) {
	disk := session.diskID.String()
	if session.readOnly {
		commit, err := m.pebble.OpenCommit(disk, session.commitID.String())
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, commit.Close()) }()
		return fn(commit.DB(), commit.Blobs())
	}
	unlock := m.locks.LockBranchRead(disk, session.branch)
	defer unlock()
	if current, ok := m.peekPebbleSession(session.id); !ok || !samePebbleSession(current, session) {
		return ErrBadFileHandle
	}
	branch, err := m.pebble.OpenBranch(disk, session.branch)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, branch.Close()) }()
	snapshot := branch.DB().NewSnapshot()
	defer func() { err = errors.Join(err, snapshot.Close()) }()
	return fn(snapshot, branch.Blobs())
}

// withPebblePointReadSession 直接读取分支实例，单键点查不创建 Pebble Snapshot。
func (m *manager) withPebblePointReadSession(ctx context.Context, session pebbleReadSession, fn func(pebbleReader, *pebblekv.BlobStore) error) (err error) {
	disk := session.diskID.String()
	if session.readOnly {
		commit, err := m.pebble.OpenCommit(disk, session.commitID.String())
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, commit.Close()) }()
		return fn(commit.DB(), commit.Blobs())
	}
	unlock := m.locks.LockBranchRead(disk, session.branch)
	defer unlock()
	if current, ok := m.peekPebbleSession(session.id); !ok || !samePebbleSession(current, session) {
		return ErrBadFileHandle
	}
	branch, err := m.pebble.OpenBranch(disk, session.branch)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, branch.Close()) }()
	return fn(branch.DB(), branch.Blobs())
}

// withPebbleWriteSession 在分支共享读锁内执行显式持久化屏障。
func (m *manager) withPebbleWriteSession(ctx context.Context, session pebbleReadSession, fn func(*pebble.DB, *pebblekv.BlobStore) error) (err error) {
	disk := session.diskID.String()
	if session.readOnly {
		commit, err := m.pebble.OpenCommit(disk, session.commitID.String())
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, commit.Close()) }()
		return fn(commit.DB(), commit.Blobs())
	}
	unlock := m.locks.LockBranchRead(disk, session.branch)
	defer unlock()
	if current, ok := m.peekPebbleSession(session.id); !ok || !samePebbleSession(current, session) {
		return ErrBadFileHandle
	}
	branch, err := m.pebble.OpenBranch(disk, session.branch)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, branch.Close()) }()
	return fn(branch.DB(), branch.Blobs())
}

func readPebbleInode(store pebbleReader, ino uint64) (pebblekv.Inode, error) {
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

func checkPebbleInodeLive(store pebbleReader, inode pebblekv.Inode) error {
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

func checkPebbleSessionInodeLive(session pebbleReadSession, store pebbleReader, inode pebblekv.Inode) error {
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
	return batch.Commit(pebble.NoSync)
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

func (m *manager) Lookup(ctx context.Context, sid *uint64, parentIno uint64, name []byte) (*Inode, error) {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if len(name) > MaxFileNameBytes {
		return nil, ErrNameTooLong
	}
	if bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
		return nil, ErrInvalidArgument
	}
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	var result *Inode
	err = m.withPebbleReadSession(ctx, session, func(store pebbleReader, _ *pebblekv.BlobStore) error {
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
				m.applyDirtyPebbleAtime(session, &parent)
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
			m.applyDirtyPebbleAtime(session, &grandparent)
			result = m.newPebbleInode(grandparent)
			return nil
		}
		if parent.Nlink == 0 {
			return ErrNotFound
		}
		if bytes.Equal(name, []byte(".")) {
			m.applyDirtyPebbleAtime(session, &parent)
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
		m.applyDirtyPebbleAtime(session, &child)
		result = m.newPebbleInode(child)
		return nil
	})
	return result, err
}

func (m *manager) GetAttr(ctx context.Context, sid *uint64, ino uint64) (*Inode, error) {
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	var result *Inode
	err = m.withPebblePointReadSession(ctx, session, func(store pebbleReader, _ *pebblekv.BlobStore) error {
		inode, err := readPebbleInode(store, ino)
		if err != nil {
			return err
		}
		if err := checkPebbleSessionInodeLive(session, store, inode); err != nil {
			return err
		}
		m.applyDirtyPebbleAtime(session, &inode)
		result = m.newPebbleInode(inode)
		return nil
	})
	return result, err
}

func (m *manager) Readlink(ctx context.Context, sid *uint64, ino uint64) ([]byte, error) {
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	var result []byte
	err = m.withPebblePointReadSession(ctx, session, func(store pebbleReader, _ *pebblekv.BlobStore) error {
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
		m.markPebbleAtimeDirty(session, inode, time.Now().UTC())
		result = append([]byte(nil), inode.SymlinkTarget...)
		return nil
	})
	return result, err
}

func (m *manager) ReadDir(ctx context.Context, params ReadDirParams) ([]DirEntry, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 ||
		params.Fh > math.MaxInt64 ||
		params.Limit == 0 || params.Limit > MaxReadDirLimit ||
		params.Offset > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}
	view, err := m.requirePebbleSession(ctx, params.SessionID)
	if err != nil {
		return nil, err
	}
	return m.pebbleReadDir(ctx, view, params.Ino, params.Offset, params.Limit)
}

func (m *manager) ReadDirPlus(ctx context.Context, params ReadDirPlusParams) ([]DirEntryPlus, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 ||
		params.Fh > math.MaxInt64 ||
		params.Limit == 0 || params.Limit > MaxReadDirLimit ||
		params.Offset > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}
	view, err := m.requirePebbleSession(ctx, params.SessionID)
	if err != nil {
		return nil, err
	}
	return m.pebbleReadDirPlus(ctx, view, params.Ino, params.Offset, params.Limit)
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
	err := m.withPebbleReadSession(ctx, session, func(store pebbleReader, _ *pebblekv.BlobStore) (err error) {
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

func (m *manager) StatFS(ctx context.Context, sid *uint64, ino uint64) (*StatFSResult, error) {
	if ino == 0 || ino > math.MaxInt64 {
		return nil, fmt.Errorf("%w: ino must be between 1 and %d, got %d", ErrInvalidArgument, int64(math.MaxInt64), ino)
	}
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	err = m.withPebblePointReadSession(ctx, session, func(store pebbleReader, _ *pebblekv.BlobStore) error {
		inode, err := readPebbleInode(store, ino)
		if err != nil {
			return err
		}
		return checkPebbleSessionInodeLive(session, store, inode)
	})
	if err != nil {
		return nil, err
	}
	if quota, ok := m.lookupTreeQuota(session.diskID.String()); ok {
		maxBytes, usage := quota.Snapshot()
		return statFSForDisk(&Disk{MaxBytes: maxBytes, UsedBytes: usage.Bytes, UsedInodes: usage.Inodes}), nil
	}
	disk, err := m.GetDisk(ctx, session.diskID.String())
	if err != nil {
		return nil, err
	}
	return statFSForDisk(disk), nil
}
