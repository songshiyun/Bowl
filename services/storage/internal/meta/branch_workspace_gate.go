package meta

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

const (
	// defaultSessionCacheTTL 是会话缓存软过期时间，决定控制面变更的最长可见延迟。
	defaultSessionCacheTTL = time.Second
	// maxCachedSessions 是缓存条目上限，超出时优先清理已过期条目。
	maxCachedSessions = 4096
)

// pebbleSessionCacheEntry 是会话内存写透缓存条目，fresh 之后重新读取控制面。
type pebbleSessionCacheEntry struct {
	session pebbleReadSession
	fresh   time.Time
}

// BranchWorkspaceGate 统一管理服务端客户端会话缓存（查找、更新、失效）、
// 分支工作区锁内二次校验（防止等待 Branch 锁期间会话过期或被替换）与 Pebble 句柄打开/关闭控制流，
// 并桥接 PebbleAtimeTracker 的脏访问时间登记、叠加与快照前刷盘屏障。
type BranchWorkspaceGate struct {
	store           db.Store
	pebble          *pebblekv.Manager
	locks           *BranchLockTable
	atimes          *PebbleAtimeTracker
	sessionCacheTTL time.Duration

	mu               sync.Mutex
	sessionCache     map[uint64]pebbleSessionCacheEntry
	beforeBranchLock func(sessionID uint64)
}

// NewBranchWorkspaceGate 创建分支工作区会话与访问时间网关。
func NewBranchWorkspaceGate(
	store db.Store,
	pebbleMgr *pebblekv.Manager,
	locks *BranchLockTable,
	atimes *PebbleAtimeTracker,
	cacheTTL time.Duration,
) *BranchWorkspaceGate {
	if cacheTTL <= 0 {
		cacheTTL = defaultSessionCacheTTL
	}
	return &BranchWorkspaceGate{
		store:           store,
		pebble:          pebbleMgr,
		locks:           locks,
		atimes:          atimes,
		sessionCacheTTL: cacheTTL,
		sessionCache:    make(map[uint64]pebbleSessionCacheEntry),
	}
}

func (g *BranchWorkspaceGate) bindDependencies(store db.Store, pebbleMgr *pebblekv.Manager, locks *BranchLockTable, atimes *PebbleAtimeTracker) {
	if g == nil {
		return
	}
	if g.store == nil {
		g.store = store
	}
	if g.pebble == nil {
		g.pebble = pebbleMgr
	}
	if g.locks == nil {
		g.locks = locks
	}
	if g.atimes == nil {
		g.atimes = atimes
	}
}

// SetBeforeBranchLockHook 注册在解析会话后、获取分支锁前触发的测试钩子（仅供并发竞态测试使用）。
func (g *BranchWorkspaceGate) SetBeforeBranchLockHook(fn func(sessionID uint64)) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.beforeBranchLock = fn
	g.mu.Unlock()
}

func (g *BranchWorkspaceGate) invokeBeforeBranchLock(sessionID uint64) {
	if g == nil {
		return
	}
	g.mu.Lock()
	hook := g.beforeBranchLock
	g.mu.Unlock()
	if hook != nil {
		hook(sessionID)
	}
}

// HasCachedSession 报告指定会话编号当前是否驻留在内存会话缓存中。
func (g *BranchWorkspaceGate) HasCachedSession(id uint64) bool {
	_, ok := g.peekSession(id)
	return ok
}

// ReloadSession 从控制面重新加载指定会话并刷新内存缓存。
func (g *BranchWorkspaceGate) ReloadSession(ctx context.Context, id uint64) error {
	session, err := g.findSession(ctx, &id)
	if err != nil {
		return err
	}
	g.cacheSession(&session)
	return nil
}

// findSession 从控制面读取会话权威状态，调用方通常应使用带缓存的 loadSession。
func (g *BranchWorkspaceGate) findSession(ctx context.Context, sid *uint64) (pebbleReadSession, error) {
	if g == nil || sid == nil || *sid == 0 || *sid > math.MaxInt64 {
		return pebbleReadSession{}, ErrInvalidArgument
	}
	return newBranchControlStore(g.store).FindSession(ctx, *sid)
}

// loadSession 命中内存缓存时直接返回，避免每次数据面 RPC 查询 PostgreSQL。
func (g *BranchWorkspaceGate) loadSession(ctx context.Context, sid *uint64) (pebbleReadSession, error) {
	if g == nil || sid == nil || *sid == 0 || *sid > math.MaxInt64 {
		return pebbleReadSession{}, ErrInvalidArgument
	}
	g.mu.Lock()
	entry, cached := g.sessionCache[*sid]
	g.mu.Unlock()
	if cached && time.Now().Before(entry.fresh) {
		return entry.session, nil
	}
	session, err := g.findSession(ctx, sid)
	if err != nil {
		return pebbleReadSession{}, err
	}
	g.cacheSession(&session)
	return session, nil
}

// peekSession 只读取缓存，不触发数据库查询；用于锁内复查会话是否已被注销或替换。
func (g *BranchWorkspaceGate) peekSession(id uint64) (pebbleReadSession, bool) {
	if g == nil {
		return pebbleReadSession{}, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	entry, ok := g.sessionCache[id]
	return entry.session, ok
}

func (g *BranchWorkspaceGate) cacheSession(session *pebbleReadSession) {
	if g == nil || session == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.sessionCache == nil {
		g.sessionCache = make(map[uint64]pebbleSessionCacheEntry)
	}
	now := time.Now()
	if len(g.sessionCache) >= maxCachedSessions {
		for id, entry := range g.sessionCache {
			if now.After(entry.fresh) {
				delete(g.sessionCache, id)
			}
		}
	}
	ttl := g.sessionCacheTTL
	if ttl <= 0 {
		ttl = defaultSessionCacheTTL
	}
	g.sessionCache[session.id] = pebbleSessionCacheEntry{session: *session, fresh: now.Add(ttl)}
}

// InvalidateSession 驱逐单个会话的内存缓存条目。
func (g *BranchWorkspaceGate) InvalidateSession(id uint64) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.sessionCache, id)
}

// InvalidateBranchSessions 驱逐指定磁盘分支下所有会话的内存缓存条目。
func (g *BranchWorkspaceGate) InvalidateBranchSessions(disk, branch string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, entry := range g.sessionCache {
		if entry.session.diskID.String() == disk && entry.session.branch == branch {
			delete(g.sessionCache, id)
		}
	}
}

// InvalidateDiskSessions 驱逐指定磁盘下所有会话的内存缓存条目。
func (g *BranchWorkspaceGate) InvalidateDiskSessions(disk string) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, entry := range g.sessionCache {
		if entry.session.diskID.String() == disk {
			delete(g.sessionCache, id)
		}
	}
}

func samePebbleSession(a, b *pebbleReadSession) bool {
	return a.id == b.id && a.diskID == b.diskID && a.branch == b.branch && a.readOnly == b.readOnly &&
		a.commitID == b.commitID && a.createdAt.Equal(b.createdAt)
}

func (g *BranchWorkspaceGate) requireSession(ctx context.Context, sid *uint64) (pebbleReadSession, error) {
	session, err := g.loadSession(ctx, sid)
	if err != nil {
		return pebbleReadSession{}, err
	}
	if !session.managed || !session.active {
		return pebbleReadSession{}, ErrBadFileHandle
	}
	return session, nil
}

// withOpenSession 统一收拢 4 处会话开库控制流：
//  1. 只读会话直接打开不可变 Commit 句柄并自动关闭；
//  2. 可写会话获取目标分支共享读锁，在锁内二次校验 samePebbleSession 防止等待锁期间会话失效或缓存漂移，
//     随后打开常驻 Branch 句柄并在回调结束后自动关闭。
func (g *BranchWorkspaceGate) withOpenSession(session *pebbleReadSession, fn func(disk string, store *pebble.DB, blobs *pebblekv.BlobStore) error) (err error) {
	disk := session.diskID.String()
	if session.readOnly {
		var commit *pebblekv.Commit
		commit, err = g.pebble.OpenCommit(disk, session.commitID.String())
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, commit.Close()) }()
		return fn(disk, commit.DB(), commit.Blobs())
	}
	g.invokeBeforeBranchLock(session.id)
	unlock := g.locks.LockBranchRead(disk, session.branch)
	defer unlock()
	// 锁内复查会话缓存：CloseSession、过期清扫或会话重建替换后立即拒绝请求。
	if current, ok := g.peekSession(session.id); !ok || !samePebbleSession(&current, session) {
		return ErrBadFileHandle
	}
	branch, err := g.pebble.OpenBranch(disk, session.branch)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, branch.Close()) }()
	return fn(disk, branch.DB(), branch.Blobs())
}

// withSnapshotReadSession 在分支读锁内用短命快照执行多键读取，保证 Inode 与 Blob 视图一致。
func (g *BranchWorkspaceGate) withSnapshotReadSession(_ context.Context, session *pebbleReadSession, fn func(pebbleReader, *pebblekv.BlobStore) error) error {
	return g.withOpenSession(session, func(_ string, store *pebble.DB, blobs *pebblekv.BlobStore) (err error) {
		if session.readOnly {
			return fn(store, blobs)
		}
		snapshot := store.NewSnapshot()
		defer func() { err = errors.Join(err, snapshot.Close()) }()
		return fn(snapshot, blobs)
	})
}

// withPointReadSession 直接读取分支实例，单键点查不创建 Pebble Snapshot。
func (g *BranchWorkspaceGate) withPointReadSession(_ context.Context, session *pebbleReadSession, fn func(pebbleReader, *pebblekv.BlobStore) error) error {
	return g.withOpenSession(session, func(_ string, store *pebble.DB, blobs *pebblekv.BlobStore) error {
		return fn(store, blobs)
	})
}

// withWriteSession 在分支共享读锁内执行显式持久化屏障（如 Fsync / FsyncDir）。
func (g *BranchWorkspaceGate) withWriteSession(_ context.Context, session *pebbleReadSession, fn func(*pebble.DB, *pebblekv.BlobStore) error) error {
	return g.withOpenSession(session, func(_ string, store *pebble.DB, blobs *pebblekv.BlobStore) error {
		return fn(store, blobs)
	})
}

// withWritableBranchFor 解析并校验可写会话、加载磁盘配额，在分支锁内二次核验会话并执行可写分支变更后标记分支为脏。
func (g *BranchWorkspaceGate) withWritableBranchFor(
	ctx context.Context,
	sid *uint64,
	check func(pgtype.UUID, string) error,
	resolveQuota func(context.Context, pgtype.UUID) (*TreeQuota, error),
	fn func(string, string, *TreeQuota, *pebble.DB, *pebblekv.BlobStore) error,
) error {
	if sid == nil || *sid == 0 || *sid > math.MaxInt64 {
		return ErrInvalidArgument
	}
	session, err := g.requireSession(ctx, sid)
	if err != nil {
		return err
	}
	if session.readOnly {
		return ErrPerm
	}
	if check != nil {
		err = check(session.diskID, session.branch)
		if err != nil {
			return err
		}
	}
	var quota *TreeQuota
	if resolveQuota != nil {
		quota, err = resolveQuota(ctx, session.diskID)
		if err != nil {
			return err
		}
	}
	return g.withOpenSession(&session, func(disk string, store *pebble.DB, blobs *pebblekv.BlobStore) error {
		if err := fn(disk, session.branch, quota, store, blobs); err != nil {
			return err
		}
		g.pebble.MarkBranchDirty(disk, session.branch)
		return nil
	})
}

// RecordAtime 在可写会话读热路径登记内存脏访问时间。
func (g *BranchWorkspaceGate) RecordAtime(session *pebbleReadSession, inode *pebblekv.Inode, now time.Time) {
	if g == nil || session == nil || session.readOnly || g.atimes == nil {
		return
	}
	g.atimes.MarkDirty(session.diskID.String(), session.branch, inode, now)
}

// OverlayAtime 将内存脏访问时间叠加到可写会话读取的节点属性上。
func (g *BranchWorkspaceGate) OverlayAtime(session *pebbleReadSession, inode *pebblekv.Inode) {
	if g == nil || session == nil || session.readOnly || g.atimes == nil {
		return
	}
	g.atimes.Overlay(session.diskID.String(), session.branch, inode)
}

// FlushInodeAtimeLocked 在持有目标 Inode 写锁时合并单节点内存脏访问时间。
func (g *BranchWorkspaceGate) FlushInodeAtimeLocked(disk, branch string, store *pebble.DB, ino uint64) error {
	if g == nil || g.atimes == nil {
		return nil
	}
	return g.atimes.FlushInodeLocked(disk, branch, store, ino)
}

// FlushAndCheckBranchDirtyLocked 在持有分支独占锁下先刷盘内存脏 atime 再检测分支是否存在 dirty:* 记录。
func (g *BranchWorkspaceGate) FlushAndCheckBranchDirtyLocked(disk, branch string, store *pebble.DB) (bool, error) {
	if g == nil || g.atimes == nil {
		return branchHasDirty(store)
	}
	return g.atimes.FlushAndCheckDirtyLocked(disk, branch, store)
}

// CheckpointBranchLocked 在持有分支独占锁下先刷盘内存脏 atime 再生成 Pebble Checkpoint。
func (g *BranchWorkspaceGate) CheckpointBranchLocked(disk, branchName string, branch *pebblekv.Branch, commitID string) error {
	if g == nil || g.atimes == nil {
		return branch.Checkpoint(commitID)
	}
	return g.atimes.CheckpointBranchLocked(disk, branchName, branch, commitID)
}
