package pebblekv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

var (
	// ErrBusy 表示仍有句柄占用实例。
	ErrBusy = errors.New("pebblekv: 实例正在使用")
	// ErrClosed 表示管理器或句柄已经关闭。
	ErrClosed = errors.New("pebblekv: 已关闭")
	// ErrInvalidID 表示标识符不能作为单个目录名。
	ErrInvalidID = errors.New("pebblekv: 无效标识符")
	// ErrInodeExhausted 表示该 Disk 的 Inode 编号已用尽。
	ErrInodeExhausted = errors.New("pebblekv: Inode 编号已用尽")
)

type instanceID struct{ disk, id string }
type commitInstance struct {
	db   *pebble.DB
	refs int
}

// Manager 管理本进程内的分支和快照实例。同一存储根目录必须共用一个 Manager。
// 调用方应在结束使用后关闭所有句柄。
type Manager struct {
	root       string
	mu         sync.Mutex
	closed     bool
	branches   map[instanceID]*Branch
	commits    map[instanceID]*commitInstance
	allocators map[string]*pebble.DB
}

// NewManager 创建存储实例管理器。
func NewManager(root string) *Manager {
	return &Manager{root: root, branches: make(map[instanceID]*Branch), commits: make(map[instanceID]*commitInstance), allocators: make(map[string]*pebble.DB)}
}

func branchOptions(readOnly bool) *pebble.Options {
	opts := &pebble.Options{Merger: Merger, ReadOnly: readOnly, ErrorIfNotExists: readOnly, FormatMajorVersion: pebble.FormatValueSeparation}
	opts.Experimental.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
		return pebble.ValueSeparationPolicy{Enabled: true, MinimumSize: BlobSize, MaxBlobReferenceDepth: 5, TargetGarbageRatio: 0.2}
	}
	return opts
}

func validID(id string) bool {
	if id == "" || id == "." || id == ".." || strings.ContainsRune(id, '\x00') {
		return false
	}
	for i := range len(id) {
		if os.IsPathSeparator(id[i]) {
			return false
		}
	}
	return true
}
func (m *Manager) path(disk, kind, id string) (string, error) {
	if !validID(disk) || !validID(id) {
		return "", ErrInvalidID
	}
	return filepath.Join(m.root, "disks", disk, kind, id), nil
}

// Branch 是可写分支的独占实例句柄。
type Branch struct {
	manager *Manager
	key     instanceID
	db      *pebble.DB
	closed  bool
}

// DB 返回分支的 Pebble 实例。调用方不得直接关闭实例，也不得在句柄关闭后使用它。
func (b *Branch) DB() *pebble.DB { return b.db }

// OpenBranch 打开或创建分支；同一分支只能拥有一个可写句柄。
func (m *Manager) OpenBranch(disk, branch string) (*Branch, error) {
	path, err := m.path(disk, "branches", branch)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	key := instanceID{disk, branch}
	if _, exists := m.branches[key]; exists {
		return nil, ErrBusy
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := pebble.Open(path, branchOptions(false))
	if err != nil {
		return nil, err
	}
	b := &Branch{manager: m, key: key, db: db}
	m.branches[key] = b
	return b, nil
}

// Checkpoint 刷新分支并以硬链接生成不可覆盖的 Commit 快照。
func (b *Branch) Checkpoint(commit string) error {
	m := b.manager
	path, err := m.path(b.key.disk, "commits", commit)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || b.closed {
		return ErrClosed
	}
	if err := b.db.Flush(); err != nil {
		return err
	}
	return b.db.Checkpoint(path, pebble.WithFlushedWAL())
}

// Close 释放分支实例。
func (b *Branch) Close() error {
	m := b.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	delete(m.branches, b.key)
	return b.db.Close()
}

// Commit 是共享只读快照的引用。
type Commit struct {
	manager  *Manager
	key      instanceID
	instance *commitInstance
	closed   bool
}

// DB 返回只读 Pebble 实例。调用方不得直接关闭实例，也不得在句柄关闭后使用它。
func (c *Commit) DB() *pebble.DB { return c.instance.db }

// OpenCommit 共享同一 Disk 和 Commit 的只读实例。
func (m *Manager) OpenCommit(disk, commit string) (*Commit, error) {
	path, err := m.path(disk, "commits", commit)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	key := instanceID{disk, commit}
	inst := m.commits[key]
	if inst == nil {
		db, err := pebble.Open(path, branchOptions(true))
		if err != nil {
			return nil, err
		}
		inst = &commitInstance{db: db}
		m.commits[key] = inst
	}
	inst.refs++
	return &Commit{manager: m, key: key, instance: inst}, nil
}

// Close 释放快照引用；最后一个引用关闭时才关闭实例。
func (c *Commit) Close() error {
	m := c.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.instance.refs--
	if c.instance.refs != 0 {
		return nil
	}
	delete(m.commits, c.key)
	return c.instance.db.Close()
}

// Fork 从不可变快照生成新的分支工作区。
func (m *Manager) Fork(disk, commit, branch string) (err error) {
	path, err := m.path(disk, "branches", branch)
	if err != nil {
		return err
	}
	snapshot, err := m.OpenCommit(disk, commit)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, snapshot.Close()) }()
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.branches[instanceID{disk, branch}]; exists {
		return ErrBusy
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	staging, err := os.MkdirTemp(filepath.Dir(path), ".fork-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(staging)) }()
	source, _ := m.path(disk, "commits", commit)
	if err := stageCheckpoint(source, staging); err != nil {
		return err
	}
	opts := branchOptions(false)
	opts.ErrorIfNotExists = true
	db, err := pebble.Open(staging, opts)
	if err != nil {
		return err
	}
	if err := db.Flush(); err != nil {
		return errors.Join(err, db.Close())
	}
	return errors.Join(db.Checkpoint(path, pebble.WithFlushedWAL()), db.Close())
}

// 只读 Pebble 实例无法调用 Checkpoint；先复制控制文件并硬链接不可变表。
func stageCheckpoint(source, destination string) error {
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == "LOCK" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("快照包含非普通文件 %s", entry.Name())
		}
		from, to := filepath.Join(source, entry.Name()), filepath.Join(destination, entry.Name())
		if strings.HasSuffix(entry.Name(), ".sst") || strings.HasSuffix(entry.Name(), ".blob") {
			if err := os.Link(from, to); err != nil {
				return err
			}
			continue
		}
		in, err := os.Open(from)
		if err != nil {
			return err
		}
		out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, info.Mode().Perm())
		if err != nil {
			_ = in.Close()
			return err
		}
		_, copyErr := io.Copy(out, in)
		if err := errors.Join(copyErr, in.Close(), out.Close()); err != nil {
			return err
		}
	}
	return nil
}

// AllocateInode 在 Disk 级数据库同步记录编号，不随分支和快照回退。
func (m *Manager) AllocateInode(disk string) (uint64, error) {
	path, err := m.path(disk, "", "inode-allocator")
	if err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, ErrClosed
	}
	db := m.allocators[disk]
	if db == nil {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return 0, err
		}
		db, err = pebble.Open(path, &pebble.Options{})
		if err != nil {
			return 0, err
		}
		m.allocators[disk] = db
	}
	const key = "last-inode"
	last := uint64(1)
	value, closer, err := db.Get([]byte(key))
	if err == nil {
		if len(value) != 8 {
			_ = closer.Close()
			return 0, ErrInvalidValue
		}
		last = binary.BigEndian.Uint64(value)
		if err := closer.Close(); err != nil {
			return 0, err
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return 0, err
	}
	if last == math.MaxUint64 {
		return 0, ErrInodeExhausted
	}
	next := last + 1
	if err := db.Set([]byte(key), binary.BigEndian.AppendUint64(nil, next), pebble.Sync); err != nil {
		return 0, err
	}
	return next, nil
}

// InitInodeAllocator 持久化根节点编号，已有分配器不会被重置。
func (m *Manager) InitInodeAllocator(disk string) error {
	path, err := m.path(disk, "", "inode-allocator")
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if m.allocators[disk] != nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return err
	}
	value, closer, err := db.Get([]byte("last-inode"))
	if errors.Is(err, pebble.ErrNotFound) {
		err = db.Set([]byte("last-inode"), binary.BigEndian.AppendUint64(nil, 1), pebble.Sync)
	} else if err == nil {
		if len(value) != 8 || binary.BigEndian.Uint64(value) < 1 {
			err = ErrInvalidValue
		}
		err = errors.Join(err, closer.Close())
	}
	return errors.Join(err, db.Close())
}

// RemoveDisk 关闭编号分配器并删除没有打开句柄的磁盘目录。
func (m *Manager) RemoveDisk(disk string) error {
	path, err := m.path(disk, "", "inode-allocator")
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	for key := range m.branches {
		if key.disk == disk {
			return ErrBusy
		}
	}
	for key := range m.commits {
		if key.disk == disk {
			return ErrBusy
		}
	}
	if db := m.allocators[disk]; db != nil {
		if err := db.Close(); err != nil {
			return err
		}
		delete(m.allocators, disk)
	}
	return os.RemoveAll(filepath.Dir(path))
}

// Close 在所有分支及快照句柄关闭后释放管理器。
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	if len(m.branches) != 0 || len(m.commits) != 0 {
		return ErrBusy
	}
	m.closed = true
	var errs []error
	for disk, db := range m.allocators {
		if err := db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("关闭磁盘 %s 的编号分配器: %w", disk, err))
		}
	}
	return errors.Join(errs...)
}
