package fusefs

import (
	"sync"
	"syscall"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// FileHandle 记录一个本地句柄的目标节点、打开标志和类型。
type FileHandle struct {
	Ino   uint64
	Flags uint32
	IsDir bool
}

// CanRead 判断句柄是否可用于读取常规文件。
func (h FileHandle) CanRead() bool {
	return !h.IsDir && h.Flags&uint32(syscall.O_ACCMODE) != syscall.O_WRONLY
}

// CanWrite 判断句柄是否可用于写入常规文件。
func (h FileHandle) CanWrite() bool {
	return !h.IsDir && h.Flags&uint32(syscall.O_ACCMODE) != syscall.O_RDONLY
}

// HandleTable 管理当前客户端的打开句柄与节点引用数。
type HandleTable struct {
	mu          sync.RWMutex
	next        uint64
	handles     map[uint64]FileHandle
	counts      map[uint64]int
	orphans     map[uint64]struct{}
	removedDirs map[uint64]*storagev1.Inode
	onOrphan    func(uint64)
}

// NewHandleTable 创建本地句柄表。
func NewHandleTable(onOrphan func(uint64)) *HandleTable {
	return &HandleTable{
		handles:     make(map[uint64]FileHandle),
		counts:      make(map[uint64]int),
		orphans:     make(map[uint64]struct{}),
		removedDirs: make(map[uint64]*storagev1.Inode),
		onOrphan:    onOrphan,
	}
}

// Open 分配与其他活动句柄不同的本地标识。
func (t *HandleTable) Open(ino uint64, flags uint32, isDir bool) (uint64, error) {
	mode := flags & uint32(syscall.O_ACCMODE)
	if ino == 0 || mode == syscall.O_ACCMODE {
		return 0, syscall.EINVAL
	}
	if isDir && (mode != syscall.O_RDONLY || flags&uint32(syscall.O_TRUNC) != 0) {
		return 0, syscall.EISDIR
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for {
		t.next++
		if t.next == 0 {
			continue
		}
		if _, exists := t.handles[t.next]; !exists {
			break
		}
	}
	t.handles[t.next] = FileHandle{Ino: ino, Flags: flags, IsDir: isDir}
	t.counts[ino]++
	return t.next, nil
}

// Get 读取句柄信息，不返回表内可变对象。
func (t *HandleTable) Get(fh uint64) (FileHandle, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	handle, ok := t.handles[fh]
	return handle, ok
}

// Release 释放一个句柄；重复释放不会减少引用数。
func (t *HandleTable) Release(fh uint64) (FileHandle, bool) {
	t.mu.Lock()
	handle, ok := t.handles[fh]
	if !ok {
		t.mu.Unlock()
		return FileHandle{}, false
	}
	delete(t.handles, fh)
	t.counts[handle.Ino]--
	reclaim := false
	if t.counts[handle.Ino] == 0 {
		delete(t.counts, handle.Ino)
		delete(t.removedDirs, handle.Ino)
		_, reclaim = t.orphans[handle.Ino]
		delete(t.orphans, handle.Ino)
	}
	t.mu.Unlock()
	if reclaim && t.onOrphan != nil {
		t.onOrphan(handle.Ino)
	}
	return handle, true
}

// OpenCount 返回指定节点的活动句柄数。
func (t *HandleTable) OpenCount(ino uint64) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.counts[ino]
}

// IsOpen 判断指定节点是否有活动句柄。
func (t *HandleTable) IsOpen(ino uint64) bool {
	return t.OpenCount(ino) != 0
}

// MarkOrphan 记录已解链的服务端节点；若句柄已全部关闭则立即触发回收。
// 调用方须在服务端确认节点没有目录项后调用，并协调新句柄打开与回收。
func (t *HandleTable) MarkOrphan(ino uint64) {
	t.mu.Lock()
	if t.counts[ino] != 0 {
		t.orphans[ino] = struct{}{}
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()
	if t.onOrphan != nil {
		t.onOrphan(ino)
	}
}

// AnyOpen 判断是否存在本地活动句柄。
func (t *HandleTable) AnyOpen() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.handles) != 0
}

func (t *HandleTable) clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	clear(t.handles)
	clear(t.counts)
	clear(t.orphans)
	clear(t.removedDirs)
}

// ClearOrphan 在孤儿重新具名化后取消回收标记。
func (t *HandleTable) ClearOrphan(ino uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.orphans, ino)
}

// IsOrphan 判断节点是否等待最后一个句柄关闭。
func (t *HandleTable) IsOrphan(ino uint64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.orphans[ino]
	return ok
}

// MarkRemovedDir 保留已删除但仍打开的目录属性，直到最后一个句柄关闭。
func (t *HandleTable) MarkRemovedDir(ino uint64, inode *storagev1.Inode) {
	if inode == nil || inode.Attr == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.counts[ino] == 0 {
		return
	}
	removed := proto.Clone(inode).(*storagev1.Inode)
	removed.Attr.Nlink = 0
	removed.Attr.Ctime = timestamppb.Now()
	t.removedDirs[ino] = removed
}

// RemovedDir 返回已删除的打开目录的本地属性。
func (t *HandleTable) RemovedDir(ino uint64) (*storagev1.Inode, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	inode, ok := t.removedDirs[ino]
	return inode, ok
}
