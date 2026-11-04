package fusefs

import (
	"container/list"
	"sync"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DefaultNamespaceCacheCapacity 限制缓存的节点、目录项和目录流所占条目数。
const DefaultNamespaceCacheCapacity = 16384

type cacheKey struct {
	kind byte
	ino  uint64
	name string
}

type cachedInode struct {
	inode        *storagev1.Inode
	entryTimeout float64
	attrTimeout  float64
}

type cacheItem struct {
	key    cacheKey
	value  any
	weight int
}

// NamespaceCache 按最近使用顺序淘汰属性、目录项和完整目录流。
// 目录流按包含的目录项数计费，避免单个目录绕过容量限制。
//
// 除 LRU 链表外维护三组二级索引，把失效与回填从 O(N) 全链表扫描降为 O(1)/O(K) 定向查找：
//   - byParent：父目录 Inode -> 其独立目录项名称集合；
//   - dirsByChild：子 Inode -> 引用它的目录流父 Inode 集合；
//   - inodeDirs：目录流父 Inode -> 它引用的子 Inode 集合（用于定向清理前两者）。
type NamespaceCache struct {
	mu          sync.Mutex
	capacity    int
	used        int
	epoch       uint64
	mutations   uint64
	items       map[cacheKey]*list.Element
	lru         list.List
	byParent    map[uint64]map[string]struct{}
	dirsByChild map[uint64]map[uint64]struct{}
	inodeDirs   map[uint64]map[uint64]struct{}
}

// NewNamespaceCache 创建容量有界的缓存。
func NewNamespaceCache(capacity int) *NamespaceCache {
	if capacity <= 0 {
		capacity = DefaultNamespaceCacheCapacity
	}
	return &NamespaceCache{
		capacity:    capacity,
		items:       make(map[cacheKey]*list.Element),
		byParent:    make(map[uint64]map[string]struct{}),
		dirsByChild: make(map[uint64]map[uint64]struct{}),
		inodeDirs:   make(map[uint64]map[uint64]struct{}),
	}
}

func (c *NamespaceCache) remove(el *list.Element) {
	if el == nil {
		return
	}
	item := el.Value.(*cacheItem)
	delete(c.items, item.key)
	c.used -= item.weight
	c.lru.Remove(el)
	c.unindex(item)
}

func (c *NamespaceCache) removeKey(key cacheKey) {
	if el := c.items[key]; el != nil {
		c.remove(el)
	}
}

// index 登记缓存项对应的二级索引。
func (c *NamespaceCache) index(item *cacheItem) {
	switch item.key.kind {
	case 'e':
		names := c.byParent[item.key.ino]
		if names == nil {
			names = make(map[string]struct{})
			c.byParent[item.key.ino] = names
		}
		names[item.key.name] = struct{}{}
	case 'd':
		dir := item.key.ino
		children := make(map[uint64]struct{})
		for _, entry := range item.value.([]*storagev1.DirEntryPlus) {
			child := entry.GetIno()
			if child == 0 {
				continue
			}
			children[child] = struct{}{}
			dirs := c.dirsByChild[child]
			if dirs == nil {
				dirs = make(map[uint64]struct{})
				c.dirsByChild[child] = dirs
			}
			dirs[dir] = struct{}{}
		}
		c.inodeDirs[dir] = children
	}
}

// unindex 在缓存项移除前撤销其二级索引。
func (c *NamespaceCache) unindex(item *cacheItem) {
	switch item.key.kind {
	case 'e':
		names := c.byParent[item.key.ino]
		if names == nil {
			return
		}
		delete(names, item.key.name)
		if len(names) == 0 {
			delete(c.byParent, item.key.ino)
		}
	case 'd':
		dir := item.key.ino
		for child := range c.inodeDirs[dir] {
			dirs := c.dirsByChild[child]
			if dirs == nil {
				continue
			}
			delete(dirs, dir)
			if len(dirs) == 0 {
				delete(c.dirsByChild, child)
			}
		}
		delete(c.inodeDirs, dir)
	}
}

func (c *NamespaceCache) put(key cacheKey, value any, weight int) {
	if old := c.items[key]; old != nil {
		c.remove(old)
	}
	if weight > c.capacity {
		return
	}
	for c.used+weight > c.capacity {
		c.remove(c.lru.Back())
	}
	item := &cacheItem{key: key, value: value, weight: weight}
	c.items[key] = c.lru.PushFront(item)
	c.used += weight
	c.index(item)
}

func (c *NamespaceCache) get(key cacheKey) (any, bool) {
	el := c.items[key]
	if el == nil {
		return nil, false
	}
	c.lru.MoveToFront(el)
	return el.Value.(*cacheItem).value, true
}

func cloneTimestamp(ts *timestamppb.Timestamp) *timestamppb.Timestamp {
	if ts == nil {
		return nil
	}
	return &timestamppb.Timestamp{Seconds: ts.GetSeconds(), Nanos: ts.GetNanos()}
}

// cloneInode 手写复制 Inode，避免 proto.Clone 的反射深拷贝开销。
func cloneInode(inode *storagev1.Inode) *storagev1.Inode {
	if inode == nil {
		return nil
	}
	out := &storagev1.Inode{Ino: inode.GetIno(), Generation: inode.GetGeneration()}
	if attr := inode.GetAttr(); attr != nil {
		out.Attr = &storagev1.InodeAttribute{
			Mode:    attr.GetMode(),
			Size:    attr.GetSize(),
			Nlink:   attr.GetNlink(),
			Uid:     attr.GetUid(),
			Gid:     attr.GetGid(),
			Rdev:    attr.GetRdev(),
			Blksize: attr.GetBlksize(),
			Blocks:  attr.GetBlocks(),
			Atime:   cloneTimestamp(attr.GetAtime()),
			Mtime:   cloneTimestamp(attr.GetMtime()),
			Ctime:   cloneTimestamp(attr.GetCtime()),
			Flags:   attr.GetFlags(),
		}
	}
	return out
}

// cloneDirEntry 手写复制目录项，名称字节与内嵌 Inode 均独立持有。
func cloneDirEntry(entry *storagev1.DirEntryPlus) *storagev1.DirEntryPlus {
	if entry == nil {
		return nil
	}
	out := &storagev1.DirEntryPlus{
		Ino:                 entry.GetIno(),
		NextOffset:          entry.GetNextOffset(),
		Inode:               cloneInode(entry.GetInode()),
		EntryTimeoutSeconds: entry.GetEntryTimeoutSeconds(),
		AttrTimeoutSeconds:  entry.GetAttrTimeoutSeconds(),
	}
	if name := entry.GetName(); len(name) > 0 {
		out.Name = append([]byte(nil), name...)
	}
	return out
}

func cloneDir(entries []*storagev1.DirEntryPlus) []*storagev1.DirEntryPlus {
	out := make([]*storagev1.DirEntryPlus, len(entries))
	for i, entry := range entries {
		out[i] = cloneDirEntry(entry)
	}
	return out
}

func (c *NamespaceCache) inode(ino uint64) (cachedInode, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if value, ok := c.get(cacheKey{kind: 'i', ino: ino}); ok {
		v := value.(cachedInode)
		v.inode = cloneInode(v.inode)
		return v, true
	}
	// 完整目录流本身包含属性，即使独立属性项被淘汰也可定向命中。
	for dir := range c.dirsByChild[ino] {
		el := c.items[cacheKey{kind: 'd', ino: dir}]
		if el == nil {
			continue
		}
		for _, entry := range el.Value.(*cacheItem).value.([]*storagev1.DirEntryPlus) {
			if entry.GetIno() == ino && entry.GetInode() != nil && entry.GetInode().GetAttr() != nil {
				c.lru.MoveToFront(el)
				return cachedInode{cloneInode(entry.GetInode()), entry.GetEntryTimeoutSeconds(), entry.GetAttrTimeoutSeconds()}, true
			}
		}
	}
	return cachedInode{}, false
}

func (c *NamespaceCache) putInode(ino uint64, inode *storagev1.Inode, entryTimeout, attrTimeout float64) {
	c.putInodeAt(^uint64(0), ino, inode, entryTimeout, attrTimeout)
}

func (c *NamespaceCache) putInodeAt(epoch, ino uint64, inode *storagev1.Inode, entryTimeout, attrTimeout float64) {
	if inode == nil || inode.GetAttr() == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != ^uint64(0) && epoch != c.epoch {
		return
	}
	c.putInodeLocked(ino, inode, entryTimeout, attrTimeout)
}

// putInodeAtIf 仅在缓存未被任何目录变更改写时回填，避免在途读响应覆盖更新的属性。
func (c *NamespaceCache) putInodeAtIf(generation, epoch, ino uint64, inode *storagev1.Inode, entryTimeout, attrTimeout float64) {
	if inode == nil || inode.GetAttr() == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.mutations || (epoch != ^uint64(0) && epoch != c.epoch) {
		return
	}
	c.putInodeLocked(ino, inode, entryTimeout, attrTimeout)
}

func (c *NamespaceCache) putInodeLocked(ino uint64, inode *storagev1.Inode, entryTimeout, attrTimeout float64) {
	for dir := range c.dirsByChild[ino] {
		el := c.items[cacheKey{kind: 'd', ino: dir}]
		if el == nil {
			continue
		}
		for _, entry := range el.Value.(*cacheItem).value.([]*storagev1.DirEntryPlus) {
			if entry.GetIno() == ino {
				entry.Inode = cloneInode(inode)
				entry.AttrTimeoutSeconds = attrTimeout
			}
		}
	}
	c.put(cacheKey{kind: 'i', ino: ino}, cachedInode{cloneInode(inode), entryTimeout, attrTimeout}, 1)
}

func (c *NamespaceCache) invalidateInode(ino uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeKey(cacheKey{kind: 'i', ino: ino})
	for dir := range copyInoSet(c.dirsByChild[ino]) {
		c.removeKey(cacheKey{kind: 'd', ino: dir})
	}
	c.mutations++
}

// entry 返回子节点号；0 表示已缓存的 ENOENT。
func (c *NamespaceCache) entry(parent uint64, name string) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if value, ok := c.get(cacheKey{kind: 'e', ino: parent, name: name}); ok {
		return value.(uint64), true
	}
	if value, ok := c.get(cacheKey{kind: 'd', ino: parent}); ok {
		for _, entry := range value.([]*storagev1.DirEntryPlus) {
			if string(entry.GetName()) == name {
				return entry.GetIno(), true
			}
		}
		return 0, true
	}
	return 0, false
}

func (c *NamespaceCache) putEntry(parent uint64, name string, ino uint64) {
	c.putEntryAt(^uint64(0), parent, name, ino)
}

func (c *NamespaceCache) putEntryAt(epoch, parent uint64, name string, ino uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != ^uint64(0) && epoch != c.epoch {
		return
	}
	c.putEntryLocked(parent, name, ino)
	c.mutations++
}

// putEntryAtIf 仅在缓存未被任何目录变更改写时回填，避免在途 Lookup 的过期结果覆盖新目录项。
func (c *NamespaceCache) putEntryAtIf(generation, epoch, parent uint64, name string, ino uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.mutations || (epoch != ^uint64(0) && epoch != c.epoch) {
		return
	}
	c.putEntryLocked(parent, name, ino)
}

func (c *NamespaceCache) putEntryLocked(parent uint64, name string, ino uint64) {
	// 独立目录项与旧目录流冲突，先按父目录定向移除旧目录流。
	c.removeKey(cacheKey{kind: 'd', ino: parent})
	c.put(cacheKey{kind: 'e', ino: parent, name: name}, ino, 1)
}

func (c *NamespaceCache) invalidateDir(parent uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeKey(cacheKey{kind: 'd', ino: parent})
	c.removeKey(cacheKey{kind: 'i', ino: parent})
	for _, name := range copyNames(c.byParent[parent]) {
		c.removeKey(cacheKey{kind: 'e', ino: parent, name: name})
	}
	// 父节点属性变化会使其出现在其它目录流的目录项中，一并定向失效。
	for dir := range copyInoSet(c.dirsByChild[parent]) {
		c.removeKey(cacheKey{kind: 'd', ino: dir})
	}
	c.mutations++
}

func (c *NamespaceCache) dir(ino uint64) ([]*storagev1.DirEntryPlus, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.get(cacheKey{kind: 'd', ino: ino})
	if !ok {
		return nil, false
	}
	return cloneDir(value.([]*storagev1.DirEntryPlus)), true
}

func (c *NamespaceCache) putDir(ino uint64, entries []*storagev1.DirEntryPlus) {
	c.putDirAt(^uint64(0), ino, entries)
}

func (c *NamespaceCache) putDirAt(epoch, ino uint64, entries []*storagev1.DirEntryPlus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != ^uint64(0) && epoch != c.epoch {
		return
	}
	c.putDirLocked(ino, entries)
	c.mutations++
}

// putDirAtIf 仅在缓存未被任何目录变更改写时回填，避免在途目录流覆盖新目录项。
func (c *NamespaceCache) putDirAtIf(generation, epoch, ino uint64, entries []*storagev1.DirEntryPlus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.mutations || (epoch != ^uint64(0) && epoch != c.epoch) {
		return
	}
	c.putDirLocked(ino, entries)
}

func (c *NamespaceCache) putDirLocked(ino uint64, entries []*storagev1.DirEntryPlus) {
	// 旧的独立目录项可能与新目录流冲突，按父目录定向清理。
	for _, name := range copyNames(c.byParent[ino]) {
		c.removeKey(cacheKey{kind: 'e', ino: ino, name: name})
	}
	c.put(cacheKey{kind: 'd', ino: ino}, cloneDir(entries), len(entries)+1)
}

// clear 在会话失效后丢弃旧会话期间获得的全部缓存。
func (c *NamespaceCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.epoch++
	c.mutations++
	c.items = make(map[cacheKey]*list.Element)
	c.lru.Init()
	c.used = 0
	c.byParent = make(map[uint64]map[string]struct{})
	c.dirsByChild = make(map[uint64]map[uint64]struct{})
	c.inodeDirs = make(map[uint64]map[uint64]struct{})
}

func (c *NamespaceCache) version() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}

// snapshot 同时返回会话代数与目录变更代数，供在途 RPC 回填前校验。
func (c *NamespaceCache) snapshot() (epoch, generation uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch, c.mutations
}

func copyNames(names map[string]struct{}) []string {
	if len(names) == 0 {
		return nil
	}
	out := make([]string, 0, len(names))
	for name := range names {
		out = append(out, name)
	}
	return out
}

func copyInoSet(inos map[uint64]struct{}) map[uint64]struct{} {
	if len(inos) == 0 {
		return nil
	}
	out := make(map[uint64]struct{}, len(inos))
	for ino := range inos {
		out[ino] = struct{}{}
	}
	return out
}
