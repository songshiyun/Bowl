package fusefs

import (
	"container/list"
	"sync"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"google.golang.org/protobuf/proto"
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
type NamespaceCache struct {
	mu       sync.Mutex
	capacity int
	used     int
	epoch    uint64
	items    map[cacheKey]*list.Element
	lru      list.List
}

// NewNamespaceCache 创建容量有界的缓存。
func NewNamespaceCache(capacity int) *NamespaceCache {
	if capacity <= 0 {
		capacity = DefaultNamespaceCacheCapacity
	}
	return &NamespaceCache{capacity: capacity, items: make(map[cacheKey]*list.Element)}
}

func (c *NamespaceCache) remove(el *list.Element) {
	item := el.Value.(*cacheItem)
	delete(c.items, item.key)
	c.used -= item.weight
	c.lru.Remove(el)
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
	c.items[key] = c.lru.PushFront(&cacheItem{key: key, value: value, weight: weight})
	c.used += weight
}

func (c *NamespaceCache) get(key cacheKey) (any, bool) {
	el := c.items[key]
	if el == nil {
		return nil, false
	}
	c.lru.MoveToFront(el)
	return el.Value.(*cacheItem).value, true
}

func cloneInode(ino *storagev1.Inode) *storagev1.Inode {
	if ino == nil {
		return nil
	}
	return proto.Clone(ino).(*storagev1.Inode)
}

func cloneDir(entries []*storagev1.DirEntryPlus) []*storagev1.DirEntryPlus {
	out := make([]*storagev1.DirEntryPlus, len(entries))
	for i, entry := range entries {
		out[i] = proto.Clone(entry).(*storagev1.DirEntryPlus)
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
	// 完整目录流本身包含属性，即使独立属性项被淘汰也可直接命中。
	for el := c.lru.Front(); el != nil; el = el.Next() {
		item := el.Value.(*cacheItem)
		if item.key.kind != 'd' {
			continue
		}
		for _, entry := range item.value.([]*storagev1.DirEntryPlus) {
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
	for el := c.lru.Front(); el != nil; el = el.Next() {
		item := el.Value.(*cacheItem)
		if item.key.kind == 'd' {
			for _, entry := range item.value.([]*storagev1.DirEntryPlus) {
				if entry.GetIno() == ino {
					entry.Inode = cloneInode(inode)
					entry.AttrTimeoutSeconds = attrTimeout
				}
			}
		}
	}
	c.put(cacheKey{kind: 'i', ino: ino}, cachedInode{cloneInode(inode), entryTimeout, attrTimeout}, 1)
}

func (c *NamespaceCache) invalidateInode(ino uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := c.lru.Front(); el != nil; {
		next := el.Next()
		item := el.Value.(*cacheItem)
		remove := item.key.kind == 'i' && item.key.ino == ino
		if item.key.kind == 'd' {
			for _, entry := range item.value.([]*storagev1.DirEntryPlus) {
				if entry.GetIno() == ino {
					remove = true
					break
				}
			}
		}
		if remove {
			c.remove(el)
		}
		el = next
	}
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
	if el := c.items[cacheKey{kind: 'd', ino: parent}]; el != nil {
		c.remove(el)
	}
	c.put(cacheKey{kind: 'e', ino: parent, name: name}, ino, 1)
}

func (c *NamespaceCache) invalidateDir(parent uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := c.lru.Front(); el != nil; {
		next := el.Next()
		item := el.Value.(*cacheItem)
		remove := item.key.ino == parent && (item.key.kind == 'd' || item.key.kind == 'e' || item.key.kind == 'i')
		if item.key.kind == 'd' {
			for _, entry := range item.value.([]*storagev1.DirEntryPlus) {
				if entry.GetIno() == parent {
					remove = true
					break
				}
			}
		}
		if remove {
			c.remove(el)
		}
		el = next
	}
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
	// 旧的独立目录项可能与新目录流冲突。
	for el := c.lru.Front(); el != nil; {
		next := el.Next()
		item := el.Value.(*cacheItem)
		if item.key.kind == 'e' && item.key.ino == ino {
			c.remove(el)
		}
		el = next
	}
	c.put(cacheKey{kind: 'd', ino: ino}, cloneDir(entries), len(entries)+1)
}

// clear 在会话失效后丢弃旧会话期间获得的全部缓存。
func (c *NamespaceCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.epoch++
	c.items = make(map[cacheKey]*list.Element)
	c.lru.Init()
	c.used = 0
}

func (c *NamespaceCache) version() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}
