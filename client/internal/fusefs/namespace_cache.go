package fusefs

import (
	"container/list"
	"sync"

	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// DefaultNamespaceCacheCapacity 限制缓存的节点、目录项和目录流所占条目数。
// mdtest 1proc 会创建约 1.2 万个节点，16K 容量会在创建阶段持续 LRU 抖动，
// 64K 可容纳整棵工作集，同时常驻内存增量仍在数十 MB 量级。
const DefaultNamespaceCacheCapacity = 65536

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
	inode  cachedInode
	entry  uint64
	dir    []*storagev1.DirEntryPlus
	weight int
}

func itemOf(el *list.Element) *cacheItem {
	item, _ := el.Value.(*cacheItem)
	return item
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
	// completeDirs 记录“目录内容已被完整掌握”的父节点：目录流已加载，或目录刚由 Mkdir 创建且为空。
	// 命中时未找到的名字可以确定返回 ENOENT，无需再发 Lookup RPC。
	// 任一目录项被淘汰或目录发生未知变更时必须同步撤销该标记，避免把已存在名字误判为不存在。
	completeDirs map[uint64]struct{}
}

// NewNamespaceCache 创建容量有界的缓存。
func NewNamespaceCache(capacity int) *NamespaceCache {
	if capacity <= 0 {
		capacity = DefaultNamespaceCacheCapacity
	}
	return &NamespaceCache{
		capacity:     capacity,
		items:        make(map[cacheKey]*list.Element),
		byParent:     make(map[uint64]map[string]struct{}),
		dirsByChild:  make(map[uint64]map[uint64]struct{}),
		inodeDirs:    make(map[uint64]map[uint64]struct{}),
		completeDirs: make(map[uint64]struct{}),
	}
}

func (c *NamespaceCache) remove(el *list.Element) {
	if el == nil {
		return
	}
	item := itemOf(el)
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
		for _, entry := range item.dir {
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
		// 目录流一旦移除，就不再掌握该目录的完整内容：必须撤销权威完整标记，
		// 否则后续缺失名字会被误判为 ENOENT（目录流本身已不再持有目录项）。
		delete(c.completeDirs, dir)
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

func (c *NamespaceCache) putItem(item *cacheItem) {
	key := item.key
	weight := item.weight
	if old := c.items[key]; old != nil {
		c.remove(old)
	}
	if weight > c.capacity {
		// 无法容纳的目录项意味着该父目录的完整内容不再可信。
		if key.kind == 'e' {
			delete(c.completeDirs, key.ino)
		}
		return
	}
	for c.used+weight > c.capacity {
		back := c.lru.Back()
		if back == nil {
			break
		}
		// 目录项被容量淘汰后，该父目录的完整内容不再可信；属性与目录流的淘汰不影响完整标记。
		if evicted := itemOf(back); evicted.key.kind == 'e' {
			delete(c.completeDirs, evicted.key.ino)
		}
		c.remove(back)
	}
	c.items[key] = c.lru.PushFront(item)
	c.used += weight
	c.index(item)
}

func (c *NamespaceCache) get(key cacheKey) (*cacheItem, bool) {
	el := c.items[key]
	if el == nil {
		return nil, false
	}
	c.lru.MoveToFront(el)
	return itemOf(el), true
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
	if item, ok := c.get(cacheKey{kind: 'i', ino: ino}); ok {
		v := item.inode
		v.inode = cloneInode(v.inode)
		return v, true
	}
	// 完整目录流本身包含属性，即使独立属性项被淘汰也可定向命中。
	for dir := range c.dirsByChild[ino] {
		el := c.items[cacheKey{kind: 'd', ino: dir}]
		if el == nil {
			continue
		}
		for _, entry := range itemOf(el).dir {
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
		entries := itemOf(el).dir
		for i, entry := range entries {
			if entry.GetIno() == ino {
				// 替换而非原地改写：目录流一旦发布就不再被修改，
				// dir() 只需做指针快照即可安全并发读取。
				updated := cloneDirEntry(entry)
				updated.Inode = cloneInode(inode)
				updated.AttrTimeoutSeconds = attrTimeout
				entries[i] = updated
			}
		}
	}
	c.putItem(&cacheItem{
		key:    cacheKey{kind: 'i', ino: ino},
		inode:  cachedInode{cloneInode(inode), entryTimeout, attrTimeout},
		weight: 1,
	})
}

// refreshInodeAt 用写操作返回的最新属性刷新缓存项，但保留既有目录项有效期。
// Write / SetAttr / Fallocate / Open(O_TRUNC) 的响应不携带 entry timeout；
// 若沿用 putInodeAt(..., 0, ...) 把 0 写回缓存，后续 Lookup 命中缓存时会把
// EntryValid=0 交给内核，内核据此拒绝缓存 dentry，使该文件每次 stat 都穿透到 FUSE Lookup。
// 同时原地刷新所有引用该 Inode 的目录流，避免无谓丢弃整条目录流。
func (c *NamespaceCache) refreshInodeAt(epoch, ino uint64, inode *storagev1.Inode, attrTimeout float64) {
	if inode == nil || inode.GetAttr() == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != ^uint64(0) && epoch != c.epoch {
		return
	}
	c.refreshInodeLocked(ino, inode, attrTimeout)
}

// refreshInodeAtIf 是 refreshInodeAt 的代数校验版本，供 GetAttr 回填使用。
func (c *NamespaceCache) refreshInodeAtIf(generation, epoch, ino uint64, inode *storagev1.Inode, attrTimeout float64) {
	if inode == nil || inode.GetAttr() == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.mutations || (epoch != ^uint64(0) && epoch != c.epoch) {
		return
	}
	c.refreshInodeLocked(ino, inode, attrTimeout)
}

func (c *NamespaceCache) refreshInodeLocked(ino uint64, inode *storagev1.Inode, attrTimeout float64) {
	entryTimeout := c.entryTimeoutLocked(ino, attrTimeout)
	c.removeKey(cacheKey{kind: 'i', ino: ino})
	c.mutations++
	c.putInodeLocked(ino, inode, entryTimeout, attrTimeout)
}

// entryTimeoutLocked 取回该 Inode 已知的目录项有效期：优先独立属性项，
// 其次仍引用它的目录流，都没有时才回落到 fallback。
// fallback 由调用方提供（GetAttr / SetAttr 用服务端返回的 attr timeout，Write 用默认写回填值），
// 保证 STORAGE_ENTRY_TIMEOUT_SECONDS=0 的 pjdfstest 场景不会产生非预期的非零 EntryValid。
func (c *NamespaceCache) entryTimeoutLocked(ino uint64, fallback float64) float64 {
	if el := c.items[cacheKey{kind: 'i', ino: ino}]; el != nil {
		return itemOf(el).inode.entryTimeout
	}
	for dir := range c.dirsByChild[ino] {
		el := c.items[cacheKey{kind: 'd', ino: dir}]
		if el == nil {
			continue
		}
		for _, entry := range itemOf(el).dir {
			if entry.GetIno() == ino {
				return entry.GetEntryTimeoutSeconds()
			}
		}
	}
	return fallback
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
	if item, ok := c.get(cacheKey{kind: 'e', ino: parent, name: name}); ok {
		return item.entry, true
	}
	if item, ok := c.get(cacheKey{kind: 'd', ino: parent}); ok {
		for _, entry := range item.dir {
			if string(entry.GetName()) == name {
				return entry.GetIno(), true
			}
		}
		return 0, true
	}
	// 目录内容已被完整掌握（目录流已降级为独立目录项，或目录刚创建且为空）：
	// 未命中即权威 ENOENT，可直接返回 0 而不发网络 Lookup。
	if _, ok := c.completeDirs[parent]; ok {
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
	// 独立目录项与旧目录流冲突：先把目录流按条目降级，保留未变动兄弟项的权威命中，
	// 再写入本次变更的目录项。
	c.demoteDirStreamToEntriesLocked(parent)
	c.putItem(&cacheItem{
		key:    cacheKey{kind: 'e', ino: parent, name: name},
		entry:  ino,
		weight: 1,
	})
}

func (c *NamespaceCache) invalidateDir(parent uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.completeDirs, parent)
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

// invalidateDirListing 只失效父目录的目录流、父目录属性以及引用了父目录的祖先目录流，
// 保留父目录下未变动的兄弟目录项。单写者约束保证命名空间变化只来自本客户端，
// 因此无需为一次增删清空整个父目录的兄弟项。
//
// 目录流不会被直接丢弃，而是按条目降级为独立目录项并标记为权威完整目录：
// 这样后续同名 Lookup 仍可命中，目录内新增名字也可直接判定 ENOENT，避免每次创建
// 都先发一次必然返回 ENOENT 的 Lookup RPC。
func (c *NamespaceCache) invalidateDirListing(parent uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.demoteDirStreamToEntriesLocked(parent)
	c.removeKey(cacheKey{kind: 'i', ino: parent})
	for dir := range copyInoSet(c.dirsByChild[parent]) {
		c.removeKey(cacheKey{kind: 'd', ino: dir})
	}
	c.mutations++
}

// demoteDirStreamToEntriesLocked 把父目录的完整目录流拆成独立目录项。
// 只在目录流条目数不超过容量四分之一时降级，避免小容量缓存被单个大目录挤爆；
// 若降级过程中同父目录的目录项被 LRU 淘汰，则不能声称内容完整，撤销权威标记。
func (c *NamespaceCache) demoteDirStreamToEntriesLocked(parent uint64) {
	el := c.items[cacheKey{kind: 'd', ino: parent}]
	if el == nil {
		return
	}
	entries := itemOf(el).dir
	c.remove(el)
	if len(entries) > c.capacity/4 {
		return
	}
	evictedSameParent := false
	for _, entry := range entries {
		name := string(entry.GetName())
		if name == "" || name == "." || name == ".." || entry.GetIno() == 0 {
			continue
		}
		key := cacheKey{kind: 'e', ino: parent, name: name}
		// put 每个目录项最多触发一次容量淘汰，预先判断被淘汰项是否属于同一父目录。
		if c.items[key] == nil && c.used+1 > c.capacity {
			if back := c.lru.Back(); back != nil {
				if item := itemOf(back); item.key.kind == 'e' && item.key.ino == parent {
					evictedSameParent = true
				}
			}
		}
		c.putItem(&cacheItem{key: key, entry: entry.GetIno(), weight: 1})
	}
	if !evictedSameParent {
		c.completeDirs[parent] = struct{}{}
	}
}

// markNewDirAt 把刚由 Mkdir 创建的空目录登记为权威完整目录。
// 新建目录在本次会话内没有任何子项，缺失名字可直接返回 ENOENT。
func (c *NamespaceCache) markNewDirAt(epoch, dirIno uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != ^uint64(0) && epoch != c.epoch {
		return
	}
	// 标记不参与 LRU 计费，用容量作为上界，避免长生命周期客户端创建海量目录后无界增长。
	if len(c.completeDirs) < c.capacity {
		c.completeDirs[dirIno] = struct{}{}
	}
}

// invalidateDirListingTouchParentAt 在目录项变更后失效父目录的目录流与属性，
// 并用服务端返回的子节点时间戳就地刷新父目录属性（mtime/ctime，Mkdir 时 nlink+1）。
// 内核在 Create/Mkdir/Mknod/Symlink/Link 后会主动失效父目录属性并触发 FUSE_GETATTR；
// 保留一份已更新的父属性可消除这次网络 GetAttr RPC。
func (c *NamespaceCache) invalidateDirListingTouchParentAt(epoch, parent uint64, child *storagev1.Inode, nlinkDelta int32, attrTimeout float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var parentEntry cachedInode
	var hadParent bool
	if el := c.items[cacheKey{kind: 'i', ino: parent}]; el != nil {
		parentEntry = itemOf(el).inode
		hadParent = true
	}
	c.demoteDirStreamToEntriesLocked(parent)
	c.removeKey(cacheKey{kind: 'i', ino: parent})
	for dir := range copyInoSet(c.dirsByChild[parent]) {
		c.removeKey(cacheKey{kind: 'd', ino: dir})
	}
	c.mutations++
	if !hadParent || (epoch != ^uint64(0) && epoch != c.epoch) ||
		parentEntry.inode == nil || parentEntry.inode.GetAttr() == nil ||
		child == nil || child.GetAttr() == nil {
		return
	}
	ts := child.GetAttr().GetCtime()
	if ts == nil {
		ts = child.GetAttr().GetMtime()
	}
	if ts == nil {
		return
	}
	updatedParent := cloneInode(parentEntry.inode)
	updatedParent.Attr.Mtime = cloneTimestamp(ts)
	updatedParent.Attr.Ctime = cloneTimestamp(ts)
	if nlinkDelta > 0 {
		updatedParent.Attr.Nlink += uint32(nlinkDelta)
	} else if nlinkDelta < 0 && updatedParent.Attr.Nlink > 0 {
		updatedParent.Attr.Nlink--
	}
	at := attrTimeout
	if at <= 0 {
		at = parentEntry.attrTimeout
	}
	c.putInodeLocked(parent, updatedParent, parentEntry.entryTimeout, at)
}

// invalidateDirListingTouchParentNowAt 在 Unlink/Rmdir/Rename 成功后失效父目录的
// 目录流与引用它的祖先流，但保留父目录 Inode 缓存并用当前时间刷新 mtime/ctime。
// 内核在每次目录变更后会 fuse_invalidate_attr(dir)，下一次 unlink/rename 前的
// inode_permission(MAY_EXEC) 会下发 FUSE_GETATTR；保留一份刚更新的父属性可消除
// 这次每操作一次的跨机 GetAttr RPC（cold unlink 500 次即 500 次 RPC）。
// nlinkDelta 仅在目录被删除/移出/移入父目录时非零。
func (c *NamespaceCache) invalidateDirListingTouchParentNowAt(epoch, parent uint64, nlinkDelta int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var parentEntry cachedInode
	var hadParent bool
	if el := c.items[cacheKey{kind: 'i', ino: parent}]; el != nil {
		parentEntry = itemOf(el).inode
		hadParent = true
	}
	c.demoteDirStreamToEntriesLocked(parent)
	c.removeKey(cacheKey{kind: 'i', ino: parent})
	for dir := range copyInoSet(c.dirsByChild[parent]) {
		c.removeKey(cacheKey{kind: 'd', ino: dir})
	}
	c.mutations++
	if !hadParent || (epoch != ^uint64(0) && epoch != c.epoch) ||
		parentEntry.inode == nil || parentEntry.inode.GetAttr() == nil {
		return
	}
	ts := timestamppb.Now()
	updated := cloneInode(parentEntry.inode)
	updated.Attr.Mtime = cloneTimestamp(ts)
	updated.Attr.Ctime = cloneTimestamp(ts)
	if nlinkDelta > 0 {
		updated.Attr.Nlink += uint32(nlinkDelta)
	} else if nlinkDelta < 0 && updated.Attr.Nlink > 0 {
		updated.Attr.Nlink--
	}
	c.putInodeLocked(parent, updated, parentEntry.entryTimeout, parentEntry.attrTimeout)
}

// removeEntry 定向删除单个目录项，用于 Unlink/Rmdir/Rename 的旧名。
func (c *NamespaceCache) removeEntry(parent uint64, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 删除单个名字后无法再断言目录内容完整，撤销权威标记，避免误判其它名字。
	delete(c.completeDirs, parent)
	c.removeKey(cacheKey{kind: 'e', ino: parent, name: name})
	c.mutations++
}

func (c *NamespaceCache) dir(ino uint64) ([]*storagev1.DirEntryPlus, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.get(cacheKey{kind: 'd', ino: ino})
	if !ok {
		return nil, false
	}
	// 只做指针快照：缓存项一经发布就不再原地修改（putInodeLocked 以替换元素的方式更新），
	// 因此调用方可以安全并发读取。500 项目录从 2,001 次分配 / 65~97µs 降到 1 次分配 / ~0.3µs。
	return append([]*storagev1.DirEntryPlus(nil), item.dir...), true
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
	c.putItem(&cacheItem{
		key:    cacheKey{kind: 'd', ino: ino},
		dir:    cloneDir(entries),
		weight: len(entries) + 1,
	})
	// 目录流已完整加载：未命中的名字可权威判定为 ENOENT。
	if len(entries)+1 <= c.capacity {
		c.completeDirs[ino] = struct{}{}
	} else {
		delete(c.completeDirs, ino)
	}
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
	c.completeDirs = make(map[uint64]struct{})
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
