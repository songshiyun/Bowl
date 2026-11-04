package fusefs

import "sync"

// securityCapabilityXattr 是 Linux 用于存放文件安全能力的扩展属性名。
// 内核在写、截断或变更属主/权限后要求清除该属性（killpriv 语义）。
const securityCapabilityXattr = "security.capability"

// capabilityCache 维护每个内核 Inode 是否带有 security.capability 的本地判定。
//
// 普通文件绝大多数从未设置过该属性，内核若每次写入前都回源 GetXattr 探测会引入
// 串行 RPC 放大。这里用“已知无”负缓存直接短路，仅在显式 SetXattr 后标记为存在；
// 任何可能触发 killpriv 的变更都会把该 Inode 重置为“已知无”。
type capabilityCache struct {
	mu      sync.Mutex
	entries map[uint64]bool
}

func newCapabilityCache() *capabilityCache {
	return &capabilityCache{entries: make(map[uint64]bool)}
}

// knownAbsent 报告该 Inode 是否已被确定不携带 security.capability。
func (c *capabilityCache) knownAbsent(ino uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	present, ok := c.entries[ino]
	return ok && !present
}

// markAbsent 在新建文件或发生 killpriv 变更后标记为不携带 security.capability。
func (c *capabilityCache) markAbsent(ino uint64) {
	c.mu.Lock()
	c.entries[ino] = false
	c.mu.Unlock()
}

// markPresent 在显式设置 security.capability 成功后标记为携带该属性。
func (c *capabilityCache) markPresent(ino uint64) {
	c.mu.Lock()
	c.entries[ino] = true
	c.mu.Unlock()
}

// forget 在 Inode 被内核彻底回收后丢弃对应条目，避免编号复用导致误判。
func (c *capabilityCache) forget(ino uint64) {
	c.mu.Lock()
	delete(c.entries, ino)
	c.mu.Unlock()
}

// reset 在会话失效后清空全部判定。
func (c *capabilityCache) reset() {
	c.mu.Lock()
	c.entries = make(map[uint64]bool)
	c.mu.Unlock()
}
