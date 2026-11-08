package store_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

type refNode struct {
	ino           uint64
	parentIno     uint64
	mode          uint32
	nlink         uint32
	uid           uint32
	gid           uint32
	size          uint64
	symlinkTarget []byte
	data          []byte
	xattrs        map[string][]byte
	children      map[string]uint64
	orphan        bool
}

type formalEnv struct {
	t          *testing.T
	dir        string
	actor      *store.StoreActor
	actionRng  *rand.Rand
	probeRng   *rand.Rand
	nextNameID uint64

	nodes          map[uint64]*refNode
	deletedInos    map[uint64]struct{}
	logicalBytes   uint64
	liveInodeCount uint64

	// 追踪 ExtendTail 顺延 -> Fsync 固化 -> 追加写 CoW -> CompactPacks 生命周期计数
	tailExtendCount      int
	fsyncSolidifiedCount int
	appendCoWCount       int
	compactPacksCount    int
	reopenRecoveryCount  int
}

func newFormalEnv(t *testing.T, seed uint64) *formalEnv {
	t.Helper()
	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-formal",
		MaxBytes: 512 << 20,
	})
	require.NoError(t, err)
	env := &formalEnv{
		t:              t,
		dir:            dir,
		actor:          actor,
		actionRng:      rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		probeRng:       rand.New(rand.NewPCG(seed^0xbf58476d1ce4e5b9, seed^0x94d049bb133111eb)),
		nodes:          make(map[uint64]*refNode),
		deletedInos:    make(map[uint64]struct{}),
		logicalBytes:   0,
		liveInodeCount: 1,
	}
	t.Cleanup(func() {
		if env.actor != nil {
			require.NoError(t, env.actor.Close())
		}
	})
	env.nodes[store.RootIno] = &refNode{
		ino:       store.RootIno,
		parentIno: store.RootIno,
		mode:      store.S_IFDIR | 0o755,
		nlink:     2,
		children:  make(map[string]uint64),
		xattrs:    make(map[string][]byte),
	}
	return env
}

// ProbeRNG 返回独立于主动作流的只读探针随机数生成器（遵循 CODING_STANDARDS.md §2 PRNG Stream Isolation）。
func (e *formalEnv) ProbeRNG() *rand.Rand {
	return e.probeRng
}

func resolveFormalSeed(t *testing.T) uint64 {
	t.Helper()
	if raw := strings.TrimSpace(os.Getenv("BOWL_FORMAL_SEED")); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		require.NoError(t, err, "invalid BOWL_FORMAL_SEED=%q", raw)
		return parsed
	}
	return 20261108
}

func (e *formalEnv) nextName(prefix string) string {
	e.nextNameID++
	return fmt.Sprintf("%s_%04d", prefix, e.nextNameID)
}

func (e *formalEnv) sortedDirInos(rng *rand.Rand) []uint64 {
	var out []uint64
	for ino, n := range e.nodes {
		if n.mode&store.S_IFMT == store.S_IFDIR && n.nlink > 0 {
			out = append(out, ino)
		}
	}
	slices.Sort(out)
	_ = rng
	return out
}

func (e *formalEnv) sortedRegInos() []uint64 {
	var out []uint64
	for ino, n := range e.nodes {
		if n.mode&store.S_IFMT == store.S_IFREG && (n.nlink > 0 || n.orphan) {
			out = append(out, ino)
		}
	}
	slices.Sort(out)
	return out
}

func (e *formalEnv) sortedNamedRegInos() []uint64 {
	var out []uint64
	for ino, n := range e.nodes {
		if n.mode&store.S_IFMT == store.S_IFREG && n.nlink > 0 {
			out = append(out, ino)
		}
	}
	slices.Sort(out)
	return out
}

func (e *formalEnv) sortedLiveInos() []uint64 {
	out := make([]uint64, 0, len(e.nodes))
	for ino := range e.nodes {
		out = append(out, ino)
	}
	slices.Sort(out)
	return out
}

func sortedMapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func (e *formalEnv) pickDir(rng *rand.Rand) *refNode {
	dirs := e.sortedDirInos(rng)
	require.NotEmpty(e.t, dirs)
	return e.nodes[dirs[rng.IntN(len(dirs))]]
}

func (e *formalEnv) getBlockBlobRef(ino, blockIdx uint64) (store.BlobRef, bool) {
	val, closer, err := e.actor.DB().Get(store.DataKey(ino, blockIdx))
	if errors.Is(err, pebble.ErrNotFound) {
		return store.BlobRef{}, false
	}
	require.NoError(e.t, err)
	ref, decErr := store.DecodeBlobRef(val)
	require.NoError(e.t, closer.Close())
	require.NoError(e.t, decErr)
	return ref, true
}

func (e *formalEnv) stepCreate() {
	parent := e.pickDir(e.actionRng)
	name := e.nextName("f")
	uid := uint32(1000 + e.actionRng.IntN(10))
	gid := uint32(2000 + e.actionRng.IntN(10))
	attr, err := e.actor.Create(store.CreateReq{
		ParentIno: parent.ino,
		Name:      []byte(name),
		Mode:      store.S_IFREG | 0o644,
		UID:       uid,
		GID:       gid,
	})
	require.NoError(e.t, err)

	effGID := gid
	if parent.mode&store.S_ISGID != 0 {
		effGID = parent.gid
	}
	e.nodes[attr.Ino] = &refNode{
		ino:       attr.Ino,
		parentIno: 0,
		mode:      store.S_IFREG | 0o644,
		nlink:     1,
		uid:       uid,
		gid:       effGID,
		xattrs:    make(map[string][]byte),
	}
	parent.children[name] = attr.Ino
	delete(e.deletedInos, attr.Ino)
	e.liveInodeCount++
}

func (e *formalEnv) stepMkdir() {
	parent := e.pickDir(e.actionRng)
	name := e.nextName("d")
	uid := uint32(1000 + e.actionRng.IntN(10))
	gid := uint32(2000 + e.actionRng.IntN(10))
	attr, err := e.actor.Mkdir(store.MkdirReq{
		ParentIno: parent.ino,
		Name:      []byte(name),
		Mode:      store.S_IFDIR | 0o755,
		UID:       uid,
		GID:       gid,
	})
	require.NoError(e.t, err)

	effGID := gid
	effMode := store.S_IFDIR | uint32(0o755)
	if parent.mode&store.S_ISGID != 0 {
		effGID = parent.gid
		effMode |= store.S_ISGID
	}
	e.nodes[attr.Ino] = &refNode{
		ino:       attr.Ino,
		parentIno: parent.ino,
		mode:      effMode,
		nlink:     2,
		uid:       uid,
		gid:       effGID,
		children:  make(map[string]uint64),
		xattrs:    make(map[string][]byte),
	}
	parent.children[name] = attr.Ino
	parent.nlink++
	delete(e.deletedInos, attr.Ino)
	e.liveInodeCount++
}

func (e *formalEnv) stepSymlink() {
	parent := e.pickDir(e.actionRng)
	name := e.nextName("s")
	target := "/target/" + name
	attr, err := e.actor.Symlink(store.SymlinkReq{
		ParentIno: parent.ino,
		Name:      []byte(name),
		Target:    []byte(target),
		UID:       1001,
		GID:       2001,
	})
	require.NoError(e.t, err)

	effGID := uint32(2001)
	if parent.mode&store.S_ISGID != 0 {
		effGID = parent.gid
	}
	e.nodes[attr.Ino] = &refNode{
		ino:           attr.Ino,
		parentIno:     0,
		mode:          store.S_IFLNK | 0o777,
		nlink:         1,
		uid:           1001,
		gid:           effGID,
		size:          uint64(len(target)),
		symlinkTarget: []byte(target),
		xattrs:        make(map[string][]byte),
	}
	parent.children[name] = attr.Ino
	delete(e.deletedInos, attr.Ino)
	e.liveInodeCount++
	e.logicalBytes += uint64(len(target))
}

func (e *formalEnv) stepLink() {
	regs := e.sortedNamedRegInos()
	if len(regs) == 0 {
		e.stepCreate()
		regs = e.sortedNamedRegInos()
	}
	targetIno := regs[e.actionRng.IntN(len(regs))]
	target := e.nodes[targetIno]
	parent := e.pickDir(e.actionRng)
	name := e.nextName("ln")

	attr, err := e.actor.Link(store.LinkReq{
		Ino:          target.ino,
		NewParentIno: parent.ino,
		NewName:      []byte(name),
	})
	require.NoError(e.t, err)
	target.nlink++
	parent.children[name] = target.ino
	require.Equal(e.t, target.nlink, attr.Nlink)
}

func (e *formalEnv) stepUnlink() {
	dirs := e.sortedDirInos(e.actionRng)
	type candidate struct {
		dirIno uint64
		name   string
		child  *refNode
	}
	var cands []candidate
	for _, dIno := range dirs {
		d := e.nodes[dIno]
		for _, name := range sortedMapKeys(d.children) {
			cIno := d.children[name]
			c := e.nodes[cIno]
			if c != nil && c.mode&store.S_IFMT != store.S_IFDIR {
				cands = append(cands, candidate{dirIno: dIno, name: name, child: c})
			}
		}
	}
	if len(cands) == 0 {
		return
	}
	picked := cands[e.actionRng.IntN(len(cands))]
	openHandle := picked.child.nlink == 1 && e.actionRng.IntN(3) == 0

	err := e.actor.Unlink(picked.dirIno, []byte(picked.name), openHandle)
	require.NoError(e.t, err)

	delete(e.nodes[picked.dirIno].children, picked.name)
	if picked.child.nlink > 1 {
		picked.child.nlink--
		return
	}
	if openHandle {
		picked.child.nlink = 0
		picked.child.orphan = true
		return
	}
	e.reclaimModelNode(picked.child)
}

func (e *formalEnv) stepPurgeOrCleanOrphans() {
	var orphans []uint64
	for ino, n := range e.nodes {
		if n.orphan {
			orphans = append(orphans, ino)
		}
	}
	slices.Sort(orphans)
	if len(orphans) == 0 {
		return
	}
	if e.actionRng.IntN(2) == 0 {
		ino := orphans[e.actionRng.IntN(len(orphans))]
		require.NoError(e.t, e.actor.PurgeOrphanInode(ino))
		e.reclaimModelNode(e.nodes[ino])
	} else {
		require.NoError(e.t, e.actor.CleanOrphans())
		for _, ino := range orphans {
			e.reclaimModelNode(e.nodes[ino])
		}
	}
}

func (e *formalEnv) reclaimModelNode(n *refNode) {
	if n == nil {
		return
	}
	e.logicalBytes -= n.size
	e.liveInodeCount--
	delete(e.nodes, n.ino)
	e.deletedInos[n.ino] = struct{}{}
}

func (e *formalEnv) stepRmdir() {
	dirs := e.sortedDirInos(e.actionRng)
	type candidate struct {
		parentIno uint64
		name      string
		dir       *refNode
	}
	var emptyDirs []candidate
	var nonEmptyDirs []candidate
	for _, pIno := range dirs {
		p := e.nodes[pIno]
		for _, name := range sortedMapKeys(p.children) {
			cIno := p.children[name]
			c := e.nodes[cIno]
			if c != nil && c.mode&store.S_IFMT == store.S_IFDIR {
				cand := candidate{parentIno: pIno, name: name, dir: c}
				if len(c.children) == 0 {
					emptyDirs = append(emptyDirs, cand)
				} else {
					nonEmptyDirs = append(nonEmptyDirs, cand)
				}
			}
		}
	}
	if len(nonEmptyDirs) > 0 && e.actionRng.IntN(4) == 0 {
		picked := nonEmptyDirs[e.actionRng.IntN(len(nonEmptyDirs))]
		require.ErrorIs(e.t, e.actor.Rmdir(picked.parentIno, []byte(picked.name)), store.ErrNotEmpty)
		return
	}
	if len(emptyDirs) == 0 {
		return
	}
	picked := emptyDirs[e.actionRng.IntN(len(emptyDirs))]
	require.NoError(e.t, e.actor.Rmdir(picked.parentIno, []byte(picked.name)))

	parent := e.nodes[picked.parentIno]
	delete(parent.children, picked.name)
	parent.nlink--
	e.reclaimModelNode(picked.dir)
}

func (e *formalEnv) isSubdirInModel(childIno, ancestorIno uint64) bool {
	for cur := childIno; ; {
		if cur == ancestorIno {
			return true
		}
		if cur == store.RootIno || cur == 0 {
			return false
		}
		n := e.nodes[cur]
		if n == nil {
			return false
		}
		cur = n.parentIno
	}
}

func (e *formalEnv) stepRename() {
	dirs := e.sortedDirInos(e.actionRng)
	type entryRef struct {
		parentIno uint64
		name      string
		child     *refNode
	}
	var entries []entryRef
	for _, dIno := range dirs {
		d := e.nodes[dIno]
		for _, name := range sortedMapKeys(d.children) {
			c := e.nodes[d.children[name]]
			if c != nil {
				entries = append(entries, entryRef{parentIno: dIno, name: name, child: c})
			}
		}
	}
	if len(entries) == 0 {
		return
	}
	src := entries[e.actionRng.IntN(len(entries))]
	dstParent := e.pickDir(e.actionRng)

	mode := e.actionRng.IntN(3)
	switch mode {
	case 0: // 移动至新名称
		newName := e.nextName("rn")
		if src.child.mode&store.S_IFMT == store.S_IFDIR && e.isSubdirInModel(dstParent.ino, src.child.ino) {
			require.ErrorIs(e.t, e.actor.Rename(store.RenameReq{
				OldParentIno: src.parentIno,
				OldName:      []byte(src.name),
				NewParentIno: dstParent.ino,
				NewName:      []byte(newName),
			}), store.ErrInvalid)
			return
		}
		require.NoError(e.t, e.actor.Rename(store.RenameReq{
			OldParentIno: src.parentIno,
			OldName:      []byte(src.name),
			NewParentIno: dstParent.ino,
			NewName:      []byte(newName),
		}))
		oldParent := e.nodes[src.parentIno]
		delete(oldParent.children, src.name)
		dstParent.children[newName] = src.child.ino
		if src.child.mode&store.S_IFMT == store.S_IFDIR && oldParent.ino != dstParent.ino {
			oldParent.nlink--
			dstParent.nlink++
			src.child.parentIno = dstParent.ino
		}

	case 1: // 覆盖已有同类型非目录节点或测试 RenameNoreplace
		var fileTargets []entryRef
		for _, ent := range entries {
			if ent.child.mode&store.S_IFMT != store.S_IFDIR && (ent.parentIno != src.parentIno || ent.name != src.name) {
				fileTargets = append(fileTargets, ent)
			}
		}
		if src.child.mode&store.S_IFMT == store.S_IFDIR || len(fileTargets) == 0 {
			return
		}
		dst := fileTargets[e.actionRng.IntN(len(fileTargets))]
		if e.actionRng.IntN(2) == 0 {
			require.ErrorIs(e.t, e.actor.Rename(store.RenameReq{
				OldParentIno: src.parentIno,
				OldName:      []byte(src.name),
				NewParentIno: dst.parentIno,
				NewName:      []byte(dst.name),
				Flags:        store.RenameNoreplace,
			}), store.ErrExist)
			return
		}
		require.NoError(e.t, e.actor.Rename(store.RenameReq{
			OldParentIno: src.parentIno,
			OldName:      []byte(src.name),
			NewParentIno: dst.parentIno,
			NewName:      []byte(dst.name),
		}))
		delete(e.nodes[src.parentIno].children, src.name)
		e.nodes[dst.parentIno].children[dst.name] = src.child.ino
		if dst.child.ino != src.child.ino {
			if dst.child.nlink > 1 {
				dst.child.nlink--
			} else {
				e.reclaimModelNode(dst.child)
			}
		}

	case 2: // RenameExchange 双向交换两个非目录项
		var fileTargets []entryRef
		for _, ent := range entries {
			if ent.child.mode&store.S_IFMT != store.S_IFDIR && ent.child.ino != src.child.ino {
				fileTargets = append(fileTargets, ent)
			}
		}
		if src.child.mode&store.S_IFMT == store.S_IFDIR || len(fileTargets) == 0 {
			return
		}
		dst := fileTargets[e.actionRng.IntN(len(fileTargets))]
		require.NoError(e.t, e.actor.Rename(store.RenameReq{
			OldParentIno: src.parentIno,
			OldName:      []byte(src.name),
			NewParentIno: dst.parentIno,
			NewName:      []byte(dst.name),
			Flags:        store.RenameExchange,
		}))
		e.nodes[src.parentIno].children[src.name] = dst.child.ino
		e.nodes[dst.parentIno].children[dst.name] = src.child.ino
	}
}

func (e *formalEnv) stepSetAttr() {
	regs := e.sortedRegInos()
	if len(regs) == 0 {
		return
	}
	ino := regs[e.actionRng.IntN(len(regs))]
	n := e.nodes[ino]

	if e.actionRng.IntN(2) == 0 {
		// 修改权限与属主
		newPerm := uint32(0o600 + e.actionRng.IntN(0o177))
		newUID := uint32(1000 + e.actionRng.IntN(20))
		newGID := uint32(2000 + e.actionRng.IntN(20))
		attr, err := e.actor.SetAttr(ino, store.SetAttrReq{
			Mode: &newPerm,
			UID:  &newUID,
			GID:  &newGID,
		})
		require.NoError(e.t, err)
		n.mode = (n.mode & store.S_IFMT) | (newPerm & store.PermMask)
		n.uid = newUID
		n.gid = newGID
		delete(n.xattrs, "security.capability")
		require.Equal(e.t, n.mode, attr.Mode)
	} else {
		// Size 截断或扩容（覆盖块内截断、跨块截断、归零截断与稀疏扩容）
		var newSize uint64
		switch e.actionRng.IntN(4) {
		case 0:
			newSize = 0
		case 1:
			newSize = uint64(e.actionRng.IntN(store.BlobSize + 65536))
		case 2:
			newSize = n.size / 2
		default:
			newSize = n.size + uint64(e.actionRng.IntN(131072))
		}
		attr, err := e.actor.SetAttr(ino, store.SetAttrReq{Size: &newSize})
		require.NoError(e.t, err)
		if newSize > n.size {
			e.logicalBytes += newSize - n.size
			grown := make([]byte, newSize)
			copy(grown, n.data)
			n.data = grown
		} else if newSize < n.size {
			e.logicalBytes -= n.size - newSize
			n.data = append([]byte(nil), n.data[:newSize]...)
		}
		n.size = newSize
		n.mode &^= store.S_ISUID | store.S_ISGID
		delete(n.xattrs, "security.capability")
		require.Equal(e.t, newSize, attr.Size)
	}
}

func (e *formalEnv) stepXattr() {
	live := e.sortedLiveInos()
	ino := live[e.actionRng.IntN(len(live))]
	n := e.nodes[ino]
	if n.mode&store.S_IFMT == store.S_IFLNK {
		// 符号链接不允许 user.* 扩展属性
		err := e.actor.SetXattr(ino, []byte("user.denied"), []byte("v"), 0)
		require.ErrorIs(e.t, err, store.ErrPerm)
		return
	}

	keys := []string{"user.alpha", "user.beta", "user.gamma", "trusted.meta"}
	key := keys[e.actionRng.IntN(len(keys))]
	_, exists := n.xattrs[key]

	if exists && e.actionRng.IntN(3) == 0 {
		// RemoveXattr
		require.NoError(e.t, e.actor.RemoveXattr(ino, []byte(key)))
		delete(n.xattrs, key)
		return
	}

	val := fmt.Appendf(nil, "val_%d_%d", ino, e.actionRng.IntN(10000))
	var flags uint32
	if exists {
		if e.actionRng.IntN(4) == 0 {
			require.ErrorIs(e.t, e.actor.SetXattr(ino, []byte(key), val, store.XattrCreate), store.ErrExist)
			return
		}
		flags = store.XattrReplace
	} else {
		if e.actionRng.IntN(4) == 0 {
			require.ErrorIs(e.t, e.actor.SetXattr(ino, []byte(key), val, store.XattrReplace), store.ErrNoData)
			return
		}
		flags = store.XattrCreate
	}
	require.NoError(e.t, e.actor.SetXattr(ino, []byte(key), val, flags))
	n.xattrs[key] = val
}

func (e *formalEnv) stepWrite() {
	regs := e.sortedRegInos()
	if len(regs) == 0 {
		e.stepCreate()
		regs = e.sortedRegInos()
	}
	ino := regs[e.actionRng.IntN(len(regs))]
	n := e.nodes[ino]

	var offset uint64
	var payload []byte

	if e.actionRng.IntN(5) == 0 {
		// 满 256 KiB 对齐整块写入（1 ~ 2 块）
		blockIdx := uint64(e.actionRng.IntN(2))
		numBlocks := 1 + e.actionRng.IntN(2)
		offset = blockIdx * store.BlobSize
		fill := byte(1 + e.actionRng.IntN(250))
		payload = bytes.Repeat([]byte{fill}, numBlocks*store.BlobSize)
	} else {
		// 局部或跨 256 KiB 块边界写入
		offset = uint64(e.actionRng.IntN(store.BlobSize + 65536))
		length := 1 + e.actionRng.IntN(65536)
		payload = make([]byte, length)
		fill := byte(1 + e.actionRng.IntN(250))
		for i := range payload {
			payload[i] = fill ^ byte(i&0xff)
		}
	}

	firstBlock := offset / store.BlobSize
	lastBlock := (offset + uint64(len(payload)) - 1) / store.BlobSize
	singlePartial := firstBlock == lastBlock && len(payload) < store.BlobSize
	var (
		preRef store.BlobRef
		preOK  bool
	)
	if singlePartial {
		preRef, preOK = e.getBlockBlobRef(ino, firstBlock)
	}

	written, attr, err := e.actor.Write(ino, offset, payload)
	require.NoError(e.t, err)
	require.Equal(e.t, uint32(len(payload)), written)

	end := offset + uint64(len(payload))
	if end > n.size {
		e.logicalBytes += end - n.size
		grown := make([]byte, end)
		copy(grown, n.data)
		n.data = grown
		n.size = end
	}
	copy(n.data[offset:end], payload)
	n.mode &^= store.S_ISUID | store.S_ISGID
	delete(n.xattrs, "security.capability")
	require.Equal(e.t, n.size, attr.Size)

	if singlePartial && preOK {
		postRef, postOK := e.getBlockBlobRef(ino, firstBlock)
		require.True(e.t, postOK)
		if postRef.PackID == preRef.PackID && postRef.Offset == preRef.Offset {
			e.tailExtendCount++
		} else {
			e.appendCoWCount++
		}
	}
}

func (e *formalEnv) stepFallocate() {
	regs := e.sortedRegInos()
	if len(regs) == 0 {
		return
	}
	ino := regs[e.actionRng.IntN(len(regs))]
	n := e.nodes[ino]

	modes := []uint32{
		0,
		store.FallocKeepSize,
		store.FallocPunchHole | store.FallocKeepSize,
		store.FallocZeroRange,
		store.FallocZeroRange | store.FallocKeepSize,
	}
	mode := modes[e.actionRng.IntN(len(modes))]
	offset := uint64(e.actionRng.IntN(store.BlobSize + 32768))
	length := uint64(1 + e.actionRng.IntN(store.BlobSize+16384))

	require.NoError(e.t, e.actor.Fallocate(ino, offset, length, mode))

	end := offset + length
	keepSize := mode&store.FallocKeepSize != 0
	zeroRange := mode&store.FallocPunchHole != 0 || mode&store.FallocZeroRange != 0

	if !keepSize && end > n.size {
		e.logicalBytes += end - n.size
		grown := make([]byte, end)
		copy(grown, n.data)
		n.data = grown
		n.size = end
	}
	if zeroRange && offset < n.size {
		clearEnd := min(end, n.size)
		clear(n.data[offset:clearEnd])
		n.mode &^= store.S_ISUID | store.S_ISGID
	}
	delete(n.xattrs, "security.capability")
}

func (e *formalEnv) stepFsync() {
	regs := e.sortedRegInos()
	if len(regs) == 0 {
		return
	}
	ino := regs[e.actionRng.IntN(len(regs))]
	n := e.nodes[ino]

	numBlocks := (n.size + store.BlobSize - 1) / store.BlobSize
	for b := range numBlocks {
		if _, ok := e.getBlockBlobRef(ino, b); ok {
			if e.actor.Blobs().HasDirty() {
				e.fsyncSolidifiedCount++
			}
		}
	}

	require.NoError(e.t, e.actor.Fsync(ino))
	require.False(e.t, e.actor.WALDirty())
}

func (e *formalEnv) stepFsyncDir() {
	dirs := e.sortedDirInos(e.actionRng)
	dirIno := dirs[e.actionRng.IntN(len(dirs))]
	require.NoError(e.t, e.actor.FsyncDir(dirIno))
	require.False(e.t, e.actor.WALDirty())
}

func (e *formalEnv) stepCompactPacks() {
	require.NoError(e.t, e.actor.CompactPacks())
	require.False(e.t, e.actor.WALDirty())
	e.compactPacksCount++
}

func (e *formalEnv) stepReopenAndRecover() {
	// 重启前先清理孤儿节点（模拟真实分支会话切换/重启恢复后 CleanOrphans）
	e.stepPurgeOrCleanOrphans()
	maxCap := e.actor.Blobs().MaxPackSize()
	require.NoError(e.t, e.actor.Close())
	e.actor = nil

	reopened, err := store.Open(store.Options{
		Dir:      e.dir,
		DiskID:   "disk-formal",
		MaxBytes: 512 << 20,
	})
	require.NoError(e.t, err)
	reopened.Blobs().SetMaxPackSize(maxCap)
	e.actor = reopened
	e.reopenRecoveryCount++
}

// runReadProbes 使用独立 ProbeRNG 执行只读探针（不改变主状态机 PRNG 轨迹）。
func (e *formalEnv) runReadProbes() {
	rng := e.ProbeRNG()

	// 1. StatFS 对拍
	st, err := e.actor.StatFS()
	require.NoError(e.t, err)
	require.Equal(e.t, e.logicalBytes, st.UsedBytes)
	require.Equal(e.t, e.liveInodeCount, st.Files)

	// 2. 随机抽样存活节点校验 GetAttr / Read / Readlink / ListXattr / GetXattr / ReadDir / ReadDirPlus / Lookup
	live := e.sortedLiveInos()
	sampleCount := min(len(live), 6)
	for range sampleCount {
		ino := live[rng.IntN(len(live))]
		n := e.nodes[ino]
		attr, err := e.actor.GetAttr(ino)
		require.NoError(e.t, err)
		require.Equal(e.t, n.mode, attr.Mode)
		require.Equal(e.t, n.nlink, attr.Nlink)
		require.Equal(e.t, n.uid, attr.UID)
		require.Equal(e.t, n.gid, attr.GID)

		switch n.mode & store.S_IFMT {
		case store.S_IFREG:
			require.Equal(e.t, n.size, attr.Size)
			var gotData []byte
			gotData, err = e.actor.Read(ino, 0, uint32(n.size)+64)
			require.NoError(e.t, err)
			require.True(e.t, bytes.Equal(n.data, gotData), "ino %d data mismatch (len expected=%d got=%d)", ino, len(n.data), len(gotData))

		case store.S_IFLNK:
			var target []byte
			target, err = e.actor.Readlink(ino)
			require.NoError(e.t, err)
			require.Equal(e.t, n.symlinkTarget, target)

		case store.S_IFDIR:
			var entries []store.DirEntry
			entries, err = e.actor.ReadDir(ino, 0, store.MaxReadDirLimit)
			require.NoError(e.t, err)
			var plusEntries []store.DirEntryPlus
			plusEntries, err = e.actor.ReadDirPlus(ino, 0, store.MaxReadDirLimit)
			require.NoError(e.t, err)
			require.Len(e.t, plusEntries, len(entries))
			require.Len(e.t, entries, len(n.children)+2)

			for _, name := range sortedMapKeys(n.children) {
				childIno := n.children[name]
				childModel := e.nodes[childIno]
				var ent store.Entry
				var cAttr store.Attribute
				ent, cAttr, err = e.actor.Lookup(ino, []byte(name))
				require.NoError(e.t, err)
				require.Equal(e.t, childIno, ent.Ino)
				require.Equal(e.t, childModel.mode, cAttr.Mode)
			}
		}

		xattrNames, err := e.actor.ListXattr(ino)
		require.NoError(e.t, err)
		expectedXattrKeys := sortedMapKeys(n.xattrs)
		require.Len(e.t, xattrNames, len(expectedXattrKeys))
		for i, k := range expectedXattrKeys {
			require.Equal(e.t, k, string(xattrNames[i]))
			var v []byte
			v, err = e.actor.GetXattr(ino, []byte(k))
			require.NoError(e.t, err)
			require.Equal(e.t, n.xattrs[k], v)
		}
	}
}

// auditFullStateAndBlobConsistency 对全量内存参考状态机、已删除节点残留及磁盘 .blob 文件集合与 Pebble DataKey 引用集做 100% 精确对拍。
func (e *formalEnv) auditFullStateAndBlobConsistency() {
	// 1. 全量存活节点对拍
	for _, ino := range e.sortedLiveInos() {
		n := e.nodes[ino]
		attr, err := e.actor.GetAttr(ino)
		require.NoError(e.t, err)
		require.Equal(e.t, n.mode, attr.Mode)
		require.Equal(e.t, n.nlink, attr.Nlink)
		if n.mode&store.S_IFMT == store.S_IFREG {
			require.Equal(e.t, n.size, attr.Size)
			var got []byte
			got, err = e.actor.Read(ino, 0, uint32(n.size)+16)
			require.NoError(e.t, err)
			require.True(e.t, bytes.Equal(n.data, got))
		}
	}

	// 2. 直接按已删除节点编号检查 Pebble 中 InodeKey、OrphanKey、XattrPrefix、DataPrefix 均已彻底清除（CODING_STANDARDS.md §1）
	deleted := make([]uint64, 0, len(e.deletedInos))
	for ino := range e.deletedInos {
		deleted = append(deleted, ino)
	}
	slices.Sort(deleted)
	for _, ino := range deleted {
		_, _, err := e.actor.DB().Get(store.InodeKey(ino))
		require.ErrorIs(e.t, err, pebble.ErrNotFound, "已物理删除的节点 %d 不得残留 InodeKey", ino)

		_, _, err = e.actor.DB().Get(store.OrphanKey(ino))
		require.ErrorIs(e.t, err, pebble.ErrNotFound, "已物理删除的节点 %d 不得残留 OrphanKey", ino)

		iter, err := e.actor.DB().NewIter(nil)
		require.NoError(e.t, err)
		require.False(e.t, iter.SeekPrefixGE(store.XattrPrefix(ino)), "已物理删除的节点 %d 不得残留 XattrKey", ino)
		require.False(e.t, iter.SeekPrefixGE(store.DataPrefix(ino)), "已物理删除的节点 %d 不得残留 DataKey", ino)
		require.NoError(e.t, iter.Close())
	}

	// 3. 校验磁盘 blobs/ 实际 .blob 文件集合与 Pebble DataKey 引用集 100% 精确一致
	auditDiskBlobsMatchPebbleDataKeys(e.t, e.actor)
}

func auditDiskBlobsMatchPebbleDataKeys(t *testing.T, actor *store.StoreActor) {
	t.Helper()

	require.NoError(t, actor.FlushWAL())

	// 扫描 Pebble 中所有 DataKey -> BlobRef，记录每个被引用 PackID 所需的最大结尾偏移与存活字节总和
	referencedPacks := make(map[uint64]uint32)
	liveBytesByPack := make(map[uint64]uint64)
	iter, err := actor.DB().NewIter(nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()

	for valid := iter.SeekGE([]byte("data:")); valid && bytes.HasPrefix(iter.Key(), []byte("data:")); valid = iter.Next() {
		_, _, err = store.DecodeDataKey(iter.Key())
		require.NoError(t, err)
		ref, decErr := store.DecodeBlobRef(iter.Value())
		require.NoError(t, decErr)
		end := ref.Offset + ref.Length
		if end > referencedPacks[ref.PackID] {
			referencedPacks[ref.PackID] = end
		}
		liveBytesByPack[ref.PackID] += uint64(ref.Length)
	}
	require.NoError(t, iter.Error())

	// 扫描 Pebble 中所有 sys:pack:<pack_id> 记账条目，校验与 DataKey 实际存活字节 100% 吻合
	activePackID := actor.Blobs().ActivePackID()
	sysPacks := make(map[uint64]store.PackMeta)
	packPrefix := store.PackStatPrefix()
	for valid := iter.SeekGE(packPrefix); valid && bytes.HasPrefix(iter.Key(), packPrefix); valid = iter.Next() {
		packID, decErr := store.DecodePackStatKey(iter.Key())
		require.NoError(t, decErr)
		meta, decErr := store.DecodePackMeta(iter.Value())
		require.NoError(t, decErr)
		sysPacks[packID] = meta
		if meta.LiveBytes == 0 {
			require.Equal(t, activePackID, packID, "已封口 .pack (packID=%d != active=%d) 在 LiveBytes==0 时必须已从 Pebble 删除 sys:pack 条目", packID, activePackID)
		}
	}
	require.NoError(t, iter.Error())

	for packID, actualLive := range liveBytesByPack {
		meta, ok := sysPacks[packID]
		require.True(t, ok, "存在存活 DataKey 的 pack %d 必须在 Pebble 中有 sys:pack 条目", packID)
		require.Equal(t, actualLive, meta.LiveBytes, "pack %d 的 sys:pack LiveBytes 必须严格等于存活 DataKey 长度之和", packID)
		require.GreaterOrEqual(t, meta.TotalBytes, meta.LiveBytes, "pack %d 的 TotalBytes 必须 >= LiveBytes", packID)
		require.GreaterOrEqual(t, meta.TotalBytes, uint64(referencedPacks[packID]), "pack %d 的 TotalBytes 必须覆盖最大被引用结尾偏移", packID)
	}
	for packID, meta := range sysPacks {
		require.Equal(t, liveBytesByPack[packID], meta.LiveBytes, "sys:pack:%d 的 LiveBytes 必须与 DataKey 汇总一致", packID)
	}

	// 扫描磁盘 packs/*.pack 实际物理文件
	diskPacks := make(map[uint64]uint32)
	entries, err := os.ReadDir(actor.Blobs().Root())
	if !os.IsNotExist(err) {
		require.NoError(t, err)
		for _, f := range entries {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".pack") {
				continue
			}
			var id uint64
			id, err = strconv.ParseUint(strings.TrimSuffix(f.Name(), ".pack"), 16, 64)
			require.NoError(t, err)
			var info os.FileInfo
			info, err = f.Info()
			require.NoError(t, err)
			diskPacks[id] = uint32(info.Size())
		}
	}

	for packID, minEnd := range referencedPacks {
		actualSize, ok := diskPacks[packID]
		require.True(t, ok, "Pebble DataKey 引用的 pack %d 必须在磁盘 packs/ 下存在", packID)
		require.GreaterOrEqual(t, actualSize, minEnd, "磁盘 pack %d 大小必须覆盖被引用的最大块结尾偏移", packID)
	}
	for packID := range diskPacks {
		if packID == activePackID {
			continue
		}
		_, referenced := referencedPacks[packID]
		require.True(t, referenced, "WAL 落盘后磁盘上不得残留无任何存活 DataKey 引用的已封口死包 %d", packID)
	}
}

func TestFormalStoreActorStateMachine(t *testing.T) {
	seed := resolveFormalSeed(t)
	t.Logf("Running TestFormalStoreActorStateMachine with BOWL_FORMAL_SEED=%d", seed)
	env := newFormalEnv(t, seed)

	// 先确定性验证一次完整的 ExtendTail 尾部顺延 -> Fsync 固化 -> 非尾部局部写 CoW 追加新块段生命周期
	fAttr, err := env.actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("lifecycle_seed.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	env.nodes[fAttr.Ino] = &refNode{
		ino:    fAttr.Ino,
		mode:   store.S_IFREG | 0o644,
		nlink:  1,
		size:   8192,
		data:   make([]byte, 8192),
		xattrs: make(map[string][]byte),
	}
	env.nodes[store.RootIno].children["lifecycle_seed.bin"] = fAttr.Ino
	env.liveInodeCount++
	env.logicalBytes += 8192

	chunk1 := bytes.Repeat([]byte{0x31}, 4096)
	_, _, err = env.actor.Write(fAttr.Ino, 0, chunk1)
	require.NoError(t, err)
	copy(env.nodes[fAttr.Ino].data[0:4096], chunk1)
	ref1, ok := env.getBlockBlobRef(fAttr.Ino, 0)
	require.True(t, ok)
	require.True(t, env.actor.Blobs().HasDirty())

	// 活跃 .pack 尾部顺延局部写（ExtendTail）
	chunk2 := bytes.Repeat([]byte{0x32}, 4096)
	_, _, err = env.actor.Write(fAttr.Ino, 4096, chunk2)
	require.NoError(t, err)
	copy(env.nodes[fAttr.Ino].data[4096:8192], chunk2)
	ref2, ok := env.getBlockBlobRef(fAttr.Ino, 0)
	require.True(t, ok)
	require.Equal(t, ref1.PackID, ref2.PackID)
	require.Equal(t, ref1.Offset, ref2.Offset)
	env.tailExtendCount++

	// Fsync 固化
	require.NoError(t, env.actor.Fsync(fAttr.Ino))
	require.False(t, env.actor.Blobs().HasDirty())
	env.fsyncSolidifiedCount++

	// 块内中间局部覆写：追加新块段（CoW）
	chunk3 := bytes.Repeat([]byte{0x33}, 2048)
	_, _, err = env.actor.Write(fAttr.Ino, 1024, chunk3)
	require.NoError(t, err)
	copy(env.nodes[fAttr.Ino].data[1024:3072], chunk3)
	ref3, ok := env.getBlockBlobRef(fAttr.Ino, 0)
	require.True(t, ok)
	require.NotEqual(t, ref2, ref3)
	require.True(t, env.actor.Blobs().HasDirty())
	env.appendCoWCount++

	// 压测跨包自动切包、CompactPacks 搬迁与全死封口包并发回收：将封包上限设为 2 个 256 KiB 块
	env.actor.Blobs().SetMaxPackSize(2 * store.BlobSize)

	// 执行 240 步随机状态机演进（含 CompactPacks、重启恢复）与独立只读探针对拍
	const totalSteps = 240
	for step := range totalSteps {
		op := env.actionRng.IntN(100)
		switch {
		case op < 12:
			env.stepCreate()
		case op < 19:
			env.stepMkdir()
		case op < 24:
			env.stepSymlink()
		case op < 29:
			env.stepLink()
		case op < 49:
			env.stepWrite()
		case op < 58:
			env.stepFallocate()
		case op < 68:
			env.stepSetAttr()
		case op < 75:
			env.stepXattr()
		case op < 82:
			env.stepRename()
		case op < 87:
			env.stepUnlink()
		case op < 90:
			env.stepRmdir()
		case op < 93:
			env.stepFsync()
		case op < 95:
			env.stepFsyncDir()
		default:
			env.stepCompactPacks()
		}

		if step%8 == 0 {
			env.stepPurgeOrCleanOrphans()
		}
		if step%15 == 14 {
			env.stepCompactPacks()
		}
		if step == 80 || step == 160 {
			env.stepReopenAndRecover()
			env.auditFullStateAndBlobConsistency()
		}
		if step%6 == 0 {
			env.runReadProbes()
		}
	}

	// 收尾清理所有孤儿节点、执行 CompactPacks + 重启恢复，最后执行全量不变量审计
	env.stepPurgeOrCleanOrphans()
	env.stepCompactPacks()
	env.stepReopenAndRecover()
	env.auditFullStateAndBlobConsistency()

	require.Positive(t, env.tailExtendCount, "状态机测试必须触发过 ExtendTail 活跃包尾部顺延")
	require.Positive(t, env.fsyncSolidifiedCount, "状态机测试必须触发过 Fsync 块落盘")
	require.Positive(t, env.appendCoWCount, "状态机测试必须触发过 Append CoW 新块段分配")
	require.Positive(t, env.compactPacksCount, "状态机测试必须触发过 CompactPacks")
	require.Positive(t, env.reopenRecoveryCount, "状态机测试必须触发过重启恢复对拍")
}

func TestStoreActorConcurrentReadWriteStressAndBlobReclaim(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:       t.TempDir(),
		DiskID:    "disk-concurrent-stress",
		AtimeMode: store.AtimeModeRelAtime,
		MaxBytes:  512 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()
	actor.Blobs().SetMaxPackSize(2 * store.BlobSize)

	const (
		numWorkers = 6
		numReaders = 4
		opsPerWork = 35
	)

	// 为每个写协程建立专属子目录，既压测跨协程共享 MPSC 队列与共享根目录并发操作，又允许每个写协程精确断言其子目录内的数据完整性
	workerDirs := make([]uint64, numWorkers)
	sharedFiles := make([]uint64, numWorkers)
	for w := range numWorkers {
		dAttr, err := actor.Mkdir(store.MkdirReq{
			ParentIno: store.RootIno,
			Name:      fmt.Appendf(nil, "worker_%d", w),
			Mode:      store.S_IFDIR | 0o755,
		})
		require.NoError(t, err)
		workerDirs[w] = dAttr.Ino

		fAttr, err := actor.Create(store.CreateReq{
			ParentIno: dAttr.Ino,
			Name:      []byte("primary.bin"),
			Mode:      store.S_IFREG | 0o644,
		})
		require.NoError(t, err)
		sharedFiles[w] = fAttr.Ino
	}

	var stopReaders atomic.Bool
	var readerWG sync.WaitGroup
	readerErrCh := make(chan error, numReaders)

	// 启动并发直读快路径协程池：持续并发调用 StatFS / GetAttr / Lookup / ReadDir / ReadDirPlus / GetXattr / ListXattr / Read
	for r := range numReaders {
		readerWG.Add(1)
		go func(readerID int) {
			defer readerWG.Done()
			rng := rand.New(rand.NewPCG(uint64(1000+readerID), uint64(2000+readerID)))
			for !stopReaders.Load() {
				wIdx := rng.IntN(numWorkers)
				dirIno := workerDirs[wIdx]
				fileIno := sharedFiles[wIdx]

				if _, err := actor.StatFS(); err != nil {
					readerErrCh <- fmt.Errorf("StatFS: %w", err)
					return
				}
				if _, err := actor.GetAttr(dirIno); err != nil {
					readerErrCh <- fmt.Errorf("GetAttr dir: %w", err)
					return
				}
				if _, err := actor.GetAttr(fileIno); err != nil {
					readerErrCh <- fmt.Errorf("GetAttr file: %w", err)
					return
				}
				if _, _, err := actor.Lookup(store.RootIno, fmt.Appendf(nil, "worker_%d", wIdx)); err != nil {
					readerErrCh <- fmt.Errorf("Lookup worker dir: %w", err)
					return
				}
				if _, err := actor.ReadDir(dirIno, 0, 128); err != nil {
					readerErrCh <- fmt.Errorf("ReadDir: %w", err)
					return
				}
				if _, err := actor.ReadDirPlus(dirIno, 0, 128); err != nil {
					readerErrCh <- fmt.Errorf("ReadDirPlus: %w", err)
					return
				}
				if _, err := actor.ListXattr(fileIno); err != nil {
					readerErrCh <- fmt.Errorf("ListXattr: %w", err)
					return
				}
				_, xErr := actor.GetXattr(fileIno, []byte("user.tag"))
				if xErr != nil && !errors.Is(xErr, store.ErrNoData) {
					readerErrCh <- fmt.Errorf("GetXattr: %w", xErr)
					return
				}
				if _, err := actor.Read(fileIno, 0, 65536); err != nil {
					readerErrCh <- fmt.Errorf("Read: %w", err)
					return
				}
			}
		}(r)
	}

	var writerWG sync.WaitGroup
	writerErrCh := make(chan error, numWorkers)

	// 启动并发写协程池：混合执行全部 MPSC 写接口与 Fsync / FsyncDir 双层屏障
	for w := range numWorkers {
		writerWG.Add(1)
		go func(workerID int) {
			defer writerWG.Done()
			rng := rand.New(rand.NewPCG(uint64(5000+workerID), uint64(9000+workerID)))
			dirIno := workerDirs[workerID]
			primaryIno := sharedFiles[workerID]
			expectedPrimary := make([]byte, 0)

			for step := range opsPerWork {
				// 1. 对 primary.bin 执行局部写 -> Fsync 固化 -> CoW 局部写 / 截断 / 打洞并实时校验内容
				offset := uint64(rng.IntN(65536))
				chunkLen := 512 + rng.IntN(8192)
				chunk := bytes.Repeat([]byte{byte(workerID*31 + step + 1)}, chunkLen)
				if _, _, err := actor.Write(primaryIno, offset, chunk); err != nil {
					writerErrCh <- fmt.Errorf("worker %d Write: %w", workerID, err)
					return
				}
				if offset+uint64(chunkLen) > uint64(len(expectedPrimary)) {
					grown := make([]byte, offset+uint64(chunkLen))
					copy(grown, expectedPrimary)
					expectedPrimary = grown
				}
				copy(expectedPrimary[offset:], chunk)

				if step%4 == 0 {
					if err := actor.Fsync(primaryIno); err != nil {
						writerErrCh <- fmt.Errorf("worker %d Fsync: %w", workerID, err)
						return
					}
				}
				if step%6 == 2 && len(expectedPrimary) > 1024 {
					punchOff := uint64(rng.IntN(len(expectedPrimary) / 2))
					punchLen := uint64(256 + rng.IntN(512))
					if err := actor.Fallocate(primaryIno, punchOff, punchLen, store.FallocPunchHole|store.FallocKeepSize); err != nil {
						writerErrCh <- fmt.Errorf("worker %d Fallocate: %w", workerID, err)
						return
					}
					clearEnd := min(punchOff+punchLen, uint64(len(expectedPrimary)))
					clear(expectedPrimary[punchOff:clearEnd])
				}
				if step%7 == 3 {
					newSize := uint64(len(expectedPrimary)) / 2
					if _, err := actor.SetAttr(primaryIno, store.SetAttrReq{Size: &newSize}); err != nil {
						writerErrCh <- fmt.Errorf("worker %d SetAttr Size: %w", workerID, err)
						return
					}
					expectedPrimary = expectedPrimary[:newSize]
				}

				got, err := actor.Read(primaryIno, 0, uint32(len(expectedPrimary))+16)
				if err != nil {
					writerErrCh <- fmt.Errorf("worker %d Read verify: %w", workerID, err)
					return
				}
				if !bytes.Equal(expectedPrimary, got) {
					writerErrCh <- fmt.Errorf("worker %d step %d primary data mismatch", workerID, step)
					return
				}

				// 2. Xattr 设置与移除
				tagVal := fmt.Appendf(nil, "w%d_s%d", workerID, step)
				err = actor.SetXattr(primaryIno, []byte("user.tag"), tagVal, 0)
				if err != nil {
					writerErrCh <- fmt.Errorf("worker %d SetXattr: %w", workerID, err)
					return
				}
				if step%5 == 0 {
					err = actor.RemoveXattr(primaryIno, []byte("user.tag"))
					if err != nil {
						writerErrCh <- fmt.Errorf("worker %d RemoveXattr: %w", workerID, err)
						return
					}
				}

				// 3. 创建临时文件、写入数据、Link、Rename、Unlink（覆盖有句柄孤儿回收与无句柄直接回收）与 Mkdir/Rmdir/Symlink
				tmpName := fmt.Appendf(nil, "tmp_%d", step)
				renamedName := fmt.Appendf(nil, "ren_%d", step)
				linkName := fmt.Appendf(nil, "lnk_%d", step)

				tmpAttr, err := actor.Create(store.CreateReq{
					ParentIno: dirIno,
					Name:      tmpName,
					Mode:      store.S_IFREG | 0o644,
				})
				if err != nil {
					writerErrCh <- fmt.Errorf("worker %d Create tmp: %w", workerID, err)
					return
				}
				_, _, err = actor.Write(tmpAttr.Ino, 0, chunk)
				if err != nil {
					writerErrCh <- fmt.Errorf("worker %d Write tmp: %w", workerID, err)
					return
				}
				_, err = actor.Link(store.LinkReq{
					Ino:          tmpAttr.Ino,
					NewParentIno: dirIno,
					NewName:      linkName,
				})
				if err != nil {
					writerErrCh <- fmt.Errorf("worker %d Link tmp: %w", workerID, err)
					return
				}
				err = actor.Rename(store.RenameReq{
					OldParentIno: dirIno,
					OldName:      tmpName,
					NewParentIno: dirIno,
					NewName:      renamedName,
				})
				if err != nil {
					writerErrCh <- fmt.Errorf("worker %d Rename tmp: %w", workerID, err)
					return
				}
				err = actor.Unlink(dirIno, linkName, false)
				if err != nil {
					writerErrCh <- fmt.Errorf("worker %d Unlink link: %w", workerID, err)
					return
				}
				openH := step%2 == 0
				err = actor.Unlink(dirIno, renamedName, openH)
				if err != nil {
					writerErrCh <- fmt.Errorf("worker %d Unlink renamed: %w", workerID, err)
					return
				}
				if openH {
					err = actor.PurgeOrphanInode(tmpAttr.Ino)
					if err != nil {
						writerErrCh <- fmt.Errorf("worker %d PurgeOrphanInode: %w", workerID, err)
						return
					}
				}

				if step%5 == 1 {
					subName := fmt.Appendf(nil, "sub_%d", step)
					_, err = actor.Mkdir(store.MkdirReq{
						ParentIno: dirIno,
						Name:      subName,
						Mode:      store.S_IFDIR | 0o755,
					})
					if err != nil {
						writerErrCh <- fmt.Errorf("worker %d Mkdir: %w", workerID, err)
						return
					}
					err = actor.Rmdir(dirIno, subName)
					if err != nil {
						writerErrCh <- fmt.Errorf("worker %d Rmdir: %w", workerID, err)
						return
					}

					symName := fmt.Appendf(nil, "sym_%d", step)
					var symAttr store.Attribute
					symAttr, err = actor.Symlink(store.SymlinkReq{
						ParentIno: dirIno,
						Name:      symName,
						Target:    []byte("primary.bin"),
					})
					if err != nil {
						writerErrCh <- fmt.Errorf("worker %d Symlink: %w", workerID, err)
						return
					}
					_, err = actor.Readlink(symAttr.Ino)
					if err != nil {
						writerErrCh <- fmt.Errorf("worker %d Readlink: %w", workerID, err)
						return
					}
					err = actor.Unlink(dirIno, symName, false)
					if err != nil {
						writerErrCh <- fmt.Errorf("worker %d Unlink sym: %w", workerID, err)
						return
					}
				}

				if step%8 == 0 {
					err = actor.FsyncDir(dirIno)
					if err != nil {
						writerErrCh <- fmt.Errorf("worker %d FsyncDir: %w", workerID, err)
						return
					}
				}
				if workerID == 0 && step%6 == 3 {
					err = actor.CompactPacks()
					if err != nil {
						writerErrCh <- fmt.Errorf("worker %d CompactPacks: %w", workerID, err)
						return
					}
				}
			}
		}(w)
	}

	writerWG.Wait()
	stopReaders.Store(true)
	readerWG.Wait()

	close(writerErrCh)
	for err := range writerErrCh {
		require.NoError(t, err)
	}
	close(readerErrCh)
	for err := range readerErrCh {
		require.NoError(t, err)
	}

	require.NoError(t, actor.CleanOrphans())
	require.NoError(t, actor.CompactPacks())
	require.NoError(t, actor.FsyncDir(store.RootIno))

	// 压测结束后验证磁盘 packs/ 目录下的实际 .pack 文件集合与 Pebble DataKey 引用集 100% 精确一致（0 孤儿废包残留）
	auditDiskBlobsMatchPebbleDataKeys(t, actor)
}
