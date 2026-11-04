package meta

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble/v2"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

type mergeEntry struct {
	parent uint64
	name   string
}

type mergeInode struct {
	value  pebblekv.Inode
	exists bool
}

type mergedDirectory struct {
	parent uint64
	live   bool
}

func checkMergedDirectoryParents(store *pebble.DB, dirs map[uint64]mergedDirectory) (uint64, error) {
	for start, dir := range dirs {
		if !dir.live {
			continue
		}
		seen := make(map[uint64]bool)
		for ino := start; ino != RootIno; {
			if seen[ino] {
				return start, nil
			}
			seen[ino] = true
			if current, ok := dirs[ino]; ok {
				if !current.live {
					return start, nil
				}
				ino = current.parent
				continue
			}
			inode, err := getMergeInode(store, ino)
			if err != nil {
				return 0, err
			}
			if !inode.exists || inode.value.Mode&S_IFMT != S_IFDIR {
				return start, nil
			}
			ino = inode.value.ParentIno
		}
	}
	return 0, nil
}

func getMergeInode(store *pebble.DB, ino uint64) (mergeInode, error) {
	inode, err := readPebbleInode(store, ino)
	if errors.Is(err, ErrNotFound) || err == nil && inode.Nlink == 0 {
		return mergeInode{}, nil
	}
	return mergeInode{value: inode, exists: err == nil}, err
}

func mergeEntries(store *pebble.DB, dir uint64) (entries map[string]pebblekv.Entry, err error) {
	lower, upper := pebblekv.EntryRange(dir)
	iter, err := store.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()
	entries = make(map[string]pebblekv.Entry)
	for iter.First(); iter.Valid(); iter.Next() {
		_, name, err := pebblekv.DecodeEntryKey(iter.Key())
		if err != nil {
			return nil, err
		}
		entry, err := pebblekv.DecodeEntry(iter.Value())
		if err != nil {
			return nil, err
		}
		entries[string(name)] = entry
	}
	return entries, iter.Error()
}

// 比较实际文件内容，忽略由目录操作产生的链接数、时间和写序列差异。
func mergeComparableInode(inode pebblekv.Inode) pebblekv.Inode {
	inode.Ino, inode.ParentIno, inode.Generation, inode.WriteSeq = 0, 0, 0, 0
	inode.Nlink = 0
	inode.AtimeSec, inode.MtimeSec, inode.CtimeSec = 0, 0, 0
	inode.AtimeNsec, inode.MtimeNsec, inode.CtimeNsec = 0, 0, 0
	return inode
}

func mergeTimesEqual(a, b pebblekv.Inode) bool {
	return a.AtimeSec == b.AtimeSec && a.AtimeNsec == b.AtimeNsec &&
		a.MtimeSec == b.MtimeSec && a.MtimeNsec == b.MtimeNsec
}

func mergeRangeEqual(a *pebble.DB, aBlobs *pebblekv.BlobStore, aLower, aUpper []byte, b *pebble.DB, bBlobs *pebblekv.BlobStore, bLower, bUpper []byte, data bool) (equal bool, err error) {
	ai, err := a.NewIter(&pebble.IterOptions{LowerBound: aLower, UpperBound: aUpper})
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, ai.Close()) }()
	bi, err := b.NewIter(&pebble.IterOptions{LowerBound: bLower, UpperBound: bUpper})
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, bi.Close()) }()
	av, bv := ai.First(), bi.First()
	for av && bv {
		if !bytes.Equal(ai.Key()[len(aLower):], bi.Key()[len(bLower):]) {
			return false, nil
		}
		left, right := ai.Value(), bi.Value()
		if data {
			// 块编号全局唯一且内容不可变，12B BlobRef 相等即可 O(1) 短路。
			aRef, err := pebblekv.DecodeBlobRef(left)
			if err != nil {
				return false, err
			}
			bRef, err := pebblekv.DecodeBlobRef(right)
			if err != nil {
				return false, err
			}
			if aRef.ID != bRef.ID || aRef.Length != bRef.Length {
				same, err := pebbleBlobEqual(aBlobs, aRef, bBlobs, bRef)
				if err != nil || !same {
					return same, err
				}
			}
		} else if !bytes.Equal(left, right) {
			return false, nil
		}
		av, bv = ai.Next(), bi.Next()
	}
	return !av && !bv, errors.Join(ai.Error(), bi.Error())
}

func mergeInodesEqual(a *pebble.DB, aBlobs *pebblekv.BlobStore, ai mergeInode, b *pebble.DB, bBlobs *pebblekv.BlobStore, bi mergeInode) (bool, error) {
	if !ai.exists || !bi.exists {
		return ai.exists == bi.exists, nil
	}
	if !reflect.DeepEqual(mergeComparableInode(ai.value), mergeComparableInode(bi.value)) {
		return false, nil
	}
	al, au := pebblekv.XattrRange(ai.value.Ino)
	bl, bu := pebblekv.XattrRange(bi.value.Ino)
	equal, err := mergeRangeEqual(a, aBlobs, al, au, b, bBlobs, bl, bu, false)
	if err != nil || !equal {
		return equal, err
	}
	al, au = pebblekv.DataRange(ai.value.Ino, 0)
	bl, bu = pebblekv.DataRange(bi.value.Ino, 0)
	// 数据键的下界含块编号，仅前缀应被排除。
	return mergeRangeEqual(a, aBlobs, al[:len(al)-8], au, b, bBlobs, bl[:len(bl)-8], bu, true)
}

func copyMergeRange(batch *pebble.Batch, source *pebble.DB, lower, upper []byte) (err error) {
	if err := batch.DeleteRange(lower, upper, nil); err != nil {
		return err
	}
	iter, err := source.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Set(iter.Key(), iter.Value(), nil); err != nil {
			return err
		}
	}
	return iter.Error()
}

func mergePath(store *pebble.DB, wanted uint64) (string, error) {
	if wanted == RootIno {
		return "/", nil
	}
	type node struct {
		ino  uint64
		path string
	}
	queue := []node{{RootIno, ""}}
	seen := map[uint64]bool{RootIno: true}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		entries, err := mergeEntries(store, current.ino)
		if err != nil {
			return "", err
		}
		names := make([]string, 0, len(entries))
		for name := range entries {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			entry := entries[name]
			path := current.path + "/" + name
			if entry.Ino == wanted {
				return path, nil
			}
			if entry.Mode&S_IFMT == S_IFDIR && !seen[entry.Ino] {
				seen[entry.Ino] = true
				queue = append(queue, node{entry.Ino, path})
			}
		}
	}
	return "", nil
}

func mergePathInode(store *pebble.DB, path string) (mergeInode, error) {
	ino := uint64(RootIno)
	if path == "/" {
		return getMergeInode(store, ino)
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if part == "" {
			return mergeInode{}, ErrInvalidArgument
		}
		value, closer, err := store.Get(pebblekv.EntryKey(ino, []byte(part)))
		if errors.Is(err, pebble.ErrNotFound) {
			return mergeInode{}, nil
		}
		if err != nil {
			return mergeInode{}, err
		}
		entry, decodeErr := pebblekv.DecodeEntry(value)
		if err := errors.Join(decodeErr, closer.Close()); err != nil {
			return mergeInode{}, err
		}
		ino = entry.Ino
	}
	return getMergeInode(store, ino)
}

func (m *manager) mergeConflict(stores [3]*pebble.DB, ino uint64, path, reason string) (MergeConflict, error) {
	c := MergeConflict{Path: path, Reason: reason}
	attrs := []**Inode{&c.BaseAttr, &c.OursAttr, &c.TheirsAttr}
	for i, store := range stores {
		inode, err := getMergeInode(store, ino)
		if err != nil {
			return c, err
		}
		if inode.exists {
			*attrs[i] = m.newPebbleInode(inode.value)
		}
		if c.Path == "" {
			c.Path, err = mergePath(store, ino)
			if err != nil {
				return c, err
			}
		}
	}
	if c.Path == "" {
		c.Path = "/"
	}
	// 同一路径上的节点可能被一侧替换为另一个 Inode。
	for i, store := range stores {
		if *attrs[i] != nil || c.Path == "/" {
			continue
		}
		node, err := mergePathInode(store, c.Path)
		if err != nil {
			return c, err
		}
		if node.exists {
			*attrs[i] = m.newPebbleInode(node.value)
		}
	}
	return c, nil
}

// mergeWorkspace 比较候选节点和受影响目录；未修改节点不会参与扫描。
func (m *manager) mergeWorkspace(stage *pebble.DB, stageBlobs *pebblekv.BlobStore, stores [3]*pebble.DB, blobs [3]*pebblekv.BlobStore, dirty map[uint64]bool, strategy ConflictStrategy) (sizes map[uint64]uint64, conflicts []MergeConflict, err error) {
	return m.mergeWorkspaceWithForced(stage, stageBlobs, stores, blobs, dirty, strategy, nil)
}

func (m *manager) mergeWorkspaceWithForced(stage *pebble.DB, stageBlobs *pebblekv.BlobStore, stores [3]*pebble.DB, blobs [3]*pebblekv.BlobStore, dirty map[uint64]bool, strategy ConflictStrategy, forcedInodes map[uint64]bool) (sizes map[uint64]uint64, conflicts []MergeConflict, err error) {
	abort := strategy == ConflictStrategyUnspecified || strategy == ConflictStrategyAbortOnConflict
	conflictedInodes := make(map[uint64]bool)
	for ino := range forcedInodes {
		conflict, err := m.mergeConflict(stores, ino, "", "目录移动导致父目录循环或丢失")
		if err != nil {
			return nil, nil, err
		}
		conflicts = append(conflicts, conflict)
		conflictedInodes[ino] = true
	}
	entries := make(map[mergeEntry][3]pebblekv.Entry)
	exists := make(map[mergeEntry][3]bool)
	directories := make(map[uint64][3]bool)
	for ino := range dirty {
		isDir := false
		var present [3]bool
		for side, store := range stores {
			inode, err := getMergeInode(store, ino)
			if err != nil {
				return nil, nil, err
			}
			present[side] = inode.exists && inode.value.Mode&S_IFMT == S_IFDIR
			isDir = isDir || present[side]
		}
		if !isDir {
			continue
		}
		directories[ino] = present
		for side, store := range stores {
			items, err := mergeEntries(store, ino)
			if err != nil {
				return nil, nil, err
			}
			for name, entry := range items {
				key := mergeEntry{ino, name}
				triple, present := entries[key], exists[key]
				triple[side], present[side] = entry, true
				entries[key], exists[key] = triple, present
			}
		}
	}
	keys := make([]mergeEntry, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].parent != keys[j].parent {
			return keys[i].parent < keys[j].parent
		}
		return keys[i].name < keys[j].name
	})
	removedBoth, oursAdded, theirsAdded := make(map[uint64]bool), make(map[uint64]bool), make(map[uint64]bool)
	for _, key := range keys {
		v, e := entries[key], exists[key]
		if e[0] && (!e[1] || v[1].Ino != v[0].Ino) && (!e[2] || v[2].Ino != v[0].Ino) {
			removedBoth[v[0].Ino] = true
		}
		if e[1] && (!e[0] || v[1].Ino != v[0].Ino) && (!e[2] || v[1].Ino != v[2].Ino) {
			oursAdded[v[1].Ino] = true
		}
		if e[2] && (!e[0] || v[2].Ino != v[0].Ino) && (!e[1] || v[2].Ino != v[1].Ino) {
			theirsAdded[v[2].Ino] = true
		}
	}
	for ino := range removedBoth {
		if oursAdded[ino] && theirsAdded[ino] && !conflictedInodes[ino] {
			conflict, err := m.mergeConflict(stores, ino, "", "节点被移动到不同位置")
			if err != nil {
				return nil, nil, err
			}
			conflicts = append(conflicts, conflict)
			conflictedInodes[ino] = true
		}
	}
	// 同一路径独立创建的相同文件统一使用源分支编号，方便后续再次合并。
	identical := make(map[mergeEntry]bool)
	type inodePair struct{ ours, theirs uint64 }
	pairs := make(map[mergeEntry]inodePair)
	counts := make(map[inodePair]uint32)
	links := make(map[inodePair]uint32)
	oursPairs := make(map[uint64]inodePair)
	theirsPairs := make(map[uint64]inodePair)
	invalidPairs := make(map[inodePair]bool)
	for _, key := range keys {
		v, e := entries[key], exists[key]
		if e[0] || !e[1] || !e[2] || v[1].Ino == v[2].Ino {
			continue
		}
		o, err := getMergeInode(stores[1], v[1].Ino)
		if err != nil {
			return nil, nil, err
		}
		t, err := getMergeInode(stores[2], v[2].Ino)
		if err != nil {
			return nil, nil, err
		}
		baseO, err := getMergeInode(stores[0], v[1].Ino)
		if err != nil {
			return nil, nil, err
		}
		baseT, err := getMergeInode(stores[0], v[2].Ino)
		if err != nil {
			return nil, nil, err
		}
		equal, err := mergeInodesEqual(stores[1], blobs[1], o, stores[2], blobs[2], t)
		if err != nil {
			return nil, nil, err
		}
		if equal && !baseO.exists && !baseT.exists && o.value.Mode&S_IFMT != S_IFDIR && o.value.Nlink == t.value.Nlink {
			pair := inodePair{v[1].Ino, v[2].Ino}
			if previous, ok := oursPairs[pair.ours]; ok && previous != pair {
				invalidPairs[previous], invalidPairs[pair] = true, true
			}
			if previous, ok := theirsPairs[pair.theirs]; ok && previous != pair {
				invalidPairs[previous], invalidPairs[pair] = true, true
			}
			oursPairs[pair.ours], theirsPairs[pair.theirs] = pair, pair
			pairs[key], links[pair] = pair, o.value.Nlink
			counts[pair]++
		}
	}
	for key, pair := range pairs {
		identical[key] = !invalidPairs[pair] && counts[pair] == links[pair]
	}
	// 先识别节点冲突，再决定目录项，以免删除或移动与另一侧的修改交叉应用。
	ids := make([]uint64, 0, len(dirty))
	for ino := range dirty {
		ids = append(ids, ino)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, ino := range ids {
		var state [3]mergeInode
		for side := range stores {
			state[side], err = getMergeInode(stores[side], ino)
			if err != nil {
				return nil, nil, err
			}
		}
		bo, err := mergeInodesEqual(stores[0], blobs[0], state[0], stores[1], blobs[1], state[1])
		if err != nil {
			return nil, nil, err
		}
		bt, err := mergeInodesEqual(stores[0], blobs[0], state[0], stores[2], blobs[2], state[2])
		if err != nil {
			return nil, nil, err
		}
		ot, err := mergeInodesEqual(stores[1], blobs[1], state[1], stores[2], blobs[2], state[2])
		if err != nil {
			return nil, nil, err
		}
		timeConflict := false
		if state[0].exists && state[1].exists && state[2].exists && state[0].value.Mode&S_IFMT != S_IFDIR {
			timeConflict = (!bo && bt && !mergeTimesEqual(state[0].value, state[2].value)) ||
				(bo && !bt && !mergeTimesEqual(state[0].value, state[1].value)) ||
				(bo && bt && !mergeTimesEqual(state[0].value, state[1].value) && !mergeTimesEqual(state[0].value, state[2].value) && !mergeTimesEqual(state[1].value, state[2].value))
		}
		if (!bo && !bt && !ot || timeConflict) && !conflictedInodes[ino] {
			conflict, err := m.mergeConflict(stores, ino, "", "节点被双方修改")
			if err != nil {
				return nil, nil, err
			}
			conflicts = append(conflicts, conflict)
			conflictedInodes[ino] = true
		}
	}
	conflictedDirectories := make(map[uint64]bool)
	for _, key := range keys {
		v, e := entries[key], exists[key]
		same := func(a, b int) bool { return e[a] == e[b] && (!e[a] || v[a].Ino == v[b].Ino) }
		dirs := directories[key.parent]
		if dirs[0] && (!dirs[1] && (!same(0, 2) || e[2] && conflictedInodes[v[2].Ino]) ||
			!dirs[2] && (!same(0, 1) || e[1] && conflictedInodes[v[1].Ino])) {
			if !conflictedInodes[key.parent] {
				conflict, err := m.mergeConflict(stores, key.parent, "", "目录被删除，同时包含另一侧的变更")
				if err != nil {
					return nil, nil, err
				}
				conflicts = append(conflicts, conflict)
			}
			conflictedInodes[key.parent] = true
			conflictedDirectories[key.parent] = true
		}
	}
	for _, key := range keys {
		v, e := entries[key], exists[key]
		same := func(a, b int) bool { return e[a] == e[b] && (!e[a] || v[a].Ino == v[b].Ino) }
		if same(1, 2) {
			continue
		}
		conflict := !same(0, 1) && !same(0, 2) && !identical[key]
		var first uint64
		different := false
		for side := range stores {
			conflict = conflict || e[side] && conflictedInodes[v[side].Ino]
			if e[side] {
				if first != 0 && first != v[side].Ino {
					different = true
				}
				first = v[side].Ino
			}
		}
		if conflict && different {
			for side := range stores {
				if e[side] && v[side].Mode&S_IFMT == S_IFDIR {
					conflictedDirectories[v[side].Ino] = true
				}
			}
		}
	}
	// 目录删除冲突须同时选定其子树，避免留下不可访问的节点。
	for changed := true; changed; {
		changed = false
		for _, key := range keys {
			if !conflictedDirectories[key.parent] {
				continue
			}
			v, e := entries[key], exists[key]
			for side := range stores {
				if !e[side] {
					continue
				}
				conflictedInodes[v[side].Ino] = true
				if v[side].Mode&S_IFMT == S_IFDIR && !conflictedDirectories[v[side].Ino] {
					conflictedDirectories[v[side].Ino] = true
					changed = true
				}
			}
		}
	}
	batch := stage.NewBatch()
	defer func() { err = errors.Join(err, batch.Close()) }()
	var removedBlobs []uint64
	linkDelta := make(map[uint64]int64)
	dirDelta := make(map[uint64]int64)
	parent := make(map[uint64]uint64)
	changed := make(map[uint64]bool)
	for _, key := range keys {
		v, e := entries[key], exists[key]
		same := func(a, b int) bool { return e[a] == e[b] && (!e[a] || v[a].Ino == v[b].Ino) }
		forced := conflictedDirectories[key.parent]
		for side := range stores {
			forced = forced || e[side] && conflictedInodes[v[side].Ino]
		}
		if forced && strategy != ConflictStrategyTheirs {
			continue
		}
		if same(1, 2) || !forced && same(0, 2) {
			continue
		}
		if !forced && !same(0, 1) && !identical[key] {
			path, err := mergePath(stores[1], key.parent)
			if err != nil {
				return nil, nil, err
			}
			if path == "" {
				path, err = mergePath(stores[2], key.parent)
				if err != nil {
					return nil, nil, err
				}
			}
			if path == "/" {
				path = ""
			}
			conflict := MergeConflict{Path: path + "/" + key.name, Reason: "目录项被双方修改"}
			attrs := []**Inode{&conflict.BaseAttr, &conflict.OursAttr, &conflict.TheirsAttr}
			for side := range stores {
				if !e[side] {
					continue
				}
				inode, err := getMergeInode(stores[side], v[side].Ino)
				if err != nil {
					return nil, nil, err
				}
				if inode.exists {
					*attrs[side] = m.newPebbleInode(inode.value)
				}
			}
			conflicts = append(conflicts, conflict)
			if strategy != ConflictStrategyTheirs {
				continue
			}
		}
		if e[1] {
			dirty[v[1].Ino] = true
			linkDelta[v[1].Ino]--
			if v[1].Mode&S_IFMT == S_IFDIR {
				dirDelta[key.parent]--
			}
		}
		if e[2] {
			dirty[v[2].Ino] = true
			linkDelta[v[2].Ino]++
			if v[2].Mode&S_IFMT == S_IFDIR {
				dirDelta[key.parent]++
				parent[v[2].Ino] = key.parent
			}
			if err := batch.Set(pebblekv.EntryKey(key.parent, []byte(key.name)), pebblekv.EncodeEntry(v[2]), nil); err != nil {
				return nil, nil, err
			}
		} else if err := batch.Delete(pebblekv.EntryKey(key.parent, []byte(key.name)), nil); err != nil {
			return nil, nil, err
		}
		changed[key.parent] = true
	}
	if len(conflicts) > 0 && abort {
		sort.Slice(conflicts, func(i, j int) bool { return conflicts[i].Path < conflicts[j].Path })
		return nil, conflicts, nil
	}
	stat, err := readCreateStat(stage)
	if err != nil {
		return nil, nil, err
	}
	for _, store := range stores {
		s, err := readCreateStat(store)
		if err != nil {
			return nil, nil, err
		}
		stat.NextWriteSeq = max(stat.NextWriteSeq, s.NextWriteSeq)
	}
	if stat.NextWriteSeq == math.MaxUint64 {
		return nil, nil, ErrNoSpace
	}
	sequence := stat.NextWriteSeq
	stat.NextWriteSeq++
	ids = make([]uint64, 0, len(dirty))
	for ino := range dirty {
		ids = append(ids, ino)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	sizes = make(map[uint64]uint64)
	mergedDirs := make(map[uint64]mergedDirectory)
	for _, ino := range ids {
		var state [3]mergeInode
		for i, store := range stores {
			state[i], err = getMergeInode(store, ino)
			if err != nil {
				return nil, nil, err
			}
		}
		bo, err := mergeInodesEqual(stores[0], blobs[0], state[0], stores[1], blobs[1], state[1])
		if err != nil {
			return nil, nil, err
		}
		bt, err := mergeInodesEqual(stores[0], blobs[0], state[0], stores[2], blobs[2], state[2])
		if err != nil {
			return nil, nil, err
		}
		chosen := state[1]
		copySource := !bt && bo
		if conflictedInodes[ino] {
			copySource = strategy == ConflictStrategyTheirs && state[2].exists
			if strategy == ConflictStrategyTheirs {
				chosen = state[2]
			}
		}
		if !conflictedInodes[ino] && bo && bt && state[0].exists && state[1].exists && state[2].exists &&
			mergeTimesEqual(state[0].value, state[1].value) && !mergeTimesEqual(state[0].value, state[2].value) {
			copySource = true
		}
		if copySource {
			chosen = state[2]
		}
		// 目录的父节点由目录项决定，复制属性不能覆盖目标侧的移动。
		if chosen.exists && state[1].exists && chosen.value.Mode&S_IFMT == S_IFDIR {
			chosen.value.ParentIno = state[1].value.ParentIno
		}
		var live bool
		var links int64
		isDir := state[1].exists && state[1].value.Mode&S_IFMT == S_IFDIR || chosen.exists && chosen.value.Mode&S_IFMT == S_IFDIR
		if isDir {
			refs := linkDelta[ino]
			if state[1].exists {
				refs++
			}
			if ino == RootIno {
				refs = 1
			}
			if refs > 1 {
				conflict, err := m.mergeConflict(stores, ino, "", "目录被移动到多个位置")
				if err != nil {
					return nil, nil, err
				}
				if abort {
					return nil, append(conflicts, conflict), nil
				}
				if forcedInodes[ino] {
					return nil, nil, ErrInvalidArgument
				}
				next := make(map[uint64]bool, len(forcedInodes)+1)
				for id := range forcedInodes {
					next[id] = true
				}
				next[ino] = true
				return m.mergeWorkspaceWithForced(stage, stageBlobs, stores, blobs, dirty, strategy, next)
			}
			live = refs > 0
			links = 2 + dirDelta[ino]
			if state[1].exists {
				links = int64(state[1].value.Nlink) + dirDelta[ino]
			}
		} else {
			links = linkDelta[ino]
			if state[1].exists {
				links += int64(state[1].value.Nlink)
			}
			live = links > 0
		}
		if links < 0 || links > math.MaxUint32 || live && !chosen.exists {
			return nil, nil, fmt.Errorf("合并节点 %d 的链接无效", ino)
		}
		if isDir {
			parentIno := chosen.value.ParentIno
			if p, ok := parent[ino]; ok {
				parentIno = p
			}
			mergedDirs[ino] = mergedDirectory{parent: parentIno, live: live}
		}
		if state[1].exists {
			stat.Inodes--
			stat.LogicalBytes -= min(stat.LogicalBytes, state[1].value.Size)
		}
		if !live {
			if state[1].exists {
				if err := removeTopologyInode(stage, batch, &pebblekv.BranchStat{}, state[1].value, &removedBlobs); err != nil {
					return nil, nil, err
				}
				changed[ino] = true
			}
			continue
		}
		chosen.value.Nlink = uint32(links)
		if p, ok := parent[ino]; ok {
			if chosen.value.ParentIno != p {
				changed[ino] = true
			}
			chosen.value.ParentIno = p
		}
		if copySource {
			lower, upper := pebblekv.XattrRange(ino)
			if err := copyMergeRange(batch, stores[2], lower, upper); err != nil {
				return nil, nil, err
			}
			lower, upper = pebblekv.DataRange(ino, 0)
			if err := copyMergeRange(batch, stores[2], lower, upper); err != nil {
				return nil, nil, err
			}
		}
		if copySource || linkDelta[ino] != 0 || dirDelta[ino] != 0 || changed[ino] {
			chosen.value.WriteSeq = sequence
			if err := setCreateInode(batch, chosen.value); err != nil {
				return nil, nil, err
			}
			changed[ino] = true
		}
		if chosen.value.Size > math.MaxUint64-stat.LogicalBytes {
			return nil, nil, ErrNoSpace
		}
		stat.LogicalBytes += chosen.value.Size
		stat.Inodes++
		sizes[ino] = chosen.value.Size
	}
	if len(conflicts) > 0 && abort {
		return nil, conflicts, nil
	}
	invalidDir, err := checkMergedDirectoryParents(stores[1], mergedDirs)
	if err != nil {
		return nil, nil, err
	}
	if invalidDir != 0 {
		conflict, err := m.mergeConflict(stores, invalidDir, "", "目录移动导致父目录循环或丢失")
		if err != nil {
			return nil, nil, err
		}
		if abort {
			return nil, append(conflicts, conflict), nil
		}
		next := make(map[uint64]bool, len(forcedInodes)+len(mergedDirs))
		for id := range forcedInodes {
			next[id] = true
		}
		seen := make(map[uint64]bool)
		for ino := invalidDir; ino != RootIno && ino != 0 && !seen[ino]; {
			seen[ino] = true
			if dir, ok := mergedDirs[ino]; ok {
				next[ino] = true
				ino = dir.parent
				continue
			}
			node, err := getMergeInode(stores[1], ino)
			if err != nil {
				return nil, nil, err
			}
			if !node.exists {
				break
			}
			ino = node.value.ParentIno
		}
		if len(next) == len(forcedInodes) {
			return nil, nil, ErrInvalidArgument
		}
		return m.mergeWorkspaceWithForced(stage, stageBlobs, stores, blobs, dirty, strategy, next)
	}
	for ino := range changed {
		if err := batch.Set(pebblekv.DirtyKey(ino), []byte{1}, nil); err != nil {
			return nil, nil, err
		}
	}
	if err := batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(stat), nil); err != nil {
		return nil, nil, err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return nil, nil, err
	}
	if err := pebbleRemoveBlobs(stageBlobs, removedBlobs); err != nil {
		return nil, nil, err
	}
	sort.Slice(conflicts, func(i, j int) bool { return conflicts[i].Path < conflicts[j].Path })
	return sizes, conflicts, nil
}
