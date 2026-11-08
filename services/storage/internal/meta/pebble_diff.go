package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/cockroachdb/pebble/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

type diffRef struct {
	name     string
	isBranch bool
	head     pgtype.UUID
}

func (m *manager) resolveDiffRef(ctx context.Context, diskID pgtype.UUID, branches []storedBranch, ref string) (diffRef, error) {
	if i := branchIndex(branches, ref); i >= 0 {
		var head pgtype.UUID
		if branches[i].HeadCommitID != nil {
			var err error
			head, err = parseDiskID(*branches[i].HeadCommitID)
			if err != nil {
				return diffRef{}, err
			}
		}
		return diffRef{name: ref, isBranch: true, head: head}, nil
	}
	id, err := parseDiskID(ref)
	if err != nil {
		return diffRef{}, ErrNotFound
	}
	row, err := m.store.GetCommit(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && row.DiskID != diskID {
		return diffRef{}, ErrNotFound
	}
	if err != nil {
		return diffRef{}, err
	}
	return diffRef{name: id.String(), head: id}, nil
}

// Diff 比较磁盘的两个分支工作区或 Commit 快照。
func (m *manager) Diff(ctx context.Context, params DiffParams) (entries []DiffEntry, err error) {
	diskID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	if valErr := validateBranchName(params.FromRef); valErr != nil {
		return nil, valErr
	}
	if valErr := validateBranchName(params.ToRef); valErr != nil {
		return nil, valErr
	}
	// 与分支删除、回档和快照创建使用相同的锁顺序。
	m.writeQuotaMu.Lock()
	defer m.writeQuotaMu.Unlock()
	control, err := m.store.GetDiskControlPlane(ctx, diskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var branches []storedBranch
	if unmarshalErr := json.Unmarshal(control.Branches, &branches); unmarshalErr != nil {
		return nil, fmt.Errorf("读取分支树: %w", unmarshalErr)
	}
	from, err := m.resolveDiffRef(ctx, diskID, branches, params.FromRef)
	if err != nil {
		return nil, err
	}
	to, err := m.resolveDiffRef(ctx, diskID, branches, params.ToRef)
	if err != nil {
		return nil, err
	}
	if from == to {
		return []DiffEntry{}, nil
	}
	branchNames := make([]string, 0, 2)
	if from.isBranch {
		branchNames = append(branchNames, from.name)
	}
	if to.isBranch && (!from.isBranch || to.name != from.name) {
		branchNames = append(branchNames, to.name)
	}
	slices.Sort(branchNames)
	stores := make(map[string]pebbleDiffStore, len(branchNames))
	unlocks := make([]func(), 0, len(branchNames))
	openedBranches := make([]*pebblekv.Branch, 0, len(branchNames))
	defer func() {
		for _, openedBranche := range slices.Backward(openedBranches) {
			err = errors.Join(err, openedBranche.Close())
		}
		for _, unlock := range slices.Backward(unlocks) {
			unlock()
		}
	}()
	for _, name := range branchNames {
		unlocks = append(unlocks, m.locks.LockBranchRead(diskID.String(), name))
		branch, openErr := m.pebble.OpenBranch(diskID.String(), name)
		if openErr != nil {
			return nil, openErr
		}
		openedBranches = append(openedBranches, branch)
		stores[name] = pebbleDiffStore{db: branch.DB(), blobs: branch.Blobs()}
	}
	open := func(ref diffRef) (pebbleDiffStore, func() error, error) {
		if ref.isBranch {
			return stores[ref.name], func() error { return nil }, nil
		}
		commit, openErr := m.pebble.OpenCommit(diskID.String(), ref.name)
		if openErr != nil {
			return pebbleDiffStore{}, nil, openErr
		}
		return pebbleDiffStore{db: commit.DB(), blobs: commit.Blobs()}, commit.Close, nil
	}
	fromStore, closeFrom, err := open(from)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, closeFrom()) }()
	toStore, closeTo, err := open(to)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, closeTo()) }()
	dirty, err := m.diffDirtyInodes(ctx, diskID, from, to, stores)
	if err != nil {
		return nil, err
	}
	return m.diffStores(ctx, fromStore, toStore, dirty)
}

// pebbleDiffStore 把 Pebble 实例与所属目录的块文件存储绑定在一起。
type pebbleDiffStore struct {
	db    *pebble.DB
	blobs *pebblekv.BlobStore
}

func collectDiffDirty(ctx context.Context, store *pebble.DB, dirty map[uint64]bool) (err error) {
	lower, upper := pebblekv.DirtyRange()
	iter, err := store.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()
	for iter.First(); iter.Valid(); iter.Next() {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		ino, decodeErr := pebblekv.DecodeDirtyKey(iter.Key())
		if decodeErr != nil {
			return decodeErr
		}
		dirty[ino] = true
	}
	return iter.Error()
}

// 快照的标记属于它相对父版本的修改，共同祖先的标记不能用于本次比较。
func (m *manager) diffDirtyInodes(ctx context.Context, diskID pgtype.UUID, from, to diffRef, stores map[string]pebbleDiffStore) (map[uint64]bool, error) {
	dirty := make(map[uint64]bool)
	for _, ref := range []diffRef{from, to} {
		if ref.isBranch {
			if err := collectDiffDirty(ctx, stores[ref.name].db, dirty); err != nil {
				return nil, err
			}
		}
	}
	if from.head == to.head {
		return dirty, nil
	}
	ancestors := [2]map[pgtype.UUID]bool{make(map[pgtype.UUID]bool), make(map[pgtype.UUID]bool)}
	for i, ref := range []diffRef{from, to} {
		if !ref.head.Valid {
			continue
		}
		rows, err := m.store.ListCommitAncestors(ctx, db.ListCommitAncestorsParams{DiskID: diskID, CommitID: ref.head})
		if err != nil {
			return nil, err
		}
		for j := range rows {
			ancestors[i][rows[j].CommitID] = true
		}
	}
	for i, ids := range ancestors {
		for id := range ids {
			if ancestors[1-i][id] {
				continue
			}
			commit, err := m.pebble.OpenCommit(diskID.String(), id.String())
			if err != nil {
				return nil, err
			}
			err = errors.Join(collectDiffDirty(ctx, commit.DB(), dirty), commit.Close())
			if err != nil {
				return nil, err
			}
		}
	}
	return dirty, nil
}

type diffNode struct {
	inode pebblekv.Inode
}

// 仅从根目录可达的目录项进入结果，忽略孤儿节点。
func diffTree(ctx context.Context, store *pebble.DB) (map[string]*diffNode, error) {
	root, err := readPebbleInode(store, RootIno)
	if err != nil {
		return nil, err
	}
	result := map[string]*diffNode{".": {inode: root}}
	type pendingDir struct {
		path string
		ino  uint64
	}
	queue := []pendingDir{{ino: root.Ino}}
	visited := map[uint64]bool{root.Ino: true}
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		lower, upper := pebblekv.EntryRange(item.ino)
		iter, err := store.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			return nil, err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			if err = ctx.Err(); err != nil {
				break
			}
			_, name, e := pebblekv.DecodeEntryKey(iter.Key())
			if e != nil {
				err = e
				break
			}
			entry, e := pebblekv.DecodeEntry(iter.Value())
			if e != nil {
				err = e
				break
			}
			inode, e := readPebbleInode(store, entry.Ino)
			if e != nil {
				err = e
				break
			}
			if inode.Nlink == 0 {
				continue
			}
			path := string(name)
			if item.path != "" {
				path = item.path + "/" + path
			}
			result[path] = &diffNode{inode: inode}
			if inode.Mode&S_IFMT == S_IFDIR && !visited[inode.Ino] {
				visited[inode.Ino] = true
				queue = append(queue, pendingDir{path: path, ino: inode.Ino})
			}
		}
		err = errors.Join(err, iter.Error(), iter.Close())
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func diffXattrs(ctx context.Context, oldDB, newDB *pebble.DB, oldIno, newIno uint64) (different bool, err error) {
	oldLower, oldUpper := pebblekv.XattrRange(oldIno)
	newLower, newUpper := pebblekv.XattrRange(newIno)
	oldIter, err := oldDB.NewIter(&pebble.IterOptions{LowerBound: oldLower, UpperBound: oldUpper})
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, oldIter.Close()) }()
	newIter, err := newDB.NewIter(&pebble.IterOptions{LowerBound: newLower, UpperBound: newUpper})
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, newIter.Close()) }()
	oldValid, newValid := oldIter.First(), newIter.First()
	for oldValid && newValid {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		_, oldName, oldDecodeErr := pebblekv.DecodeXattrKey(oldIter.Key())
		if oldDecodeErr != nil {
			return false, oldDecodeErr
		}
		_, newName, newDecodeErr := pebblekv.DecodeXattrKey(newIter.Key())
		if newDecodeErr != nil {
			return false, newDecodeErr
		}
		if !bytes.Equal(oldName, newName) || !bytes.Equal(oldIter.Value(), newIter.Value()) {
			return true, nil
		}
		oldValid, newValid = oldIter.Next(), newIter.Next()
	}
	return oldValid != newValid, errors.Join(oldIter.Error(), newIter.Error())
}

func diffBlocks(ctx context.Context, oldStore, newStore pebbleDiffStore, oldIno, newIno uint64) (blocks []uint64, err error) {
	var oldIter, newIter *pebble.Iterator
	if oldIno != 0 {
		lower, upper := pebblekv.DataRange(oldIno, 0)
		oldIter, err = oldStore.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			return nil, err
		}
		defer func() { err = errors.Join(err, oldIter.Close()) }()
	}
	if newIno != 0 {
		lower, upper := pebblekv.DataRange(newIno, 0)
		newIter, err = newStore.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err != nil {
			return nil, err
		}
		defer func() { err = errors.Join(err, newIter.Close()) }()
	}
	oldValid, newValid := oldIter != nil && oldIter.First(), newIter != nil && newIter.First()
	for oldValid || newValid {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		var oldIdx, newIdx uint64
		if oldValid {
			_, oldIdx, err = pebblekv.DecodeDataKey(oldIter.Key())
			if err != nil {
				return nil, err
			}
		}
		if newValid {
			_, newIdx, err = pebblekv.DecodeDataKey(newIter.Key())
			if err != nil {
				return nil, err
			}
		}
		switch {
		case !newValid || oldValid && oldIdx < newIdx:
			blocks = append(blocks, oldIdx)
			oldValid = oldIter.Next()
		case !oldValid || newIdx < oldIdx:
			blocks = append(blocks, newIdx)
			newValid = newIter.Next()
		default:
			oldRef, oldDecodeErr := pebblekv.DecodeBlobRef(oldIter.Value())
			if oldDecodeErr != nil {
				return nil, oldDecodeErr
			}
			newRef, newDecodeErr := pebblekv.DecodeBlobRef(newIter.Value())
			if newDecodeErr != nil {
				return nil, newDecodeErr
			}
			// 块编号全局唯一且内容不可变，12B BlobRef 相等即可 O(1) 判定未变更。
			if oldRef.ID != newRef.ID || oldRef.Length != newRef.Length {
				equal, eqErr := pebbleBlobEqual(oldStore.blobs, oldRef, newStore.blobs, newRef)
				if eqErr != nil {
					return nil, eqErr
				}
				if !equal {
					blocks = append(blocks, oldIdx)
				}
			}
			oldValid, newValid = oldIter.Next(), newIter.Next()
		}
	}
	if oldIter != nil {
		err = errors.Join(err, oldIter.Error())
	}
	if newIter != nil {
		err = errors.Join(err, newIter.Error())
	}
	return blocks, err
}

func diffMetadata(oldInode, newInode *pebblekv.Inode) bool {
	if oldInode.Ino != newInode.Ino || oldInode.Mode != newInode.Mode || oldInode.UID != newInode.UID || oldInode.GID != newInode.GID || oldInode.Rdev != newInode.Rdev || oldInode.Flags != newInode.Flags || !bytes.Equal(oldInode.SymlinkTarget, newInode.SymlinkTarget) {
		return true
	}
	return oldInode.Mode&S_IFMT != S_IFDIR && (oldInode.WriteSeq != newInode.WriteSeq || oldInode.Size != newInode.Size)
}

func (m *manager) diffStores(ctx context.Context, oldStore, newStore pebbleDiffStore, dirty map[uint64]bool) ([]DiffEntry, error) {
	oldTree, err := diffTree(ctx, oldStore.db)
	if err != nil {
		return nil, err
	}
	newTree, err := diffTree(ctx, newStore.db)
	if err != nil {
		return nil, err
	}
	oldOnly, newOnly := make(map[uint64][]string), make(map[uint64][]string)
	for path, node := range oldTree {
		if _, ok := newTree[path]; !ok {
			oldOnly[node.inode.Ino] = append(oldOnly[node.inode.Ino], path)
		}
	}
	for path, node := range newTree {
		if _, ok := oldTree[path]; !ok {
			newOnly[node.inode.Ino] = append(newOnly[node.inode.Ino], path)
		}
	}
	matchedOld, matchedNew := make(map[string]bool), make(map[string]bool)
	entries := make([]DiffEntry, 0)
	makeEntry := func(kind DiffChangeType, path, oldPath string, oldInode, newInode *pebblekv.Inode) (DiffEntry, error) {
		entry := DiffEntry{ChangeType: kind, Path: path, OldPath: oldPath}
		if oldInode != nil {
			entry.OldAttr = m.newPebbleInode(oldInode)
		}
		if newInode != nil {
			entry.NewAttr = m.newPebbleInode(newInode)
		}
		var oldIno, newIno uint64
		if oldInode != nil && oldInode.Mode&S_IFMT == S_IFREG && kind != DiffChangeTypeDeleted {
			oldIno = oldInode.Ino
		}
		if newInode != nil && newInode.Mode&S_IFMT == S_IFREG {
			newIno = newInode.Ino
		}
		if newIno != 0 && (oldInode == nil || oldIno == 0 || oldInode.Ino != newInode.Ino || oldInode.WriteSeq != newInode.WriteSeq || oldInode.Size != newInode.Size || dirty[oldIno] || oldInode.MtimeSec != newInode.MtimeSec || oldInode.MtimeNsec != newInode.MtimeNsec || oldInode.CtimeSec != newInode.CtimeSec || oldInode.CtimeNsec != newInode.CtimeNsec) || oldIno != 0 && newIno == 0 {
			blocks, blocksErr := diffBlocks(ctx, oldStore, newStore, oldIno, newIno)
			if blocksErr != nil {
				return DiffEntry{}, blocksErr
			}
			entry.ModifiedBlockIndices = blocks
		}
		return entry, nil
	}
	for ino, oldPaths := range oldOnly {
		newPaths := newOnly[ino]
		slices.Sort(oldPaths)
		slices.Sort(newPaths)
		for i := 0; i < min(len(oldPaths), len(newPaths)); i++ {
			oldPath, newPath := oldPaths[i], newPaths[i]
			oldNode, newNode := &oldTree[oldPath].inode, &newTree[newPath].inode
			entry, entryErr := makeEntry(DiffChangeTypeRenamed, newPath, oldPath, oldNode, newNode)
			if entryErr != nil {
				return nil, entryErr
			}
			entries = append(entries, entry)
			matchedOld[oldPath], matchedNew[newPath] = true, true
		}
	}
	for path, oldNode := range oldTree {
		newNode, ok := newTree[path]
		if !ok {
			if !matchedOld[path] {
				entry, entryErr := makeEntry(DiffChangeTypeDeleted, path, "", &oldNode.inode, nil)
				if entryErr != nil {
					return nil, entryErr
				}
				entries = append(entries, entry)
			}
			continue
		}
		changed := diffMetadata(&oldNode.inode, &newNode.inode)
		if !changed {
			changed, err = diffXattrs(ctx, oldStore.db, newStore.db, oldNode.inode.Ino, newNode.inode.Ino)
			if err != nil {
				return nil, err
			}
		}
		regular := oldNode.inode.Mode&S_IFMT == S_IFREG && newNode.inode.Mode&S_IFMT == S_IFREG
		checkBlocks := regular && (dirty[oldNode.inode.Ino] || oldNode.inode.MtimeSec != newNode.inode.MtimeSec || oldNode.inode.MtimeNsec != newNode.inode.MtimeNsec || oldNode.inode.CtimeSec != newNode.inode.CtimeSec || oldNode.inode.CtimeNsec != newNode.inode.CtimeNsec)
		if changed || checkBlocks {
			entry, entryErr := makeEntry(DiffChangeTypeModified, path, "", &oldNode.inode, &newNode.inode)
			if entryErr != nil {
				return nil, entryErr
			}
			if changed || len(entry.ModifiedBlockIndices) > 0 {
				entries = append(entries, entry)
			}
		}
	}
	for path, node := range newTree {
		if _, ok := oldTree[path]; !ok && !matchedNew[path] {
			entry, entryErr := makeEntry(DiffChangeTypeAdded, path, "", nil, &node.inode)
			if entryErr != nil {
				return nil, entryErr
			}
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries, nil
}
