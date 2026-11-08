package meta

import (
	"errors"
	"math"
	"strings"

	"github.com/cockroachdb/pebble/v2"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

// copyMergeSidecarRange 将快照中的节点数据复制到新编号。
func copyMergeSidecarRange(batch *pebble.Batch, source *pebble.DB, lower, upper, targetPrefix []byte) (err error) {
	iter, err := source.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()
	for iter.First(); iter.Valid(); iter.Next() {
		key := append(append([]byte(nil), targetPrefix...), iter.Key()[len(lower):]...)
		if setErr := batch.Set(key, iter.Value(), nil); setErr != nil {
			return setErr
		}
	}
	return iter.Error()
}

func mergeSidecarParent(store *pebble.DB, path string) (parent uint64, name string, err error) {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) == 0 || parts[len(parts)-1] == "" {
		return 0, "", ErrInvalidArgument
	}
	parent = RootIno
	for _, part := range parts[:len(parts)-1] {
		value, closer, getErr := store.Get(pebblekv.EntryKey(parent, []byte(part)))
		if errors.Is(getErr, pebble.ErrNotFound) {
			return 0, "", ErrNotFound
		}
		if getErr != nil {
			return 0, "", getErr
		}
		entry, decodeErr := pebblekv.DecodeEntry(value)
		if closeErr := errors.Join(decodeErr, closer.Close()); closeErr != nil {
			return 0, "", closeErr
		}
		if entry.Mode&S_IFMT != S_IFDIR {
			return 0, "", ErrNotDir
		}
		parent = entry.Ino
	}
	return parent, parts[len(parts)-1], nil
}

func mergeMaterializedPath(stage *pebble.DB, stores [3]*pebble.DB, conflict MergeConflict) (string, error) {
	if conflict.OursAttr != nil {
		node, err := mergePathInode(stage, conflict.Path)
		if err != nil {
			return "", err
		}
		if node.exists && node.value.Ino == conflict.OursAttr.Ino {
			return conflict.Path, nil
		}
		path, err := mergePath(stage, conflict.OursAttr.Ino)
		if err != nil {
			return "", err
		}
		if path != "" {
			return path, nil
		}
	}
	for _, side := range []int{0, 1, 2} {
		parent, name, err := mergeSidecarParent(stores[side], conflict.Path)
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotDir) {
			continue
		}
		if err != nil {
			return "", err
		}
		node, err := getMergeInode(stage, parent)
		if err != nil {
			return "", err
		}
		if !node.exists || node.value.Mode&S_IFMT != S_IFDIR {
			continue
		}
		parentPath, err := mergePath(stage, parent)
		if err != nil {
			return "", err
		}
		if parentPath != "" {
			return strings.TrimSuffix(parentPath, "/") + "/" + name, nil
		}
	}
	return conflict.Path, nil
}

func (m *manager) materializeMergeConflicts(disk string, stage *pebble.DB, stores [3]*pebble.DB, conflicts []MergeConflict) (paths []string, inodes []uint64, err error) {
	stat, err := readCreateStat(stage)
	if err != nil {
		return nil, nil, err
	}
	if stat.NextWriteSeq == math.MaxUint64 {
		return nil, nil, ErrNoSpace
	}
	sequence := stat.NextWriteSeq
	stat.NextWriteSeq++
	batch := stage.NewBatch()
	defer func() { err = errors.Join(err, batch.Close()) }()
	created := make(map[string]bool)
	parents := make(map[uint64]bool)
	for i := range conflicts {
		conflict := &conflicts[i]
		var path string
		for _, version := range []struct {
			attr   *Inode
			side   int
			suffix string
		}{{conflict.TheirsAttr, 2, ".theirs"}, {conflict.BaseAttr, 0, ".base"}} {
			if version.attr == nil {
				continue
			}
			node, getErr := getMergeInode(stores[version.side], version.attr.Ino)
			if getErr != nil {
				return nil, nil, getErr
			}
			if !node.exists || node.value.Mode&S_IFMT != S_IFREG && node.value.Mode&S_IFMT != S_IFLNK {
				continue
			}
			if path == "" {
				path, err = mergeMaterializedPath(stage, stores, *conflict)
				if err != nil {
					return nil, nil, err
				}
				conflict.Path = path
			}
			parent, name, parentErr := mergeSidecarParent(stage, path)
			if parentErr != nil {
				return nil, nil, parentErr
			}
			name += version.suffix
			if valErr := validateEntryName([]byte(name)); valErr != nil {
				return nil, nil, valErr
			}
			sidecarPath := path + version.suffix
			if created[sidecarPath] {
				continue
			}
			if entryErr := checkCreateEntry(stage, parent, []byte(name)); entryErr != nil {
				return nil, nil, entryErr
			}
			if stat.Inodes == math.MaxUint64 || node.value.Size > math.MaxUint64-stat.LogicalBytes {
				return nil, nil, ErrNoSpace
			}
			ino, allocErr := m.pebble.AllocateInode(disk)
			if allocErr != nil {
				return nil, nil, allocErr
			}
			original := node.value.Ino
			node.value.Ino, node.value.ParentIno, node.value.Nlink, node.value.Generation, node.value.WriteSeq = ino, parent, 1, 1, sequence
			if setErr := setCreateInode(batch, &node.value); setErr != nil {
				return nil, nil, setErr
			}
			lower, upper := pebblekv.XattrRange(original)
			target, _ := pebblekv.XattrRange(ino)
			if copyErr := copyMergeSidecarRange(batch, stores[version.side], lower, upper, target); copyErr != nil {
				return nil, nil, copyErr
			}
			lower, upper = pebblekv.DataRange(original, 0)
			target, _ = pebblekv.DataRange(ino, 0)
			lower, target = lower[:len(lower)-8], target[:len(target)-8]
			if copyErr := copyMergeSidecarRange(batch, stores[version.side], lower, upper, target); copyErr != nil {
				return nil, nil, copyErr
			}
			if setErr := batch.Set(pebblekv.EntryKey(parent, []byte(name)), pebblekv.EncodeEntry(pebblekv.Entry{Ino: ino, Mode: node.value.Mode}), nil); setErr != nil {
				return nil, nil, setErr
			}
			if setErr := batch.Set(pebblekv.DirtyKey(ino), []byte{1}, nil); setErr != nil {
				return nil, nil, setErr
			}
			stat.Inodes++
			stat.LogicalBytes += node.value.Size
			created[sidecarPath], parents[parent] = true, true
			paths, inodes = append(paths, sidecarPath), append(inodes, ino)
		}
	}
	for ino := range parents {
		parent, readErr := readCreateInode(stage, ino)
		if readErr != nil {
			return nil, nil, readErr
		}
		parent.WriteSeq = sequence
		if setErr := setCreateInode(batch, &parent); setErr != nil {
			return nil, nil, setErr
		}
		if setErr := batch.Set(pebblekv.DirtyKey(ino), []byte{1}, nil); setErr != nil {
			return nil, nil, setErr
		}
	}
	if setErr := batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(stat), nil); setErr != nil {
		return nil, nil, setErr
	}
	if commitErr := batch.Commit(pebble.Sync); commitErr != nil {
		return nil, nil, commitErr
	}
	return paths, inodes, nil
}
