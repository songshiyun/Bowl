package meta_test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

func blobFiles(t *testing.T, root, disk string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "disks", disk, "branches", "main", "blobs", "*", "*.blob"))
	require.NoError(t, err)
	return files
}

// 顺序写入 128 个 1 MiB 块时 Pebble 只存 12 字节 BlobRef，不触发 MemTable Flush；
// 覆写与删除会当场回收当前分支的块文件。
func TestPebbleBlobDecoupledFromLSMAndReclaimedImmediately(t *testing.T) {
	const blocks = 128
	const size = blocks * pebblekv.BlobSize
	e := setupPebbleCreate(t, 2*size)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("blob"), Mode: 0o644})
	require.NoError(t, err)
	ino := file.Inode.Ino

	var baselineFlushes int64
	e.withBranch(t, "main", func(store *pebble.DB) {
		baselineFlushes = store.Metrics().Flush.Count
	})

	model := make([]byte, size)
	for block := 0; block < blocks; block++ {
		data := bytes.Repeat([]byte{byte(block + 1)}, pebblekv.BlobSize)
		copy(model[block*pebblekv.BlobSize:], data)
		_, err := e.mgr.Write(e.ctx, &meta.WriteRequest{
			Ino: ino, Offset: uint64(block * pebblekv.BlobSize), Data: data, SessionID: &e.sid,
		})
		require.NoError(t, err)
	}

	e.withBranch(t, "main", func(store *pebble.DB) {
		require.Equal(t, baselineFlushes, store.Metrics().Flush.Count, "纯元数据写入不得触发 MemTable Flush")
		for block := 0; block < blocks; block++ {
			value := getPebbleValue(t, store, pebblekv.DataKey(ino, uint64(block)))
			require.Len(t, value, 12)
			ref, err := pebblekv.DecodeBlobRef(value)
			require.NoError(t, err)
			require.Equal(t, uint32(pebblekv.BlobSize), ref.Length)
		}
	})
	files := blobFiles(t, e.root, e.disk.DiskID)
	require.Len(t, files, blocks)
	oldIDs := make(map[uint64]struct{}, blocks)
	e.withBranch(t, "main", func(store *pebble.DB) {
		for block := 0; block < blocks; block++ {
			ref, err := pebblekv.DecodeBlobRef(getPebbleValue(t, store, pebblekv.DataKey(ino, uint64(block))))
			require.NoError(t, err)
			oldIDs[ref.ID] = struct{}{}
		}
	})

	// 原地整块覆写：生成新块并即时删除旧块，文件数量保持不变。
	for block := 0; block < blocks; block++ {
		data := bytes.Repeat([]byte{byte(255 - block)}, pebblekv.BlobSize)
		_, err := e.mgr.Write(e.ctx, &meta.WriteRequest{
			Ino: ino, Offset: uint64(block * pebblekv.BlobSize), Data: data, SessionID: &e.sid,
		})
		require.NoError(t, err)
	}
	require.Len(t, blobFiles(t, e.root, e.disk.DiskID), blocks)
	e.withBranch(t, "main", func(store *pebble.DB) {
		for block := 0; block < blocks; block++ {
			ref, err := pebblekv.DecodeBlobRef(getPebbleValue(t, store, pebblekv.DataKey(ino, uint64(block))))
			require.NoError(t, err)
			_, reused := oldIDs[ref.ID]
			require.False(t, reused, "覆写必须分配新块编号")
		}
	})
	for _, path := range blobFiles(t, e.root, e.disk.DiskID) {
		for id := range oldIDs {
			require.NotEqual(t, fmt.Sprintf("%016x.blob", id), filepath.Base(path), "旧块文件必须被立即删除")
		}
	}

	require.NoError(t, e.mgr.Unlink(e.branchContext(), 1, []byte("blob")))
	require.Empty(t, blobFiles(t, e.root, e.disk.DiskID))
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Zero(t, quota.Usage().Bytes)
}

// 局部写入、截断与打洞在无 BlockValueMerger 下保持 POSIX 契约并即时回收旧块。
func TestPebbleBlobPartialWritesTruncateAndPunchHole(t *testing.T) {
	e := setupPebbleCreate(t, 4*pebblekv.BlobSize)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("partial"), Mode: 0o644})
	require.NoError(t, err)
	ino := file.Inode.Ino

	model := make([]byte, 0)
	writeAt := func(offset uint64, data []byte) {
		_, err := e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: offset, Data: data, SessionID: &e.sid})
		require.NoError(t, err)
		end := int(offset) + len(data)
		if end > len(model) {
			model = append(model, make([]byte, end-len(model))...)
		}
		copy(model[offset:], data)
	}
	writeAt(pebblekv.BlobSize-5, []byte("0123456789"))
	writeAt(0, []byte("abc"))
	writeAt(2*pebblekv.BlobSize+7, []byte("tail"))
	assertPebbleData(t, e, ino, model)

	shrink := uint64(pebblekv.BlobSize + 3)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Size: &shrink, SessionID: &e.sid})
	require.NoError(t, err)
	model = model[:shrink]
	assertPebbleData(t, e, ino, model)

	require.NoError(t, e.mgr.Fallocate(e.ctx, meta.FallocateParams{
		Ino: ino, Offset: 1, Length: pebblekv.BlobSize, Mode: meta.FallocFlPunchHole | meta.FallocFlKeepSize, SessionID: &e.sid,
	}))
	clear(model[1 : pebblekv.BlobSize+1])
	assertPebbleData(t, e, ino, model)

	zero := uint64(0)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Size: &zero, SessionID: &e.sid})
	require.NoError(t, err)
	require.Empty(t, blobFiles(t, e.root, e.disk.DiskID))
}
