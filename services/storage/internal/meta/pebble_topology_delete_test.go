package meta_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

// TestPebbleUnlinkDeleteStrategyCleansKeysAndBlobs 覆盖删除路径的三种规模：
// 0 键、64 键点删、65 键回退范围墓碑，三者都必须把节点、xattr、数据键与关联 blob 清理干净。
func TestPebbleUnlinkDeleteStrategyCleansKeysAndBlobs(t *testing.T) {
	e := setupPebbleCreate(t, 1000)
	ctx := e.branchContext()

	branch, err := e.storage.OpenBranch(e.disk.DiskID, "main")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, branch.Close()) })

	var nextBlobID uint64 = 10000
	for _, tc := range []struct {
		name string
		keys int
	}{
		{name: "zero", keys: 0},
		{name: "pointwise-64", keys: 64},
		{name: "range-65", keys: 65},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "victim-" + tc.name
			inode := topologySymlink(t, e, 1, name, "target")
			xattrKeys := make([][]byte, 0, tc.keys)
			dataKeys := make([][]byte, 0, tc.keys)
			blobIDs := make([]uint64, 0, tc.keys)
			for i := 0; i < tc.keys; i++ {
				xattrKey := pebblekv.XattrKey(inode.Ino, []byte(fmt.Sprintf("user.k%03d", i)))
				require.NoError(t, branch.DB().Set(xattrKey, []byte("v"), pebble.Sync))
				xattrKeys = append(xattrKeys, xattrKey)

				id := nextBlobID
				nextBlobID++
				require.NoError(t, branch.Blobs().Write(id, []byte{byte('a' + i%26)}))
				dataKey := pebblekv.DataKey(inode.Ino, uint64(i))
				require.NoError(t, branch.DB().Set(dataKey, pebblekv.EncodeBlobRef(pebblekv.BlobRef{ID: id, Length: 1}), pebble.Sync))
				dataKeys = append(dataKeys, dataKey)
				blobIDs = append(blobIDs, id)
			}

			before := topologyStat(t, branch.DB())
			require.NoError(t, e.mgr.Unlink(ctx, 1, []byte(name)))

			store := branch.DB()
			assertPebbleMissing(t, store, pebblekv.InodeKey(inode.Ino))
			assertPebbleMissing(t, store, pebblekv.OrphanKey(inode.Ino))
			assertPebbleMissing(t, store, pebblekv.EntryKey(1, []byte(name)))
			for _, key := range xattrKeys {
				assertPebbleMissing(t, store, key)
			}
			for _, key := range dataKeys {
				assertPebbleMissing(t, store, key)
			}
			for _, id := range blobIDs {
				require.False(t, branch.Blobs().Exists(id), "块 %d 必须随节点删除即时回收", id)
			}
			stat := topologyStat(t, store)
			require.Equal(t, before.Inodes-1, stat.Inodes)
			require.Zero(t, stat.LogicalBytes)
		})
	}

	// 拓扑删除必须只影响目标节点：同目录其它文件与分支根节点保持完好。
	e.withBranch(t, "main", func(store *pebble.DB) {
		require.Equal(t, uint64(1), topologyStat(t, store).Inodes)
		readPebbleCreateInode(t, store, 1)
	})
}
