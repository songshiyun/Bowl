package meta_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

// seededInodeData 记录为指定 ino 预置的物理键与块文件坐标，供测试按 ino 直接核对清理结果。
type seededInodeData struct {
	xattrKey []byte
	dataKey  []byte
	blobID   uint64
}

// seedInodeData 在分支库中为 ino 写入一个 xattr、一个数据键，并落一个块文件。
func seedInodeData(t *testing.T, branch *pebblekv.Branch, ino, blobID uint64, xattrName, payload []byte) seededInodeData {
	t.Helper()
	xattrKey := pebblekv.XattrKey(ino, xattrName)
	require.NoError(t, branch.DB().Set(xattrKey, []byte("seed"), pebble.Sync))
	require.NoError(t, branch.Blobs().Write(blobID, payload))
	dataKey := pebblekv.DataKey(ino, 0)
	ref := pebblekv.BlobRef{ID: blobID, Length: uint32(len(payload))}
	require.NoError(t, branch.DB().Set(dataKey, pebblekv.EncodeBlobRef(ref), pebble.Sync))
	return seededInodeData{xattrKey: xattrKey, dataKey: dataKey, blobID: blobID}
}

func assertPebbleKeyMissing(t *testing.T, store *pebble.DB, key []byte) {
	t.Helper()
	_, _, err := store.Get(key)
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func createTestRegularFile(t *testing.T, e *pebbleCreateEnv, name string) *meta.Inode {
	t.Helper()
	inode, err := e.mgr.Mknod(e.ctx, meta.MknodParams{SessionID: &e.sid, ParentIno: 1, Name: []byte(name), Mode: meta.S_IFREG | 0o644})
	require.NoError(t, err)
	return inode
}

// TestPebbleDeleteCleansPhysicalState 直接按目标 ino 检查 Unlink、孤儿回收与 DeleteDisk 的物理清理结果。
func TestPebbleDeleteCleansPhysicalState(t *testing.T) {
	e := setupPebbleCreate(t, 1024)
	branch, err := e.storage.OpenBranch(e.disk.DiskID, "main")
	require.NoError(t, err)

	victim := createTestRegularFile(t, e, "victim")
	victimData := seedInodeData(t, branch, victim.Ino, 9001, []byte("user.victim"), []byte("victim"))
	require.True(t, branch.Blobs().Exists(victimData.blobID))
	require.NoError(t, e.mgr.Unlink(e.ctx, &e.sid, 1, []byte("victim"), false))
	assertPebbleKeyMissing(t, branch.DB(), pebblekv.InodeKey(victim.Ino))
	assertPebbleKeyMissing(t, branch.DB(), pebblekv.OrphanKey(victim.Ino))
	assertPebbleKeyMissing(t, branch.DB(), pebblekv.EntryKey(1, []byte("victim")))
	assertPebbleKeyMissing(t, branch.DB(), victimData.xattrKey)
	assertPebbleKeyMissing(t, branch.DB(), victimData.dataKey)
	require.False(t, branch.Blobs().Exists(victimData.blobID))

	orphan := createTestRegularFile(t, e, "orphan")
	orphanData := seedInodeData(t, branch, orphan.Ino, 9002, []byte("user.orphan"), []byte("orphan"))
	require.NoError(t, e.mgr.Unlink(e.ctx, &e.sid, 1, []byte("orphan"), true))
	require.Equal(t, []byte{1}, getPebbleValue(t, branch.DB(), pebblekv.OrphanKey(orphan.Ino)))
	require.NoError(t, e.mgr.ReclaimOrphan(e.ctx, meta.ReclaimOrphanParams{SessionID: e.sid, DiskID: e.disk.DiskID, Ino: orphan.Ino}))
	assertPebbleKeyMissing(t, branch.DB(), pebblekv.InodeKey(orphan.Ino))
	assertPebbleKeyMissing(t, branch.DB(), pebblekv.OrphanKey(orphan.Ino))
	assertPebbleKeyMissing(t, branch.DB(), orphanData.xattrKey)
	assertPebbleKeyMissing(t, branch.DB(), orphanData.dataKey)
	require.False(t, branch.Blobs().Exists(orphanData.blobID))

	renameSrc := createTestRegularFile(t, e, "rename_src")
	renameDst := createTestRegularFile(t, e, "rename_dst")
	renameDstData := seedInodeData(t, branch, renameDst.Ino, 9004, []byte("user.rename"), []byte("rename"))
	require.NoError(t, e.mgr.Rename(e.ctx, meta.RenameParams{
		SessionID:          &e.sid,
		OldParentIno:       1,
		OldName:            []byte("rename_src"),
		NewParentIno:       1,
		NewName:            []byte("rename_dst"),
		TargetOpenInClient: true,
	}))
	require.Equal(t, []byte{1}, getPebbleValue(t, branch.DB(), pebblekv.OrphanKey(renameDst.Ino)))
	require.NoError(t, e.mgr.ReclaimOrphan(e.ctx, meta.ReclaimOrphanParams{SessionID: e.sid, DiskID: e.disk.DiskID, Ino: renameDst.Ino}))
	assertPebbleKeyMissing(t, branch.DB(), pebblekv.InodeKey(renameDst.Ino))
	assertPebbleKeyMissing(t, branch.DB(), pebblekv.OrphanKey(renameDst.Ino))
	assertPebbleKeyMissing(t, branch.DB(), renameDstData.xattrKey)
	assertPebbleKeyMissing(t, branch.DB(), renameDstData.dataKey)
	require.False(t, branch.Blobs().Exists(renameDstData.blobID))
	require.NoError(t, e.mgr.Unlink(e.ctx, &e.sid, 1, []byte("rename_dst"), false))
	assertPebbleKeyMissing(t, branch.DB(), pebblekv.InodeKey(renameSrc.Ino))
	require.NoError(t, branch.Close())

	survivor := createTestRegularFile(t, e, "survivor")
	survivorBranch, err := e.storage.OpenBranch(e.disk.DiskID, "main")
	require.NoError(t, err)
	survivorData := seedInodeData(t, survivorBranch, survivor.Ino, 9003, []byte("user.survivor"), []byte("survivor"))
	blobPath := survivorBranch.Blobs().Path(survivorData.blobID)
	require.NoError(t, survivorBranch.Close())

	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	require.NoError(t, e.mgr.DeleteDisk(e.ctx, e.disk.DiskID))
	_, err = os.Stat(filepath.Join(e.root, "disks", e.disk.DiskID))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(blobPath)
	require.ErrorIs(t, err, os.ErrNotExist)
}
