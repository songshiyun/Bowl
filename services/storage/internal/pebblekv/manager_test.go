package pebblekv_test

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

func TestBranchCheckpointForkAndSharedCommit(t *testing.T) {
	root := t.TempDir()
	manager := pebblekv.NewManager(root)
	branch, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	require.NoError(t, branch.DB().Set([]byte("key"), []byte("before"), pebble.NoSync))
	require.NoError(t, branch.Checkpoint("c1"))

	first, err := manager.OpenCommit("disk", "c1")
	require.NoError(t, err)
	second, err := manager.OpenCommit("disk", "c1")
	require.NoError(t, err)
	require.Same(t, first.DB(), second.DB())
	require.Error(t, first.DB().Set([]byte("key"), []byte("wrong"), pebble.Sync))
	require.NoError(t, manager.Fork("disk", "c1", "child"))

	sourceFiles, err := filepath.Glob(filepath.Join(root, "disks", "disk", "branches", "main", "*.sst"))
	require.NoError(t, err)
	require.NotEmpty(t, sourceFiles)
	original, err := os.Stat(sourceFiles[0])
	require.NoError(t, err)
	commitLink, err := os.Stat(filepath.Join(root, "disks", "disk", "commits", "c1", filepath.Base(sourceFiles[0])))
	require.NoError(t, err)
	require.True(t, os.SameFile(original, commitLink))
	childLink, err := os.Stat(filepath.Join(root, "disks", "disk", "branches", "child", filepath.Base(sourceFiles[0])))
	require.NoError(t, err)
	require.True(t, os.SameFile(original, childLink))

	child, err := manager.OpenBranch("disk", "child")
	require.NoError(t, err)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs <- branch.DB().Set([]byte("key"), []byte("main"), pebble.Sync)
	}()
	go func() {
		defer wg.Done()
		errs <- child.DB().Set([]byte("key"), []byte("child"), pebble.Sync)
	}()
	wg.Wait()
	close(errs)
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)

	requireKeyValue(t, branch.DB(), "main")
	requireKeyValue(t, child.DB(), "child")
	requireKeyValue(t, first.DB(), "before")
	requireKeyValue(t, second.DB(), "before")
	require.NoError(t, first.Close())
	requireKeyValue(t, second.DB(), "before")
	require.NoError(t, second.Close())
	require.NoError(t, child.Close())
	require.NoError(t, branch.Close())
	require.NoError(t, manager.Close())
}

func requireKeyValue(t *testing.T, db *pebble.DB, want string) {
	t.Helper()
	value, closer, err := db.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, want, string(value))
	require.NoError(t, closer.Close())
}

func TestFullBlobStoredSeparatelyAndSharedByCheckpoints(t *testing.T) {
	root := t.TempDir()
	manager := pebblekv.NewManager(root)
	branch, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	data := bytes.Repeat([]byte("0123456789abcdef"), pebblekv.BlobSize/16)
	id, err := manager.AllocateBlobID("disk")
	require.NoError(t, err)
	require.NoError(t, branch.Blobs().Write(id, data))
	key := pebblekv.DataKey(2, 0)
	require.NoError(t, branch.DB().Set(key, pebblekv.EncodeBlobRef(pebblekv.BlobRef{ID: id, Length: pebblekv.BlobSize}), pebble.Sync))
	require.NoError(t, branch.Checkpoint("c1"))
	require.NoError(t, manager.Fork("disk", "c1", "child"))

	files, err := filepath.Glob(filepath.Join(root, "disks", "disk", "branches", "main", "blobs", "*", "*.blob"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	info, err := os.Stat(files[0])
	require.NoError(t, err)
	shard := filepath.Base(filepath.Dir(files[0]))
	blob := filepath.Base(files[0])
	commitLinked, err := os.Stat(filepath.Join(root, "disks", "disk", "commits", "c1", "blobs", shard, blob))
	require.NoError(t, err)
	require.True(t, os.SameFile(info, commitLinked))
	childLinked, err := os.Stat(filepath.Join(root, "disks", "disk", "branches", "child", "blobs", shard, blob))
	require.NoError(t, err)
	require.True(t, os.SameFile(info, childLinked))

	child, err := manager.OpenBranch("disk", "child")
	require.NoError(t, err)
	commit, err := manager.OpenCommit("disk", "c1")
	require.NoError(t, err)

	// 覆写生成全新块并即时删除当前分支旧块，快照与派生分支仍持有旧块。
	replacement := bytes.Repeat([]byte{0x5a}, pebblekv.BlobSize)
	newID, err := manager.AllocateBlobID("disk")
	require.NoError(t, err)
	require.NoError(t, branch.Blobs().Write(newID, replacement))
	require.NoError(t, branch.DB().Set(key, pebblekv.EncodeBlobRef(pebblekv.BlobRef{ID: newID, Length: pebblekv.BlobSize}), pebble.Sync))
	require.NoError(t, branch.Blobs().Remove(id))
	require.False(t, branch.Blobs().Exists(id))

	requireBlobValue(t, branch.DB(), branch.Blobs(), key, replacement)
	requireBlobValue(t, child.DB(), child.Blobs(), key, data)
	requireBlobValue(t, commit.DB(), commit.Blobs(), key, data)
	require.NoError(t, commit.Close())
	require.NoError(t, child.Close())
	require.NoError(t, branch.Close())
	require.NoError(t, manager.Close())
}

func requireBlobValue(t *testing.T, db *pebble.DB, blobs *pebblekv.BlobStore, key, want []byte) {
	t.Helper()
	encoded, closer, err := db.Get(key)
	require.NoError(t, err)
	ref, err := pebblekv.DecodeBlobRef(encoded)
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	got, err := blobs.Read(ref.ID, int(ref.Length))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestCommitSharedReferencesAndManagerLifecycle(t *testing.T) {
	manager := pebblekv.NewManager(t.TempDir())
	branch, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	require.NoError(t, branch.DB().Set([]byte("k"), []byte("v"), pebble.Sync))
	require.NoError(t, branch.Checkpoint("commit"))
	require.NoError(t, branch.Close())
	require.NoError(t, branch.Close())

	first, err := manager.OpenCommit("disk", "commit")
	require.NoError(t, err)
	second, err := manager.OpenCommit("disk", "commit")
	require.NoError(t, err)
	require.Same(t, first.DB(), second.DB())
	require.ErrorIs(t, manager.Close(), pebblekv.ErrBusy)
	require.NoError(t, first.Close())
	require.ErrorIs(t, manager.Close(), pebblekv.ErrBusy)
	require.NoError(t, second.Close())
	require.NoError(t, manager.Close())
	_, err = manager.OpenBranch("disk", "main")
	require.ErrorIs(t, err, pebblekv.ErrClosed)
}

func TestManagerRejectsInvalidIdentifiersAndExistingTargets(t *testing.T) {
	manager := pebblekv.NewManager(t.TempDir())
	requireRejectedIdentifier(t, manager, "")
	requireRejectedIdentifier(t, manager, ".")
	requireRejectedIdentifier(t, manager, "..")
	requireRejectedIdentifier(t, manager, "../escape")
	requireRejectedIdentifier(t, manager, "a/b")
	requireRejectedIdentifier(t, manager, "a\x00b")

	branch, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	shared, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	require.Same(t, branch.DB(), shared.DB())
	require.NoError(t, shared.Close())
	require.NoError(t, branch.Checkpoint("one"))
	require.Error(t, branch.Checkpoint("one"))
	require.NoError(t, manager.Fork("disk", "one", "child"))
	require.Error(t, manager.Fork("disk", "one", "child"))
	require.NoError(t, branch.Close())
	require.ErrorIs(t, branch.Checkpoint("two"), pebblekv.ErrClosed)
	require.NoError(t, manager.Close())
}

func requireRejectedIdentifier(t *testing.T, manager *pebblekv.Manager, id string) {
	t.Helper()
	_, err := manager.OpenBranch(id, "main")
	require.ErrorIs(t, err, pebblekv.ErrInvalidID)
	_, err = manager.OpenCommit("disk", id)
	require.ErrorIs(t, err, pebblekv.ErrInvalidID)
}

func TestDiskInodeAllocatorConcurrentAndPersistent(t *testing.T) {
	root := t.TempDir()
	manager := pebblekv.NewManager(root)
	ids := make(chan uint64, 2)
	errs := make(chan error, 2)
	go func() {
		id, err := manager.AllocateInode("disk")
		ids <- id
		errs <- err
	}()
	go func() {
		id, err := manager.AllocateInode("disk")
		ids <- id
		errs <- err
	}()
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	first, second := <-ids, <-ids
	require.ElementsMatch(t, []uint64{2, 3}, []uint64{first, second})
	maxID := max(first, second)

	other, err := manager.AllocateInode("other")
	require.NoError(t, err)
	require.Equal(t, uint64(2), other)
	require.NoError(t, manager.Close())

	reopened := pebblekv.NewManager(root)
	next, err := reopened.AllocateInode("disk")
	require.NoError(t, err)
	require.Greater(t, next, maxID, "重启后新编号必须严格大于已发出的最大编号")
	require.NoError(t, reopened.Close())
}

func TestBlobIDAllocatorIsMonotonicAndPersistent(t *testing.T) {
	root := t.TempDir()
	manager := pebblekv.NewManager(root)
	first, err := manager.AllocateBlobID("disk")
	require.NoError(t, err)
	second, err := manager.AllocateBlobID("disk")
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)
	require.Equal(t, uint64(2), second)
	require.NoError(t, manager.Close())

	reopened := pebblekv.NewManager(root)
	next, err := reopened.AllocateBlobID("disk")
	require.NoError(t, err)
	require.Greater(t, next, second)
	require.NoError(t, reopened.Close())
}

func TestBranchAndCommitHandlesShareResidentInstances(t *testing.T) {
	root := t.TempDir()
	manager := pebblekv.NewManager(root)
	first, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	require.NoError(t, first.DB().Set([]byte("k"), []byte("v"), pebble.Sync))
	second, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	require.Same(t, first.DB(), second.DB())
	require.NoError(t, second.Close())
	require.NoError(t, first.Close())

	// 句柄关闭后实例常驻，下一次打开仍复用同一个实例。
	third, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	require.Same(t, first.DB(), third.DB())
	require.NoError(t, third.Checkpoint("c1"))
	require.NoError(t, third.Close())

	commitFirst, err := manager.OpenCommit("disk", "c1")
	require.NoError(t, err)
	commitSecond, err := manager.OpenCommit("disk", "c1")
	require.NoError(t, err)
	require.Same(t, commitFirst.DB(), commitSecond.DB())
	require.NoError(t, commitSecond.Close())
	require.NoError(t, commitFirst.Close())
	commitThird, err := manager.OpenCommit("disk", "c1")
	require.NoError(t, err)
	require.Same(t, commitFirst.DB(), commitThird.DB())
	require.NoError(t, commitThird.Close())
	require.NoError(t, manager.Close())
}

func TestEvictBranchReleasesUnderlyingInstance(t *testing.T) {
	root := t.TempDir()
	manager := pebblekv.NewManager(root)
	branch, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	require.ErrorIs(t, manager.EvictBranch("disk", "main"), pebblekv.ErrBusy)
	require.NoError(t, branch.Close())
	require.NoError(t, manager.EvictBranch("disk", "main"))

	// 驱逐后目录锁已释放，另一个管理器可以打开同一分支目录。
	other := pebblekv.NewManager(root)
	reopened, err := other.OpenBranch("disk", "main")
	require.NoError(t, err)
	require.NotSame(t, branch.DB(), reopened.DB())
	require.NoError(t, reopened.Close())
	require.NoError(t, other.Close())

	require.NoError(t, manager.EvictBranch("disk", "main"))
	require.NoError(t, manager.Close())
}

func TestStagingBranchesUseEphemeralHandles(t *testing.T) {
	root := t.TempDir()
	manager := pebblekv.NewManager(root)
	requireEphemeralBranch(t, manager, root, ".fork-1")
	requireEphemeralBranch(t, manager, root, ".merge-2")
	requireEphemeralBranch(t, manager, root, ".restore-3")
	require.NoError(t, manager.Close())
}

func requireEphemeralBranch(t *testing.T, manager *pebblekv.Manager, root, name string) {
	t.Helper()
	ephemeral, err := manager.OpenBranch("disk", name)
	require.NoError(t, err)
	require.NoError(t, ephemeral.Close())

	// 关闭即释放，不进入常驻池。
	other := pebblekv.NewManager(root)
	probe, err := other.OpenBranch("disk", name)
	require.NoError(t, err)
	require.NoError(t, probe.Close())
	require.NoError(t, other.Close())
}

func TestEvictDiskClosesResidentHandlesAndAllocator(t *testing.T) {
	root := t.TempDir()
	manager := pebblekv.NewManager(root)
	branch, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	require.NoError(t, branch.Checkpoint("c1"))
	require.NoError(t, branch.Close())
	commit, err := manager.OpenCommit("disk", "c1")
	require.NoError(t, err)
	require.NoError(t, commit.Close())
	allocated, err := manager.AllocateInode("disk")
	require.NoError(t, err)
	require.Equal(t, uint64(2), allocated)

	live, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	require.ErrorIs(t, manager.EvictDisk("disk"), pebblekv.ErrBusy)
	require.NoError(t, live.Close())
	require.NoError(t, manager.EvictDisk("disk"))

	// 内存分配器状态被清理后，重新分配仍从已持久化的上界继续。
	next, err := manager.AllocateInode("disk")
	require.NoError(t, err)
	require.Greater(t, next, allocated)
	require.NoError(t, manager.Close())
}

func TestRemoveDiskEvictsHandlesAndRemovesDirectory(t *testing.T) {
	root := t.TempDir()
	manager := pebblekv.NewManager(root)
	branch, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	require.ErrorIs(t, manager.RemoveDisk("disk"), pebblekv.ErrBusy)
	require.NoError(t, branch.Close())
	require.NoError(t, manager.RemoveDisk("disk"))
	_, err = os.Stat(filepath.Join(root, "disks", "disk"))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, manager.Close())
}

// Manager.Close 必须优雅停止后台刷盘协程，关闭后不再接受新分支。
func TestManagerCloseStopsWALFlusher(t *testing.T) {
	manager := pebblekv.NewManager(t.TempDir())
	branch, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	require.NoError(t, branch.DB().Set([]byte("key"), []byte("value"), pebble.NoSync))
	manager.MarkBranchDirty("disk", "main")
	require.NoError(t, branch.Close())
	require.NoError(t, manager.Close())
	_, err = manager.OpenBranch("disk", "main")
	require.ErrorIs(t, err, pebblekv.ErrClosed)
}
