package pebblekv_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/record"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/wal"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
	"github.com/stretchr/testify/require"
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
	for _, dir := range []string{filepath.Join(root, "disks", "disk", "commits", "c1"), filepath.Join(root, "disks", "disk", "branches", "child")} {
		linked, err := os.Stat(filepath.Join(dir, filepath.Base(sourceFiles[0])))
		require.NoError(t, err)
		require.True(t, os.SameFile(original, linked))
	}

	child, err := manager.OpenBranch("disk", "child")
	require.NoError(t, err)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, pair := range []struct {
		b     *pebblekv.Branch
		value string
	}{{branch, "main"}, {child, "child"}} {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- pair.b.DB().Set([]byte("key"), []byte(pair.value), pebble.Sync) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	for _, item := range []struct {
		db   *pebble.DB
		want string
	}{{branch.DB(), "main"}, {child.DB(), "child"}, {first.DB(), "before"}, {second.DB(), "before"}} {
		value, closer, err := item.db.Get([]byte("key"))
		require.NoError(t, err)
		require.Equal(t, item.want, string(value))
		require.NoError(t, closer.Close())
	}
	require.NoError(t, first.Close())
	value, closer, err := second.DB().Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, "before", string(value))
	require.NoError(t, closer.Close())
	require.NoError(t, second.Close())
	require.NoError(t, child.Close())
	require.NoError(t, branch.Close())
	require.NoError(t, manager.Close())
}

func TestFullBlobStoredSeparatelyAndSharedByCheckpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	root := t.TempDir()
	manager := pebblekv.NewManager(root)
	branch, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	data := bytes.Repeat([]byte("0123456789abcdef"), pebblekv.BlobSize/16)
	value, err := pebblekv.EncodeBase(data)
	require.NoError(t, err)
	require.NoError(t, branch.DB().Set(pebblekv.DataKey(2, 0), value, pebble.Sync))
	require.NoError(t, branch.Checkpoint("c1"))
	require.NoError(t, manager.Fork("disk", "c1", "child"))
	files, err := filepath.Glob(filepath.Join(root, "disks", "disk", "branches", "main", "*.blob"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, source := range files {
		info, err := os.Stat(source)
		require.NoError(t, err)
		for _, directory := range []string{"commits/c1", "branches/child"} {
			linked, err := os.Stat(filepath.Join(root, "disks", "disk", directory, filepath.Base(source)))
			require.NoError(t, err)
			require.True(t, os.SameFile(info, linked))
		}
	}
	child, err := manager.OpenBranch("disk", "child")
	require.NoError(t, err)
	commit, err := manager.OpenCommit("disk", "c1")
	require.NoError(t, err)
	key := pebblekv.DataKey(2, 0)
	patch, err := pebblekv.EncodePatches(pebblekv.ZeroPatch(2, 2), pebblekv.WritePatch(4, []byte("XY")))
	require.NoError(t, err)
	require.NoError(t, branch.DB().Merge(key, patch, pebble.Sync))
	require.NoError(t, branch.DB().Flush())
	require.NoError(t, branch.DB().Compact(ctx, key, append(bytes.Clone(key), 0), false))
	for _, item := range []struct {
		db      *pebble.DB
		changed bool
	}{{branch.DB(), true}, {child.DB(), false}, {commit.DB(), false}} {
		encoded, closer, err := item.db.Get(key)
		require.NoError(t, err)
		got, err := pebblekv.DecodeBlob(encoded)
		require.NoError(t, err)
		require.NoError(t, closer.Close())
		want := bytes.Clone(data)
		if item.changed {
			want[2], want[3], want[4], want[5] = 0, 0, 'X', 'Y'
		}
		require.Equal(t, want, got)
	}
	require.NoError(t, commit.Close())
	require.NoError(t, child.Close())
	require.NoError(t, branch.Close())
	require.NoError(t, manager.Close())
}

func TestCommitConcurrentReferencesAndManagerLifecycle(t *testing.T) {
	manager := pebblekv.NewManager(t.TempDir())
	branch, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	require.NoError(t, branch.DB().Set([]byte("k"), []byte("v"), pebble.Sync))
	require.NoError(t, branch.Checkpoint("commit"))
	require.NoError(t, branch.Close())
	require.NoError(t, branch.Close())

	const count = 24
	ready := make(chan *pebble.DB, count)
	results := make(chan error, count)
	release := make(chan struct{})
	defer func() {
		if release != nil {
			close(release)
		}
	}()
	for range count {
		go func(gate <-chan struct{}) {
			c, err := manager.OpenCommit("disk", "commit")
			if err != nil {
				ready <- nil
				results <- err
				return
			}
			ready <- c.DB()
			<-gate
			value, closer, err := c.DB().Get([]byte("k"))
			if err == nil {
				if string(value) != "v" {
					err = errors.New("快照值错误")
				}
				err = errors.Join(err, closer.Close())
			}
			results <- errors.Join(err, c.Close())
		}(release)
	}
	var shared *pebble.DB
	for range count {
		select {
		case db := <-ready:
			require.NotNil(t, db)
			if shared == nil {
				shared = db
			} else {
				require.Same(t, shared, db)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("等待并发快照引用超时")
		}
	}
	require.ErrorIs(t, manager.Close(), pebblekv.ErrBusy)
	close(release)
	release = nil
	for range count {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("等待并发快照读取超时")
		}
	}
	require.NoError(t, manager.Close())
	_, err = manager.OpenBranch("disk", "main")
	require.ErrorIs(t, err, pebblekv.ErrClosed)
}

func TestManagerRejectsInvalidIdentifiersAndExistingTargets(t *testing.T) {
	manager := pebblekv.NewManager(t.TempDir())
	for _, id := range []string{"", ".", "..", "../escape", "a/b", "a\x00b"} {
		_, err := manager.OpenBranch(id, "main")
		require.ErrorIs(t, err, pebblekv.ErrInvalidID)
		_, err = manager.OpenCommit("disk", id)
		require.ErrorIs(t, err, pebblekv.ErrInvalidID)
	}
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

func TestDiskInodeAllocatorConcurrentAndPersistent(t *testing.T) {
	root := t.TempDir()
	manager := pebblekv.NewManager(root)
	const count = 64
	ids := make(chan uint64, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := manager.AllocateInode("disk")
			if err != nil {
				errs <- err
				return
			}
			ids <- id
		}()
	}
	wg.Wait()
	close(errs)
	close(ids)
	for err := range errs {
		require.NoError(t, err)
	}
	seen := make(map[uint64]bool)
	var maxID uint64
	for id := range ids {
		require.False(t, seen[id])
		seen[id] = true
		maxID = max(maxID, id)
	}
	require.Len(t, seen, count)
	for id := uint64(2); id < 2+count; id++ {
		require.True(t, seen[id])
	}
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

func TestInodeAllocatorPersistsOneSegmentReservation(t *testing.T) {
	root := t.TempDir()
	manager := pebblekv.NewManager(root)
	const count = 4096
	for i := range count {
		id, err := manager.AllocateInode("disk")
		require.NoError(t, err)
		require.Equal(t, uint64(i+2), id)
	}
	require.Equal(t, 1, countAllocatorReservations(t, root, "disk"))
	require.NoError(t, manager.Close())

	reopened := pebblekv.NewManager(root)
	next, err := reopened.AllocateInode("disk")
	require.NoError(t, err)
	require.Equal(t, uint64(count+2), next)
	require.NoError(t, reopened.Close())
}

// countAllocatorReservations 统计分配器 WAL 中持久化上界的写入次数。
func countAllocatorReservations(t *testing.T, root, disk string) int {
	t.Helper()
	dirname := filepath.Join(root, "disks", disk, "inode-allocator")
	logs, err := wal.Scan(wal.Dir{FS: vfs.Default, Dirname: dirname})
	require.NoError(t, err)
	count := 0
	for _, log := range logs {
		var info pebble.BlobFileInfo
		_, err := fmt.Sscanf(log.Num.String(), "%d", &info.DiskFileNum)
		require.NoError(t, err)
		for i := 0; i < log.NumSegments(); i++ {
			_, filename := log.SegmentLocation(i)
			file, err := os.Open(filename)
			require.NoError(t, err)
			reader := record.NewReader(file, info.DiskFileNum)
			for {
				entry, err := reader.Next()
				if err == io.EOF || err != nil && record.IsInvalidRecord(err) {
					break
				}
				require.NoError(t, err)
				repr, err := io.ReadAll(entry)
				require.NoError(t, err)
				var batch pebble.Batch
				require.NoError(t, batch.SetRepr(repr))
				batchReader := batch.Reader()
				for {
					kind, key, _, ok, err := batchReader.Next()
					require.NoError(t, err)
					if !ok {
						break
					}
					if kind == pebble.InternalKeyKindSet && string(key) == "last-inode" {
						count++
					}
				}
			}
			require.NoError(t, file.Close())
		}
	}
	return count
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

	stats := manager.Stats()
	require.Equal(t, int64(1), stats.BranchOpens)
	require.Equal(t, int64(2), stats.BranchReuses)
	require.Equal(t, int64(1), stats.CommitOpens)
	require.Equal(t, int64(2), stats.CommitReuses)
	require.Equal(t, 1, stats.ResidentBranches)
	require.Equal(t, 1, stats.ResidentCommits)
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
	for _, name := range []string{".fork-1", ".merge-2", ".restore-3"} {
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
	require.Zero(t, manager.Stats().ResidentBranches)
	require.Zero(t, manager.Stats().ResidentCommits)
	require.NoError(t, manager.Close())
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
	stats := manager.Stats()
	require.Zero(t, stats.ResidentBranches)
	require.Zero(t, stats.ResidentCommits)

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

// 后台兜底刷盘只处理被标记为脏写的常驻分支，空闲分支不产生额外同步。
func TestManagerFlushesDirtyBranchWALPeriodically(t *testing.T) {
	manager := pebblekv.NewManager(t.TempDir(), pebblekv.WithWALFlushInterval(10*time.Millisecond))
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	branch, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	defer func() { require.NoError(t, branch.Close()) }()

	// 未标记脏写时分枝不触发兜底刷盘。
	require.Never(t, func() bool { return manager.Stats().WALFlushes > 0 }, 60*time.Millisecond, 10*time.Millisecond)

	require.NoError(t, branch.DB().Set([]byte("key"), []byte("value"), pebble.NoSync))
	manager.MarkBranchDirty("disk", "main")
	require.Eventually(t, func() bool { return manager.Stats().WALFlushes == 1 }, 2*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return manager.Stats().WALFlushes > 1 }, 80*time.Millisecond, 10*time.Millisecond)

	require.NoError(t, branch.DB().Set([]byte("key2"), []byte("value2"), pebble.NoSync))
	manager.MarkBranchDirty("disk", "main")
	require.Eventually(t, func() bool { return manager.Stats().WALFlushes == 2 }, 2*time.Second, 10*time.Millisecond)
}

// Manager.Close 必须优雅停止后台刷盘协程，关闭后不再接受新分支。
func TestManagerCloseStopsWALFlusher(t *testing.T) {
	manager := pebblekv.NewManager(t.TempDir(), pebblekv.WithWALFlushInterval(5*time.Millisecond))
	branch, err := manager.OpenBranch("disk", "main")
	require.NoError(t, err)
	require.NoError(t, branch.DB().Set([]byte("key"), []byte("value"), pebble.NoSync))
	manager.MarkBranchDirty("disk", "main")
	require.NoError(t, branch.Close())
	require.NoError(t, manager.Close())
	_, err = manager.OpenBranch("disk", "main")
	require.ErrorIs(t, err, pebblekv.ErrClosed)
}
