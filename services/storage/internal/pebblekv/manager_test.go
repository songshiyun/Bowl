package pebblekv_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
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
	_, err = manager.OpenBranch("disk", "main")
	require.ErrorIs(t, err, pebblekv.ErrBusy)
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
	for id := range ids {
		require.False(t, seen[id])
		seen[id] = true
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
	require.Equal(t, uint64(2+count), next)
	require.NoError(t, reopened.Close())
}
