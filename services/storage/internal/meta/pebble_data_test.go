package meta_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/record"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/wal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

type dataWALRecord struct {
	kind  pebble.InternalKeyKind
	key   []byte
	value []byte
}

// 读取实际 WAL，检查整块写入、补丁及同步屏障产生的记录。
func readDataWAL(t *testing.T, e *pebbleCreateEnv) []dataWALRecord {
	t.Helper()
	path := filepath.Join(e.root, "disks", e.disk.DiskID, "branches", "main")
	logs, err := wal.Scan(wal.Dir{FS: vfs.Default, Dirname: path})
	require.NoError(t, err)
	var records []dataWALRecord
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
					kind, key, value, ok, err := batchReader.Next()
					require.NoError(t, err)
					if !ok {
						break
					}
					records = append(records, dataWALRecord{kind: kind, key: bytes.Clone(key), value: bytes.Clone(value)})
				}
			}
			require.NoError(t, file.Close())
		}
	}
	return records
}

func requireWALData(t *testing.T, records []dataWALRecord, kind pebble.InternalKeyKind, key []byte, frame byte) {
	t.Helper()
	for _, item := range records {
		if item.kind == kind && bytes.Equal(item.key, key) {
			require.GreaterOrEqual(t, len(item.value), 2)
			require.Equal(t, frame, item.value[1])
			return
		}
	}
	t.Fatalf("WAL 中未找到 %s 记录，键 %x", kind, key)
}

func assertPebbleData(t *testing.T, e *pebbleCreateEnv, ino uint64, expected []byte) {
	t.Helper()
	for offset := 0; offset < len(expected); offset += pebblekv.BlobSize {
		end := min(offset+pebblekv.BlobSize, len(expected))
		response, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Offset: uint64(offset), Size: pebblekv.BlobSize, SessionID: &e.sid})
		require.NoError(t, err)
		require.True(t, bytes.Equal(expected[offset:end], response.Data), "读取偏移 %d，长度 %d", offset, end-offset)
	}
	response, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Offset: uint64(len(expected)), Size: 10, SessionID: &e.sid})
	require.NoError(t, err)
	require.Empty(t, response.Data)
}

func TestPebbleDataWriteTruncateAndCompact(t *testing.T) {
	const blob = pebblekv.BlobSize
	e := setupPebbleCreate(t, 3*blob+1024)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("data"), Mode: 0o6755})
	require.NoError(t, err)
	ino := file.Inode.Ino
	model := make([]byte, blob)
	for i := range model {
		model[i] = byte((i*17 + 31) % 251)
	}
	written, err := e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: bytes.Clone(model), SessionID: &e.sid})
	require.NoError(t, err)
	require.Equal(t, uint32(blob), written.Size)
	require.Zero(t, written.Inode.Attr.Mode&(meta.S_ISUID|meta.S_ISGID))
	requireWALData(t, readDataWAL(t, e), pebble.InternalKeyKindSet, pebblekv.DataKey(ino, 0), pebblekv.BaseFrame)

	patch := bytes.Repeat([]byte{0xe1}, 31)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: blob - 11, Data: patch, SessionID: &e.sid})
	require.NoError(t, err)
	model = append(model, make([]byte, 20)...)
	copy(model[blob-11:], patch)
	walRecords := readDataWAL(t, e)
	requireWALData(t, walRecords, pebble.InternalKeyKindMerge, pebblekv.DataKey(ino, 0), pebblekv.PatchListFrame)
	requireWALData(t, walRecords, pebble.InternalKeyKindMerge, pebblekv.DataKey(ino, 1), pebblekv.PatchListFrame)

	sparseOffset := 2*blob + 17
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: uint64(sparseOffset), Data: []byte("sparse"), SessionID: &e.sid})
	require.NoError(t, err)
	model = append(model, make([]byte, sparseOffset+len("sparse")-len(model))...)
	copy(model[sparseOffset:], "sparse")
	assertPebbleData(t, e, ino, model)
	cross, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Offset: blob - 20, Size: 50, SessionID: &e.sid})
	require.NoError(t, err)
	require.Equal(t, model[blob-20:blob+30], cross.Data)

	var beforeInode, beforeStat []byte
	e.withBranch(t, "main", func(store *pebble.DB) {
		beforeInode = getPebbleValue(t, store, pebblekv.InodeKey(ino))
		beforeStat = getPebbleValue(t, store, pebblekv.StatKey())
	})
	zero, err := e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: 2 * blob, SessionID: &e.sid})
	require.NoError(t, err)
	require.Zero(t, zero.Size)
	e.withBranch(t, "main", func(store *pebble.DB) {
		require.Equal(t, beforeInode, getPebbleValue(t, store, pebblekv.InodeKey(ino)))
		require.Equal(t, beforeStat, getPebbleValue(t, store, pebblekv.StatKey()))
	})

	shrink := uint64(blob + 7)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Size: &shrink, SessionID: &e.sid})
	require.NoError(t, err)
	model = model[:shrink]
	expand := uint64(2*blob + 23)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Size: &expand, SessionID: &e.sid})
	require.NoError(t, err)
	model = append(model, make([]byte, int(expand)-len(model))...)
	assertPebbleData(t, e, ino, model)

	mode := uint32(0o6755)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Mode: &mode, SessionID: &e.sid})
	require.NoError(t, err)
	err = e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: ino, Offset: blob - 5, Length: 20, Mode: meta.FallocFlPunchHole | meta.FallocFlKeepSize, SessionID: &e.sid})
	require.NoError(t, err)
	clear(model[blob-5 : blob+15])
	attr, err := e.mgr.GetAttr(e.branchContext(), ino)
	require.NoError(t, err)
	require.Zero(t, attr.Attr.Mode&(meta.S_ISUID|meta.S_ISGID))
	err = e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: ino, Offset: 50, Length: blob + 20, Mode: meta.FallocFlZeroRange | meta.FallocFlKeepSize, SessionID: &e.sid})
	require.NoError(t, err)
	clear(model[50 : blob+70])
	err = e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: ino, Offset: uint64(len(model) - 8), Length: 24, Mode: meta.FallocFlZeroRange, SessionID: &e.sid})
	require.NoError(t, err)
	clear(model[len(model)-8:])
	model = append(model, make([]byte, 16)...)
	assertPebbleData(t, e, ino, model)

	e.withBranch(t, "main", func(store *pebble.DB) {
		lower, upper := pebblekv.DataRange(ino, 0)
		require.NoError(t, store.Compact(e.ctx, lower, upper, true))
		stat, err := pebblekv.DecodeBranchStat(getPebbleValue(t, store, pebblekv.StatKey()))
		require.NoError(t, err)
		require.Equal(t, uint64(len(model)), stat.LogicalBytes)
		inode := readPebbleCreateInode(t, store, ino)
		require.Equal(t, uint64(len(model)), inode.Size)
		require.Equal(t, (inode.Size+511)/512, inode.Blocks)
		require.Equal(t, stat.NextWriteSeq-1, inode.WriteSeq)
		getPebbleValue(t, store, pebblekv.DirtyKey(ino))
	})
	assertPebbleData(t, e, ino, model)
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(len(model)), quota.Usage().Bytes)

	shrink = 0
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Size: &shrink, SessionID: &e.sid})
	require.NoError(t, err)
	e.withBranch(t, "main", func(store *pebble.DB) {
		_, closer, err := store.Get(pebblekv.DataKey(ino, 0))
		require.ErrorIs(t, err, pebble.ErrNotFound)
		require.Nil(t, closer)
	})
}

func TestPebbleDataQuotaAcrossBranchesAndSnapshot(t *testing.T) {
	e := setupPebbleCreate(t, 10)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("data"), Mode: 0o644})
	require.NoError(t, err)
	ino := file.Inode.Ino
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("12345678"), SessionID: &e.sid})
	require.NoError(t, err)
	commit := uuid.NewString()
	storage := e.storage
	branch, err := storage.OpenBranch(e.disk.DiskID, "main")
	require.NoError(t, err)
	require.NoError(t, branch.Checkpoint(commit))
	require.NoError(t, branch.Close())
	require.NoError(t, storage.Fork(e.disk.DiskID, commit, "feature"))
	_, err = e.pool.Exec(e.ctx, `INSERT INTO commits (commit_id, disk_id, branch_name, checkpoint_dir) VALUES ($1, $2, 'main', $3)`, commit, e.disk.DiskID, filepath.Join(e.root, "disks", e.disk.DiskID, "commits", commit))
	require.NoError(t, err)
	_, err = e.pool.Exec(e.ctx, `UPDATE disks SET branches = branches || jsonb_build_array(jsonb_build_object(
		'branch_id', gen_random_uuid(), 'name', 'feature', 'parent_branch', 'main',
		'fork_commit_id', $2::text, 'head_commit_id', $2::text, 'state', 'NORMAL', 'merge_state', NULL)) WHERE disk_id = $1`, e.disk.DiskID, commit)
	require.NoError(t, err)
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.NoError(t, quota.ReplaceBranch("feature", map[uint64]uint64{1: 0, ino: 8}))
	feature, featureSID, snapSID := "feature", uint64(102), uint64(103)
	_, err = e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: featureSID, DiskID: e.disk.DiskID, BranchName: &feature})
	require.NoError(t, err)
	_, err = e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: snapSID, DiskID: e.disk.DiskID, ReadOnly: true, CommitID: &commit})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Fh: 77, Offset: 1, Data: []byte("abc"), SessionID: &featureSID})
	require.NoError(t, err)
	require.Equal(t, uint64(8), quota.Usage().Bytes)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: 8, Data: []byte("90"), SessionID: &featureSID})
	require.NoError(t, err)
	require.Equal(t, uint64(10), quota.Usage().Bytes)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: 4, Data: []byte("ZZ"), SessionID: &featureSID})
	require.NoError(t, err)
	require.Equal(t, uint64(10), quota.Usage().Bytes)
	mode := uint32(0o600)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Mode: &mode, SessionID: &featureSID})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: 10, Data: []byte("x"), SessionID: &featureSID})
	require.ErrorIs(t, err, meta.ErrNoSpace)
	err = e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: ino, Offset: 10, Length: 1, SessionID: &featureSID})
	require.ErrorIs(t, err, meta.ErrNoSpace)
	oversize := uint64(11)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Size: &oversize, SessionID: &featureSID})
	require.ErrorIs(t, err, meta.ErrNoSpace)
	response, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Size: 20, SessionID: &featureSID})
	require.NoError(t, err)
	require.Equal(t, []byte("1abcZZ7890"), response.Data)
	snapshot, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Size: 20, SessionID: &snapSID})
	require.NoError(t, err)
	require.Equal(t, []byte("12345678"), snapshot.Data)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("x"), SessionID: &snapSID})
	require.ErrorIs(t, err, meta.ErrPerm)
	require.NoError(t, e.mgr.Fsync(e.ctx, meta.FsyncParams{Ino: ino, SessionID: &snapSID}))
	require.NoError(t, e.mgr.FsyncDir(e.ctx, meta.FsyncDirParams{Ino: 1, SessionID: &snapSID}))

	smaller := uint64(3)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Size: &smaller, SessionID: &featureSID})
	require.NoError(t, err)
	require.Equal(t, uint64(8), quota.Usage().Bytes)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Mode: &mode, SessionID: &e.sid})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: 7, Data: []byte("!"), SessionID: &e.sid})
	require.NoError(t, err)
	require.Equal(t, uint64(8), quota.Usage().Bytes)
	restarted := meta.New(db.NewStore(e.pool), meta.WithStorageRoot(e.root), meta.WithPebbleManager(e.storage))
	recovered, err := restarted.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, quota.Usage(), recovered.Usage())
}

func TestPebbleDataSparseRangeAndTimestamps(t *testing.T) {
	const blob = pebblekv.BlobSize
	const end = uint64(1027*blob + 11)
	e := setupPebbleCreate(t, end+100)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("sparse"), Mode: 0o6755})
	require.NoError(t, err)
	ino := file.Inode.Ino
	oldTime := time.Date(2001, 2, 3, 4, 5, 6, 123, time.FixedZone("test", 8*3600))
	attr, err := e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Size: ptr(end), Atime: &oldTime, Mtime: &oldTime, SessionID: &e.sid})
	require.NoError(t, err)
	require.Equal(t, oldTime.UTC(), attr.Attr.Atime)
	require.Equal(t, oldTime.UTC(), attr.Attr.Mtime)
	for _, offset := range []uint64{0, 100 * blob, end - 2} {
		_, err := e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: offset, Data: []byte("ab"), SessionID: &e.sid})
		require.NoError(t, err)
	}
	err = e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: ino, Offset: 1, Length: end - 2, Mode: meta.FallocFlPunchHole | meta.FallocFlKeepSize, SessionID: &e.sid})
	require.NoError(t, err)
	for _, tc := range []struct {
		offset uint64
		want   []byte
	}{
		{0, []byte{'a', 0}},
		{100 * blob, []byte{0, 0}},
		{end - 2, []byte{0, 'b'}},
	} {
		got, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Offset: tc.offset, Size: 2, SessionID: &e.sid})
		require.NoError(t, err)
		require.Equal(t, tc.want, got.Data)
	}
	e.withBranch(t, "main", func(store *pebble.DB) {
		lower, upper := pebblekv.DataRange(ino, 0)
		require.NoError(t, store.Compact(e.ctx, lower, upper, true))
	})
	got, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Offset: end - 2, Size: 2, SessionID: &e.sid})
	require.NoError(t, err)
	require.Equal(t, []byte{0, 'b'}, got.Data)
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, end, quota.Usage().Bytes)

	attr, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Mtime: &oldTime, SessionID: &e.sid})
	require.NoError(t, err)
	err = e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: ino, Offset: end + 1, Length: 2, Mode: meta.FallocFlKeepSize, SessionID: &e.sid})
	require.NoError(t, err)
	kept, err := e.mgr.GetAttr(e.branchContext(), ino)
	require.NoError(t, err)
	require.Equal(t, end, kept.Attr.Size)
	require.Equal(t, attr.Attr.Mtime, kept.Attr.Mtime)
	err = e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: ino, Offset: end, Length: 4, SessionID: &e.sid})
	require.NoError(t, err)
	grown, err := e.mgr.GetAttr(e.branchContext(), ino)
	require.NoError(t, err)
	require.Equal(t, end+4, grown.Attr.Size)
	require.True(t, grown.Attr.Mtime.After(kept.Attr.Mtime))
	require.Equal(t, end+4, quota.Usage().Bytes)
}

func TestPebbleDataSnapshotOrphans(t *testing.T) {
	f := newPebbleReadFixture(t)
	_, err := f.mgr.Read(f.ctx, &meta.ReadRequest{Ino: 9, Size: 20, SessionID: &f.snap})
	require.ErrorIs(t, err, meta.ErrNotFound)
	require.ErrorIs(t, f.mgr.Fsync(f.ctx, meta.FsyncParams{Ino: 9, SessionID: &f.snap}), meta.ErrNotFound)
	require.ErrorIs(t, f.mgr.FsyncDir(f.ctx, meta.FsyncDirParams{Ino: 11, SessionID: &f.snap}), meta.ErrNotFound)
	_, err = f.mgr.Read(f.ctx, &meta.ReadRequest{Ino: 10, Size: 1, SessionID: &f.snap})
	require.ErrorIs(t, err, meta.ErrNotFound)
}

func TestPebbleDataConcurrentBranchQuota(t *testing.T) {
	e := setupPebbleCreate(t, 10)
	mainFile, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("main"), Mode: 0o644})
	require.NoError(t, err)
	storage := e.storage
	require.NoError(t, storage.Fork(e.disk.DiskID, *e.disk.Branches[0].HeadCommitID, "feature"))
	_, err = e.pool.Exec(e.ctx, `UPDATE disks SET branches = branches || jsonb_build_array(jsonb_build_object(
		'branch_id', gen_random_uuid(), 'name', 'feature', 'parent_branch', 'main',
		'fork_commit_id', NULL, 'head_commit_id', NULL, 'state', 'NORMAL', 'merge_state', NULL)) WHERE disk_id = $1`, e.disk.DiskID)
	require.NoError(t, err)
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.NoError(t, quota.ReplaceBranch("feature", map[uint64]uint64{1: 0}))
	feature, sid := "feature", uint64(102)
	_, err = e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: sid, DiskID: e.disk.DiskID, BranchName: &feature})
	require.NoError(t, err)
	featureFile, err := e.mgr.Create(meta.WithSessionID(e.ctx, &sid), meta.CreateParams{ParentIno: 1, Name: []byte("feature"), Mode: 0o644})
	require.NoError(t, err)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, request := range []meta.WriteRequest{
		{Ino: mainFile.Inode.Ino, Data: []byte("123456"), SessionID: &e.sid},
		{Ino: featureFile.Inode.Ino, Data: []byte("abcdef"), SessionID: &sid},
	} {
		go func(req meta.WriteRequest) {
			<-start
			_, err := e.mgr.Write(e.ctx, &req)
			results <- err
		}(request)
	}
	close(start)
	var success, denied int
	for range 2 {
		err := <-results
		if err == nil {
			success++
		} else if errors.Is(err, meta.ErrNoSpace) {
			denied++
		} else {
			t.Fatalf("并发写入出现意外错误: %v", err)
		}
	}
	require.Equal(t, 1, success)
	require.Equal(t, 1, denied)
	require.Equal(t, uint64(6), quota.Usage().Bytes)
	var total int
	for _, test := range []struct {
		request meta.ReadRequest
		data    []byte
	}{
		{meta.ReadRequest{Ino: mainFile.Inode.Ino, Size: 6, SessionID: &e.sid}, []byte("123456")},
		{meta.ReadRequest{Ino: featureFile.Inode.Ino, Size: 6, SessionID: &sid}, []byte("abcdef")},
	} {
		result, err := e.mgr.Read(e.ctx, &test.request)
		require.NoError(t, err)
		require.Contains(t, []int{0, 6}, len(result.Data))
		if len(result.Data) != 0 {
			require.Equal(t, test.data, result.Data)
		}
		total += len(result.Data)
	}
	require.Equal(t, 6, total)
}

func TestPebbleDataBoundsAndSequenceExhaustion(t *testing.T) {
	e := setupPebbleCreate(t, 100)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("data"), Mode: 0o644})
	require.NoError(t, err)
	ino := file.Inode.Ino
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("abc"), SessionID: &e.sid})
	require.NoError(t, err)
	_, err = e.mgr.Read(e.ctx, nil)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.Write(e.ctx, nil)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	invalidID := uint64(0)
	for _, request := range []meta.ReadRequest{
		{Ino: 0, SessionID: &e.sid},
		{Ino: ino, Fh: math.MaxUint64, SessionID: &e.sid},
		{Ino: ino, Offset: math.MaxUint64, SessionID: &e.sid},
		{Ino: ino, Size: meta.BlockSize + 1, SessionID: &e.sid},
		{Ino: ino, SessionID: &invalidID},
	} {
		_, err := e.mgr.Read(e.ctx, &request)
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	}
	for _, request := range []meta.WriteRequest{
		{Ino: 0, SessionID: &e.sid},
		{Ino: ino, Fh: math.MaxUint64, SessionID: &e.sid},
		{Ino: ino, Offset: math.MaxInt64, Data: []byte("a"), SessionID: &e.sid},
		{Ino: ino, Data: make([]byte, meta.BlockSize+1), SessionID: &e.sid},
		{Ino: ino, SessionID: &invalidID},
	} {
		_, err := e.mgr.Write(e.ctx, &request)
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	}
	require.ErrorIs(t, e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: ino, Offset: math.MaxInt64, Length: 1, SessionID: &e.sid}), meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.Fsync(e.ctx, meta.FsyncParams{Ino: ino, Fh: math.MaxUint64, SessionID: &e.sid}), meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.FsyncDir(e.ctx, meta.FsyncDirParams{Ino: 0, SessionID: &e.sid}), meta.ErrInvalidArgument)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{SessionID: &e.sid})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Atime: ptr(time.Now()), AtimeNow: true, SessionID: &e.sid})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)

	var original []byte
	e.withBranch(t, "main", func(store *pebble.DB) {
		original = getPebbleValue(t, store, pebblekv.InodeKey(ino))
		stat, err := pebblekv.DecodeBranchStat(getPebbleValue(t, store, pebblekv.StatKey()))
		require.NoError(t, err)
		stat.NextWriteSeq = math.MaxUint64
		require.NoError(t, store.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(stat), pebble.Sync))
	})
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: 3, Data: []byte("d"), SessionID: &e.sid})
	require.ErrorIs(t, err, meta.ErrNoSpace)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Mode: ptr(uint32(0o600)), SessionID: &e.sid})
	require.ErrorIs(t, err, meta.ErrNoSpace)
	require.ErrorIs(t, e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: ino, Offset: 3, Length: 1, SessionID: &e.sid}), meta.ErrNoSpace)
	e.withBranch(t, "main", func(store *pebble.DB) {
		require.Equal(t, original, getPebbleValue(t, store, pebblekv.InodeKey(ino)))
	})
	got, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Size: 10, SessionID: &e.sid})
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), got.Data)
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(3), quota.Usage().Bytes)
}

func TestPebbleDataValidationFlagsOrphansAndSync(t *testing.T) {
	e := setupPebbleCreate(t, 100)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("data"), Mode: 0o6755})
	require.NoError(t, err)
	ino := file.Inode.Ino
	sym, err := e.mgr.Symlink(e.branchContext(), meta.SymlinkParams{ParentIno: 1, Name: []byte("link"), Target: []byte("x")})
	require.NoError(t, err)
	flags := uint32(meta.FlagImmutable)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Flags: &flags, SessionID: &e.sid})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, SessionID: &e.sid})
	require.ErrorIs(t, err, meta.ErrPerm)
	mode := uint32(0o600)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Mode: &mode, SessionID: &e.sid})
	require.ErrorIs(t, err, meta.ErrPerm)
	require.ErrorIs(t, e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: ino, Length: 1, SessionID: &e.sid}), meta.ErrPerm)
	flags = meta.FlagAppend
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Flags: &flags, SessionID: &e.sid})
	require.NoError(t, err)
	size := uint64(1)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Size: &size, SessionID: &e.sid})
	require.ErrorIs(t, err, meta.ErrPerm)
	require.ErrorIs(t, e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: ino, Length: 1, SessionID: &e.sid}), meta.ErrPerm)
	flags = 0
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Flags: &flags, SessionID: &e.sid})
	require.NoError(t, err)

	e.withBranch(t, "main", func(store *pebble.DB) {
		inode := readPebbleCreateInode(t, store, ino)
		inode.Nlink = 0
		value, err := pebblekv.EncodeInode(inode)
		require.NoError(t, err)
		require.NoError(t, store.Set(pebblekv.InodeKey(ino), value, pebble.Sync))
		require.NoError(t, store.Set(pebblekv.OrphanKey(ino), []byte{1}, pebble.Sync))
	})
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("abc"), SessionID: &e.sid})
	require.NoError(t, err)
	got, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Size: 5, SessionID: &e.sid})
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), got.Data)
	_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Mode: &mode, SessionID: &e.sid})
	require.NoError(t, err)
	require.NoError(t, e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: ino, Offset: 1, Length: 1, Mode: meta.FallocFlPunchHole | meta.FallocFlKeepSize, SessionID: &e.sid}))
	require.NoError(t, e.mgr.Fsync(e.ctx, meta.FsyncParams{Ino: ino, SessionID: &e.sid}))
	records := readDataWAL(t, e)
	var barrier bool
	for _, entry := range records {
		barrier = barrier || entry.kind == pebble.InternalKeyKindLogData
	}
	require.True(t, barrier, "文件同步必须写入 WAL 屏障")
	require.NoError(t, e.mgr.FsyncDir(e.ctx, meta.FsyncDirParams{Ino: 1, SessionID: &e.sid}))
	records = readDataWAL(t, e)
	barrier = false
	for _, entry := range records {
		barrier = barrier || entry.kind == pebble.InternalKeyKindLogData
	}
	require.True(t, barrier, "目录同步必须写入 WAL 屏障")
	require.ErrorIs(t, e.mgr.Fsync(e.ctx, meta.FsyncParams{Ino: 1, SessionID: &e.sid}), meta.ErrIsDir)
	require.ErrorIs(t, e.mgr.FsyncDir(e.ctx, meta.FsyncDirParams{Ino: ino, SessionID: &e.sid}), meta.ErrNotDir)
	_, err = e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: 1, SessionID: &e.sid})
	require.ErrorIs(t, err, meta.ErrIsDir)
	_, err = e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: sym.Ino, SessionID: &e.sid})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: sym.Ino, SessionID: &e.sid})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: sym.Ino, Length: 1, SessionID: &e.sid}), meta.ErrNotSupported)
	_, err = e.mgr.SetAttr(e.ctx, sym.Ino, meta.SetAttrInput{Size: &size, SessionID: &e.sid})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: ino, Length: 1, Mode: meta.FallocFlPunchHole, SessionID: &e.sid}), meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: ino, Length: 1, Mode: meta.FallocFlInsertRange, SessionID: &e.sid}), meta.ErrNotSupported)
	_, err = e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: math.MaxInt64, Data: []byte("x"), SessionID: &e.sid})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	e.withBranch(t, "main", func(store *pebble.DB) {
		require.NoError(t, store.Delete(pebblekv.OrphanKey(ino), pebble.Sync))
	})
	_, err = e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, SessionID: &e.sid})
	require.ErrorIs(t, err, meta.ErrNotFound)
}
