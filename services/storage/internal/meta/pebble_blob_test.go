package meta_test

import (
	"bytes"
	"fmt"
	"math"
	"os"
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

// Commit / Fork / Rollback 通过硬链接共享未修改块，子分支覆写或删除不影响父快照。
func TestPebbleBlobCommitForkRollbackIsolation(t *testing.T) {
	const size = pebblekv.BlobSize
	e := setupPebbleCreate(t, 8*size)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("f"), Mode: 0o644})
	require.NoError(t, err)
	ino := file.Inode.Ino
	original := bytes.Repeat([]byte{0x21}, size)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: original, SessionID: &e.sid})
	require.NoError(t, err)
	commit, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)

	refOf := func(branch string) pebblekv.BlobRef {
		t.Helper()
		handle, err := e.storage.OpenBranch(e.disk.DiskID, branch)
		require.NoError(t, err)
		defer func() { require.NoError(t, handle.Close()) }()
		value, closer, err := handle.DB().Get(pebblekv.DataKey(ino, 0))
		require.NoError(t, err)
		ref, err := pebblekv.DecodeBlobRef(value)
		require.NoError(t, err)
		require.NoError(t, closer.Close())
		return ref
	}
	mainRef := refOf("main")
	commitHandle, err := e.storage.OpenCommit(e.disk.DiskID, commit.CommitID)
	require.NoError(t, err)
	commitValue, closer, err := commitHandle.DB().Get(pebblekv.DataKey(ino, 0))
	require.NoError(t, err)
	commitRef, err := pebblekv.DecodeBlobRef(commitValue)
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	require.NoError(t, commitHandle.Close())
	mainInfo, err := os.Stat(metaBlobPath(e.root, e.disk.DiskID, "branches", "main", mainRef.ID))
	require.NoError(t, err)
	commitInfo, err := os.Stat(metaBlobPath(e.root, e.disk.DiskID, "commits", commit.CommitID, commitRef.ID))
	require.NoError(t, err)
	require.True(t, os.SameFile(mainInfo, commitInfo), "Commit 必须硬链接未修改块")

	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "dev"})
	require.NoError(t, err)
	versionSession(t, e, "dev", 102, false, nil)
	devCtx := meta.WithSessionID(e.ctx, ptrVersionSID(102))
	devRef := refOf("dev")
	devInfo, err := os.Stat(metaBlobPath(e.root, e.disk.DiskID, "branches", "dev", devRef.ID))
	require.NoError(t, err)
	require.True(t, os.SameFile(commitInfo, devInfo), "Fork 必须硬链接未修改块")

	overwritten := bytes.Repeat([]byte{0x22}, size)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: overwritten, SessionID: ptrVersionSID(102)})
	require.NoError(t, err)
	versionRead(t, e, e.sid, ino, original)
	versionRead(t, e, 102, ino, overwritten)
	require.NoError(t, e.mgr.Unlink(devCtx, 1, []byte("f")))
	versionRead(t, e, e.sid, ino, original)

	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	require.NoError(t, e.mgr.Rollback(e.ctx, meta.RollbackParams{DiskID: e.disk.DiskID, BranchName: "main", CommitID: commit.CommitID}))
	versionSession(t, e, "main", 103, false, nil)
	versionRead(t, e, 103, ino, original)
}

// refOf 读取指定块在 Pebble 中的 BlobRef。
func refOf(t *testing.T, e *pebbleCreateEnv, ino, block uint64) pebblekv.BlobRef {
	t.Helper()
	var ref pebblekv.BlobRef
	e.withBranch(t, "main", func(store *pebble.DB) {
		decoded, err := pebblekv.DecodeBlobRef(getPebbleValue(t, store, pebblekv.DataKey(ino, block)))
		require.NoError(t, err)
		ref = decoded
	})
	return ref
}

// 窗口内块的内区间覆写必须复用同一块编号，不分配新块、不重写整个 1 MiB 块。
func TestPebbleWindowLocalInteriorOverwriteReusesBlob(t *testing.T) {
	e := setupPebbleCreate(t, 4*pebblekv.BlobSize)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("interior"), Mode: 0o644})
	require.NoError(t, err)
	ino := file.Inode.Ino

	base := bytes.Repeat([]byte{0x11}, 4096)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: base, SessionID: &e.sid})
	require.NoError(t, err)
	first := refOf(t, e, ino, 0)
	filesBefore := len(blobFiles(t, e.root, e.disk.DiskID))

	patch := bytes.Repeat([]byte{0x22}, 16)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: 100, Data: patch, SessionID: &e.sid})
	require.NoError(t, err)
	after := refOf(t, e, ino, 0)

	require.Equal(t, first.ID, after.ID, "窗口内块内覆写必须复用同一块编号")
	require.Equal(t, first.Length, after.Length, "块内覆写不得改变逻辑长度")
	require.Len(t, blobFiles(t, e.root, e.disk.DiskID), filesBefore, "块内覆写不得产生新块文件")

	model := bytes.Clone(base)
	copy(model[100:], patch)
	assertPebbleData(t, e, ino, model)

	// 独立判据：绕过热缓存直接从磁盘读取块文件，内容必须与内存模型一致。
	cold := pebblekv.NewBlobStore(filepath.Join(e.root, "disks", e.disk.DiskID, "branches", "main"))
	onDisk, err := cold.Read(after.ID, int(after.Length))
	require.NoError(t, err)
	require.Equal(t, model, onDisk)
}

// 窗口内跨越旧块尾部的空洞写入必须复用同一块编号，并把空洞补零后写入新数据。
func TestPebbleWindowLocalGapWriteReusesBlob(t *testing.T) {
	e := setupPebbleCreate(t, 4*pebblekv.BlobSize)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("gap"), Mode: 0o644})
	require.NoError(t, err)
	ino := file.Inode.Ino

	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("abc"), SessionID: &e.sid})
	require.NoError(t, err)
	first := refOf(t, e, ino, 0)
	filesBefore := len(blobFiles(t, e.root, e.disk.DiskID))

	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: 10, Data: []byte("xyz"), SessionID: &e.sid})
	require.NoError(t, err)
	after := refOf(t, e, ino, 0)

	require.Equal(t, first.ID, after.ID, "窗口内空洞写入必须复用同一块编号")
	require.Equal(t, uint32(13), after.Length)
	require.Len(t, blobFiles(t, e.root, e.disk.DiskID), filesBefore)

	model := append([]byte("abc"), make([]byte, 7)...)
	model = append(model, []byte("xyz")...)
	assertPebbleData(t, e, ino, model)

	cold := pebblekv.NewBlobStore(filepath.Join(e.root, "disks", e.disk.DiskID, "branches", "main"))
	onDisk, err := cold.Read(after.ID, int(after.Length))
	require.NoError(t, err)
	require.Equal(t, model, onDisk)
}

// 配额或写序号耗尽导致 ErrNoSpace 时，原地补丁绝不能提前落盘，旧块内容必须保持完整。
func TestPebbleWindowLocalPatchPreservedOnNoSpace(t *testing.T) {
	e := setupPebbleCreate(t, 4*pebblekv.BlobSize)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("nospace"), Mode: 0o644})
	require.NoError(t, err)
	ino := file.Inode.Ino

	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("abc"), SessionID: &e.sid})
	require.NoError(t, err)
	before := refOf(t, e, ino, 0)

	e.withBranch(t, "main", func(store *pebble.DB) {
		stat, err := pebblekv.DecodeBranchStat(getPebbleValue(t, store, pebblekv.StatKey()))
		require.NoError(t, err)
		stat.NextWriteSeq = math.MaxUint64
		require.NoError(t, store.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(stat), pebble.Sync))
	})

	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: 1, Data: []byte("Z"), SessionID: &e.sid})
	require.ErrorIs(t, err, meta.ErrNoSpace)

	after := refOf(t, e, ino, 0)
	require.Equal(t, before, after, "ErrNoSpace 时 BlobRef 必须保持不变")
	got, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Size: 3, SessionID: &e.sid})
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), got.Data, "ErrNoSpace 时旧块内容必须保持完整")

	cold := pebblekv.NewBlobStore(filepath.Join(e.root, "disks", e.disk.DiskID, "branches", "main"))
	onDisk, err := cold.Read(after.ID, int(after.Length))
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), onDisk, "磁盘上的旧块不得被未提交补丁污染")
}

// TestPebbleWindowLocalAppendNoFullCopy 锁定窗口内尾部追加的零拷贝补丁路径：
// 对同一个 1 MiB 块连续 256 次 4 KiB 追加，不得读取旧块、不得分配新块，
// 最终块文件内容必须与写入模型逐字节一致。
func TestPebbleWindowLocalAppendNoFullCopy(t *testing.T) {
	e := setupPebbleCreate(t, 4*pebblekv.BlobSize)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("append"), Mode: 0o644})
	require.NoError(t, err)
	ino := file.Inode.Ino

	branch, err := e.storage.OpenBranch(e.disk.DiskID, "main")
	require.NoError(t, err)
	defer func() { require.NoError(t, branch.Close()) }()
	blobs := branch.Blobs()

	const chunkSize = 4096
	model := make([]byte, 0, pebblekv.BlobSize)
	for i := 0; i < pebblekv.BlobSize/chunkSize; i++ {
		chunk := bytes.Repeat([]byte{byte(i + 1)}, chunkSize)
		model = append(model, chunk...)
		_, err := e.mgr.Write(e.ctx, &meta.WriteRequest{
			Ino: ino, Offset: uint64(i * chunkSize), Data: chunk, SessionID: &e.sid,
		})
		require.NoError(t, err)
	}

	require.Zero(t, blobs.ReadOps(), "窗口内尾部追加不得读取旧块构造整块缓冲区")
	require.Len(t, blobFiles(t, e.root, e.disk.DiskID), 1, "256 次追加必须复用同一个块文件")
	ref := refOf(t, e, ino, 0)
	require.Equal(t, uint32(pebblekv.BlobSize), ref.Length)

	got, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Size: uint32(len(model)), SessionID: &e.sid})
	require.NoError(t, err)
	require.Equal(t, model, got.Data)

	cold := pebblekv.NewBlobStore(filepath.Join(e.root, "disks", e.disk.DiskID, "branches", "main"))
	onDisk, err := cold.Read(ref.ID, int(ref.Length))
	require.NoError(t, err)
	require.Equal(t, model, onDisk)
}

// TestPebbleWindowLocalGapStillFullCopy 锁定空洞写入必须仍走整块拷贝补零：
// 旧块内容被读入新缓冲区，跨越旧尾部的区间补零后再追加新数据。
func TestPebbleWindowLocalGapStillFullCopy(t *testing.T) {
	e := setupPebbleCreate(t, 4*pebblekv.BlobSize)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("gapcopy"), Mode: 0o644})
	require.NoError(t, err)
	ino := file.Inode.Ino

	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("abc"), SessionID: &e.sid})
	require.NoError(t, err)

	branch, err := e.storage.OpenBranch(e.disk.DiskID, "main")
	require.NoError(t, err)
	defer func() { require.NoError(t, branch.Close()) }()
	blobs := branch.Blobs()
	readsBefore := blobs.ReadOps()

	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: 10, Data: []byte("xyz"), SessionID: &e.sid})
	require.NoError(t, err)

	require.Equal(t, readsBefore+1, blobs.ReadOps(), "空洞写入必须读取旧块并补零")

	want := append([]byte("abc"), make([]byte, 7)...)
	want = append(want, []byte("xyz")...)
	ref := refOf(t, e, ino, 0)
	require.Equal(t, uint32(len(want)), ref.Length)

	got, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Size: uint32(len(want)), SessionID: &e.sid})
	require.NoError(t, err)
	require.Equal(t, want, got.Data)

	cold := pebblekv.NewBlobStore(filepath.Join(e.root, "disks", e.disk.DiskID, "branches", "main"))
	onDisk, err := cold.Read(ref.ID, int(ref.Length))
	require.NoError(t, err)
	require.Equal(t, want, onDisk)
}
