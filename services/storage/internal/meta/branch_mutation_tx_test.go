package meta_test

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

func TestBranchMutationTxDirectMechanics(t *testing.T) {
	root := t.TempDir()
	storage := newTestStorage(t, root)
	branch, err := storage.OpenBranch("disk-tx", "main")
	require.NoError(t, err)
	defer func() { require.NoError(t, branch.Close()) }()

	// 初始化分支 StatKey
	require.NoError(t, branch.DB().Set(
		pebblekv.StatKey(),
		pebblekv.EncodeBranchStat(pebblekv.BranchStat{Inodes: 1, NextWriteSeq: 10}),
		pebble.Sync,
	))
	quota := meta.NewTreeQuota(100)
	require.NoError(t, quota.ReplaceBranch("main", map[uint64]uint64{1: 0}))

	// 1. 新建节点事务：校验 WriteSeq 自动写入、DirtyKey 自动登记、Stat 递增与 MarkBranchDirty
	var createdInode pebblekv.Inode
	err = meta.RunBranchMutationTx(storage, "disk-tx", "main", branch.DB(), branch.Blobs(), quota, func(tx *meta.BranchMutationTx) error {
		if reqErr := tx.RequireNewInode(40); reqErr != nil {
			return reqErr
		}
		createdInode = pebblekv.Inode{Ino: 2, Mode: meta.S_IFREG | 0o644, Nlink: 1, Size: 40}
		if putErr := tx.PutInode(&createdInode); putErr != nil {
			return putErr
		}
		if entryErr := tx.PutEntry(1, []byte("hello"), 2, createdInode.Mode); entryErr != nil {
			return entryErr
		}
		tx.AddInodes(1)
		tx.AddLogicalBytes(40)
		return tx.CommitCreate(2, 40)
	})
	require.NoError(t, err)
	require.Equal(t, uint64(10), createdInode.WriteSeq)
	require.True(t, storage.BranchDirty("disk-tx", "main"))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 40, Inodes: 2}, quota.Usage())
	require.Equal(t, []byte{1}, getPebbleValue(t, branch.DB(), pebblekv.DirtyKey(2)))

	// 2. 数据写入事务：当配额不足返回 ErrNoSpace 时，beforeCommit 不得执行，且新分配的临时 Blob 自动回滚删除
	require.NoError(t, branch.Blobs().Write(501, []byte("temp-new-blob")))
	require.NoError(t, branch.Blobs().Write(500, []byte("keep-old-blob")))
	beforeCommitCalled := false
	err = meta.RunBranchMutationTx(storage, "disk-tx", "main", branch.DB(), branch.Blobs(), quota, func(tx *meta.BranchMutationTx) error {
		tx.TrackNewBlobs(501)
		tx.TrackRemovedBlobs(500)
		tx.SetBeforeCommit(func() error {
			beforeCommitCalled = true
			return nil
		})
		updated := createdInode
		updated.Size = 200 // 超过配额上限 100
		if putErr := tx.PutInode(&updated); putErr != nil {
			return putErr
		}
		if adjErr := tx.AdjustLogicalBytes(40, 200); adjErr != nil {
			return adjErr
		}
		return tx.CommitApplyChange(2, 200, true, true)
	})
	require.ErrorIs(t, err, meta.ErrNoSpace)
	require.False(t, beforeCommitCalled, "配额拒绝时 beforeCommit 钩子不得执行")
	require.False(t, branch.Blobs().Exists(501), "提交失败时新分配的 Blob 必须自动回滚清理")
	require.True(t, branch.Blobs().Exists(500), "提交失败时旧 Blob 必须保留")
}

func TestBranchMutationTxWriteSeqDirtyKeyAndBlobCleanup(t *testing.T) {
	e := setupPebbleCreate(t, 2*pebblekv.BlobSize)

	// 1. 创建节点（Create）递增 WriteSeq、Inodes 并记录父子 DirtyKey
	created, err := e.mgr.Create(e.ctx, meta.CreateParams{
		ParentIno: 1, Name: []byte("f1"), Mode: 0o644, SessionID: &e.sid,
	})
	require.NoError(t, err)
	ino := created.Inode.Ino

	e.withBranch(t, "main", func(store *pebble.DB) {
		root := readPebbleCreateInode(t, store, 1)
		child := readPebbleCreateInode(t, store, ino)
		require.Equal(t, uint64(1), root.WriteSeq)
		require.Equal(t, uint64(1), child.WriteSeq)
		require.Equal(t, []byte{1}, getPebbleValue(t, store, pebblekv.DirtyKey(1)))
		require.Equal(t, []byte{1}, getPebbleValue(t, store, pebblekv.DirtyKey(ino)))
	})

	// 2. 硬链接（Link）递增 WriteSeq 并校验配额
	_, err = e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main", Message: "c1"})
	require.NoError(t, err)
	linked, err := e.mgr.Link(e.ctx, meta.LinkParams{
		SessionID: &e.sid, Ino: ino, NewParentIno: 1, NewName: []byte("f1_link"),
	})
	require.NoError(t, err)
	require.Equal(t, uint32(2), linked.Attr.Nlink)

	e.withBranch(t, "main", func(store *pebble.DB) {
		root := readPebbleCreateInode(t, store, 1)
		child := readPebbleCreateInode(t, store, ino)
		require.Equal(t, uint64(2), root.WriteSeq)
		require.Equal(t, uint64(2), child.WriteSeq)
		require.Equal(t, []byte{1}, getPebbleValue(t, store, pebblekv.DirtyKey(1)))
		require.Equal(t, []byte{1}, getPebbleValue(t, store, pebblekv.DirtyKey(ino)))
	})

	// 3. 扩展属性（SetXattr / RemoveXattr）通过统一事务更新 WriteSeq 与 DirtyKey
	require.NoError(t, e.mgr.SetXattr(e.ctx, meta.SetXattrParams{
		SessionID: &e.sid, Ino: ino, Name: []byte("user.tag"), Value: []byte("v1"),
	}))
	e.withBranch(t, "main", func(store *pebble.DB) {
		child := readPebbleCreateInode(t, store, ino)
		require.Equal(t, uint64(3), child.WriteSeq)
	})
	require.NoError(t, e.mgr.RemoveXattr(e.ctx, &e.sid, ino, []byte("user.tag")))
	e.withBranch(t, "main", func(store *pebble.DB) {
		child := readPebbleCreateInode(t, store, ino)
		require.Equal(t, uint64(4), child.WriteSeq)
	})

	// 4. 首次写入窗口内块，随后触发配额超限（ErrNoSpace）：
	// 验证 beforeCommit 补丁钩子在配额拒绝时不执行，且超额失败的新块被清理、原窗口内脏块内容保持不变。
	initialPayload := bytes.Repeat([]byte("A"), 4096)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{
		Ino: ino, Offset: 0, Data: initialPayload, SessionID: &e.sid,
	})
	require.NoError(t, err)

	var firstBlobID uint64
	e.withBranch(t, "main", func(store *pebble.DB) {
		ref, decodeErr := pebblekv.DecodeBlobRef(getPebbleValue(t, store, pebblekv.DataKey(ino, 0)))
		require.NoError(t, decodeErr)
		firstBlobID = ref.ID
	})

	// 在偏移 0 覆盖 "BBBB" 并同时跨越配额上限写入第 3 个 1MiB 块：
	// 若 beforeCommit 在配额检查前执行，块 0 的前 4 字节会被污染为 "BBBB"。
	// 正确行为是：配额检查返回 ErrNoSpace，beforeCommit 不执行，新分配的跨块临时 Blob 被回滚清理，块 0 仍为全 'A'。
	oversize := make([]byte, 2*pebblekv.BlobSize+4096)
	copy(oversize, "BBBB")
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{
		Ino: ino, Offset: 0, Data: oversize, SessionID: &e.sid,
	})
	require.ErrorIs(t, err, meta.ErrNoSpace)

	readBack, err := e.mgr.Read(e.ctx, &meta.ReadRequest{
		Ino: ino, Offset: 0, Size: 4096, SessionID: &e.sid,
	})
	require.NoError(t, err)
	require.Equal(t, initialPayload, readBack.Data, "ErrNoSpace 时 beforeCommit 原地补丁不得污染旧块")

	// 5. 整块覆写（fullBlockWrite）提交成功后，立即删除被替换的旧 Blob 文件
	fullBlock := bytes.Repeat([]byte("C"), pebblekv.BlobSize)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{
		Ino: ino, Offset: 0, Data: fullBlock, SessionID: &e.sid,
	})
	require.NoError(t, err)

	branch, err := e.storage.OpenBranch(e.disk.DiskID, "main")
	require.NoError(t, err)
	defer func() { require.NoError(t, branch.Close()) }()
	require.False(t, branch.Blobs().Exists(firstBlobID), "被整块覆写替换的旧块文件必须在提交后清理")
}
