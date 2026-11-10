package server_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/server"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func requireConnectError(t *testing.T, err error, code connect.Code, reason string) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, code, connect.CodeOf(err))
	info := server.ExtractErrorInfo(err)
	require.NotNil(t, info, "期望错误附带 google.rpc.ErrorInfo: %v", err)
	require.Equal(t, reason, info.GetReason())
	require.Equal(t, "bowl.storage", info.GetDomain())
}

func TestControlPlaneAndMountDiskRPCs(t *testing.T) {
	client, mgr, ctx := newTestServerAndClient(t, "srv_ctrl_plane")

	// 1. CreateDisk
	createResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "alpha",
		MaxBytes: 8192,
	}))
	require.NoError(t, err)
	disk := createResp.Msg.GetDisk()
	require.NotEmpty(t, disk.GetDiskId())
	require.Equal(t, "alpha", disk.GetName())
	require.Equal(t, uint64(1), disk.GetRootIno())
	require.Equal(t, uint64(8192), disk.GetMaxBytes())
	require.Zero(t, disk.GetUsedBytes())
	require.Equal(t, uint64(1), disk.GetUsedInodes())

	// 2. GetDisk
	getResp, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{
		DiskId: disk.GetDiskId(),
	}))
	require.NoError(t, err)
	require.Equal(t, disk.GetDiskId(), getResp.Msg.GetDisk().GetDiskId())
	require.Equal(t, "alpha", getResp.Msg.GetDisk().GetName())

	// 3. ListDisks
	listResp, err := client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{
		PageSize: 10,
	}))
	require.NoError(t, err)
	require.Len(t, listResp.Msg.GetDisks(), 1)
	require.Equal(t, disk.GetDiskId(), listResp.Msg.GetDisks()[0].GetDiskId())

	// 4. MountDisk 只读握手 (epoch=0) 与可写握手 (epoch=1)
	roMount, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   disk.GetDiskId(),
		ReadOnly: true,
	}))
	require.NoError(t, err)
	require.Zero(t, roMount.Msg.GetEpoch())
	require.Equal(t, uint64(1), roMount.Msg.GetRootInode().GetIno())
	require.InDelta(t, 1.5, roMount.Msg.GetEntryTimeoutSeconds(), 1e-6)
	require.InDelta(t, 2.5, roMount.Msg.GetAttrTimeoutSeconds(), 1e-6)

	rwMount, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   disk.GetDiskId(),
		ReadOnly: false,
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(1), rwMount.Msg.GetEpoch())
	require.Equal(t, uint64(1), rwMount.Msg.GetRootInode().GetIno())

	// 5. 写入 1024B 后验证 UpdateDisk 改名、扩容与缩容保护拦截 (CodeFailedPrecondition / EINVAL)
	actor, err := mgr.Actor(ctx, disk.GetDiskId())
	require.NoError(t, err)
	created, err := actor.Create(store.CreateReq{
		ParentIno: 1,
		Name:      []byte("seed.bin"),
		Mode:      0o644,
		Epoch:     rwMount.Msg.GetEpoch(),
	})
	require.NoError(t, err)
	_, _, err = actor.Write(created.Ino, 0, make([]byte, 1024), rwMount.Msg.GetEpoch())
	require.NoError(t, err)

	newName := "alpha-renamed"
	expanded := uint64(16384)
	updateResp, err := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   disk.GetDiskId(),
		Name:     &newName,
		MaxBytes: &expanded,
	}))
	require.NoError(t, err)
	require.Equal(t, "alpha-renamed", updateResp.Msg.GetDisk().GetName())
	require.Equal(t, uint64(16384), updateResp.Msg.GetDisk().GetMaxBytes())
	require.Equal(t, uint64(1024), updateResp.Msg.GetDisk().GetUsedBytes())

	tooSmall := uint64(512)
	_, err = client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   disk.GetDiskId(),
		MaxBytes: &tooSmall,
	}))
	requireConnectError(t, err, connect.CodeFailedPrecondition, "EINVAL")

	// 6. DeleteDisk
	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
		DiskId: disk.GetDiskId(),
	}))
	require.NoError(t, err)

	_, err = client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{
		DiskId: disk.GetDiskId(),
	}))
	requireConnectError(t, err, connect.CodeNotFound, "ENOENT")
}

func TestDataPlaneRPCsEndToEnd(t *testing.T) {
	client, _, ctx := newTestServerAndClient(t, "srv_data_plane")

	createDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "data-disk",
		MaxBytes: 1 << 20,
	}))
	require.NoError(t, err)
	diskID := createDiskResp.Msg.GetDisk().GetDiskId()

	mountResp, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: false,
	}))
	require.NoError(t, err)
	epoch := mountResp.Msg.GetEpoch()
	require.Equal(t, uint64(1), epoch)

	// 1. Mkdir & Lookup & GetAttr & FsyncDir
	mkdirResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		ParentIno: 1,
		Name:      []byte("dir1"),
		Mode:      0o755,
		Uid:       1000,
		Gid:       1000,
		DiskId:    diskID,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	dirIno := mkdirResp.Msg.GetInode().GetIno()
	require.Greater(t, dirIno, uint64(1))
	require.InDelta(t, 3.5, mkdirResp.Msg.GetEntryTimeoutSeconds(), 1e-6)

	lookupResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: 1,
		Name:      []byte("dir1"),
		DiskId:    diskID,
	}))
	require.NoError(t, err)
	require.Equal(t, dirIno, lookupResp.Msg.GetInode().GetIno())

	getAttrResp, err := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		Ino:    dirIno,
		DiskId: diskID,
	}))
	require.NoError(t, err)
	require.Equal(t, dirIno, getAttrResp.Msg.GetInode().GetIno())

	_, err = client.FsyncDir(ctx, connect.NewRequest(&storagev1.FsyncDirRequest{
		Ino:      dirIno,
		Fh:       1,
		Datasync: true,
		DiskId:   diskID,
		Epoch:    epoch,
	}))
	require.NoError(t, err)

	// 2. Create, Write, Read, Fallocate, SetAttr, Fsync
	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: dirIno,
		Name:      []byte("file.txt"),
		Mode:      0o644,
		Uid:       1000,
		Gid:       1000,
		Flags:     0o2,
		DiskId:    diskID,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	fileIno := createResp.Msg.GetInode().GetIno()
	require.Equal(t, uint64(1), createResp.Msg.GetFh())
	require.Equal(t, uint32(1<<1), createResp.Msg.GetOpenFlags())

	payload := []byte("hello-actor-data-plane")
	writeResp, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		Ino:    fileIno,
		Fh:     1,
		Offset: 0,
		Data:   payload,
		DiskId: diskID,
		Epoch:  epoch,
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(len(payload)), writeResp.Msg.GetSize())
	require.Equal(t, uint64(len(payload)), writeResp.Msg.GetInode().GetAttr().GetSize())

	readResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		Ino:    fileIno,
		Fh:     1,
		Offset: 0,
		Size:   64,
		DiskId: diskID,
	}))
	require.NoError(t, err)
	require.Equal(t, payload, readResp.Msg.GetData())

	_, err = client.Fallocate(ctx, connect.NewRequest(&storagev1.FallocateRequest{
		Ino:    fileIno,
		Fh:     1,
		Offset: 0,
		Length: 4096,
		Mode:   0,
		DiskId: diskID,
		Epoch:  epoch,
	}))
	require.NoError(t, err)

	newMode := uint32(0o600)
	setAttrResp, err := client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
		Ino:    fileIno,
		Mode:   &newMode,
		DiskId: diskID,
		Epoch:  epoch,
	}))
	require.NoError(t, err)
	require.Equal(t, store.S_IFREG|0o600, setAttrResp.Msg.GetInode().GetAttr().GetMode())
	require.Equal(t, uint64(4096), setAttrResp.Msg.GetInode().GetAttr().GetSize())

	_, err = client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
		Ino:      fileIno,
		Fh:       1,
		Datasync: false,
		DiskId:   diskID,
		Epoch:    epoch,
	}))
	require.NoError(t, err)

	// 3. Xattr: SetXattr, GetXattr, ListXattr, RemoveXattr
	_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
		Ino:    fileIno,
		Name:   []byte("user.tag"),
		Value:  []byte("v1"),
		Flags:  0,
		DiskId: diskID,
		Epoch:  epoch,
	}))
	require.NoError(t, err)

	getXattrResp, err := client.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{
		Ino:    fileIno,
		Name:   []byte("user.tag"),
		DiskId: diskID,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), getXattrResp.Msg.GetValue())

	listXattrResp, err := client.ListXattr(ctx, connect.NewRequest(&storagev1.ListXattrRequest{
		Ino:    fileIno,
		DiskId: diskID,
	}))
	require.NoError(t, err)
	require.Contains(t, listXattrResp.Msg.GetNames(), []byte("user.tag"))

	_, err = client.RemoveXattr(ctx, connect.NewRequest(&storagev1.RemoveXattrRequest{
		Ino:    fileIno,
		Name:   []byte("user.tag"),
		DiskId: diskID,
		Epoch:  epoch,
	}))
	require.NoError(t, err)

	// 4. Symlink, Readlink, Mknod, Link, Rename
	symResp, err := client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
		ParentIno: dirIno,
		Name:      []byte("file.lnk"),
		Target:    []byte("file.txt"),
		Uid:       1000,
		Gid:       1000,
		DiskId:    diskID,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	symIno := symResp.Msg.GetInode().GetIno()

	readlinkResp, err := client.Readlink(ctx, connect.NewRequest(&storagev1.ReadlinkRequest{
		Ino:    symIno,
		DiskId: diskID,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("file.txt"), readlinkResp.Msg.GetTarget())

	mknodResp, err := client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
		ParentIno: dirIno,
		Name:      []byte("fifo0"),
		Mode:      store.S_IFIFO | 0o644,
		Rdev:      0,
		Uid:       1000,
		Gid:       1000,
		DiskId:    diskID,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	fifoIno := mknodResp.Msg.GetInode().GetIno()

	linkResp, err := client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		Ino:          fileIno,
		NewParentIno: dirIno,
		NewName:      []byte("file.hard"),
		DiskId:       diskID,
		Epoch:        epoch,
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(2), linkResp.Msg.GetInode().GetAttr().GetNlink())

	_, err = client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
		OldParentIno: dirIno,
		OldName:      []byte("file.hard"),
		NewParentIno: dirIno,
		NewName:      []byte("file.renamed"),
		Flags:        0,
		DiskId:       diskID,
		Epoch:        epoch,
	}))
	require.NoError(t, err)

	// 5. ReadDir & ReadDirPlus & StatFS
	readDirResp, err := client.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{
		Ino:    dirIno,
		Fh:     1,
		Offset: 0,
		Limit:  100,
		DiskId: diskID,
	}))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(readDirResp.Msg.GetEntries()), 6) // ., .., file.txt, file.lnk, fifo0, file.renamed

	readDirPlusResp, err := client.ReadDirPlus(ctx, connect.NewRequest(&storagev1.ReadDirPlusRequest{
		Ino:    dirIno,
		Fh:     1,
		Offset: 0,
		Limit:  100,
		DiskId: diskID,
	}))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(readDirPlusResp.Msg.GetEntries()), 6)
	require.Zero(t, readDirPlusResp.Msg.GetEntries()[0].GetEntryTimeoutSeconds()) // "."

	statFSResp, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		Ino:    1,
		DiskId: diskID,
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(4096), statFSResp.Msg.GetBsize())
	require.Equal(t, uint32(255), statFSResp.Msg.GetNamelen())

	// 6. Unlink (open_in_client=true -> ReclaimOrphan) & Rmdir
	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno: dirIno,
		Name:      []byte("file.renamed"),
		DiskId:    diskID,
		Epoch:     epoch,
	}))
	require.NoError(t, err)

	openInClient := true
	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno:    dirIno,
		Name:         []byte("file.txt"),
		DiskId:       diskID,
		OpenInClient: &openInClient,
		Epoch:        epoch,
	}))
	require.NoError(t, err)

	_, err = client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{
		DiskId: diskID,
		Ino:    fileIno,
		Epoch:  epoch,
	}))
	require.NoError(t, err)

	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno: dirIno,
		Name:      []byte("file.lnk"),
		DiskId:    diskID,
		Epoch:     epoch,
	}))
	require.NoError(t, err)

	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno: dirIno,
		Name:      []byte("fifo0"),
		DiskId:    diskID,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	_ = fifoIno

	_, err = client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
		ParentIno: 1,
		Name:      []byte("dir1"),
		DiskId:    diskID,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
}

func TestEpochFencingAcrossWriteRPCs(t *testing.T) {
	client, _, ctx := newTestServerAndClient(t, "srv_epoch_fence")

	createDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "fenced-disk",
		MaxBytes: 1 << 20,
	}))
	require.NoError(t, err)
	diskID := createDiskResp.Msg.GetDisk().GetDiskId()

	// 第一次可写挂载：epoch = 1
	mount1, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: false,
	}))
	require.NoError(t, err)
	staleEpoch := mount1.Msg.GetEpoch()
	require.Equal(t, uint64(1), staleEpoch)

	// 在 epoch=1 下预置一个目录和一个文件，供后续栅栏拦截测试使用
	dirResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		ParentIno: 1,
		Name:      []byte("sub"),
		Mode:      0o755,
		DiskId:    diskID,
		Epoch:     staleEpoch,
	}))
	require.NoError(t, err)
	dirIno := dirResp.Msg.GetInode().GetIno()

	fileResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: 1,
		Name:      []byte("f.txt"),
		Mode:      0o644,
		DiskId:    diskID,
		Epoch:     staleEpoch,
	}))
	require.NoError(t, err)
	fileIno := fileResp.Msg.GetInode().GetIno()

	// 第二次可写挂载抢占：epoch 递增至 2，使 staleEpoch=1 失效
	mount2, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: false,
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(2), mount2.Msg.GetEpoch())

	// 只读挂载不推进 Epoch，且旧客户端读请求不受影响
	roMount, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: true,
	}))
	require.NoError(t, err)
	require.Zero(t, roMount.Msg.GetEpoch())

	_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		Ino:    fileIno,
		DiskId: diskID,
	}))
	require.NoError(t, err)

	// 全部 16 个写 RPC 携带过期 Epoch=1 必须返回 CodeFailedPrecondition (Reason="EBUSY")
	writeCalls := []struct {
		name string
		call func() error
	}{
		{
			name: "Mknod",
			call: func() error {
				_, e := client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
					ParentIno: 1, Name: []byte("n"), Mode: store.S_IFREG | 0o644, DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
		{
			name: "Mkdir",
			call: func() error {
				_, e := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
					ParentIno: 1, Name: []byte("d"), Mode: 0o755, DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
		{
			name: "Symlink",
			call: func() error {
				_, e := client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
					ParentIno: 1, Name: []byte("s"), Target: []byte("f.txt"), DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
		{
			name: "Link",
			call: func() error {
				_, e := client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
					Ino: fileIno, NewParentIno: 1, NewName: []byte("l"), DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
		{
			name: "Unlink",
			call: func() error {
				_, e := client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
					ParentIno: 1, Name: []byte("f.txt"), DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
		{
			name: "Rmdir",
			call: func() error {
				_, e := client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
					ParentIno: 1, Name: []byte("sub"), DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
		{
			name: "Rename",
			call: func() error {
				_, e := client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
					OldParentIno: 1, OldName: []byte("f.txt"), NewParentIno: 1, NewName: []byte("f2.txt"), DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
		{
			name: "Create",
			call: func() error {
				_, e := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
					ParentIno: 1, Name: []byte("c.txt"), Mode: 0o644, DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
		{
			name: "Write",
			call: func() error {
				_, e := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
					Ino: fileIno, Fh: 1, Offset: 0, Data: []byte("x"), DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
		{
			name: "SetAttr",
			call: func() error {
				mode := uint32(0o600)
				_, e := client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
					Ino: fileIno, Mode: &mode, DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
		{
			name: "Fallocate",
			call: func() error {
				_, e := client.Fallocate(ctx, connect.NewRequest(&storagev1.FallocateRequest{
					Ino: fileIno, Fh: 1, Offset: 0, Length: 128, DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
		{
			name: "SetXattr",
			call: func() error {
				_, e := client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
					Ino: fileIno, Name: []byte("user.k"), Value: []byte("v"), DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
		{
			name: "RemoveXattr",
			call: func() error {
				_, e := client.RemoveXattr(ctx, connect.NewRequest(&storagev1.RemoveXattrRequest{
					Ino: fileIno, Name: []byte("user.k"), DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
		{
			name: "Fsync",
			call: func() error {
				_, e := client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
					Ino: fileIno, Fh: 1, DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
		{
			name: "FsyncDir",
			call: func() error {
				_, e := client.FsyncDir(ctx, connect.NewRequest(&storagev1.FsyncDirRequest{
					Ino: dirIno, Fh: 1, DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
		{
			name: "ReclaimOrphan",
			call: func() error {
				_, e := client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{
					Ino: fileIno, DiskId: diskID, Epoch: staleEpoch,
				}))
				return e
			},
		},
	}

	for _, tc := range writeCalls {
		t.Run(tc.name, func(t *testing.T) {
			requireConnectError(t, tc.call(), connect.CodeFailedPrecondition, "EBUSY")
		})
	}
}

func TestServerHTTP2ConfigAndH2CLargeReadWrite(t *testing.T) {
	pool, ctx := setupIsolatedTestDB(t, "srv_http2_cfg")
	dbStore := db.NewStore(pool)
	mgr := control.New(dbStore, control.WithStorageRoot(t.TempDir()))
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	var srvCfg config.ServerConfig
	srvCfg.ApplyDefaultPort(0)
	srvCfg.ServerReadMaxBytes = config.DefaultServerReadMaxBytes

	srv, err := server.New(server.Params{
		Config:  srvCfg,
		Control: mgr,
	})
	require.NoError(t, err)

	httpSrv := srv.HTTPServer()
	require.NotNil(t, httpSrv)
	require.NotNil(t, httpSrv.HTTP2)
	require.Equal(t, 1<<20, httpSrv.HTTP2.MaxReadFrameSize)
	require.Equal(t, (4<<20)-1, httpSrv.HTTP2.MaxReceiveBufferPerStream)
	require.Equal(t, (4<<20)-1, httpSrv.HTTP2.MaxReceiveBufferPerConnection)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- httpSrv.Serve(ln)
	}()
	t.Cleanup(func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, httpSrv.Shutdown(shutCtx))
		serveErr := <-serveErrCh
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			require.NoError(t, serveErr)
		}
	})

	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	h2cClient := &http.Client{
		Transport: &http.Transport{
			Protocols: &protocols,
			HTTP2: &http.HTTP2Config{
				MaxReadFrameSize:              1 << 20,
				MaxReceiveBufferPerStream:     (4 << 20) - 1,
				MaxReceiveBufferPerConnection: (4 << 20) - 1,
			},
		},
	}

	client := storagev1connect.NewStorageServiceClient(h2cClient, "http://"+ln.Addr().String())

	createDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "h2c-large-disk",
		MaxBytes: 16 << 20,
	}))
	require.NoError(t, err)
	diskID := createDiskResp.Msg.GetDisk().GetDiskId()

	mountResp, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: false,
	}))
	require.NoError(t, err)
	epoch := mountResp.Msg.GetEpoch()

	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: 1,
		Name:      []byte("large.bin"),
		Mode:      0o644,
		DiskId:    diskID,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	fileIno := createResp.Msg.GetInode().GetIno()

	payload := bytes.Repeat([]byte{0x5a, 0xa5, 0x3c, 0xc3}, (1<<20)/4) // 1 MiB
	writeResp, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		Ino:    fileIno,
		Fh:     1,
		Offset: 0,
		Data:   payload,
		DiskId: diskID,
		Epoch:  epoch,
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(len(payload)), writeResp.Msg.GetSize())

	readResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		Ino:    fileIno,
		Fh:     1,
		Offset: 0,
		Size:   uint32(len(payload)),
		DiskId: diskID,
	}))
	require.NoError(t, err)
	require.Equal(t, payload, readResp.Msg.GetData())
}
