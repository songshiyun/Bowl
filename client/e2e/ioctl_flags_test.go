//go:build e2e && linux

package e2e_test

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/starsrc/bowl/client/internal/session"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	fsImmutableFl      = 0x00000010
	fsAppendFl         = 0x00000020
	protoFlagImmutable = 0x1
	protoFlagAppend    = 0x2
)

func getIoctlFlags(fd int) (int, error) { return unix.IoctlGetInt(fd, unix.FS_IOC_GETFLAGS) }
func setIoctlFlags(fd, flags int) error {
	return unix.IoctlSetPointerInt(fd, unix.FS_IOC_SETFLAGS, flags)
}

// prepareFlagFile 先通过分支写会话准备文件及标志，再关闭会话以允许挂载。
func prepareFlagFile(t *testing.T, client storagev1connect.StorageServiceClient, diskID, name string, flags uint32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mgr, err := session.New(ctx, client, session.Config{DiskID: diskID, BranchName: "main"})
	require.NoError(t, err)
	defer mgr.Close()
	sid := mgr.CurrentSessionID()
	created, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: 1, Name: []byte(name), Mode: syscall.S_IFREG | 0o644, Flags: 0o2, SessionId: &sid,
	}))
	require.NoError(t, err)
	ino := created.Msg.GetInode().GetIno()
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{Ino: ino, Data: []byte("line 1\n"), SessionId: &sid}))
	require.NoError(t, err)
	_, err = client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{Ino: ino, Flags: &flags, SessionId: &sid}))
	require.NoError(t, err)
}

func requireIoctlFlagCase(t *testing.T, client storagev1connect.StorageServiceClient, endpoint, name string, proto uint32, vfs int, afterOpen func(*testing.T, string)) {
	t.Helper()
	disk := createTestDisk(t, client)
	prepareFlagFile(t, client, disk.GetDiskId(), "target.txt", proto)
	mp := t.TempDir()
	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mp, "--debug")
	waitForMount(t, mp, "挂载失败: %s %s", stdout.String(), stderr.String())
	defer stopBowlMountGracefully(t, cmd, mp)
	path := filepath.Join(mp, "target.txt")
	f, err := os.Open(path)
	require.NoError(t, err)
	got, err := getIoctlFlags(int(f.Fd()))
	require.NoError(t, err)
	require.Equal(t, vfs, got)
	require.NoError(t, f.Close())
	afterOpen(t, path)
}

func requireWriteDenied(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteString("denied")
	require.ErrorIs(t, err, syscall.EPERM)
	require.NoError(t, f.Close())
}

func requireTruncateDenied(t *testing.T, path string) {
	t.Helper()
	require.ErrorIs(t, os.Truncate(path, 0), syscall.EPERM)
}

func requireRenameDenied(t *testing.T, path string) {
	t.Helper()
	require.ErrorIs(t, os.Rename(path, path+".renamed"), syscall.EPERM)
}

func requireUnlinkDenied(t *testing.T, path string) {
	t.Helper()
	require.ErrorIs(t, os.Remove(path), syscall.EPERM)
}

func verifyImmutable(t *testing.T, path string) {
	t.Helper()
	requireWriteDenied(t, path)
	requireTruncateDenied(t, path)
	requireRenameDenied(t, path)
	requireUnlinkDenied(t, path)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "line 1\n", string(data))
}

func verifyAppendWrite(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = f.WriteString("line 2\n")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "line 1\nline 2\n", string(data))

	requireWriteDenied(t, path)
	requireTruncateDenied(t, path)
	data, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "line 1\nline 2\n", string(data))
}

func TestE2E_Ioctl(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("immutable", func(t *testing.T) {
		requireIoctlFlagCase(t, client, endpoint, "immutable", protoFlagImmutable, fsImmutableFl, verifyImmutable)
	})
	t.Run("append", func(t *testing.T) {
		requireIoctlFlagCase(t, client, endpoint, "append", protoFlagAppend, fsAppendFl, verifyAppendWrite)
	})
	t.Run("both", func(t *testing.T) {
		requireIoctlFlagCase(t, client, endpoint, "both", protoFlagImmutable|protoFlagAppend, fsImmutableFl|fsAppendFl, verifyImmutable)
	})

	t.Run("设置与不支持的命令", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mp := t.TempDir()
		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mp)
		waitForMount(t, mp)
		defer stopBowlMountGracefully(t, cmd, mp)
		path := filepath.Join(mp, "target.txt")
		require.NoError(t, os.WriteFile(path, []byte("data"), 0o644))
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		defer f.Close()
		require.NoError(t, setIoctlFlags(int(f.Fd()), fsImmutableFl))
		flags, err := getIoctlFlags(int(f.Fd()))
		require.NoError(t, err)
		require.Equal(t, fsImmutableFl, flags)
		require.NoError(t, setIoctlFlags(int(f.Fd()), 0))
		require.ErrorIs(t, setIoctlFlags(int(f.Fd()), 0x40), syscall.EOPNOTSUPP)
		_, err = unix.IoctlGetInt(int(f.Fd()), unix.TIOCGWINSZ)
		require.ErrorIs(t, err, syscall.ENOTTY)
	})

	t.Run("只读挂载", func(t *testing.T) {
		disk := createTestDisk(t, client)
		prepareSnapshot(t, client, disk.GetDiskId(), func(mp string) {
			require.NoError(t, os.WriteFile(filepath.Join(mp, "ro-test.txt"), nil, 0o644))
		})
		mp := t.TempDir()
		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mp, "--ro")
		waitForMount(t, mp)
		defer stopBowlMountGracefully(t, cmd, mp)
		f, err := os.Open(filepath.Join(mp, "ro-test.txt"))
		require.NoError(t, err)
		defer f.Close()
		require.ErrorIs(t, setIoctlFlags(int(f.Fd()), fsImmutableFl), syscall.EROFS)
		flags, err := getIoctlFlags(int(f.Fd()))
		require.NoError(t, err)
		require.Zero(t, flags)
	})

	recordArtifact(t, map[string]string{
		"immutable_getflags": "0x10",
		"append_getflags":    "0x20",
		"both_getflags":      "0x30",
		"append_write":       "line 1\\nline 2\\n",
		"immutable_write":    "EPERM",
		"immutable_truncate": "EPERM",
		"immutable_rename":   "EPERM",
		"immutable_unlink":   "EPERM",
		"append_overwrite":   "EPERM",
		"append_truncate":    "EPERM",
		"both_write":         "EPERM",
		"both_truncate":      "EPERM",
		"both_rename":        "EPERM",
		"both_unlink":        "EPERM",
		"set_immutable":      "ok",
		"unknown_flag":       "EOPNOTSUPP",
		"unsupported_ioctl":  "ENOTTY",
		"readonly_setflag":   "EROFS",
	})
}
