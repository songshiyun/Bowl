//go:build e2e && linux

package e2e_test

import (
	"context"
	"errors"
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

func TestE2E_Ioctl(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	for _, tc := range []struct {
		name  string
		proto uint32
		vfs   int
	}{
		{"immutable", protoFlagImmutable, fsImmutableFl},
		{"append", protoFlagAppend, fsAppendFl},
		{"both", protoFlagImmutable | protoFlagAppend, fsImmutableFl | fsAppendFl},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disk := createTestDisk(t, client)
			prepareFlagFile(t, client, disk.GetDiskId(), "target.txt", tc.proto)
			mp := t.TempDir()
			cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mp, "--debug")
			require.NoError(t, waitForMount(mp, 10*time.Second), "挂载失败: %s %s", stdout.String(), stderr.String())
			defer stopBowlMountGracefully(t, cmd, mp, 5*time.Second)
			path := filepath.Join(mp, "target.txt")
			f, err := os.Open(path)
			require.NoError(t, err)
			got, err := getIoctlFlags(int(f.Fd()))
			require.NoError(t, err)
			require.Equal(t, tc.vfs, got)
			require.NoError(t, f.Close())
			if tc.proto == protoFlagAppend {
				f, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
				require.NoError(t, err)
				_, err = f.WriteString("line 2\n")
				require.NoError(t, err)
				require.NoError(t, f.Close())
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, "line 1\nline 2\n", string(data))
			}
		})
	}
	t.Run("设置与不支持的命令", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mp := t.TempDir()
		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mp)
		require.NoError(t, waitForMount(mp, 10*time.Second))
		defer stopBowlMountGracefully(t, cmd, mp, 5*time.Second)
		path := filepath.Join(mp, "target.txt")
		require.NoError(t, os.WriteFile(path, []byte("data"), 0o644))
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		defer f.Close()
		err = setIoctlFlags(int(f.Fd()), fsImmutableFl)
		if errors.Is(err, syscall.EPERM) {
			t.Log("环境缺少设置 immutable 标志所需的权限")
		} else {
			require.NoError(t, err)
			flags, err := getIoctlFlags(int(f.Fd()))
			require.NoError(t, err)
			require.Equal(t, fsImmutableFl, flags)
			require.NoError(t, setIoctlFlags(int(f.Fd()), 0))
		}
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
		require.NoError(t, waitForMount(mp, 10*time.Second))
		defer stopBowlMountGracefully(t, cmd, mp, 5*time.Second)
		f, err := os.Open(filepath.Join(mp, "ro-test.txt"))
		require.NoError(t, err)
		defer f.Close()
		require.ErrorIs(t, setIoctlFlags(int(f.Fd()), fsImmutableFl), syscall.EROFS)
		flags, err := getIoctlFlags(int(f.Fd()))
		require.NoError(t, err)
		require.Zero(t, flags)
	})
}
