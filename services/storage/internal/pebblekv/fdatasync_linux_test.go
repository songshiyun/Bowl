//go:build linux

package pebblekv

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSyncFileDataIssuesFdatasync 证明 Linux 上的块文件屏障确实是真实的 fdatasync
// 系统调用而不是空实现：合法 fd 成功，已关闭 fd 返回 EBADF。
func TestSyncFileDataIssuesFdatasync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "block.blob")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	_, err = file.Write([]byte("data"))
	require.NoError(t, err)

	require.NoError(t, syncFileData(file))
	require.NoError(t, file.Close())

	require.True(t, errors.Is(syncFileData(file), syscall.EBADF), "已关闭的 fd 必须返回 EBADF")
}
