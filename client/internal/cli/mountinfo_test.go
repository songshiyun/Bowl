package cli

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const sampleMountInfo = `23 61 0:21 / /sys rw,nosuid,nodev,noexec,relatime shared:7 - sysfs sysfs rw
24 61 0:22 / /proc rw,nosuid,nodev,noexec,relatime shared:13 - proc proc rw
30 61 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw,errors=remount-ro
100 30 0:50 / /mnt/my\040dir rw,nosuid,nodev shared:55 - fuse.bowl bowl:0192a3b4-c5d6-7e8f-9012-3456789abcd1 rw
101 30 0:51 / /mnt/foo/bar rw,nosuid,nodev shared:56 - fuse.bowl bowl:0192a3b4-c5d6-7e8f-9012-3456789abcd2 rw
`

func TestParseMountPointsFromReader(t *testing.T) {
	points, err := parseMountPointsFromReader(strings.NewReader(sampleMountInfo))
	require.NoError(t, err)
	require.Len(t, points, 5)
	require.True(t, containsMountPoint(points, "/mnt/foo/bar"))
	require.True(t, containsMountPoint(points, "/mnt/my dir"), "八进制转义的空格必须被还原")
	require.False(t, containsMountPoint(points, "/mnt/unmounted"))

	mountpoint, mounted := findDiskMountpoint(points, "0192a3b4-c5d6-7e8f-9012-3456789abcd1")
	require.True(t, mounted)
	require.Equal(t, "/mnt/my dir", mountpoint)
	mountpoint, mounted = findDiskMountpoint(points, "0192a3b4-c5d6-7e8f-9012-3456789abcd2")
	require.True(t, mounted)
	require.Equal(t, "/mnt/foo/bar", mountpoint)
	_, mounted = findDiskMountpoint(points, "0192a3b4-c5d6-7e8f-9012-3456789abcd3")
	require.False(t, mounted)
}

func TestParseMountPointsFromReaderBoundaries(t *testing.T) {
	empty, err := parseMountPointsFromReader(strings.NewReader(""))
	require.NoError(t, err)
	require.Empty(t, empty)

	malformed, err := parseMountPointsFromReader(strings.NewReader("1 2 3\n\nnot-a-mountinfo-line\n"))
	require.NoError(t, err)
	require.Empty(t, malformed)

	noSeparator, err := parseMountPointsFromReader(strings.NewReader("100 30 0:50 / /mnt/no-separator rw\n"))
	require.NoError(t, err)
	require.Len(t, noSeparator, 1)
	require.Equal(t, "/mnt/no-separator", noSeparator[0].path)
	require.Empty(t, noSeparator[0].source)

	invalidEscape, err := parseMountPointsFromReader(strings.NewReader(`100 30 0:50 / /mnt/keep\999 rw - fuse.bowl bowl:disk rw` + "\n"))
	require.NoError(t, err)
	require.Len(t, invalidEscape, 1)
	require.Equal(t, `/mnt/keep\999`, invalidEscape[0].path, "非八进制转义必须原样保留")
}

func TestIsMountPointAndFindDiskMountpointOnProc(t *testing.T) {
	mounted, err := IsMountPoint("/")
	require.NoError(t, err)
	require.True(t, mounted, "/ 必须是活跃挂载点")

	mounted, err = IsMountPoint("/definitely-not-mounted-by-bowl")
	require.NoError(t, err)
	require.False(t, mounted)

	_, mounted, err = FindDiskMountpoint("0192a3b4-c5d6-7e8f-9012-3456789abcd3")
	require.NoError(t, err)
	require.False(t, mounted)
}
