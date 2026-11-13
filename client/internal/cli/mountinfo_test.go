package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const sampleMountInfo = `23 61 0:21 / /sys rw,nosuid,nodev,noexec,relatime shared:7 - sysfs sysfs rw
24 61 0:22 / /proc rw,nosuid,nodev,noexec,relatime shared:13 - proc proc rw
30 61 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw,errors=remount-ro
100 30 0:50 / /mnt/my\040dir rw,nosuid,nodev shared:55 - fuse.bowl bowl:0192a3b4-c5d6-7e8f-9012-3456789abcd1 rw
101 30 0:51 / /mnt/foo/bar rw,nosuid,nodev shared:56 - fuse.bowl bowl:0192a3b4-c5d6-7e8f-9012-3456789abcd2 rw
`

func TestIsMountPoint_GivenMountedAndOctalEscapedPaths_ReturnsExpectedState(t *testing.T) {
	// Given
	withMountInfoContent(t, sampleMountInfo)

	// When / Then
	mountedBar, err := IsMountPoint("/mnt/foo/bar/")
	require.NoError(t, err)
	require.True(t, mountedBar)

	mountedSpace, err := IsMountPoint("/mnt/my dir")
	require.NoError(t, err)
	require.True(t, mountedSpace, "八进制转义的空格必须被还原")

	unmounted, err := IsMountPoint("/mnt/unmounted")
	require.NoError(t, err)
	require.False(t, unmounted)
}

func TestIsMountPoint_GivenMalformedAndUnescapedLines_HandlesGracefully(t *testing.T) {
	// Given
	withMountInfoContent(t, "1 2 3\n\nnot-a-mountinfo-line\n100 30 0:50 / /mnt/no-separator rw\n101 30 0:51 / /mnt/keep\\999 rw - fuse.bowl bowl:disk rw\n")

	// When / Then
	noSep, err := IsMountPoint("/mnt/no-separator")
	require.NoError(t, err)
	require.True(t, noSep)

	keepInvalidEscape, err := IsMountPoint(`/mnt/keep\999`)
	require.NoError(t, err)
	require.True(t, keepInvalidEscape, "非八进制转义必须原样保留")

	absent, err := IsMountPoint("/1/2/3")
	require.NoError(t, err)
	require.False(t, absent)
}

func TestIsMountPoint_GivenMissingMountInfoFile_ReturnsFalseWithoutError(t *testing.T) {
	// Given
	withMissingMountInfo(t)

	// When
	mounted, err := IsMountPoint("/mnt/anything")

	// Then
	require.NoError(t, err)
	require.False(t, mounted)
}

func TestIsMountPoint_GivenUnreadableMountInfo_ReturnsError(t *testing.T) {
	// Given
	withUnreadableMountInfo(t)

	// When
	_, err := IsMountPoint("/mnt/anything")

	// Then
	require.Error(t, err)
}

func TestFindDiskMountpoint_GivenActiveAndAbsentDisks_MatchesCaseInsensitively(t *testing.T) {
	// Given
	withMountInfoContent(t, sampleMountInfo)

	// When / Then
	mp1, mounted1, err := FindDiskMountpoint("0192A3B4-C5D6-7E8F-9012-3456789ABCD1")
	require.NoError(t, err)
	require.True(t, mounted1)
	require.Equal(t, "/mnt/my dir", mp1)

	mp2, mounted2, err := FindDiskMountpoint("0192a3b4-c5d6-7e8f-9012-3456789abcd2")
	require.NoError(t, err)
	require.True(t, mounted2)
	require.Equal(t, "/mnt/foo/bar", mp2)

	_, mounted3, err := FindDiskMountpoint("0192a3b4-c5d6-7e8f-9012-3456789abcd3")
	require.NoError(t, err)
	require.False(t, mounted3)
}
