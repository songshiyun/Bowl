package cli_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/cli"
)

func requireParsedSize(t *testing.T, raw string, expected uint64) {
	t.Helper()
	got, err := cli.ParseSize(raw)
	require.NoError(t, err, "容量 %q 应解析成功", raw)
	require.Equal(t, expected, got, "容量 %q 解析结果不匹配", raw)
}

func requireParseSizeInvalid(t *testing.T, raw string) {
	t.Helper()
	_, err := cli.ParseSize(raw)
	require.Error(t, err, "非法容量输入 %q 必须返回错误", raw)
}

func requireFormattedSize(t *testing.T, bytes uint64, expected string) {
	t.Helper()
	require.Equal(t, expected, cli.FormatSize(bytes), "字节数 %d 格式化结果不匹配", bytes)
}

func TestParseSize_GivenBinaryAndPlainUnits_ReturnsExpectedBytes(t *testing.T) {
	requireParsedSize(t, "1", 1)
	requireParsedSize(t, "  4096  ", 4096)
	requireParsedSize(t, "512B", 512)
	requireParsedSize(t, "512b", 512)
	requireParsedSize(t, "512 B", 512)
	requireParsedSize(t, "1K", 1024)
	requireParsedSize(t, "2KB", 2*1024)
	requireParsedSize(t, "4KiB", 4*1024)
	requireParsedSize(t, "4kib", 4*1024)
	requireParsedSize(t, "1M", 1024*1024)
	requireParsedSize(t, "10MB", 10*1024*1024)
	requireParsedSize(t, "128MiB", 128*1024*1024)
	requireParsedSize(t, "1G", 1024*1024*1024)
	requireParsedSize(t, "5GB", 5*1024*1024*1024)
	requireParsedSize(t, "10GiB", 10*1024*1024*1024)
	requireParsedSize(t, "10gib", 10*1024*1024*1024)
	requireParsedSize(t, "1T", 1024*1024*1024*1024)
	requireParsedSize(t, "2TB", 2*1024*1024*1024*1024)
	requireParsedSize(t, "3TiB", 3*1024*1024*1024*1024)
	requireParsedSize(t, "1P", 1024*1024*1024*1024*1024)
	requireParsedSize(t, "2PB", 2*1024*1024*1024*1024*1024)
	requireParsedSize(t, "4PiB", 4*1024*1024*1024*1024*1024)
	requireParsedSize(t, "8191PiB", 8191*1024*1024*1024*1024*1024)
	requireParsedSize(t, "9223372036854775807", uint64(math.MaxInt64))
}

func TestParseSize_GivenInvalidOrOverflowInputs_ReturnsError(t *testing.T) {
	requireParseSizeInvalid(t, "")
	requireParseSizeInvalid(t, "   ")
	requireParseSizeInvalid(t, "0")
	requireParseSizeInvalid(t, "0GiB")
	requireParseSizeInvalid(t, "-10")
	requireParseSizeInvalid(t, "-10GiB")
	requireParseSizeInvalid(t, "GiB")
	requireParseSizeInvalid(t, "1.5GiB")
	requireParseSizeInvalid(t, "10EiB")
	requireParseSizeInvalid(t, "100xyz")
	requireParseSizeInvalid(t, "9223372036854775808")
	requireParseSizeInvalid(t, "8192PiB")
	requireParseSizeInvalid(t, "20000PiB")
	requireParseSizeInvalid(t, "99999999999999999999999999999")
}

func TestFormatSize_GivenByteCounts_FormatsBinaryUnits(t *testing.T) {
	requireFormattedSize(t, 0, "0 B")
	requireFormattedSize(t, 512, "512 B")
	requireFormattedSize(t, 1023, "1023 B")
	requireFormattedSize(t, 1024, "1.00 KiB")
	requireFormattedSize(t, 1536, "1.50 KiB")
	requireFormattedSize(t, 1024*1024, "1.00 MiB")
	requireFormattedSize(t, 10*1024*1024*1024, "10.00 GiB")
	requireFormattedSize(t, 2*1024*1024*1024*1024, "2.00 TiB")
	requireFormattedSize(t, 3*1024*1024*1024*1024*1024, "3.00 PiB")
}
