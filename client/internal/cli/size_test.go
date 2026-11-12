package cli_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/cli"
)

func requireParseSize(t *testing.T, name, raw string, expected uint64) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		got, err := cli.ParseSize(raw)
		require.NoError(t, err)
		require.Equal(t, expected, got)
	})
}

func requireParseSizeError(t *testing.T, name, raw string) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		_, err := cli.ParseSize(raw)
		require.Error(t, err, "非法容量输入 %q 必须返回错误", raw)
	})
}

func requireFormatSize(t *testing.T, bytes uint64, expected string) {
	t.Helper()
	t.Run(expected, func(t *testing.T) {
		require.Equal(t, expected, cli.FormatSize(bytes))
	})
}

func TestParseSize_Valid(t *testing.T) {
	requireParseSize(t, "plain bytes", "1", 1)
	requireParseSize(t, "plain bytes with spaces", "  4096  ", 4096)
	requireParseSize(t, "unit B", "512B", 512)
	requireParseSize(t, "unit b lowercase", "512b", 512)
	requireParseSize(t, "unit B with inner space", "512 B", 512)
	requireParseSize(t, "unit K", "1K", 1024)
	requireParseSize(t, "unit KB", "2KB", 2*1024)
	requireParseSize(t, "unit KiB", "4KiB", 4*1024)
	requireParseSize(t, "unit kib lowercase", "4kib", 4*1024)
	requireParseSize(t, "unit M", "1M", 1024*1024)
	requireParseSize(t, "unit MB", "10MB", 10*1024*1024)
	requireParseSize(t, "unit MiB", "128MiB", 128*1024*1024)
	requireParseSize(t, "unit G", "1G", 1024*1024*1024)
	requireParseSize(t, "unit GB", "5GB", 5*1024*1024*1024)
	requireParseSize(t, "unit GiB", "10GiB", 10*1024*1024*1024)
	requireParseSize(t, "unit gib lowercase", "10gib", 10*1024*1024*1024)
	requireParseSize(t, "unit T", "1T", 1024*1024*1024*1024)
	requireParseSize(t, "unit TB", "2TB", 2*1024*1024*1024*1024)
	requireParseSize(t, "unit TiB", "3TiB", 3*1024*1024*1024*1024)
	requireParseSize(t, "unit P", "1P", 1024*1024*1024*1024*1024)
	requireParseSize(t, "unit PB", "2PB", 2*1024*1024*1024*1024*1024)
	requireParseSize(t, "unit PiB", "4PiB", 4*1024*1024*1024*1024*1024)
	requireParseSize(t, "max PiB without int64 overflow", "8191PiB", 8191*1024*1024*1024*1024*1024)
	requireParseSize(t, "exact math.MaxInt64", "9223372036854775807", uint64(math.MaxInt64))
}

func TestParseSize_Invalid(t *testing.T) {
	requireParseSizeError(t, "empty", "")
	requireParseSizeError(t, "whitespace only", "   ")
	requireParseSizeError(t, "zero plain", "0")
	requireParseSizeError(t, "zero with unit", "0GiB")
	requireParseSizeError(t, "negative plain", "-10")
	requireParseSizeError(t, "negative with unit", "-10GiB")
	requireParseSizeError(t, "unit only", "GiB")
	requireParseSizeError(t, "decimal float", "1.5GiB")
	requireParseSizeError(t, "unknown unit", "10EiB")
	requireParseSizeError(t, "unknown unit xyz", "100xyz")
	requireParseSizeError(t, "exceeds math.MaxInt64 by 1", "9223372036854775808")
	requireParseSizeError(t, "exceeds math.MaxInt64 in PiB", "8192PiB")
	requireParseSizeError(t, "uint64 overflow in PiB", "20000PiB")
	requireParseSizeError(t, "exceeds uint64 digits", "99999999999999999999999999999")
}

func TestFormatSize(t *testing.T) {
	requireFormatSize(t, 0, "0 B")
	requireFormatSize(t, 512, "512 B")
	requireFormatSize(t, 1023, "1023 B")
	requireFormatSize(t, 1024, "1.00 KiB")
	requireFormatSize(t, 1536, "1.50 KiB")
	requireFormatSize(t, 1024*1024, "1.00 MiB")
	requireFormatSize(t, 10*1024*1024*1024, "10.00 GiB")
	requireFormatSize(t, 2*1024*1024*1024*1024, "2.00 TiB")
	requireFormatSize(t, 3*1024*1024*1024*1024*1024, "3.00 PiB")
}
