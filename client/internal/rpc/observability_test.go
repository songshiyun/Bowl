package rpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFallbackRequestIDSeededNonSequential(t *testing.T) {
	first := nextFallbackRequestID()
	second := nextFallbackRequestID()
	require.Greater(t, first, uint64(1), "X-Request-Id 兜底序列必须经随机种子初始化，避免多 CLI 进程从 1 碰撞")
	require.Equal(t, first+1, second)

	ctx, got := ensureRequestID(context.Background())
	require.Equal(t, second+1, got)
	fromCtx, ok := RequestIDFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, got, fromCtx)
}
