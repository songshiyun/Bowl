package server_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	testifymock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

// setupTestServer 启动基于 MockManager 的测试 HTTP 服务器并返回客户端与清理函数。
func setupTestServer(t *testing.T, mockMgr *metamock.MockManager) (storagev1connect.StorageServiceClient, func()) {
	t.Helper()

	srv, err := server.New(server.Params{
		Config:      defaultTestConfig(),
		MetaManager: mockMgr,
	})
	require.NoError(t, err)

	ts := httptest.NewServer(h2c.NewHandler(srv.Handler(), &http2.Server{}))
	h2cClient, _ := newH2CTestClient()
	client := storagev1connect.NewStorageServiceClient(h2cClient, ts.URL)

	return client, ts.Close
}

// TestServer_GetLk 验证 GetLk RPC 的成功路径与领域错误映射。
// TestServer_SetLk 验证 SetLk RPC 的成功路径与领域错误映射。
// TestServer_SetLkw 验证 SetLkw RPC 的成功路径与典型领域错误（EDEADLK, EINTR 等）。
// TestServer_RenewSession 验证 RenewSession RPC 的成功路径与领域错误映射。
// TestServer_CloseSession 验证 CloseSession RPC 的成功路径与领域错误映射。
func TestServer_CloseSession(t *testing.T) {
	ctx := context.Background()

	t.Run("成功路径: 正常注销客户端会话", func(t *testing.T) {
		mockMgr := metamock.NewMockManager(t)
		client, teardown := setupTestServer(t, mockMgr)
		defer teardown()

		sessionID := uint64(999)
		mockMgr.EXPECT().CloseSession(testifymock.Anything, sessionID).Return(nil).Once()

		resp, err := client.CloseSession(ctx, connect.NewRequest(&storagev1.CloseSessionRequest{
			SessionId: sessionID,
		}))
		require.NoError(t, err)
		require.NotNil(t, resp)
		mockMgr.AssertExpectations(t)
	})

	t.Run("典型领域错误映射", func(t *testing.T) {
		mockMgr := metamock.NewMockManager(t)
		client, teardown := setupTestServer(t, mockMgr)
		defer teardown()

		sessionID := uint64(999)
		mockMgr.EXPECT().CloseSession(testifymock.Anything, sessionID).Return(meta.ErrInvalidArgument).Once()

		resp, err := client.CloseSession(ctx, connect.NewRequest(&storagev1.CloseSessionRequest{
			SessionId: sessionID,
		}))
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		assert.Equal(t, "EINVAL", info.GetReason())
		mockMgr.AssertExpectations(t)
	})
}
