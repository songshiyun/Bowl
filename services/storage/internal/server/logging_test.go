package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

// TestServer_QuietExpectedNotFoundLogs 验证预期内的 POSIX 未命中（ENOENT / ENODATA）
// 不再输出 WARN 访问日志，同时非预期错误仍保持 WARN 分级。
func TestServer_QuietExpectedNotFoundLogs(t *testing.T) {
	mockMgr := metamock.NewMockManager(t)
	logCap := &logCapture{}
	logger := testLogger(logCap)

	srv, err := server.New(server.Params{
		Config:      defaultTestConfig(),
		MetaManager: mockMgr,
		Logger:      logger,
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client := storagev1connect.NewStorageServiceClient(http.DefaultClient, ts.URL)
	ctx := context.Background()

	t.Run("Lookup ENOENT 不输出 WARN", func(t *testing.T) {
		logCap.Reset()
		mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("missing.txt")).
			Return(nil, meta.ErrNotFound).Once()

		_, callErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("missing.txt"),
		}))
		require.Error(t, callErr)
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(callErr))
		assert.NotContains(t, logCap.String(), `"level":"WARN"`)
	})

	t.Run("GetXattr ENODATA 不输出 WARN", func(t *testing.T) {
		logCap.Reset()
		mockMgr.EXPECT().GetXattr(mock.Anything, uint64(2), []byte("security.capability")).
			Return(nil, meta.ErrNoData).Once()

		_, callErr := client.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{
			Ino:  2,
			Name: []byte("security.capability"),
		}))
		require.Error(t, callErr)
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(callErr))
		assert.NotContains(t, logCap.String(), `"level":"WARN"`)
	})

	t.Run("非预期 EINVAL 仍输出 WARN", func(t *testing.T) {
		logCap.Reset()
		mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("bad.txt")).
			Return(nil, meta.ErrInvalidArgument).Once()

		_, callErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("bad.txt"),
		}))
		require.Error(t, callErr)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(callErr))
		assert.Contains(t, logCap.String(), `"level":"WARN"`)
	})
}
