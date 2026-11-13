package rpc

import (
	"context"
	"crypto/tls"
	"net/http"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func buildTestShardedRoundTripper() (rt *shardedRoundTripper, metaRT *recordingRoundTripper, dataRTs *[HTTP2DataConnectionShards]*recordingRoundTripper) {
	metaRT = &recordingRoundTripper{}
	var shards [HTTP2DataConnectionShards]*recordingRoundTripper
	rt = &shardedRoundTripper{
		metaTransport: metaRT,
	}
	for i := range HTTP2DataConnectionShards {
		shards[i] = &recordingRoundTripper{}
		rt.dataTransports[i] = shards[i]
	}
	return rt, metaRT, &shards
}

func dispatchRoundTrip(t *testing.T, rt http.RoundTripper, path string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://127.0.0.1:9000"+path, http.NoBody)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
}

func requireAllDataShardCalls(t *testing.T, dataRTs *[HTTP2DataConnectionShards]*recordingRoundTripper, want int64) {
	t.Helper()
	for i := range HTTP2DataConnectionShards {
		require.Equal(t, want, dataRTs[i].calls.Load(), "数据条带 %d 请求计数不匹配", i)
	}
}

func requireAllDataShardIdleClosed(t *testing.T, dataRTs *[HTTP2DataConnectionShards]*recordingRoundTripper, want int64) {
	t.Helper()
	for i := range HTTP2DataConnectionShards {
		require.Equal(t, want, dataRTs[i].idleClosed.Load(), "数据条带 %d 空闲关闭计数不匹配", i)
	}
}

func TestShardedRoundTripper_GivenReadAndWriteTraffic_DistributesRoundRobinAcross8DataShards(t *testing.T) {
	// Given
	rt, metaRT, dataRTs := buildTestShardedRoundTripper()

	// When: 连续发送 8 次 Read 与 8 次带前缀 Write（共 16 次大块数据面调用）
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadProcedure)
	dispatchRoundTrip(t, rt, "/prefix"+storagev1connect.StorageServiceWriteProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceWriteProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceWriteProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceWriteProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceWriteProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceWriteProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceWriteProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceWriteProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadProcedure)

	// Then
	require.Zero(t, metaRT.calls.Load(), "大块数据读写不得占用元数据专用通道")
	requireAllDataShardCalls(t, dataRTs, 2)
}

func TestShardedRoundTripper_GivenMetadataAndControlTraffic_RoutesToDedicatedMetaTransport(t *testing.T) {
	// Given
	rt, metaRT, dataRTs := buildTestShardedRoundTripper()

	// When
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceLookupProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceGetAttrProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceCreateProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceUnlinkProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceFsyncProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadDirPlusProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceMountDiskProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceStatFSProcedure)

	// Then
	require.Equal(t, int64(8), metaRT.calls.Load())
	requireAllDataShardCalls(t, dataRTs, 0)
}

func TestShardedRoundTripper_WhenCloseIdleConnectionsCalled_ForwardsToMetaAndAll8DataShards(t *testing.T) {
	// Given
	rt, metaRT, dataRTs := buildTestShardedRoundTripper()
	httpCli := &http.Client{Transport: rt}

	// When
	httpCli.CloseIdleConnections()

	// Then
	require.Equal(t, int64(1), metaRT.idleClosed.Load())
	requireAllDataShardIdleClosed(t, dataRTs, 1)
}

func TestNewClient_GivenEndpointAndTokenConfig_ResolvesAndInjectsHeaders(t *testing.T) {
	// Given
	t.Setenv(EnvStorageEndpoint, "http://10.0.0.5:9001/")
	t.Setenv(EnvToken, "env-secret")
	resolvedEP, epErr := ResolveEndpoint("")
	require.NoError(t, epErr)
	require.Equal(t, "http://10.0.0.5:9001", resolvedEP)
	require.Equal(t, "env-secret", ResolveToken(""))
	require.Equal(t, "explicit-tok", ResolveToken("  explicit-tok  "))

	_, badSchemeErr := ResolveEndpoint("ftp://127.0.0.1:9000")
	require.Error(t, badSchemeErr)
	_, missingHostErr := ResolveEndpoint("http://")
	require.Error(t, missingHostErr)

	// When: 通过 NewClientFromConfig 发送请求，校验 Bearer 与 Accept-Encoding: identity 注入
	handler := &scriptedReadHandler{}
	client, err := NewClientFromConfig(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000/",
		Token:      "tok-123",
		HTTPClient: newInMemoryHTTPClient(handler),
	})
	require.NoError(t, err)
	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 2}))

	// Then
	require.NoError(t, err)
	require.Equal(t, "Bearer tok-123", handler.lastAuth)
	require.Equal(t, "identity", handler.lastAcceptEnc)
}

func TestNewShardedRoundTripper_GivenHTTPAndHTTPS_ConfiguresIsolatedH2Buffers(t *testing.T) {
	// Given / When: HTTP (h2c) 传输配置
	h2cRT, err := newShardedRoundTripper("http://127.0.0.1:9000", nil)
	require.NoError(t, err)
	defer h2cRT.CloseIdleConnections()

	metaH2C, ok := h2cRT.metaTransport.(*http.Transport)
	require.True(t, ok)
	require.Equal(t, HTTP2MetaMaxReceiveBufferPerConnection, metaH2C.HTTP2.MaxReceiveBufferPerConnection)
	data0H2C, ok := h2cRT.dataTransports[0].(*http.Transport)
	require.True(t, ok)
	require.Equal(t, HTTP2DataMaxReceiveBufferPerConnection, data0H2C.HTTP2.MaxReceiveBufferPerConnection)

	// HTTPS TLS 传输配置
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS13}
	tlsRT, err := newShardedRoundTripper("https://storage.bowl.local:9443", tlsCfg)
	require.NoError(t, err)
	defer tlsRT.CloseIdleConnections()

	metaTLS, ok := tlsRT.metaTransport.(*http.Transport)
	require.True(t, ok)
	require.True(t, metaTLS.ForceAttemptHTTP2)
	require.Same(t, tlsCfg, metaTLS.TLSClientConfig)
}
