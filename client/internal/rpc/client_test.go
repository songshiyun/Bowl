package rpc

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveEndpointExplicitEndpointTakesPrecedence(t *testing.T) {
	t.Setenv(EnvStorageEndpoint, "http://env-endpoint:9000")
	ep, err := ResolveEndpoint("http://explicit-endpoint:9000")
	require.NoError(t, err)
	require.Equal(t, "http://explicit-endpoint:9000", ep)
}

func TestResolveEndpointEnvFallback(t *testing.T) {
	t.Setenv(EnvStorageEndpoint, "http://env-endpoint:9000")
	ep, err := ResolveEndpoint("")
	require.NoError(t, err)
	require.Equal(t, "http://env-endpoint:9000", ep)
}

func TestResolveEndpointDefaultFallback(t *testing.T) {
	t.Setenv(EnvStorageEndpoint, "")
	ep, err := ResolveEndpoint("")
	require.NoError(t, err)
	require.Equal(t, DefaultEndpoint, ep)
	require.Equal(t, "http://127.0.0.1:9000", ep)
}

func TestResolveEndpointTrimsTrailingSlash(t *testing.T) {
	t.Setenv(EnvStorageEndpoint, "")
	ep, err := ResolveEndpoint("http://127.0.0.1:9000/")
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:9000", ep)
}

func TestResolveEndpointAcceptsHTTP(t *testing.T) {
	t.Setenv(EnvStorageEndpoint, "")
	ep, err := ResolveEndpoint("http://127.0.0.1:9000")
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:9000", ep)
}

func TestResolveEndpointAcceptsHTTPS(t *testing.T) {
	t.Setenv(EnvStorageEndpoint, "")
	ep, err := ResolveEndpoint("https://storage.bowl.internal:443")
	require.NoError(t, err)
	require.Equal(t, "https://storage.bowl.internal:443", ep)
}

func TestResolveEndpointRejectsFTPScheme(t *testing.T) {
	t.Setenv(EnvStorageEndpoint, "")
	_, err := ResolveEndpoint("ftp://127.0.0.1:9000")
	require.Error(t, err)
}

func TestResolveEndpointRejectsGRPCScheme(t *testing.T) {
	t.Setenv(EnvStorageEndpoint, "")
	_, err := ResolveEndpoint("grpc://127.0.0.1:9000")
	require.Error(t, err)
}

func TestResolveEndpointRejectsMissingScheme(t *testing.T) {
	t.Setenv(EnvStorageEndpoint, "")
	_, err := ResolveEndpoint("127.0.0.1:9000")
	require.Error(t, err)
}

func TestResolveEndpointRejectsMissingHost(t *testing.T) {
	t.Setenv(EnvStorageEndpoint, "")
	_, err := ResolveEndpoint("http://")
	require.Error(t, err)
}

func TestResolveTokenExplicitTokenTakesPrecedence(t *testing.T) {
	t.Setenv(EnvToken, "env-token-123")
	require.Equal(t, "explicit-token-456", ResolveToken("explicit-token-456"))
}

func TestResolveTokenEnvFallback(t *testing.T) {
	t.Setenv(EnvToken, "env-token-123")
	require.Equal(t, "env-token-123", ResolveToken(""))
}

func TestResolveTokenEmptyWhenNeitherSet(t *testing.T) {
	t.Setenv(EnvToken, "")
	require.Empty(t, ResolveToken(""))
}

func TestNewHTTPTransportConfiguresHTTP2ForH2CAndTLS(t *testing.T) {
	t.Run("http_h2c", func(t *testing.T) {
		tr, err := newHTTPTransport("http://127.0.0.1:9000", nil)
		require.NoError(t, err)
		require.NotNil(t, tr.Protocols)
		require.True(t, tr.Protocols.UnencryptedHTTP2())
		require.NotNil(t, tr.HTTP2)
		require.Equal(t, 1<<20, tr.HTTP2.MaxReadFrameSize)
		require.Equal(t, (4<<20)-1, tr.HTTP2.MaxReceiveBufferPerStream)
		require.Equal(t, (4<<20)-1, tr.HTTP2.MaxReceiveBufferPerConnection)
	})

	t.Run("https_tls", func(t *testing.T) {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS13}
		tr, err := newHTTPTransport("https://storage.bowl.internal:443", tlsCfg)
		require.NoError(t, err)
		require.True(t, tr.ForceAttemptHTTP2)
		require.Same(t, tlsCfg, tr.TLSClientConfig)
		require.NotNil(t, tr.HTTP2)
		require.Equal(t, 1<<20, tr.HTTP2.MaxReadFrameSize)
		require.Equal(t, (4<<20)-1, tr.HTTP2.MaxReceiveBufferPerStream)
		require.Equal(t, (4<<20)-1, tr.HTTP2.MaxReceiveBufferPerConnection)
	})
}
