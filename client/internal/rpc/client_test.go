package rpc

import (
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
