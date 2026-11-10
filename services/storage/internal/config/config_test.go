package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/config"
)

func TestMetaConfigDefaultsAndValidation(t *testing.T) {
	cfg := config.MetaConfig{
		AtimeModeVal:              config.DefaultAtimeMode,
		EntryTimeoutSecondsVal:    config.DefaultEntryTimeoutSeconds,
		AttrTimeoutSecondsVal:     config.DefaultAttrTimeoutSeconds,
		DirEntryTimeoutSecondsVal: config.DefaultDirEntryTimeoutSeconds,
		OpenKeepCacheVal:          true,
		OpenDirCacheVal:           true,
	}
	require.NoError(t, cfg.Validate())
	require.Equal(t, config.DefaultDataDir, cfg.DataDir())
	require.Equal(t, "noatime", cfg.AtimeMode())
	require.InDelta(t, 1.0, cfg.EntryTimeoutSeconds(), 1e-6)
	require.InDelta(t, 1.0, cfg.AttrTimeoutSeconds(), 1e-6)
	require.InDelta(t, 1.0, cfg.DirEntryTimeoutSeconds(), 1e-6)
	require.True(t, cfg.OpenKeepCache())
	require.True(t, cfg.OpenDirCache())

	cfg.StorageDataDirVal = "/var/lib/bowl-storage"
	require.Equal(t, "/var/lib/bowl-storage", cfg.DataDir())
	cfg.DataDirVal = "/var/lib/bowl-override"
	require.Equal(t, "/var/lib/bowl-override", cfg.DataDir())

	badMode := cfg
	badMode.AtimeModeVal = "invalid"
	require.ErrorContains(t, badMode.Validate(), "invalid atime mode")

	negTimeout := cfg
	negTimeout.EntryTimeoutSecondsVal = -1
	require.ErrorContains(t, negTimeout.Validate(), "invalid entry timeout")
}

func TestLoadObservabilityAndInternalPortDefaults(t *testing.T) {
	t.Setenv("DB_DSN", "postgres://postgres:postgres@127.0.0.1:5432/bowl?sslmode=disable")
	t.Setenv("SERVER_PORT", "")
	t.Setenv("INTERNAL_SERVER_PORT", "")
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_TRUST_REMOTE", "")

	cfg, err := config.Load()
	require.NoError(t, err)
	require.Equal(t, 9000, cfg.Port())
	require.Equal(t, 9090, cfg.InternalServerPort)
	require.Equal(t, "bowl-storage", cfg.ServiceName())
	require.True(t, cfg.TrustRemote())
	require.True(t, cfg.IsMetricsEnabled())
	require.True(t, cfg.IsPprofEnabled())

	t.Setenv("INTERNAL_SERVER_PORT", "19090")
	t.Setenv("OTEL_TRUST_REMOTE", "false")
	cfgOverride, err := config.Load()
	require.NoError(t, err)
	require.Equal(t, 19090, cfgOverride.InternalServerPort)
	require.False(t, cfgOverride.TrustRemote())
}
