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
