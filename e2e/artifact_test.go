//go:build e2e

package e2e_test

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// artifactDir 返回 E2E 落盘工件的稳定目录，可用 BOWL_E2E_ARTIFACT_DIR 覆盖。
func artifactDir() string {
	return cmp.Or(os.Getenv("BOWL_E2E_ARTIFACT_DIR"), "artifacts")
}

// recordArtifact 将测试观测结果写入 artifacts/<TestName>.json。
// 工件只包含确定性字段，重复执行同一测试必须产生逐字节一致的 JSON。
func recordArtifact(t *testing.T, payload map[string]string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(artifactDir(), 0o755))
	data, err := json.MarshalIndent(payload, "", "  ")
	require.NoError(t, err)
	data = append(data, '\n')
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()) + ".json"
	require.NoError(t, os.WriteFile(filepath.Join(artifactDir(), name), data, 0o644))
}

// fileSHA256 返回字节内容的十六进制 SHA-256，作为工件的独立校验值。
func fileSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
