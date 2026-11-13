package cli

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

const primaryMountMetaDir = "/run/bowl/mounts"

// mountMeta 是挂载就绪后写入的发现元数据，供 `bowl stats` 定位挂载进程的管理面地址。
type mountMeta struct {
	Mountpoint string    `json:"mountpoint"`
	PID        int       `json:"pid"`
	DiskID     string    `json:"disk_id"`
	AdminAddr  string    `json:"admin_addr"`
	StartedAt  time.Time `json:"started_at"`
}

func fallbackMountMetaDir() string {
	return filepath.Join(os.TempDir(), "bowl-mounts")
}

// mountMetaDirs 返回发现元数据目录（按优先级）：/run/bowl/mounts，无权限时回退 $TMPDIR/bowl-mounts。
// 定义为变量以便单测替换为临时目录。
var mountMetaDirs = func() []string {
	return []string{primaryMountMetaDir, fallbackMountMetaDir()}
}

// canonicalMountpoint 规范化挂载点路径（优先解析符号链接并 Clean，不存在时回退 Abs+Clean）。
func canonicalMountpoint(mountpoint string) string {
	abs, err := filepath.Abs(mountpoint)
	if err == nil {
		mountpoint = abs
	}
	if resolved, evalErr := filepath.EvalSymlinks(mountpoint); evalErr == nil {
		return filepath.Clean(resolved)
	}
	return filepath.Clean(mountpoint)
}

// mountMetaFileName 以规范化挂载点的 SHA-256 前 16 字节十六进制作为文件名。
func mountMetaFileName(mountpoint string) string {
	sum := sha256.Sum256([]byte(filepath.Clean(mountpoint)))
	return hex.EncodeToString(sum[:16]) + ".json"
}

// writeMountMeta 原子写入（临时文件 + rename）发现元数据，返回写入路径。
func writeMountMeta(meta *mountMeta) (string, error) {
	normalized := *meta
	normalized.Mountpoint = filepath.Clean(normalized.Mountpoint)
	data, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("序列化挂载元数据失败: %w", err)
	}
	fallbackDir := fallbackMountMetaDir()
	var errs error
	for _, dir := range mountMetaDirs() {
		path, werr := writeFileAtomic(dir, mountMetaFileName(normalized.Mountpoint), data, dir == fallbackDir)
		if werr == nil {
			return path, nil
		}
		errs = errors.Join(errs, werr)
	}
	return "", errs
}

func writeFileAtomic(dir, name string, data []byte, sharedSticky bool) (string, error) {
	_, statErr := os.Stat(dir)
	createdDir := errors.Is(statErr, os.ErrNotExist)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	if createdDir && sharedSticky {
		// 共享回退目录允许不同 UID 的挂载进程各自写入与清理自身元数据文件。
		_ = os.Chmod(dir, 0o777|os.ModeSticky)
	}
	tmp, err := os.CreateTemp(dir, "."+name+".tmp-*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, writeErr := tmp.Write(data); writeErr != nil {
		_ = tmp.Close()
		return "", writeErr
	}
	if closeErr := tmp.Close(); closeErr != nil {
		return "", closeErr
	}
	if chmodErr := os.Chmod(tmpName, 0o644); chmodErr != nil {
		return "", chmodErr
	}
	path := filepath.Join(dir, name)
	if renameErr := os.Rename(tmpName, path); renameErr != nil {
		return "", renameErr
	}
	return path, nil
}

// processAlive 通过 kill(pid, 0) 判断进程是否存活（EPERM 表示存在但无权发信号）。
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// listMountMetas 读取全部目录下进程仍存活的发现元数据（同一挂载点仅保留首个目录中的记录），按挂载点排序。
func listMountMetas() []mountMeta {
	seen := make(map[string]struct{})
	var out []mountMeta
	for _, dir := range mountMetaDirs() {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".json") {
				continue
			}
			data, readErr := os.ReadFile(filepath.Join(dir, name))
			if readErr != nil {
				continue
			}
			var m mountMeta
			if json.Unmarshal(data, &m) != nil || !processAlive(m.PID) {
				continue
			}
			m.Mountpoint = filepath.Clean(m.Mountpoint)
			if _, dup := seen[m.Mountpoint]; dup {
				continue
			}
			seen[m.Mountpoint] = struct{}{}
			out = append(out, m)
		}
	}
	slices.SortFunc(out, func(a, b mountMeta) int {
		return cmp.Compare(a.Mountpoint, b.Mountpoint)
	})
	return out
}

// mountMetaFilePath 查找指定挂载点对应的本地元数据文件路径（不校验 PID 存活）。
func mountMetaFilePath(mountpoint string) (string, bool) {
	cleaned := filepath.Clean(mountpoint)
	canon := canonicalMountpoint(cleaned)
	names := []string{mountMetaFileName(cleaned)}
	if canon != cleaned {
		names = append(names, mountMetaFileName(canon))
	}
	for _, dir := range mountMetaDirs() {
		for _, name := range names {
			path := filepath.Join(dir, name)
			if _, err := os.Stat(path); err == nil {
				return path, true
			}
		}
	}
	return "", false
}

// readMountMetaPID 读取元数据文件记录的守护进程 PID。
func readMountMetaPID(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	var m mountMeta
	if json.Unmarshal(data, &m) != nil {
		return 0
	}
	return m.PID
}

// findMountMeta 按规范化挂载点查找存活进程的发现元数据（兼容符号链接路径）。
func findMountMeta(mountpoint string) (mountMeta, bool) {
	cleaned := filepath.Clean(mountpoint)
	canon := canonicalMountpoint(cleaned)
	for _, m := range listMountMetas() {
		if m.Mountpoint == cleaned || canonicalMountpoint(m.Mountpoint) == canon {
			return m, true
		}
	}
	return mountMeta{}, false
}
