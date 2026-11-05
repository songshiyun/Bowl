//go:build !linux

package pebblekv

import "os"

// syncFileData 在非 Linux 平台回退到 fsync，保证测试与开发机行为一致。
func syncFileData(file *os.File) error {
	return file.Sync()
}
