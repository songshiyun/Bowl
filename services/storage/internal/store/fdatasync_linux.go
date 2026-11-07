//go:build linux

package store

import (
	"os"
	"syscall"
)

// syncFileData 只刷块文件的数据与检索所需元数据，跳过 inode 时间戳等
// 与数据可见性无关的元数据日志提交。
func syncFileData(file *os.File) error {
	return syscall.Fdatasync(int(file.Fd()))
}
