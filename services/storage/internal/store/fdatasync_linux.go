//go:build linux

package store

import (
	"os"
	"syscall"
)

// syncFileRangeWrite 对应 Linux uapi SYNC_FILE_RANGE_WRITE (2)，
// 异步将指定文件区间的脏页提交至内核块层回写队列而不阻塞等待落盘完成。
const syncFileRangeWrite = 2

// syncFileData 只刷块文件的数据与检索所需元数据，跳过 inode 时间戳等
// 与数据可见性无关的元数据日志提交。
func syncFileData(file *os.File) error {
	return syscall.Fdatasync(int(file.Fd()))
}

// startAsyncWriteback 在活跃 .pack 顺序写入跨过对齐窗口时触发内核非阻塞后台回写，
// 削平后续封口或 Fsync 屏障一次性刷出整包脏页的尾延迟毛刺。
func startAsyncWriteback(file *os.File, offset, nbytes int64) error {
	return syscall.SyncFileRange(int(file.Fd()), offset, nbytes, syncFileRangeWrite)
}
