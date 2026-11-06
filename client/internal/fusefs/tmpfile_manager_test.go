package fusefs_test

import (
	"bytes"
	"context"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestTmpFileManager_CreateGetAttrAndInodeMapping(t *testing.T) {
	const rootIno = uint64(100)
	mgr := fusefs.NewTmpFileManager(rootIno)

	// 1. 根节点双向转换与直接访问 RootIno 越权拦截
	srvRoot, st := mgr.ToServerIno(fuse.FUSE_ROOT_ID)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, rootIno, srvRoot)

	_, st = mgr.ToServerIno(rootIno)
	require.Equal(t, fuse.ENOENT, st, "非 1 的 RootIno 直接作为 kernelIno 访问必须拒绝返回 ENOENT")

	require.Equal(t, uint64(fuse.FUSE_ROOT_ID), mgr.ToKernelIno(rootIno))
	require.Equal(t, uint64(200), mgr.ToKernelIno(200))

	// 2. 未知高位本地 Inode 返回 ENOENT
	_, st = mgr.ToServerIno((uint64(1) << 63) | 999)
	require.Equal(t, fuse.ENOENT, st)

	// 3. Create 分配高位匿名 Inode，初始化属性与稀疏页大小
	ino1, inode1 := mgr.Create(fuse.FUSE_ROOT_ID, 1000, 1001, 0o640, syscall.O_RDWR)
	require.NotZero(t, ino1&(uint64(1)<<63), "本地匿名 Inode 必须置最高位 1<<63")
	require.NotNil(t, inode1)
	require.Equal(t, ino1, inode1.GetIno())
	require.Equal(t, uint64(0), inode1.GetGeneration())
	require.Equal(t, uint32(syscall.S_IFREG|0o640), inode1.GetAttr().GetMode())
	require.Equal(t, uint32(1000), inode1.GetAttr().GetUid())
	require.Equal(t, uint32(1001), inode1.GetAttr().GetGid())

	ino2, _ := mgr.Create(fuse.FUSE_ROOT_ID, 1000, 1001, 0o600, syscall.O_WRONLY|syscall.O_EXCL)
	require.NotEqual(t, ino1, ino2)

	// 未具名化前 ToServerIno 返回自身 kernelIno
	mappedIno, st := mgr.ToServerIno(ino1)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, ino1, mappedIno)

	// GetAttr 命中本地匿名文件
	var attr fuse.Attr
	handled, st := mgr.GetAttr(ino1, &attr)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, ino1, attr.Ino)
	require.Equal(t, uint32(syscall.S_IFREG|0o640), attr.Mode)
	require.Equal(t, uint32(1000), attr.Uid)
	require.Equal(t, uint32(1001), attr.Gid)
	require.Equal(t, uint64(0), attr.Size)

	// 非匿名文件 GetAttr 返回 handled=false
	handled, st = mgr.GetAttr(200, &attr)
	require.False(t, handled)
	require.Equal(t, fuse.OK, st)
}

func TestTmpFileManager_SparseWriteAndRead(t *testing.T) {
	mgr := fusefs.NewTmpFileManager(1)
	ino, _ := mgr.Create(1, 0, 0, 0o600, syscall.O_RDWR)

	// 1. 跨 4 KiB 页边界写入（offset = 4090，长 16 字节，跨越页 0 与页 1）
	crossPayload := []byte("0123456789ABCDEF")
	written, handled, st := mgr.Write(ino, 4090, crossPayload, false)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint32(len(crossPayload)), written)

	// 2. 跳过页 2（空洞），在页 3 (offset = 3*4096 + 100 = 12388) 写入
	tailPayload := []byte("SPARSE_PAGE_3_DATA")
	written, handled, st = mgr.Write(ino, 3*4096+100, tailPayload, false)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint32(len(tailPayload)), written)

	expectedSize := uint64(3*4096 + 100 + len(tailPayload))
	var attr fuse.Attr
	handled, st = mgr.GetAttr(ino, &attr)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, expectedSize, attr.Size)
	// 已分配页为 page 0, page 1, page 3 共 3 个 4 KiB 页 = 3 * 8 = 24 个 512B 扇区
	require.Equal(t, uint64(3*(4096/512)), attr.Blocks)

	// 3. 完整读取并对照独立期望模型：前 4090 字节全 0，随后 crossPayload，中间空洞全 0，尾部 tailPayload，且超出 EOF 截断
	model := make([]byte, expectedSize)
	copy(model[4090:], crossPayload)
	copy(model[3*4096+100:], tailPayload)

	readBuf := make([]byte, expectedSize+1024)
	n, handled, st := mgr.Read(ino, 0, readBuf)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, int(expectedSize), n, "超出 EOF 的读取必须精确截断到文件大小")
	require.Equal(t, model, readBuf[:n])

	// 4. 从跨页中点与空洞中点读取子区间
	subBuf := make([]byte, 200)
	n, handled, st = mgr.Read(ino, 4088, subBuf)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, 200, n)
	require.Equal(t, model[4088:4288], subBuf[:n])

	// 5. 起始偏移 >= EOF 返回 0 字节
	n, handled, st = mgr.Read(ino, expectedSize+10, subBuf)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, 0, n)

	// 6. appendMode 追加写自动定位到当前文件末尾
	appendPayload := []byte("_APPENDED")
	written, handled, st = mgr.Write(ino, 0, appendPayload, true)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint32(len(appendPayload)), written)
	model = append(model, appendPayload...)

	fullBuf := make([]byte, len(model))
	n, handled, st = mgr.Read(ino, 0, fullBuf)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, len(model), n)
	require.Equal(t, model, fullBuf)
}

func TestTmpFileManager_SetAttrAndAllocate(t *testing.T) {
	mgr := fusefs.NewTmpFileManager(1)
	ino, _ := mgr.Create(1, 1000, 1000, 0o600, syscall.O_RDWR)

	// 写入 3 个 4 KiB 页（共 12 KiB = 12288 字节）
	initialData := bytes.Repeat([]byte("A"), 12288)
	_, handled, st := mgr.Write(ino, 0, initialData, false)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)

	// 1. SetAttr 缩小尺寸至 5000 字节（页 2 被释放，页 1 从偏移 904 起清零）
	var attr fuse.Attr
	setShrink := &fuse.SetAttrIn{}
	setShrink.NodeId = ino
	setShrink.Valid = fuse.FATTR_SIZE
	setShrink.Size = 5000
	_, handled, st = mgr.SetAttr(ino, setShrink, nil, &attr)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint64(5000), attr.Size)
	require.Equal(t, uint64(2*(4096/512)), attr.Blocks, "缩小至 5000 字节后应只剩 2 个 4 KiB 页")

	// 2. SetAttr 零扩展至 16384 字节：前 5000 字节为 'A'，[5000, 16384) 必须全部为 0（包含原页 1 被清零的尾部）
	setGrow := &fuse.SetAttrIn{}
	setGrow.NodeId = ino
	setGrow.Valid = fuse.FATTR_SIZE
	setGrow.Size = 16384
	_, handled, st = mgr.SetAttr(ino, setGrow, nil, &attr)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint64(16384), attr.Size)

	readBack := make([]byte, 16384)
	n, handled, st := mgr.Read(ino, 0, readBack)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, 16384, n)
	require.Equal(t, bytes.Repeat([]byte("A"), 5000), readBack[:5000])
	require.Equal(t, make([]byte, 16384-5000), readBack[5000:], "缩容再扩容后的区间必须全部为零字节")

	// 3. SetAttr 变更权限、UID、GID 与显式时间戳
	setMeta := &fuse.SetAttrIn{}
	setMeta.NodeId = ino
	setMeta.Valid = fuse.FATTR_MODE | fuse.FATTR_UID | fuse.FATTR_GID | fuse.FATTR_ATIME | fuse.FATTR_MTIME
	setMeta.Mode = syscall.S_IFREG | 0o755
	setMeta.Uid = 2001
	setMeta.Gid = 2002
	setMeta.Atime = 1700000100
	setMeta.Atimensec = 123
	setMeta.Mtime = 1700000200
	setMeta.Mtimensec = 456
	_, handled, st = mgr.SetAttr(ino, setMeta, nil, &attr)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint32(syscall.S_IFREG|0o755), attr.Mode)
	require.Equal(t, uint32(2001), attr.Uid)
	require.Equal(t, uint32(2002), attr.Gid)
	require.Equal(t, uint64(1700000100), attr.Atime)
	require.Equal(t, uint32(123), attr.Atimensec)
	require.Equal(t, uint64(1700000200), attr.Mtime)
	require.Equal(t, uint32(456), attr.Mtimensec)

	// 4. Allocate 预分配扩展（mode = 0 扩展文件大小至 24576；KEEP_SIZE 不改变文件大小）
	handled, st = mgr.Allocate(ino, 0, 24576, 0)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	handled, st = mgr.GetAttr(ino, &attr)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint64(24576), attr.Size)

	handled, st = mgr.Allocate(ino, 0, 32768, unix.FALLOC_FL_KEEP_SIZE)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	handled, st = mgr.GetAttr(ino, &attr)
	require.True(t, handled)
	require.Equal(t, uint64(24576), attr.Size, "KEEP_SIZE 预分配不得改变文件逻辑大小")

	// 5. Allocate 打洞清零（FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE）
	// 先在 [0, 12288) 重新写满 'B'，再对 [2048, 10240) 打洞（跨越页 0 后半段、整页 1、页 2 前半段）
	_, _, st = mgr.Write(ino, 0, bytes.Repeat([]byte("B"), 12288), false)
	require.Equal(t, fuse.OK, st)

	handled, st = mgr.Allocate(ino, 2048, 8192, unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)

	punched := make([]byte, 12288)
	n, handled, st = mgr.Read(ino, 0, punched)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, 12288, n)
	require.Equal(t, bytes.Repeat([]byte("B"), 2048), punched[:2048])
	require.Equal(t, make([]byte, 8192), punched[2048:10240], "打洞区间必须清零且整页 1 释放")
	require.Equal(t, bytes.Repeat([]byte("B"), 2048), punched[10240:12288])

	handled, st = mgr.GetAttr(ino, &attr)
	require.True(t, handled)
	require.Equal(t, uint64(2*(4096/512)), attr.Blocks, "整页 1 被打洞后应从稀疏页表中释放")
}

type fakeMaterializer struct {
	nextServerIno   uint64
	createStatus    fuse.Status
	writeFailAtCall int
	writeStatus     fuse.Status
	commitStatus    fuse.Status
	unlinkStatus    fuse.Status
	linkStatus      fuse.Status

	createdParent uint64
	createdName   string
	writes        []fusefs.TmpUploadRange
	committedSize uint64
	rollbacks     []string
	unlinkedOpen  []string
	linkedNames   []string
}

func (f *fakeMaterializer) CreateTmpInode(_ context.Context, parentIno uint64, name string, mode, uid, gid uint32) (*storagev1.Inode, float64, float64, fuse.Status) {
	if f.createStatus != fuse.OK {
		return nil, 0, 0, f.createStatus
	}
	f.nextServerIno++
	f.createdParent = parentIno
	f.createdName = name
	inode := &storagev1.Inode{
		Ino:        f.nextServerIno,
		Generation: 0,
		Attr: &storagev1.InodeAttribute{
			Mode:  mode,
			Uid:   uid,
			Gid:   gid,
			Nlink: 1,
		},
	}
	return inode, 1.5, 2.5, fuse.OK
}

func (f *fakeMaterializer) WriteChunk(_ context.Context, _, offset uint64, data []byte) fuse.Status {
	callIdx := len(f.writes) + 1
	if f.writeFailAtCall > 0 && callIdx == f.writeFailAtCall {
		return f.writeStatus
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	f.writes = append(f.writes, fusefs.TmpUploadRange{Offset: offset, Data: cp})
	return fuse.OK
}

func (f *fakeMaterializer) CommitTmpAttr(_ context.Context, serverIno, size uint64, atime, mtime *timestamppb.Timestamp) (*storagev1.Inode, fuse.Status) {
	if f.commitStatus != fuse.OK {
		return nil, f.commitStatus
	}
	f.committedSize = size
	return &storagev1.Inode{
		Ino:        serverIno,
		Generation: 0,
		Attr: &storagev1.InodeAttribute{
			Mode:  syscall.S_IFREG | 0o644,
			Size:  size,
			Nlink: 1,
			Atime: atime,
			Mtime: mtime,
		},
	}, fuse.OK
}

func (f *fakeMaterializer) RollbackTmpInode(_ context.Context, _ uint64, name string) {
	f.rollbacks = append(f.rollbacks, name)
}

func (f *fakeMaterializer) UnlinkOpenTmpInode(_ context.Context, _ uint64, name string) fuse.Status {
	if f.unlinkStatus != fuse.OK {
		return f.unlinkStatus
	}
	f.unlinkedOpen = append(f.unlinkedOpen, name)
	return fuse.OK
}

func (f *fakeMaterializer) LinkInode(_ context.Context, serverIno, _ uint64, name string) (*storagev1.Inode, float64, float64, fuse.Status) {
	if f.linkStatus != fuse.OK {
		return nil, 0, 0, f.linkStatus
	}
	f.linkedNames = append(f.linkedNames, name)
	return &storagev1.Inode{
		Ino:        serverIno,
		Generation: 0,
		Attr: &storagev1.InodeAttribute{
			Mode:  syscall.S_IFREG | 0o644,
			Size:  f.committedSize,
			Nlink: 1,
		},
	}, 1.0, 2.0, fuse.OK
}

func TestTmpFileManager_LinkAndSync1MiBChunkingAliasAndRollback(t *testing.T) {
	const rootIno = uint64(100)
	mgr := fusefs.NewTmpFileManager(rootIno)
	localIno, _ := mgr.Create(fuse.FUSE_ROOT_ID, 1000, 1000, 0o644, syscall.O_RDWR)

	// 写入 1.5 MiB (1536 KiB) 连续数据 + 跳过空洞后在偏移 3 MiB 处写入 8 KiB 数据
	part1 := bytes.Repeat([]byte("X"), 1536*1024)
	part2 := bytes.Repeat([]byte("Y"), 8*1024)
	_, _, st := mgr.Write(localIno, 0, part1, false)
	require.Equal(t, fuse.OK, st)
	_, _, st = mgr.Write(localIno, 3*1024*1024, part2, false)
	require.Equal(t, fuse.OK, st)

	// 1. 模拟第 2 个分片写入失败（ENOSPC）：必须触发 RollbackTmpInode，且保留本地匿名页缓冲与未上云状态
	failMat := &fakeMaterializer{
		nextServerIno:   500,
		writeFailAtCall: 2,
		writeStatus:     fuse.Status(syscall.ENOSPC),
	}
	var entryOut fuse.EntryOut
	_, _, _, _, _, handled, st := mgr.Link(context.Background(), localIno, fuse.FUSE_ROOT_ID, "named.bin", true, failMat, &entryOut)
	require.True(t, handled)
	require.Equal(t, fuse.Status(syscall.ENOSPC), st)
	require.Equal(t, []string{"named.bin"}, failMat.rollbacks, "写入失败必须回滚撤销服务端创建的临时目录项")

	// 验证回滚后本地状态完好：ToServerIno 仍返回 localIno，本地数据仍可读
	srvIno, st := mgr.ToServerIno(localIno)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, localIno, srvIno)
	probe := make([]byte, 16)
	n, handled, st := mgr.Read(localIno, 0, probe)
	require.True(t, handled, "上云失败回滚后本地页缓冲不得丢失")
	require.Equal(t, fuse.OK, st)
	require.Equal(t, 16, n)
	require.Equal(t, bytes.Repeat([]byte("X"), 16), probe)

	// 2. 正常 Link 具名化：验证按 1 MiB 聚合分片上云（1.5 MiB 拆为 1 MiB + 512 KiB，3 MiB 处独立 8 KiB 分片，共 3 次 WriteChunk）
	okMat := &fakeMaterializer{nextServerIno: 600}
	gotSrvIno, firstUpload, gotInode, _, _, handled, st := mgr.Link(context.Background(), localIno, fuse.FUSE_ROOT_ID, "named.bin", true, okMat, &entryOut)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.True(t, firstUpload)
	require.Equal(t, uint64(601), gotSrvIno)
	require.NotNil(t, gotInode)
	require.Equal(t, rootIno, okMat.createdParent)
	require.Len(t, okMat.writes, 3, "连续 4 KiB 页必须按 1 MiB 聚合分片，跨空洞断开")
	require.Equal(t, uint64(0), okMat.writes[0].Offset)
	require.Equal(t, 1024*1024, len(okMat.writes[0].Data))
	require.Equal(t, part1[:1024*1024], okMat.writes[0].Data)
	require.Equal(t, uint64(1024*1024), okMat.writes[1].Offset)
	require.Equal(t, 512*1024, len(okMat.writes[1].Data))
	require.Equal(t, part1[1024*1024:], okMat.writes[1].Data)
	require.Equal(t, uint64(3*1024*1024), okMat.writes[2].Offset)
	require.Equal(t, part2, okMat.writes[2].Data)
	require.Equal(t, uint64(3*1024*1024+len(part2)), okMat.committedSize)

	// 3. 验证具名化成功后双向别名映射一致性及本地缓冲已移交服务端
	require.Equal(t, localIno, entryOut.NodeId, "返回给内核的 EntryOut.NodeId 必须保持原高位本地 Inode 别名")
	require.Equal(t, localIno, mgr.ToKernelIno(601))
	require.Equal(t, localIno, mgr.ObserveKernelLookup(601))
	mappedSrv, st := mgr.ToServerIno(localIno)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint64(601), mappedSrv)

	// 已上云后本地 Read/Write/GetAttr 均返回 handled=false，交由服务端路径处理
	_, handled, _ = mgr.Read(localIno, 0, probe)
	require.False(t, handled)
	handled, _ = mgr.GetAttr(localIno, &fuse.Attr{})
	require.False(t, handled)

	// 已具名化的匿名文件不可再次通过匿名路径 Link
	_, _, _, _, _, handled, st = mgr.Link(context.Background(), localIno, fuse.FUSE_ROOT_ID, "again.bin", true, okMat, &entryOut)
	require.True(t, handled)
	require.Equal(t, fuse.ENOENT, st)

	// 4. 验证先 Sync（上云并解链为孤儿）再 Link（通过服务端 LinkInode 重新具名化）
	syncIno, _ := mgr.Create(fuse.FUSE_ROOT_ID, 1000, 1000, 0o600, syscall.O_RDWR)
	_, _, st = mgr.Write(syncIno, 0, []byte("sync-then-link"), false)
	require.Equal(t, fuse.OK, st)

	syncMat := &fakeMaterializer{nextServerIno: 700}
	parentKernel, tempName, syncedSrvIno, uploaded, st := mgr.Sync(context.Background(), syncIno, syncMat)
	require.Equal(t, fuse.OK, st)
	require.True(t, uploaded)
	require.Equal(t, uint64(fuse.FUSE_ROOT_ID), parentKernel)
	require.NotEmpty(t, tempName)
	require.Equal(t, uint64(701), syncedSrvIno)
	require.Equal(t, []string{tempName}, syncMat.unlinkedOpen)

	// 再次 Sync 已上云节点应幂等成功且不再重复上传
	_, _, _, uploadedAgain, st := mgr.Sync(context.Background(), syncIno, syncMat)
	require.Equal(t, fuse.OK, st)
	require.False(t, uploadedAgain)

	// Sync 后执行 Link 走 LinkInode 分支
	gotSrvIno2, firstUpload2, _, _, _, handled, st := mgr.Link(context.Background(), syncIno, fuse.FUSE_ROOT_ID, "from-synced.bin", true, syncMat, &entryOut)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.False(t, firstUpload2, "已通过 Sync 上云的节点在 Link 时应走服务端 LinkInode 而非首次上传")
	require.Equal(t, uint64(701), gotSrvIno2)
	require.Equal(t, []string{"from-synced.bin"}, syncMat.linkedNames)
}

func TestTmpFileManager_DualZeroReclamationAndClear(t *testing.T) {
	mgr := fusefs.NewTmpFileManager(1)

	// 场景 A：纯内存匿名文件（未上云），Create 初始 nlookup=1，且句柄打开
	localOnlyIno, _ := mgr.Create(1, 0, 0, 0o600, syscall.O_RDWR)
	_, _, st := mgr.Write(localOnlyIno, 0, []byte("pure-memory-data"), false)
	require.Equal(t, fuse.OK, st)

	openHandles := map[uint64]int{localOnlyIno: 1}
	hasOpen := func(ino uint64) bool { return openHandles[ino] > 0 }

	// A1. 句柄仍打开时 OnForget 把 nlookup 减到 0：不得提前回收内存页
	dropped := mgr.OnForget(localOnlyIno, 1, hasOpen)
	require.False(t, dropped, "句柄尚未释放时 OnForget 不得提前回收本地匿名文件")
	buf := make([]byte, 16)
	n, handled, st := mgr.Read(localOnlyIno, 0, buf)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, []byte("pure-memory-data"), buf[:n])

	// A2. 恢复 nlookup=1，模拟句柄先关闭但内核 nlookup 尚未归零：不得提前回收
	mgr.AddLookup(localOnlyIno)
	openHandles[localOnlyIno] = 0
	dropped = mgr.OnHandleReleased(localOnlyIno, false, hasOpen)
	require.False(t, dropped, "nlookup 尚未归零时 OnHandleReleased 不得提前回收本地匿名文件")
	n, handled, st = mgr.Read(localOnlyIno, 0, buf)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, []byte("pure-memory-data"), buf[:n])

	// A3. 当句柄已关闭 (open=0) 且 OnForget 将 nlookup 归零：双重归零立即回收
	dropped = mgr.OnForget(localOnlyIno, 1, hasOpen)
	require.True(t, dropped, "nlookup 与句柄双重归零后必须立即回收")
	_, st = mgr.ToServerIno(localOnlyIno)
	require.Equal(t, fuse.ENOENT, st)

	// 场景 B：已具名化上云的匿名文件（Create nlookup=1 -> Link nlookup=2 -> ObserveKernelLookup nlookup=3）
	namedIno, _ := mgr.Create(1, 0, 0, 0o644, syscall.O_RDWR)
	openHandles[namedIno] = 2
	mat := &fakeMaterializer{nextServerIno: 800}
	var entryOut fuse.EntryOut
	srvIno, _, _, _, _, handled, st := mgr.Link(context.Background(), namedIno, 1, "linked.txt", true, mat, &entryOut)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint64(801), srvIno)

	// ObserveKernelLookup 再增加 1 次内核引用（当前 lookups = 3）
	require.Equal(t, namedIno, mgr.ObserveKernelLookup(srvIno))

	// B1. 释放第 1 个句柄（剩余 1 个打开句柄）：不回收别名
	openHandles[namedIno] = 1
	require.False(t, mgr.OnHandleReleased(namedIno, false, hasOpen))
	require.Equal(t, namedIno, mgr.ToKernelIno(srvIno))

	// B2. 释放最后 1 个句柄（open=0，但 lookups=3）：不回收别名
	openHandles[namedIno] = 0
	require.False(t, mgr.OnHandleReleased(namedIno, false, hasOpen))
	require.Equal(t, namedIno, mgr.ToKernelIno(srvIno))
	mappedSrv, st := mgr.ToServerIno(namedIno)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, srvIno, mappedSrv)

	// B3. 部分 Forget（nlookup 减 2，剩余 lookups=1）：仍不回收别名
	require.False(t, mgr.OnForget(namedIno, 2, hasOpen))
	require.Equal(t, namedIno, mgr.ToKernelIno(srvIno))

	// B4. 最终 Forget（nlookup 归零，且 open=0）：双重归零回收双向别名
	require.True(t, mgr.OnForget(namedIno, 1, hasOpen))
	_, st = mgr.ToServerIno(namedIno)
	require.Equal(t, fuse.ENOENT, st, "双重归零后本地高位 Inode 必须失效返回 ENOENT")
	require.Equal(t, srvIno, mgr.ToKernelIno(srvIno), "双重归零后服务端 Inode 别名映射必须移除")

	// 场景 C：Clear 清空所有状态
	tempIno, _ := mgr.Create(1, 0, 0, 0o600, syscall.O_RDWR)
	mgr.Clear()
	_, st = mgr.ToServerIno(tempIno)
	require.Equal(t, fuse.ENOENT, st)
}

func TestTmpFileManager_BoundaryAndErrorPaths(t *testing.T) {
	mgr := fusefs.NewTmpFileManager(1)
	ino, _ := mgr.Create(1, 100, 200, 0o644, syscall.O_RDWR)

	// 1. Write 偏移溢出 math.MaxInt64 返回 EFBIG
	_, handled, st := mgr.Write(ino, uint64(1)<<63, []byte("a"), false)
	require.True(t, handled)
	require.Equal(t, fuse.Status(syscall.EFBIG), st)

	// 2. SetAttr FATTR_SIZE 超过 math.MaxInt64 返回 EFBIG
	setBig := &fuse.SetAttrIn{}
	setBig.NodeId = ino
	setBig.Valid = fuse.FATTR_SIZE
	setBig.Size = uint64(1) << 63
	_, handled, st = mgr.SetAttr(ino, setBig, nil, &fuse.Attr{})
	require.True(t, handled)
	require.Equal(t, fuse.Status(syscall.EFBIG), st)

	// 3. SetAttr FATTR_FH 句柄权限校验失败返回 EBADF
	setFH := &fuse.SetAttrIn{}
	setFH.NodeId = ino
	setFH.Valid = fuse.FATTR_FH | fuse.FATTR_SIZE
	setFH.Fh = 99
	setFH.Size = 0
	_, handled, st = mgr.SetAttr(ino, setFH, func(fh uint64, needWrite bool) fuse.Status {
		require.Equal(t, uint64(99), fh)
		require.True(t, needWrite)
		return fuse.EBADF
	}, &fuse.Attr{})
	require.True(t, handled)
	require.Equal(t, fuse.EBADF, st)

	// 4. SetAttr FATTR_ATIME_NOW | FATTR_MTIME_NOW
	setNow := &fuse.SetAttrIn{}
	setNow.NodeId = ino
	setNow.Valid = fuse.FATTR_ATIME_NOW | fuse.FATTR_MTIME_NOW
	var attr fuse.Attr
	_, handled, st = mgr.SetAttr(ino, setNow, nil, &attr)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.NotZero(t, attr.Atime)
	require.NotZero(t, attr.Mtime)

	// 5. Allocate 非法模式（单独 PUNCH_HOLE 或 ZERO_RANGE）返回 EOPNOTSUPP
	handled, st = mgr.Allocate(ino, 0, 4096, unix.FALLOC_FL_PUNCH_HOLE)
	require.True(t, handled)
	require.Equal(t, fuse.Status(syscall.EOPNOTSUPP), st)

	handled, st = mgr.Allocate(ino, 0, 4096, unix.FALLOC_FL_ZERO_RANGE)
	require.True(t, handled)
	require.Equal(t, fuse.Status(syscall.EOPNOTSUPP), st)

	// 6. Allocate length=0 或偏移溢出返回 EINVAL
	handled, st = mgr.Allocate(ino, 0, 0, 0)
	require.True(t, handled)
	require.Equal(t, fuse.EINVAL, st)

	// 7. O_EXCL 匿名文件或已关闭句柄（isOpen=false）拒绝 Link 返回 ENOENT
	exclIno, _ := mgr.Create(1, 100, 200, 0o600, syscall.O_RDWR|syscall.O_EXCL)
	mat := &fakeMaterializer{nextServerIno: 900}
	_, _, _, _, _, handled, st = mgr.Link(context.Background(), exclIno, 1, "excl.bin", true, mat, &fuse.EntryOut{})
	require.True(t, handled)
	require.Equal(t, fuse.ENOENT, st)

	_, _, _, _, _, handled, st = mgr.Link(context.Background(), ino, 1, "closed.bin", false, mat, &fuse.EntryOut{})
	require.True(t, handled)
	require.Equal(t, fuse.ENOENT, st)

	// 8. CommitTmpAttr 失败与 UnlinkOpenTmpInode 失败时均触发回滚且保持本地未上云状态
	commitFailMat := &fakeMaterializer{
		nextServerIno: 910,
		commitStatus:  fuse.EIO,
	}
	_, _, _, _, _, handled, st = mgr.Link(context.Background(), ino, 1, "commit-fail.bin", true, commitFailMat, &fuse.EntryOut{})
	require.True(t, handled)
	require.Equal(t, fuse.EIO, st)
	require.Equal(t, []string{"commit-fail.bin"}, commitFailMat.rollbacks)

	unlinkFailMat := &fakeMaterializer{
		nextServerIno: 920,
		unlinkStatus:  fuse.EIO,
	}
	_, tempName, _, uploaded, st := mgr.Sync(context.Background(), ino, unlinkFailMat)
	require.False(t, uploaded)
	require.Equal(t, fuse.EIO, st)
	require.Equal(t, []string{tempName}, unlinkFailMat.rollbacks)

	// 9. 孤儿释放（orphan=true）时即使 lookups > 0 也应立即在句柄归零时回收
	orphanIno, _ := mgr.Create(1, 100, 200, 0o600, syscall.O_RDWR)
	_, _, _, uploaded, st = mgr.Sync(context.Background(), orphanIno, mat)
	require.True(t, uploaded)
	require.Equal(t, fuse.OK, st)
	require.True(t, mgr.OnHandleReleased(orphanIno, true, func(uint64) bool { return false }))
	_, st = mgr.ToServerIno(orphanIno)
	require.Equal(t, fuse.ENOENT, st)
}
