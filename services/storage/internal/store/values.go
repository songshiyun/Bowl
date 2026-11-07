package store

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// 定长值物理编码字节数常量（ADR-0030 §2.1）。
const (
	BranchStatSize  = 25 // [ver(1B) | LogicalBytes(8B) | Inodes(8B) | NextWriteSeq(8B)]
	EntryValueSize  = 13 // [ver(1B) | Ino(8B) | Mode(4B)]
	InodeHeaderSize = 93 // 93B 定长属性头 + 可变长 SymlinkTarget
	BlobRefSize     = 12 // [BlobID(8B) | Length(4B)]
)

const codecVersion byte = 1

// BranchStat 表示分支实时逻辑用量、节点计数与写序列号。
type BranchStat struct {
	LogicalBytes uint64
	Inodes       uint64
	NextWriteSeq uint64
}

// EncodeBranchStat 编码 25 字节定长 BranchStat。
func EncodeBranchStat(s BranchStat) []byte {
	out := make([]byte, BranchStatSize)
	putBranchStat(out, s)
	return out
}

func putBranchStat(dst []byte, s BranchStat) {
	dst[0] = codecVersion
	binary.BigEndian.PutUint64(dst[1:9], s.LogicalBytes)
	binary.BigEndian.PutUint64(dst[9:17], s.Inodes)
	binary.BigEndian.PutUint64(dst[17:25], s.NextWriteSeq)
}

// DecodeBranchStat 解码 25 字节 BranchStat。
func DecodeBranchStat(b []byte) (BranchStat, error) {
	if len(b) != BranchStatSize || b[0] != codecVersion {
		return BranchStat{}, ErrInvalidValue
	}
	return BranchStat{
		LogicalBytes: binary.BigEndian.Uint64(b[1:9]),
		Inodes:       binary.BigEndian.Uint64(b[9:17]),
		NextWriteSeq: binary.BigEndian.Uint64(b[17:25]),
	}, nil
}

// Entry 表示目录项绑定目标节点编号与文件模式。
type Entry struct {
	Ino  uint64
	Mode uint32
}

// EncodeEntry 编码 13 字节定长 Entry。
func EncodeEntry(e Entry) []byte {
	out := make([]byte, EntryValueSize)
	putEntry(out, e)
	return out
}

func putEntry(dst []byte, e Entry) {
	dst[0] = codecVersion
	binary.BigEndian.PutUint64(dst[1:9], e.Ino)
	binary.BigEndian.PutUint32(dst[9:13], e.Mode)
}

// DecodeEntry 解码 13 字节 Entry。
func DecodeEntry(b []byte) (Entry, error) {
	if len(b) != EntryValueSize || b[0] != codecVersion {
		return Entry{}, ErrInvalidValue
	}
	return Entry{
		Ino:  binary.BigEndian.Uint64(b[1:9]),
		Mode: binary.BigEndian.Uint32(b[9:13]),
	}, nil
}

// Inode 表示持久化在 Pebble 中的索引节点实体（93B 定长头 + 可变长 SymlinkTarget）。
type Inode struct {
	Ino           uint64
	ParentIno     uint64
	Mode          uint32
	Nlink         uint32
	UID           uint32
	GID           uint32
	Rdev          uint32
	Size          uint64
	Atime         int64 // UnixNano
	Mtime         int64 // UnixNano
	Ctime         int64 // UnixNano
	Flags         uint32
	Generation    uint64
	WriteSeq      uint64
	SymlinkTarget []byte
}

// Attribute 表示对外返回的 POSIX 文件属性视图。
type Attribute struct {
	Ino        uint64
	ParentIno  uint64
	Generation uint64
	WriteSeq   uint64
	Mode       uint32
	Nlink      uint32
	UID        uint32
	GID        uint32
	Rdev       uint32
	Size       uint64
	Blksize    uint32
	Blocks     uint64
	Atime      int64 // UnixNano
	Mtime      int64 // UnixNano
	Ctime      int64 // UnixNano
	Flags      uint32
}

// AtimeTime 返回 UTC 访问时间。
func (a Attribute) AtimeTime() time.Time { return time.Unix(0, a.Atime).UTC() }

// MtimeTime 返回 UTC 修改时间。
func (a Attribute) MtimeTime() time.Time { return time.Unix(0, a.Mtime).UTC() }

// CtimeTime 返回 UTC 状态变更时间。
func (a Attribute) CtimeTime() time.Time { return time.Unix(0, a.Ctime).UTC() }

// ToAttribute 将持久化 Inode 转换为 POSIX Attribute。
func (i Inode) ToAttribute() Attribute {
	size := i.Size
	if i.Mode&S_IFMT == S_IFDIR && size == 0 {
		size = DefaultDirSize
	}
	var blocks uint64
	switch i.Mode & S_IFMT {
	case S_IFDIR, S_IFREG, S_IFLNK:
		blocks = (size + 511) / 512
	}
	var rdev uint32
	if i.Mode&S_IFMT == S_IFCHR || i.Mode&S_IFMT == S_IFBLK {
		rdev = i.Rdev
	} else {
		rdev = i.Rdev
	}
	return Attribute{
		Ino:        i.Ino,
		ParentIno:  i.ParentIno,
		Generation: i.Generation,
		WriteSeq:   i.WriteSeq,
		Mode:       i.Mode,
		Nlink:      i.Nlink,
		UID:        i.UID,
		GID:        i.GID,
		Rdev:       rdev,
		Size:       size,
		Blksize:    DefaultBlockSize,
		Blocks:     blocks,
		Atime:      i.Atime,
		Mtime:      i.Mtime,
		Ctime:      i.Ctime,
		Flags:      i.Flags,
	}
}

// EncodeInode 编码 93 字节定长属性头与可变长 SymlinkTarget。
func EncodeInode(i Inode) ([]byte, error) {
	if uint64(len(i.SymlinkTarget)) > math.MaxUint32 {
		return nil, ErrInvalidValue
	}
	out := make([]byte, InodeHeaderSize+len(i.SymlinkTarget))
	putInode(out, i)
	return out, nil
}

func putInode(dst []byte, i Inode) {
	dst[0] = codecVersion
	binary.BigEndian.PutUint64(dst[1:9], i.Ino)
	binary.BigEndian.PutUint64(dst[9:17], i.ParentIno)
	binary.BigEndian.PutUint32(dst[17:21], i.Mode)
	binary.BigEndian.PutUint32(dst[21:25], i.Nlink)
	binary.BigEndian.PutUint32(dst[25:29], i.UID)
	binary.BigEndian.PutUint32(dst[29:33], i.GID)
	binary.BigEndian.PutUint32(dst[33:37], i.Rdev)
	binary.BigEndian.PutUint64(dst[37:45], i.Size)
	binary.BigEndian.PutUint64(dst[45:53], uint64(i.Atime))
	binary.BigEndian.PutUint64(dst[53:61], uint64(i.Mtime))
	binary.BigEndian.PutUint64(dst[61:69], uint64(i.Ctime))
	binary.BigEndian.PutUint32(dst[69:73], i.Flags)
	binary.BigEndian.PutUint64(dst[73:81], i.Generation)
	binary.BigEndian.PutUint64(dst[81:89], i.WriteSeq)
	binary.BigEndian.PutUint32(dst[89:93], uint32(len(i.SymlinkTarget)))
	copy(dst[93:], i.SymlinkTarget)
}

// DecodeInode 解码节点属性，返回的 SymlinkTarget 与输入缓冲区完全解耦。
func DecodeInode(b []byte) (Inode, error) {
	if len(b) < InodeHeaderSize || b[0] != codecVersion {
		return Inode{}, ErrInvalidValue
	}
	targetLen := binary.BigEndian.Uint32(b[89:93])
	if uint64(len(b)-InodeHeaderSize) != uint64(targetLen) {
		return Inode{}, ErrInvalidValue
	}
	var target []byte
	if targetLen > 0 {
		target = append([]byte(nil), b[93:]...)
	}
	return Inode{
		Ino:           binary.BigEndian.Uint64(b[1:9]),
		ParentIno:     binary.BigEndian.Uint64(b[9:17]),
		Mode:          binary.BigEndian.Uint32(b[17:21]),
		Nlink:         binary.BigEndian.Uint32(b[21:25]),
		UID:           binary.BigEndian.Uint32(b[25:29]),
		GID:           binary.BigEndian.Uint32(b[29:33]),
		Rdev:          binary.BigEndian.Uint32(b[33:37]),
		Size:          binary.BigEndian.Uint64(b[37:45]),
		Atime:         int64(binary.BigEndian.Uint64(b[45:53])),
		Mtime:         int64(binary.BigEndian.Uint64(b[53:61])),
		Ctime:         int64(binary.BigEndian.Uint64(b[61:69])),
		Flags:         binary.BigEndian.Uint32(b[69:73]),
		Generation:    binary.BigEndian.Uint64(b[73:81]),
		WriteSeq:      binary.BigEndian.Uint64(b[81:89]),
		SymlinkTarget: target,
	}, nil
}

// BlobRef 表示 DataKey 指向的 12 字节定长数据块引用。
type BlobRef struct {
	ID     uint64
	Length uint32
}

// EncodeBlobRef 编码 12 字节定长 BlobRef。
func EncodeBlobRef(ref BlobRef) []byte {
	out := make([]byte, BlobRefSize)
	putBlobRef(out, ref)
	return out
}

func putBlobRef(dst []byte, ref BlobRef) {
	binary.BigEndian.PutUint64(dst[0:8], ref.ID)
	binary.BigEndian.PutUint32(dst[8:12], ref.Length)
}

// DecodeBlobRef 解码并校验 12 字节 BlobRef。
func DecodeBlobRef(b []byte) (BlobRef, error) {
	if len(b) != BlobRefSize {
		return BlobRef{}, ErrInvalidValue
	}
	ref := BlobRef{
		ID:     binary.BigEndian.Uint64(b[0:8]),
		Length: binary.BigEndian.Uint32(b[8:12]),
	}
	if ref.ID == 0 || ref.Length == 0 || ref.Length > BlobSize {
		return BlobRef{}, ErrInvalidValue
	}
	return ref, nil
}

// StatFSResult 表示文件系统容量与节点统计结果。
type StatFSResult struct {
	TotalBytes uint64
	UsedBytes  uint64
	AvailBytes uint64
	Bsize      uint32
	Frsize     uint32
	Blocks     uint64
	Bfree      uint64
	Bavail     uint64
	Files      uint64
	Ffree      uint64
	Namelen    uint32
}

// BatchSetStatDeferred 使用 SetDeferred 原地零拷贝写入 "sys:stat"。
func BatchSetStatDeferred(b *pebble.Batch, s BranchStat) {
	batchSetStatDeferred(b, s)
}

func batchSetStatDeferred(b *pebble.Batch, s BranchStat) {
	op := b.SetDeferred(StatKeyLen, BranchStatSize)
	putStatKey(op.Key)
	putBranchStat(op.Value, s)
	op.Finish()
}

// BatchSetInodeDeferred 使用 SetDeferred 原地零拷贝写入 InodeKey。
func BatchSetInodeDeferred(b *pebble.Batch, i Inode) error {
	return batchSetInodeDeferred(b, i)
}

func batchSetInodeDeferred(b *pebble.Batch, i Inode) error {
	if uint64(len(i.SymlinkTarget)) > math.MaxUint32 {
		return ErrInvalidValue
	}
	op := b.SetDeferred(InodeKeyLen, InodeHeaderSize+len(i.SymlinkTarget))
	putInodeKey(op.Key, i.Ino)
	putInode(op.Value, i)
	op.Finish()
	return nil
}

// BatchSetEntryDeferred 使用 SetDeferred 原地零拷贝写入 EntryKey。
func BatchSetEntryDeferred(b *pebble.Batch, parent uint64, name []byte, e Entry) {
	batchSetEntryDeferred(b, parent, name, e)
}

func batchSetEntryDeferred(b *pebble.Batch, parent uint64, name []byte, e Entry) {
	op := b.SetDeferred(EntryPrefixLen+len(name), EntryValueSize)
	putEntryKey(op.Key, parent, name)
	putEntry(op.Value, e)
	op.Finish()
}

// BatchSetXattrDeferred 使用 SetDeferred 原地零拷贝写入 XattrKey。
func BatchSetXattrDeferred(b *pebble.Batch, ino uint64, name, val []byte) {
	batchSetXattrDeferred(b, ino, name, val)
}

func batchSetXattrDeferred(b *pebble.Batch, ino uint64, name, val []byte) {
	op := b.SetDeferred(XattrPrefixLen+len(name), len(val))
	putXattrKey(op.Key, ino, name)
	copy(op.Value, val)
	op.Finish()
}

// BatchSetDirtyDeferred 使用 SetDeferred 原地零拷贝写入 DirtyKey。
func BatchSetDirtyDeferred(b *pebble.Batch, ino uint64) {
	batchSetDirtyDeferred(b, ino)
}

func batchSetDirtyDeferred(b *pebble.Batch, ino uint64) {
	op := b.SetDeferred(DirtyKeyLen, 1)
	putDirtyKey(op.Key, ino)
	op.Value[0] = 1
	op.Finish()
}

// BatchSetOrphanDeferred 使用 SetDeferred 原地零拷贝写入 OrphanKey。
func BatchSetOrphanDeferred(b *pebble.Batch, ino uint64) {
	batchSetOrphanDeferred(b, ino)
}

func batchSetOrphanDeferred(b *pebble.Batch, ino uint64) {
	op := b.SetDeferred(OrphanKeyLen, 1)
	putOrphanKey(op.Key, ino)
	op.Value[0] = 1
	op.Finish()
}

// BatchSetDataDeferred 使用 SetDeferred 原地零拷贝写入 DataKey。
func BatchSetDataDeferred(b *pebble.Batch, ino, blockIdx uint64, ref BlobRef) {
	batchSetDataDeferred(b, ino, blockIdx, ref)
}

func batchSetDataDeferred(b *pebble.Batch, ino, blockIdx uint64, ref BlobRef) {
	op := b.SetDeferred(DataKeyLen, BlobRefSize)
	putDataKey(op.Key, ino, blockIdx)
	putBlobRef(op.Value, ref)
	op.Finish()
}

// BatchDeleteDeferred 使用 DeleteDeferred 原地零拷贝删除任意键（严禁使用 SingleDelete）。
func BatchDeleteDeferred(b *pebble.Batch, key []byte) {
	batchDeleteDeferred(b, key)
}

func batchDeleteDeferred(b *pebble.Batch, key []byte) {
	op := b.DeleteDeferred(len(key))
	copy(op.Key, key)
	op.Finish()
}

// BatchDeleteInodeDeferred 使用 DeleteDeferred 原地零拷贝删除 InodeKey。
func BatchDeleteInodeDeferred(b *pebble.Batch, ino uint64) {
	batchDeleteInodeDeferred(b, ino)
}

func batchDeleteInodeDeferred(b *pebble.Batch, ino uint64) {
	op := b.DeleteDeferred(InodeKeyLen)
	putInodeKey(op.Key, ino)
	op.Finish()
}

// BatchDeleteEntryDeferred 使用 DeleteDeferred 原地零拷贝删除 EntryKey。
func BatchDeleteEntryDeferred(b *pebble.Batch, parent uint64, name []byte) {
	batchDeleteEntryDeferred(b, parent, name)
}

func batchDeleteEntryDeferred(b *pebble.Batch, parent uint64, name []byte) {
	op := b.DeleteDeferred(EntryPrefixLen + len(name))
	putEntryKey(op.Key, parent, name)
	op.Finish()
}

// BatchDeleteXattrDeferred 使用 DeleteDeferred 原地零拷贝删除 XattrKey。
func BatchDeleteXattrDeferred(b *pebble.Batch, ino uint64, name []byte) {
	batchDeleteXattrDeferred(b, ino, name)
}

func batchDeleteXattrDeferred(b *pebble.Batch, ino uint64, name []byte) {
	op := b.DeleteDeferred(XattrPrefixLen + len(name))
	putXattrKey(op.Key, ino, name)
	op.Finish()
}

// BatchDeleteDirtyDeferred 使用 DeleteDeferred 原地零拷贝删除 DirtyKey。
func BatchDeleteDirtyDeferred(b *pebble.Batch, ino uint64) {
	batchDeleteDirtyDeferred(b, ino)
}

func batchDeleteDirtyDeferred(b *pebble.Batch, ino uint64) {
	op := b.DeleteDeferred(DirtyKeyLen)
	putDirtyKey(op.Key, ino)
	op.Finish()
}

// BatchDeleteOrphanDeferred 使用 DeleteDeferred 原地零拷贝删除 OrphanKey。
func BatchDeleteOrphanDeferred(b *pebble.Batch, ino uint64) {
	batchDeleteOrphanDeferred(b, ino)
}

func batchDeleteOrphanDeferred(b *pebble.Batch, ino uint64) {
	op := b.DeleteDeferred(OrphanKeyLen)
	putOrphanKey(op.Key, ino)
	op.Finish()
}

// BatchDeleteDataDeferred 使用 DeleteDeferred 原地零拷贝删除 DataKey。
func BatchDeleteDataDeferred(b *pebble.Batch, ino, blockIdx uint64) {
	batchDeleteDataDeferred(b, ino, blockIdx)
}

func batchDeleteDataDeferred(b *pebble.Batch, ino, blockIdx uint64) {
	op := b.DeleteDeferred(DataKeyLen)
	putDataKey(op.Key, ino, blockIdx)
	op.Finish()
}
