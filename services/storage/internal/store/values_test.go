package store_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestValuesCodecRoundTripAndValidation(t *testing.T) {
	// 1. BranchStat (41B: [ver(1B) | LogicalBytes(8B) | Inodes(8B) | NextWriteSeq(8B) | WriterEpoch(8B) | MaxBytes(8B)])
	stat := store.BranchStat{
		LogicalBytes: 1048576,
		Inodes:       42,
		NextWriteSeq: 999,
		WriterEpoch:  7,
		MaxBytes:     100 << 30,
	}
	statBytes := store.EncodeBranchStat(stat)
	require.Len(t, statBytes, store.BranchStatSize)
	require.Len(t, statBytes, 41)
	gotStat, err := store.DecodeBranchStat(statBytes)
	require.NoError(t, err)
	require.Equal(t, stat, gotStat)

	_, err = store.DecodeBranchStat(statBytes[:40])
	require.ErrorIs(t, err, store.ErrInvalidValue)
	badStat := append([]byte(nil), statBytes...)
	badStat[0] = 2
	_, err = store.DecodeBranchStat(badStat)
	require.ErrorIs(t, err, store.ErrInvalidValue)

	// 2. Entry (13B)
	entry := store.Entry{
		Ino:  12345,
		Mode: store.S_IFREG | 0o644,
	}
	entryBytes := store.EncodeEntry(entry)
	require.Len(t, entryBytes, store.EntryValueSize)
	require.Len(t, entryBytes, 13)
	gotEntry, err := store.DecodeEntry(entryBytes)
	require.NoError(t, err)
	require.Equal(t, entry, gotEntry)

	_, err = store.DecodeEntry(entryBytes[:12])
	require.ErrorIs(t, err, store.ErrInvalidValue)
	badEntry := append([]byte(nil), entryBytes...)
	badEntry[0] = 2
	_, err = store.DecodeEntry(badEntry)
	require.ErrorIs(t, err, store.ErrInvalidValue)

	// 3. Inode (93B 定长头 + 可变长 SymlinkTarget)
	inode := store.Inode{
		Ino:           100,
		ParentIno:     1,
		Mode:          store.S_IFLNK | 0o777,
		Nlink:         1,
		UID:           1000,
		GID:           2000,
		Rdev:          7,
		Size:          11,
		Atime:         1700000000123456789,
		Mtime:         1700000001234567890,
		Ctime:         1700000002345678901,
		Flags:         store.FlagImmutable,
		Generation:    3,
		WriteSeq:      88,
		SymlinkTarget: []byte("/target/bin"),
	}
	inodeBytes, err := store.EncodeInode(inode)
	require.NoError(t, err)
	require.Equal(t, 93, store.InodeHeaderSize)
	require.Len(t, inodeBytes, store.InodeHeaderSize+len(inode.SymlinkTarget))

	gotInode, err := store.DecodeInode(inodeBytes)
	require.NoError(t, err)
	require.Equal(t, inode, gotInode)

	// 修改原编码缓冲区不影响已解码的 SymlinkTarget
	inodeBytes[len(inodeBytes)-1] = 'X'
	require.Equal(t, []byte("/target/bin"), gotInode.SymlinkTarget)

	// 截断或非法头返回 ErrInvalidValue
	_, err = store.DecodeInode(inodeBytes[:92])
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = store.DecodeInode(inodeBytes[:len(inodeBytes)-1])
	require.ErrorIs(t, err, store.ErrInvalidValue)

	// ToAttribute 纯计算换算（目录默认 Size=4096、512B 块数向上取整、设备号按节点类型过滤）
	dirInode := store.Inode{Ino: 1, ParentIno: 1, Mode: store.S_IFDIR | 0o755, Nlink: 2}
	dirAttr := dirInode.ToAttribute()
	require.Equal(t, store.DefaultDirSize, dirAttr.Size)
	require.Equal(t, store.DefaultBlockSize, dirAttr.Blksize)
	require.Equal(t, store.DefaultDirSize/512, dirAttr.Blocks)

	chrInode := store.Inode{Ino: 2, Mode: store.S_IFCHR | 0o600, Rdev: 0x0103}
	require.Equal(t, uint32(0x0103), chrInode.ToAttribute().Rdev)
	fifoInode := store.Inode{Ino: 3, Mode: store.S_IFIFO | 0o600, Rdev: 0x9999}
	require.Zero(t, fifoInode.ToAttribute().Rdev)

	// 4. BlobRef (16B: [PackID(8B) | Offset(4B) | Length(4B)])
	ref := store.BlobRef{
		PackID: 4096,
		Offset: 1048576,
		Length: store.BlobSize,
	}
	refBytes := store.EncodeBlobRef(ref)
	require.Len(t, refBytes, store.BlobRefSize)
	require.Len(t, refBytes, 16)
	gotRef, err := store.DecodeBlobRef(refBytes)
	require.NoError(t, err)
	require.Equal(t, ref, gotRef)

	// 刚好贴齐 PackMaxSize 上界 (Offset + Length == PackMaxSize) 合法
	edgeRef := store.BlobRef{
		PackID: 1,
		Offset: store.PackMaxSize - store.BlobSize,
		Length: store.BlobSize,
	}
	gotEdgeRef, err := store.DecodeBlobRef(store.EncodeBlobRef(edgeRef))
	require.NoError(t, err)
	require.Equal(t, edgeRef, gotEdgeRef)

	// 长度、PackID、Length 与 Offset+Length 越界校验
	_, err = store.DecodeBlobRef(refBytes[:15])
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = store.DecodeBlobRef(store.EncodeBlobRef(store.BlobRef{PackID: 0, Offset: 0, Length: 10}))
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = store.DecodeBlobRef(store.EncodeBlobRef(store.BlobRef{PackID: 1, Offset: 0, Length: 0}))
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = store.DecodeBlobRef(store.EncodeBlobRef(store.BlobRef{PackID: 1, Offset: 0, Length: store.BlobSize + 1}))
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = store.DecodeBlobRef(store.EncodeBlobRef(store.BlobRef{PackID: 1, Offset: store.PackMaxSize - store.BlobSize + 1, Length: store.BlobSize}))
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = store.DecodeBlobRef(store.EncodeBlobRef(store.BlobRef{PackID: 1, Offset: store.PackMaxSize, Length: 1}))
	require.ErrorIs(t, err, store.ErrInvalidValue)

	// 5. PackMeta (17B: [ver(1B) | LiveBytes(8B) | TotalBytes(8B)])
	packMeta := store.PackMeta{
		LiveBytes:  16 << 20,
		TotalBytes: 64 << 20,
	}
	packMetaBytes := store.EncodePackMeta(packMeta)
	require.Len(t, packMetaBytes, store.PackMetaSize)
	require.Len(t, packMetaBytes, 17)
	gotPackMeta, err := store.DecodePackMeta(packMetaBytes)
	require.NoError(t, err)
	require.Equal(t, packMeta, gotPackMeta)

	_, err = store.DecodePackMeta(packMetaBytes[:16])
	require.ErrorIs(t, err, store.ErrInvalidValue)
	badPackMetaVer := append([]byte(nil), packMetaBytes...)
	badPackMetaVer[0] = 2
	_, err = store.DecodePackMeta(badPackMetaVer)
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = store.DecodePackMeta(store.EncodePackMeta(store.PackMeta{LiveBytes: 8192, TotalBytes: 4096}))
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = store.DecodePackMeta(store.EncodePackMeta(store.PackMeta{LiveBytes: 0, TotalBytes: store.PackMaxSize + 1}))
	require.ErrorIs(t, err, store.ErrInvalidValue)
}
