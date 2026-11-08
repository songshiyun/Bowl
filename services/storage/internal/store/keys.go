package store

import (
	"bytes"
	"encoding/binary"
)

// 键物理编码长度常量（ADR-0030 §2.1 & ADR-0031 §2.2）。
const (
	StatKeyLen        = 8  // "sys:stat"
	PackStatPrefixLen = 9  // "sys:pack:"
	PackStatKeyLen    = 17 // "sys:pack:" + 8B
	InodeKeyLen       = 14 // "inode:" + 8B
	EntryPrefixLen    = 15 // "entry:" + 8B + ":"
	XattrPrefixLen    = 15 // "xattr:" + 8B + ":"
	DirtyKeyLen       = 14 // "dirty:" + 8B
	OrphanKeyLen      = 15 // "orphan:" + 8B
	DataPrefixLen     = 14 // "data:" + 8B + ":"
	DataKeyLen        = 22 // "data:" + 8B + ":" + 8B
)

var (
	statKeyBytes        = []byte("sys:stat")
	packStatPrefixBytes = []byte("sys:pack:")
	inodePrefixBytes    = []byte("inode:")
	entryPrefixBytes    = []byte("entry:")
	xattrPrefixBytes    = []byte("xattr:")
	dirtyPrefixBytes    = []byte("dirty:")
	orphanPrefixBytes   = []byte("orphan:")
	dataPrefixBytes     = []byte("data:")
)

// SplitKey 实现 Pebble Comparer.Split 切分函数：
//   - 对 "entry:<8B>:" 与 "xattr:<8B>:" 返回 15；
//   - 对 "data:<8B>:" 返回 14；
//   - 对其余单键（"inode:"、"dirty:"、"orphan:"、"sys:stat"、"sys:pack:<8B>"）返回 len(key)。
func SplitKey(key []byte) int {
	if len(key) >= EntryPrefixLen && key[14] == ':' {
		if bytes.HasPrefix(key, entryPrefixBytes) || bytes.HasPrefix(key, xattrPrefixBytes) {
			return EntryPrefixLen
		}
	}
	if len(key) >= DataPrefixLen && key[13] == ':' && bytes.HasPrefix(key, dataPrefixBytes) {
		return DataPrefixLen
	}
	return len(key)
}

// StatKey 返回分支统计键 "sys:stat"（8B）。
func StatKey() []byte {
	out := make([]byte, StatKeyLen)
	putStatKey(out)
	return out
}

func putStatKey(dst []byte) {
	copy(dst, statKeyBytes)
}

// PackStatPrefix 返回 .pack 存活字节统计键公共前缀 "sys:pack:"（9B）。
func PackStatPrefix() []byte {
	return append([]byte(nil), packStatPrefixBytes...)
}

// PackStatKey 编码 .pack 存活字节统计键 "sys:pack:<8B_packID>"（17B）。
func PackStatKey(packID uint64) []byte {
	out := make([]byte, PackStatKeyLen)
	putPackStatKey(out, packID)
	return out
}

func putPackStatKey(dst []byte, packID uint64) {
	copy(dst, packStatPrefixBytes)
	binary.BigEndian.PutUint64(dst[9:17], packID)
}

// DecodePackStatKey 解码 .pack 存活字节统计键，校验 packID > 0。
func DecodePackStatKey(key []byte) (uint64, error) {
	if len(key) != PackStatKeyLen || !bytes.HasPrefix(key, packStatPrefixBytes) {
		return 0, ErrInvalidKey
	}
	packID := binary.BigEndian.Uint64(key[9:17])
	if packID == 0 {
		return 0, ErrInvalidKey
	}
	return packID, nil
}

// InodeKey 编码节点属性键 "inode:<8B_ino>"（14B）。
func InodeKey(ino uint64) []byte {
	out := make([]byte, InodeKeyLen)
	putInodeKey(out, ino)
	return out
}

func putInodeKey(dst []byte, ino uint64) {
	copy(dst, inodePrefixBytes)
	binary.BigEndian.PutUint64(dst[6:14], ino)
}

// DecodeInodeKey 解码节点属性键。
func DecodeInodeKey(key []byte) (uint64, error) {
	if len(key) != InodeKeyLen || !bytes.HasPrefix(key, inodePrefixBytes) {
		return 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(key[6:14]), nil
}

// EntryPrefix 返回指定父目录下所有目录项的 15B 扫描前缀 "entry:<8B_parent>:"。
func EntryPrefix(parent uint64) []byte {
	out := make([]byte, EntryPrefixLen)
	copy(out, entryPrefixBytes)
	binary.BigEndian.PutUint64(out[6:14], parent)
	out[14] = ':'
	return out
}

// EntryKey 编码目录项键 "entry:<8B_parent>:<name>"（15B + len(name)）。
func EntryKey(parent uint64, name []byte) []byte {
	out := make([]byte, EntryPrefixLen+len(name))
	putEntryKey(out, parent, name)
	return out
}

func putEntryKey(dst []byte, parent uint64, name []byte) {
	copy(dst, entryPrefixBytes)
	binary.BigEndian.PutUint64(dst[6:14], parent)
	dst[14] = ':'
	copy(dst[15:], name)
}

// DecodeEntryKey 解码目录项键，返回父节点编号与独立拷贝的名称切片。
func DecodeEntryKey(key []byte) (parent uint64, name []byte, err error) {
	if len(key) < EntryPrefixLen || !bytes.HasPrefix(key, entryPrefixBytes) || key[14] != ':' {
		return 0, nil, ErrInvalidKey
	}
	name = append([]byte(nil), key[15:]...)
	return binary.BigEndian.Uint64(key[6:14]), name, nil
}

// XattrPrefix 返回指定节点下所有扩展属性的 15B 扫描前缀 "xattr:<8B_ino>:"。
func XattrPrefix(ino uint64) []byte {
	out := make([]byte, XattrPrefixLen)
	copy(out, xattrPrefixBytes)
	binary.BigEndian.PutUint64(out[6:14], ino)
	out[14] = ':'
	return out
}

// XattrKey 编码扩展属性键 "xattr:<8B_ino>:<name>"（15B + len(name)）。
func XattrKey(ino uint64, name []byte) []byte {
	out := make([]byte, XattrPrefixLen+len(name))
	putXattrKey(out, ino, name)
	return out
}

func putXattrKey(dst []byte, ino uint64, name []byte) {
	copy(dst, xattrPrefixBytes)
	binary.BigEndian.PutUint64(dst[6:14], ino)
	dst[14] = ':'
	copy(dst[15:], name)
}

// DecodeXattrKey 解码扩展属性键，返回节点编号与独立拷贝的属性名切片。
func DecodeXattrKey(key []byte) (ino uint64, name []byte, err error) {
	if len(key) < XattrPrefixLen || !bytes.HasPrefix(key, xattrPrefixBytes) || key[14] != ':' {
		return 0, nil, ErrInvalidKey
	}
	name = append([]byte(nil), key[15:]...)
	return binary.BigEndian.Uint64(key[6:14]), name, nil
}

// DirtyKey 编码变更节点键 "dirty:<8B_ino>"（14B）。
func DirtyKey(ino uint64) []byte {
	out := make([]byte, DirtyKeyLen)
	putDirtyKey(out, ino)
	return out
}

func putDirtyKey(dst []byte, ino uint64) {
	copy(dst, dirtyPrefixBytes)
	binary.BigEndian.PutUint64(dst[6:14], ino)
}

// DecodeDirtyKey 解码变更节点键。
func DecodeDirtyKey(key []byte) (uint64, error) {
	if len(key) != DirtyKeyLen || !bytes.HasPrefix(key, dirtyPrefixBytes) {
		return 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(key[6:14]), nil
}

// OrphanPrefix 返回孤儿节点键公共前缀 "orphan:"。
func OrphanPrefix() []byte {
	return append([]byte(nil), orphanPrefixBytes...)
}

// OrphanKey 编码孤儿节点键 "orphan:<8B_ino>"（15B）。
func OrphanKey(ino uint64) []byte {
	out := make([]byte, OrphanKeyLen)
	putOrphanKey(out, ino)
	return out
}

func putOrphanKey(dst []byte, ino uint64) {
	copy(dst, orphanPrefixBytes)
	binary.BigEndian.PutUint64(dst[7:15], ino)
}

// DecodeOrphanKey 解码孤儿节点键。
func DecodeOrphanKey(key []byte) (uint64, error) {
	if len(key) != OrphanKeyLen || !bytes.HasPrefix(key, orphanPrefixBytes) {
		return 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(key[7:15]), nil
}

// DataPrefix 返回指定节点下所有数据块索引的 14B 扫描前缀 "data:<8B_ino>:"。
func DataPrefix(ino uint64) []byte {
	out := make([]byte, DataPrefixLen)
	copy(out, dataPrefixBytes)
	binary.BigEndian.PutUint64(out[5:13], ino)
	out[13] = ':'
	return out
}

// DataKey 编码数据块索引键 "data:<8B_ino>:<8B_blockIdx>"（22B）。
func DataKey(ino, blockIdx uint64) []byte {
	out := make([]byte, DataKeyLen)
	putDataKey(out, ino, blockIdx)
	return out
}

func putDataKey(dst []byte, ino, blockIdx uint64) {
	copy(dst, dataPrefixBytes)
	binary.BigEndian.PutUint64(dst[5:13], ino)
	dst[13] = ':'
	binary.BigEndian.PutUint64(dst[14:22], blockIdx)
}

// DecodeDataKey 解码数据块索引键，返回 (ino, blockIdx)。
func DecodeDataKey(key []byte) (ino, blockIdx uint64, err error) {
	if len(key) != DataKeyLen || !bytes.HasPrefix(key, dataPrefixBytes) || key[13] != ':' {
		return 0, 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(key[5:13]), binary.BigEndian.Uint64(key[14:22]), nil
}
