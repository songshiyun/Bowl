// Package pebblekv 提供分支和快照的 Pebble 存储基础设施。
package pebblekv

import (
	"encoding/binary"
	"errors"
)

var ErrInvalidKey = errors.New("pebblekv: 无效键")

func numberedPrefix(prefix string, ino uint64) []byte {
	key := make([]byte, len(prefix)+9)
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], ino)
	key[len(key)-1] = ':'
	return key
}

// PrefixIterBounds 返回指定前缀的左闭右开扫描范围。
func PrefixIterBounds(prefix []byte) (lower, upper []byte) {
	lower = append([]byte(nil), prefix...)
	upper = append([]byte(nil), prefix...)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 0xff {
			upper[i]++
			return lower, upper[:i+1]
		}
	}
	return lower, nil
}

// DataKey 编码 Inode 和 4 MiB Blob 的编号。
func DataKey(ino, block uint64) []byte {
	key := numberedPrefix("data:", ino)
	return binary.BigEndian.AppendUint64(key, block)
}

// DecodeDataKey 解码 Blob 键。
func DecodeDataKey(key []byte) (uint64, uint64, error) {
	if len(key) != len("data:")+8+1+8 || string(key[:5]) != "data:" || key[13] != ':' {
		return 0, 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(key[5:13]), binary.BigEndian.Uint64(key[14:]), nil
}

// DataRange 返回从指定 Blob 开始到该 Inode 末尾的左闭右开范围。
func DataRange(ino, firstBlock uint64) (lower, upper []byte) {
	_, upper = PrefixIterBounds(numberedPrefix("data:", ino))
	return DataKey(ino, firstBlock), upper
}

// StatKey 返回分支统计值的键。
func StatKey() []byte { return []byte("sys:stat") }

func numberKey(prefix string, ino uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte(prefix), ino)
}

func decodeNumberKey(key []byte, prefix string) (uint64, error) {
	if len(key) != len(prefix)+8 || string(key[:len(prefix)]) != prefix {
		return 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(key[len(prefix):]), nil
}

func namedKey(prefix string, ino uint64, name []byte) []byte {
	return append(numberedPrefix(prefix, ino), name...)
}

func decodeNamedKey(key []byte, prefix string) (uint64, []byte, error) {
	n := len(prefix)
	if len(key) < n+9 || string(key[:n]) != prefix || key[n+8] != ':' {
		return 0, nil, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(key[n : n+8]), append([]byte(nil), key[n+9:]...), nil
}

// InodeKey 编码 Inode 键。
func InodeKey(ino uint64) []byte { return numberKey("inode:", ino) }

// DecodeInodeKey 解码 Inode 键。
func DecodeInodeKey(key []byte) (uint64, error) { return decodeNumberKey(key, "inode:") }

// InodeRange 返回所有 Inode 键的范围。
func InodeRange() ([]byte, []byte) { return PrefixIterBounds([]byte("inode:")) }

// EntryKey 编码父 Inode 和原始名称。
func EntryKey(parent uint64, name []byte) []byte { return namedKey("entry:", parent, name) }

// DecodeEntryKey 解码目录项键。
func DecodeEntryKey(key []byte) (uint64, []byte, error) { return decodeNamedKey(key, "entry:") }

// EntryRange 返回一个父 Inode 下的目录项范围。
func EntryRange(parent uint64) ([]byte, []byte) {
	return PrefixIterBounds(numberedPrefix("entry:", parent))
}

// XattrKey 编码 Inode 和扩展属性名称。
func XattrKey(ino uint64, name []byte) []byte { return namedKey("xattr:", ino, name) }

// DecodeXattrKey 解码扩展属性键。
func DecodeXattrKey(key []byte) (uint64, []byte, error) { return decodeNamedKey(key, "xattr:") }

// XattrRange 返回一个 Inode 的扩展属性范围。
func XattrRange(ino uint64) ([]byte, []byte) { return PrefixIterBounds(numberedPrefix("xattr:", ino)) }

// DirtyKey 编码变更节点键。
func DirtyKey(ino uint64) []byte { return numberKey("dirty:", ino) }

// DecodeDirtyKey 解码变更节点键。
func DecodeDirtyKey(key []byte) (uint64, error) { return decodeNumberKey(key, "dirty:") }

// DirtyRange 返回变更节点键的范围。
func DirtyRange() ([]byte, []byte) { return PrefixIterBounds([]byte("dirty:")) }

// OrphanKey 编码孤儿节点键。
func OrphanKey(ino uint64) []byte { return numberKey("orphan:", ino) }

// DecodeOrphanKey 解码孤儿节点键。
func DecodeOrphanKey(key []byte) (uint64, error) { return decodeNumberKey(key, "orphan:") }

// OrphanRange 返回孤儿节点键的范围。
func OrphanRange() ([]byte, []byte) { return PrefixIterBounds([]byte("orphan:")) }
