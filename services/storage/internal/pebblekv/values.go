package pebblekv

import (
	"encoding/binary"
	"errors"
	"math"
)

// ErrInvalidValue 表示值的编码不完整或格式不合法。
var ErrInvalidValue = errors.New("pebblekv: 无效值")

// BranchStat 是一个分支的逻辑用量与下一写序列号。
type BranchStat struct{ LogicalBytes, Inodes, NextWriteSeq uint64 }

// EncodeBranchStat 编码分支统计。
func EncodeBranchStat(s BranchStat) []byte {
	b := []byte{1}
	b = binary.BigEndian.AppendUint64(b, s.LogicalBytes)
	b = binary.BigEndian.AppendUint64(b, s.Inodes)
	return binary.BigEndian.AppendUint64(b, s.NextWriteSeq)
}

// DecodeBranchStat 解码分支统计。
func DecodeBranchStat(b []byte) (BranchStat, error) {
	if len(b) != 25 || b[0] != 1 {
		return BranchStat{}, ErrInvalidValue
	}
	return BranchStat{binary.BigEndian.Uint64(b[1:9]), binary.BigEndian.Uint64(b[9:17]), binary.BigEndian.Uint64(b[17:25])}, nil
}

// Entry 是目录项的目标节点和模式。
type Entry struct {
	Ino  uint64
	Mode uint32
}

// EncodeEntry 编码目录项值。
func EncodeEntry(e Entry) []byte {
	b := binary.BigEndian.AppendUint64([]byte{1}, e.Ino)
	return binary.BigEndian.AppendUint32(b, e.Mode)
}

// DecodeEntry 解码目录项值。
func DecodeEntry(b []byte) (Entry, error) {
	if len(b) != 13 || b[0] != 1 {
		return Entry{}, ErrInvalidValue
	}
	return Entry{binary.BigEndian.Uint64(b[1:9]), binary.BigEndian.Uint32(b[9:13])}, nil
}

// Inode 保存节点全部持久化属性。
type Inode struct {
	Ino, ParentIno              uint64
	Mode, Nlink, UID, GID, Rdev uint32
	Size                        uint64
	Blksize                     uint32
	Blocks                      uint64
	AtimeSec                    int64
	AtimeNsec                   uint32
	MtimeSec                    int64
	MtimeNsec                   uint32
	CtimeSec                    int64
	CtimeNsec                   uint32
	Flags                       uint32
	Generation, WriteSeq        uint64
	SymlinkTarget               []byte
}

type inodeFields struct {
	Ino, ParentIno              uint64
	Mode, Nlink, UID, GID, Rdev uint32
	Size                        uint64
	Blksize                     uint32
	Blocks                      uint64
	AtimeSec                    int64
	AtimeNsec                   uint32
	MtimeSec                    int64
	MtimeNsec                   uint32
	CtimeSec                    int64
	CtimeNsec                   uint32
	Flags                       uint32
	Generation, WriteSeq        uint64
	TargetLen                   uint32
}

// EncodeInode 编码节点属性和符号链接目标。
func EncodeInode(i Inode) ([]byte, error) {
	if uint64(len(i.SymlinkTarget)) > math.MaxUint32 {
		return nil, ErrInvalidValue
	}
	fields := inodeFields{i.Ino, i.ParentIno, i.Mode, i.Nlink, i.UID, i.GID, i.Rdev, i.Size, i.Blksize, i.Blocks, i.AtimeSec, i.AtimeNsec, i.MtimeSec, i.MtimeNsec, i.CtimeSec, i.CtimeNsec, i.Flags, i.Generation, i.WriteSeq, uint32(len(i.SymlinkTarget))}
	b, err := binary.Append([]byte{1}, binary.BigEndian, fields)
	if err != nil {
		return nil, err
	}
	return append(b, i.SymlinkTarget...), nil
}

// DecodeInode 解码节点属性，返回的符号链接目标独立于输入缓冲区。
func DecodeInode(b []byte) (Inode, error) {
	var f inodeFields
	size := binary.Size(f)
	if len(b) < 1+size || b[0] != 1 {
		return Inode{}, ErrInvalidValue
	}
	if _, err := binary.Decode(b[1:1+size], binary.BigEndian, &f); err != nil {
		return Inode{}, ErrInvalidValue
	}
	if uint64(len(b)-1-size) != uint64(f.TargetLen) {
		return Inode{}, ErrInvalidValue
	}
	return Inode{Ino: f.Ino, ParentIno: f.ParentIno, Mode: f.Mode, Nlink: f.Nlink, UID: f.UID, GID: f.GID, Rdev: f.Rdev, Size: f.Size, Blksize: f.Blksize, Blocks: f.Blocks, AtimeSec: f.AtimeSec, AtimeNsec: f.AtimeNsec, MtimeSec: f.MtimeSec, MtimeNsec: f.MtimeNsec, CtimeSec: f.CtimeSec, CtimeNsec: f.CtimeNsec, Flags: f.Flags, Generation: f.Generation, WriteSeq: f.WriteSeq, SymlinkTarget: append([]byte(nil), b[1+size:]...)}, nil
}
