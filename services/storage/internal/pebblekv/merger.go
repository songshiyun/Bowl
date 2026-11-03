package pebblekv

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/cockroachdb/pebble/v2"
)

// BlobSize 是文件数据块的固定上限。
const BlobSize = 4 << 20

// 帧类型同时是持久化格式的一部分。
const (
	BaseFrame      byte = 1
	PatchListFrame byte = 2
)

// PatchKind 表示块内修改类型。
type PatchKind byte

const (
	PatchWrite    PatchKind = 1
	PatchZero     PatchKind = 2
	PatchTruncate PatchKind = 3
)

// Patch 描述按顺序应用的一次块内修改。
type Patch struct {
	Kind   PatchKind
	Offset uint32
	Length uint32
	Data   []byte
}

// WritePatch 创建写入补丁。
func WritePatch(offset uint32, data []byte) Patch {
	return Patch{Kind: PatchWrite, Offset: offset, Length: uint32(len(data)), Data: data}
}

// ZeroPatch 创建置零补丁。
func ZeroPatch(offset, length uint32) Patch {
	return Patch{Kind: PatchZero, Offset: offset, Length: length}
}

// TruncatePatch 创建块内截断或扩展补丁。
func TruncatePatch(length uint32) Patch { return Patch{Kind: PatchTruncate, Length: length} }

func validatePatch(p Patch) error {
	switch p.Kind {
	case PatchWrite:
		if p.Length == 0 || uint64(len(p.Data)) != uint64(p.Length) || uint64(p.Offset)+uint64(p.Length) > BlobSize {
			return ErrInvalidValue
		}
	case PatchZero:
		if p.Length == 0 || len(p.Data) != 0 || uint64(p.Offset)+uint64(p.Length) > BlobSize {
			return ErrInvalidValue
		}
	case PatchTruncate:
		if p.Offset != 0 || len(p.Data) != 0 || p.Length > BlobSize {
			return ErrInvalidValue
		}
	default:
		return ErrInvalidValue
	}
	return nil
}

// EncodeBase 编码完整或截断后的 Blob。
func EncodeBase(data []byte) ([]byte, error) {
	if len(data) > BlobSize {
		return nil, ErrInvalidValue
	}
	return append([]byte{1, BaseFrame}, data...), nil
}

// EncodePatches 按输入顺序编码补丁。
func EncodePatches(patches ...Patch) ([]byte, error) {
	b := []byte{1, PatchListFrame}
	for _, p := range patches {
		if err := validatePatch(p); err != nil {
			return nil, err
		}
		b = append(b, byte(p.Kind))
		b = binary.BigEndian.AppendUint32(b, p.Offset)
		b = binary.BigEndian.AppendUint32(b, p.Length)
		b = append(b, p.Data...)
	}
	return b, nil
}

type blobFrame struct {
	kind    byte
	base    []byte
	patches []Patch
}

func decodeFrame(b []byte) (blobFrame, error) {
	if len(b) < 2 || b[0] != 1 {
		return blobFrame{}, ErrInvalidValue
	}
	switch b[1] {
	case BaseFrame:
		if len(b)-2 > BlobSize {
			return blobFrame{}, ErrInvalidValue
		}
		return blobFrame{kind: BaseFrame, base: append([]byte(nil), b[2:]...)}, nil
	case PatchListFrame:
		f := blobFrame{kind: PatchListFrame}
		b = b[2:]
		for len(b) > 0 {
			if len(b) < 9 {
				return blobFrame{}, ErrInvalidValue
			}
			p := Patch{Kind: PatchKind(b[0]), Offset: binary.BigEndian.Uint32(b[1:5]), Length: binary.BigEndian.Uint32(b[5:9])}
			b = b[9:]
			if p.Kind == PatchWrite {
				if uint64(p.Length) > uint64(len(b)) {
					return blobFrame{}, ErrInvalidValue
				}
				p.Data = append([]byte(nil), b[:p.Length]...)
				b = b[p.Length:]
			}
			if err := validatePatch(p); err != nil {
				return blobFrame{}, err
			}
			f.patches = append(f.patches, p)
		}
		return f, nil
	default:
		return blobFrame{}, ErrInvalidValue
	}
}

func applyPatches(data []byte, patches []Patch) []byte {
	for _, p := range patches {
		end := int(p.Offset + p.Length)
		if p.Kind == PatchTruncate {
			end = int(p.Length)
		}
		if end > len(data) {
			data = append(data, make([]byte, end-len(data))...)
		}
		switch p.Kind {
		case PatchWrite:
			copy(data[p.Offset:end], p.Data)
		case PatchZero:
			clear(data[p.Offset:end])
		case PatchTruncate:
			data = data[:end]
		}
	}
	return data
}

// DecodeBlob 解码读取结果。补丁帧按空块求值，仅适用于已确认没有更旧基准的值。
func DecodeBlob(b []byte) ([]byte, error) {
	f, err := decodeFrame(b)
	if err != nil {
		return nil, err
	}
	if f.kind == BaseFrame {
		return f.base, nil
	}
	return applyPatches(nil, f.patches), nil
}

type blobMerger struct{ frames []blobFrame }

func (m *blobMerger) MergeNewer(value []byte) error {
	f, err := decodeFrame(value)
	if err != nil {
		return err
	}
	m.frames = append(m.frames, f)
	return nil
}
func (m *blobMerger) MergeOlder(value []byte) error {
	f, err := decodeFrame(value)
	if err != nil {
		return err
	}
	m.frames = append([]blobFrame{f}, m.frames...)
	return nil
}
func (m *blobMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	start := -1
	for i := range m.frames {
		if m.frames[i].kind == BaseFrame {
			start = i
		}
	}
	if start < 0 && !includesBase {
		var patches []Patch
		for _, f := range m.frames {
			patches = append(patches, f.patches...)
		}
		b, err := EncodePatches(patches...)
		return b, nil, err
	}
	var data []byte
	if start >= 0 {
		data = m.frames[start].base
		start++
	} else {
		start = 0
	}
	for _, f := range m.frames[start:] {
		data = applyPatches(data, f.patches)
	}
	b, err := EncodeBase(data)
	return b, nil, err
}

type overwriteMerger struct{ value []byte }

func (m *overwriteMerger) MergeNewer(value []byte) error {
	m.value = append(m.value[:0], value...)
	return nil
}
func (m *overwriteMerger) MergeOlder(_ []byte) error                { return nil }
func (m *overwriteMerger) Finish(_ bool) ([]byte, io.Closer, error) { return m.value, nil, nil }

// Merger 为数据键按顺序合并补丁，其他键以最新值覆盖。
var Merger = &pebble.Merger{
	Name: "bowl.blob.v1",
	Merge: func(key, value []byte) (pebble.ValueMerger, error) {
		if !bytes.HasPrefix(key, []byte("data:")) {
			return &overwriteMerger{value: append([]byte(nil), value...)}, nil
		}
		if _, _, err := DecodeDataKey(key); err != nil {
			return nil, err
		}
		f, err := decodeFrame(value)
		if err != nil {
			return nil, err
		}
		return &blobMerger{frames: []blobFrame{f}}, nil
	},
}
