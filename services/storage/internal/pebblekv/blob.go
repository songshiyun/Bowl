package pebblekv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// BlobSize 是文件数据块的固定上限，与 Linux FUSE MaxWrite (1 MiB) 1:1 对齐。
const BlobSize = 1 << 20

// BlobRef 是 Pebble DataKey 中保存的定长数据块引用。
type BlobRef struct {
	ID     uint64
	Length uint32
}

// EncodeBlobRef 编码 12 字节 BlobRef：[blob_id(8B BigEndian) | length(4B BigEndian)]。
func EncodeBlobRef(ref BlobRef) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint64(b[:8], ref.ID)
	binary.BigEndian.PutUint32(b[8:], ref.Length)
	return b
}

// DecodeBlobRef 解码并校验 12 字节 BlobRef。
func DecodeBlobRef(b []byte) (BlobRef, error) {
	if len(b) != 12 {
		return BlobRef{}, ErrInvalidValue
	}
	ref := BlobRef{ID: binary.BigEndian.Uint64(b[:8]), Length: binary.BigEndian.Uint32(b[8:])}
	if ref.ID == 0 || ref.Length == 0 || ref.Length > BlobSize {
		return BlobRef{}, ErrInvalidValue
	}
	return ref, nil
}

// BlobStore 管理一个分支或快照目录下不可变的 blobs/<shard>/<blob_id>.blob 文件。
type BlobStore struct {
	dir string

	mu        sync.Mutex
	dirtyFile map[uint64]struct{}
	dirtyDir  map[string]struct{}
}

// NewBlobStore 返回以 base 目录（分支或快照目录）为根的块文件存储。
func NewBlobStore(base string) *BlobStore {
	return &BlobStore{
		dir:       filepath.Join(base, "blobs"),
		dirtyFile: make(map[uint64]struct{}),
		dirtyDir:  make(map[string]struct{}),
	}
}

// Root 返回 blobs 目录路径。
func (s *BlobStore) Root() string { return s.dir }

// Path 返回指定编号块文件的绝对路径。
func (s *BlobStore) Path(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%02x", id&0xff), fmt.Sprintf("%016x.blob", id))
}

// Write 将不可变块写入 shard 目录；先写临时文件再原子重命名，避免崩溃残留半块。
func (s *BlobStore) Write(id uint64, data []byte) error {
	if id == 0 || len(data) == 0 || len(data) > BlobSize {
		return ErrInvalidValue
	}
	path := s.Path(id)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".blob-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	_, writeErr := tmp.Write(data)
	closeErr := tmp.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	s.markFile(id)
	s.markDir(dir)
	return nil
}

// Read 读取完整块内容。
func (s *BlobStore) Read(id uint64, length int) ([]byte, error) {
	if length <= 0 || length > BlobSize {
		return nil, ErrInvalidValue
	}
	data := make([]byte, length)
	if err := s.ReadAt(id, 0, data); err != nil {
		return nil, err
	}
	return data, nil
}

// ReadAt 按块内偏移读取指定区间。
func (s *BlobStore) ReadAt(id uint64, offset int64, dst []byte) error {
	if len(dst) == 0 {
		return nil
	}
	file, err := os.Open(s.Path(id))
	if err != nil {
		return err
	}
	defer file.Close()
	n, err := file.ReadAt(dst, offset)
	if errors.Is(err, io.EOF) {
		if n == len(dst) {
			return nil
		}
		return io.ErrUnexpectedEOF
	}
	return err
}

// Exists 报告块文件是否存在。
func (s *BlobStore) Exists(id uint64) bool {
	_, err := os.Stat(s.Path(id))
	return err == nil
}

// Remove 立即删除块文件；不存在视为已回收。
func (s *BlobStore) Remove(id uint64) error {
	path := s.Path(id)
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	s.markDir(filepath.Dir(path))
	return nil
}

func (s *BlobStore) markFile(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirtyFile[id] = struct{}{}
}

func (s *BlobStore) markDir(dir string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirtyDir[dir] = struct{}{}
}

// Sync 将近期写入或删除的块文件与 shard 目录刷入磁盘。
func (s *BlobStore) Sync() error {
	s.mu.Lock()
	files := make([]uint64, 0, len(s.dirtyFile))
	for id := range s.dirtyFile {
		files = append(files, id)
	}
	dirs := make([]string, 0, len(s.dirtyDir))
	for dir := range s.dirtyDir {
		dirs = append(dirs, dir)
	}
	clear(s.dirtyFile)
	clear(s.dirtyDir)
	s.mu.Unlock()

	var errs []error
	for _, id := range files {
		file, err := os.Open(s.Path(id))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			s.markFile(id)
			errs = append(errs, err)
			continue
		}
		if err := errors.Join(file.Sync(), file.Close()); err != nil {
			s.markFile(id)
			errs = append(errs, err)
		}
	}
	for _, dir := range dirs {
		handle, err := os.Open(dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			s.markDir(dir)
			errs = append(errs, err)
			continue
		}
		if err := errors.Join(handle.Sync(), handle.Close()); err != nil {
			s.markDir(dir)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// LinkAllTo 通过硬链接把本目录下所有块共享给目标快照或分支目录，遇硬链接上限时回退复制。
func (s *BlobStore) LinkAllTo(dst *BlobStore) error {
	entries, err := os.ReadDir(s.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		shard := filepath.Join(s.dir, entry.Name())
		files, err := os.ReadDir(shard)
		if err != nil {
			return err
		}
		targetDir := filepath.Join(dst.dir, entry.Name())
		if err := os.MkdirAll(targetDir, 0o755); err != nil {
			return err
		}
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".blob") {
				continue
			}
			from := filepath.Join(shard, file.Name())
			to := filepath.Join(targetDir, file.Name())
			if _, err := os.Stat(to); err == nil {
				continue
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if err := os.Link(from, to); err != nil {
				if copyErr := copyBlobFile(from, to); copyErr != nil {
					return errors.Join(err, copyErr)
				}
			}
			dst.markDir(targetDir)
		}
	}
	return dst.Sync()
}

func copyBlobFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	return errors.Join(copyErr, out.Close())
}
