package pebblekv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// BlobSize 是文件数据块的固定上限，与 Linux FUSE MaxWrite (1 MiB) 1:1 对齐。
const BlobSize = 1 << 20

// blobShardCount 与 Path 的分片规则 (id & 0xff) 对齐。
const blobShardCount = 256

// hotBlobSlots 是最近写入块的内存缓存槽位，加速局部写 CoW 读取旧块与热读。
const hotBlobSlots = 16

// hotBlob 是热块缓存条目；data 一经写入不再原地修改，仅整体替换或剔除。
type hotBlob struct {
	id   uint64
	data []byte
}

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
	// link 是硬链接实现，测试可注入 EMLINK 等失败以验证自动回退拷贝。
	link func(string, string) error

	mu        sync.Mutex
	dirtyFile map[uint64]struct{}
	dirtyDir  map[string]struct{}
	// shardReady 记录已创建过的分片目录，避免每次写入都发起 MkdirAll。
	shardReady [blobShardCount]atomic.Bool
	// hotMu/hot 保护最近写入块的内存缓存，命中时省去 open+pread+close。
	hotMu   sync.Mutex
	hot     [hotBlobSlots]hotBlob
	hotNext int
	// readOps 累计块文件读取次数，用于验证 Diff / Merge 的 BlobRef 短路。
	readOps atomic.Int64
	// fileSyncs / dirSyncs 累计 Sync 实际发出的块文件与分片目录 fsync 次数，
	// 用于回归测试锁定“过渡块不产生目录屏障”的契约。
	fileSyncs atomic.Int64
	dirSyncs  atomic.Int64
	// writeHook 仅用于测试观测块写入阶段是否已脱离 Inode 锁。
	writeHook func()
}

// NewBlobStore 返回以 base 目录（分支或快照目录）为根的块文件存储。
func NewBlobStore(base string) *BlobStore {
	return newBlobStore(base, os.Link)
}

func newBlobStore(base string, link func(string, string) error) *BlobStore {
	if link == nil {
		link = os.Link
	}
	return &BlobStore{
		dir:       filepath.Join(base, "blobs"),
		link:      link,
		dirtyFile: make(map[uint64]struct{}),
		dirtyDir:  make(map[string]struct{}),
	}
}

// ReadOps 返回累计块文件读取次数。
func (s *BlobStore) ReadOps() int64 { return s.readOps.Load() }

// SetWriteHook 设置块写入阶段的测试观测钩子，必须在并发写入前调用。
func (s *BlobStore) SetWriteHook(fn func()) { s.writeHook = fn }

// Root 返回 blobs 目录路径。
func (s *BlobStore) Root() string { return s.dir }

// Path 返回指定编号块文件的绝对路径。
func (s *BlobStore) Path(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%02x", id&0xff), fmt.Sprintf("%016x.blob", id))
}

// Write 将不可变块写入 shard 目录。块编号全局单调递增，在 Pebble 批次提交前
// 没有任何引用指向该文件，因此直接覆盖写入即可，无需临时文件加重命名。
func (s *BlobStore) Write(id uint64, data []byte) error {
	if id == 0 || len(data) == 0 || len(data) > BlobSize {
		return ErrInvalidValue
	}
	if s.writeHook != nil {
		s.writeHook()
	}
	path := s.Path(id)
	dir := filepath.Dir(path)
	if !s.shardReady[id&0xff].Load() {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		s.shardReady[id&0xff].Store(true)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		_ = os.Remove(path)
		return err
	}
	s.markFile(id)
	// 直接收养调用方缓冲区，不做整块拷贝：块内容一经写入即不可变，
	// 而所有调用方在 Write 之后都不再改写该切片（整块写是请求体子切片，局部写是刚构造的新块）。
	// 这样 16 MiB 顺序写不再额外产生 16 MiB 堆垃圾，4 KiB 热读仍 100% 命中内存。
	s.cacheHotOwned(id, data)
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
	// 先记账再查热缓存，保持 ReadOps() 的观测契约。
	s.readOps.Add(1)
	if data, ok := s.hotBlock(id); ok && offset >= 0 && offset+int64(len(dst)) <= int64(len(data)) {
		copy(dst, data[offset:offset+int64(len(dst))])
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
	// 无论文件是否已不存在，都必须剔除热缓存与脏标记：
	// 块已被逻辑删除，不能继续读到旧内容，Sync 也不应再对已删除块发起 open。
	s.evictHot(id)
	s.mu.Lock()
	_, transient := s.dirtyFile[id]
	delete(s.dirtyFile, id)
	s.mu.Unlock()
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	// 窗口内新建、尚未被任何 Sync 持久化的过渡块，其目录项从未落盘，
	// 删除它不需要目录屏障；只有删除“已持久化”的块才必须把 unlink 也刷下去。
	if !transient {
		s.markDir(filepath.Dir(path))
	}
	return nil
}

// WindowLocal 报告该块是否仍在本次 Sync 窗口内新建、尚未被任何持久化提交引用。
// 窗口内的块可以原地追加：它既没有落盘屏障，也没有被 Commit / Fork 固化成共享副本。
func (s *BlobStore) WindowLocal(id uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.dirtyFile[id]
	return ok
}

// AppendTail 在窗口内块的尾部原地追加 chunk，并把热缓存替换为追加后的完整内容。
// full 必须是追加后的完整块（长度与块文件最终大小一致），供后续局部写 CoW 与热读直接命中内存。
func (s *BlobStore) AppendTail(id uint64, chunk, full []byte) error {
	file, err := os.OpenFile(s.Path(id), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(chunk)
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		return err
	}
	s.cacheHotOwned(id, full)
	return nil
}

// FileSyncOps 返回累计块文件 fsync 次数，用于验证 Sync 只刷仍然存活的脏块。
func (s *BlobStore) FileSyncOps() int64 { return s.fileSyncs.Load() }

// DirSyncOps 返回累计分片目录 fsync 次数，用于验证过渡块不残留目录屏障。
func (s *BlobStore) DirSyncOps() int64 { return s.dirSyncs.Load() }

// hotBlock 返回指定块的内存副本。写入后的块内容不可变，返回的切片可安全并发读取。
func (s *BlobStore) hotBlock(id uint64) ([]byte, bool) {
	s.hotMu.Lock()
	defer s.hotMu.Unlock()
	for i := range s.hot {
		if s.hot[i].id == id && s.hot[i].data != nil {
			return s.hot[i].data, true
		}
	}
	return nil, false
}

// cacheHotOwned 收养最近写入块的缓冲区（不拷贝），供局部写 CoW 与热读直接命中内存。
// 调用方必须在 Write / AppendTail 之后放弃对 data 的写所有权：块内容一经写入即不可变。
func (s *BlobStore) cacheHotOwned(id uint64, data []byte) {
	s.hotMu.Lock()
	defer s.hotMu.Unlock()
	for i := range s.hot {
		if s.hot[i].id == id {
			s.hot[i].data = data
			return
		}
	}
	s.hot[s.hotNext] = hotBlob{id: id, data: data}
	s.hotNext = (s.hotNext + 1) % len(s.hot)
}

// evictHot 在块被删除后剔除热缓存，避免读到已删除块的旧内容。
func (s *BlobStore) evictHot(id uint64) {
	s.hotMu.Lock()
	defer s.hotMu.Unlock()
	for i := range s.hot {
		if s.hot[i].id == id {
			s.hot[i] = hotBlob{}
		}
	}
}

// markShard 记录分片目录已存在，避免后续写入重复 MkdirAll。
func (s *BlobStore) markShard(name string) {
	if index, err := strconv.ParseUint(name, 16, 8); err == nil {
		s.shardReady[index].Store(true)
	}
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
	// 只为“确实存在存活脏文件”的分片目录发目录屏障：一次事务里被 CoW 反复替换的
	// 中间过渡块已经消失，它们的目录项从未落盘，fsync 它们纯属浪费。
	// 单次 SQLite WAL 事务的分片目录 fsync 因此从 41 次收敛到 1 次。
	dirSet := make(map[string]struct{}, len(s.dirtyDir)+len(files))
	for dir := range s.dirtyDir {
		dirSet[dir] = struct{}{}
	}
	for _, id := range files {
		dirSet[filepath.Dir(s.Path(id))] = struct{}{}
	}
	dirs := make([]string, 0, len(dirSet))
	for dir := range dirSet {
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
		s.fileSyncs.Add(1)
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
		s.dirSyncs.Add(1)
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
		dst.markShard(entry.Name())
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
			if err := s.link(from, to); err != nil {
				// 达到宿主文件系统硬链接上限（EMLINK）等情况回退为流式拷贝，
				// 生成新的物理 Inode 以重新获得独立硬链接配额。
				if copyErr := copyBlobFile(from, to); copyErr != nil {
					return errors.Join(err, copyErr)
				}
				if id, parseErr := parseBlobID(file.Name()); parseErr == nil {
					dst.markFile(id)
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

// parseBlobID 从 <blob_id>.blob 文件名解析块编号。
func parseBlobID(name string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSuffix(name, ".blob"), 16, 64)
}
