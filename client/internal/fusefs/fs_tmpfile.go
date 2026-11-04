package fusefs

import (
	"crypto/rand"
	"encoding/hex"
	"math"
	"sort"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const tmpPageSize = 64 * 1024

type tmpFile struct {
	mu        sync.Mutex
	inode     *storagev1.Inode
	pages     map[uint64][]byte
	exclusive bool
	named     bool
	parent    uint64
	lookups   uint64
	serverIno uint64
}

func (fs *RawFS) tmpFile(ino uint64) *tmpFile {
	fs.tmpMu.RLock()
	defer fs.tmpMu.RUnlock()
	return fs.tmpFiles[ino]
}

func (fs *RawFS) localTmpInode(ino uint64) (*storagev1.Inode, bool) {
	tmp := fs.tmpFile(ino)
	if tmp == nil {
		return nil, false
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return nil, false
	}
	return cloneInode(tmp.inode), true
}

// Tmpfile 创建只存在于客户端内存的匿名文件。
func (fs *RawFS) Tmpfile(_ <-chan struct{}, input *fuse.CreateIn, out *fuse.CreateOut) fuse.Status {
	if status := fs.CheckWritable(); status != fuse.OK {
		return status
	}
	if _, status := fs.ToServerIno(input.NodeId); status != fuse.OK {
		return status
	}
	mode := input.Flags & uint32(syscall.O_ACCMODE)
	if mode != syscall.O_WRONLY && mode != syscall.O_RDWR {
		return fuse.EINVAL
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	ino := uint64(1)<<63 | fs.nextTmp.Add(1)
	now := timestamppb.Now()
	inode := &storagev1.Inode{Ino: ino, Generation: 1, Attr: &storagev1.InodeAttribute{
		Mode: syscall.S_IFREG | input.Mode&0o7777, Uid: input.Uid, Gid: input.Gid,
		Blksize: tmpPageSize, Atime: now, Mtime: now, Ctime: now,
	}}
	fh, status := fs.openHandle(ino, input.Flags, false)
	if status != fuse.OK {
		return status
	}
	fs.tmpMu.Lock()
	fs.tmpFiles[ino] = &tmpFile{inode: inode, pages: make(map[uint64][]byte), exclusive: input.Flags&syscall.O_EXCL != 0, parent: input.NodeId, lookups: 1}
	fs.tmpMu.Unlock()
	fs.caps.markAbsent(ino)
	fs.FillEntryOut(&out.EntryOut, inode, 0, 0)
	out.Fh = fh
	if fs.cfg.DirectIO {
		out.OpenFlags = fuse.FOPEN_DIRECT_IO
	}
	return fuse.OK
}

func (fs *RawFS) readTmp(ino, offset uint64, size int, buf []byte) (fuse.ReadResult, bool) {
	tmp := fs.tmpFile(ino)
	if tmp == nil {
		return nil, false
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return nil, false
	}
	if offset >= tmp.inode.Attr.Size {
		return fuse.ReadResultData(nil), true
	}
	length := min(uint64(size), tmp.inode.Attr.Size-offset)
	data := make([]byte, int(length))
	for pos := uint64(0); pos < length; {
		at := offset + pos
		within := at % tmpPageSize
		n := min(length-pos, tmpPageSize-within)
		if page := tmp.pages[at/tmpPageSize]; page != nil {
			copy(data[pos:pos+n], page[within:within+n])
		}
		pos += n
	}
	return makeReadResult(data, buf), true
}

func (fs *RawFS) writeTmp(ino, offset uint64, data []byte, appendMode bool) (uint32, fuse.Status, bool) {
	tmp := fs.tmpFile(ino)
	if tmp == nil {
		return 0, fuse.OK, false
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return 0, fuse.OK, false
	}
	if appendMode {
		offset = tmp.inode.Attr.Size
	}
	if offset > math.MaxInt64 || uint64(len(data)) > math.MaxInt64-offset {
		return 0, fuse.Status(syscall.EFBIG), true
	}
	for pos := 0; pos < len(data); {
		at := offset + uint64(pos)
		index, within := at/tmpPageSize, at%tmpPageSize
		n := min(len(data)-pos, tmpPageSize-int(within))
		page := tmp.pages[index]
		if page == nil {
			page = make([]byte, tmpPageSize)
			tmp.pages[index] = page
		}
		copy(page[within:within+uint64(n)], data[pos:pos+n])
		pos += n
	}
	if len(data) != 0 {
		tmp.inode.Attr.Size = max(tmp.inode.Attr.Size, offset+uint64(len(data)))
		tmp.inode.Attr.Blocks = uint64(len(tmp.pages)) * (tmpPageSize / 512)
		tmp.inode.Attr.Mtime = timestamppb.Now()
		tmp.inode.Attr.Ctime = timestamppb.Now()
	}
	return uint32(len(data)), fuse.OK, true
}

func (fs *RawFS) setTmpAttr(input *fuse.SetAttrIn, out *fuse.AttrOut) (fuse.Status, bool) {
	tmp := fs.tmpFile(input.NodeId)
	if tmp == nil {
		return fuse.OK, false
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return fuse.OK, false
	}
	a := tmp.inode.Attr
	if input.Valid&fuse.FATTR_FH != 0 {
		h, _, status := fs.checkedHandle(input.NodeId, input.Fh, false)
		if status != fuse.OK {
			return status, true
		}
		if input.Valid&fuse.FATTR_SIZE != 0 && !h.CanWrite() {
			return fuse.EBADF, true
		}
	}
	if input.Valid&fuse.FATTR_SIZE != 0 {
		if input.Size > math.MaxInt64 {
			return fuse.Status(syscall.EFBIG), true
		}
		if input.Size < a.Size {
			for index, page := range tmp.pages {
				start := index * tmpPageSize
				if start >= input.Size {
					delete(tmp.pages, index)
				} else if input.Size-start < tmpPageSize {
					clear(page[input.Size-start:])
				}
			}
		}
		a.Size = input.Size
		a.Blocks = uint64(len(tmp.pages)) * (tmpPageSize / 512)
		a.Mtime = timestamppb.Now()
	}
	if input.Valid&fuse.FATTR_MODE != 0 {
		a.Mode = a.Mode&syscall.S_IFMT | input.Mode&0o7777
	}
	if input.Valid&fuse.FATTR_UID != 0 {
		a.Uid = input.Uid
	}
	if input.Valid&fuse.FATTR_GID != 0 {
		a.Gid = input.Gid
	}
	if input.Valid&fuse.FATTR_ATIME_NOW != 0 {
		a.Atime = timestamppb.Now()
	} else if input.Valid&fuse.FATTR_ATIME != 0 {
		a.Atime = timestamppb.New(time.Unix(int64(input.Atime), int64(input.Atimensec)))
	}
	if input.Valid&fuse.FATTR_MTIME_NOW != 0 {
		a.Mtime = timestamppb.Now()
	} else if input.Valid&fuse.FATTR_MTIME != 0 {
		a.Mtime = timestamppb.New(time.Unix(int64(input.Mtime), int64(input.Mtimensec)))
	}
	if input.Valid&(fuse.FATTR_SIZE|fuse.FATTR_MODE|fuse.FATTR_UID|fuse.FATTR_GID|fuse.FATTR_ATIME|fuse.FATTR_ATIME_NOW|fuse.FATTR_MTIME|fuse.FATTR_MTIME_NOW|fuse.FATTR_CTIME) != 0 {
		a.Ctime = timestamppb.Now()
	}
	fs.FillAttrOut(out, tmp.inode, 0)
	return fuse.OK, true
}

// addTmpLookup 记录内核取得的匿名节点引用。
func (fs *RawFS) addTmpLookup(ino uint64) {
	tmp := fs.tmpFile(ino)
	if tmp == nil {
		return
	}
	tmp.mu.Lock()
	tmp.lookups++
	tmp.mu.Unlock()
}

// dropTmpIfUnused 移除不再被句柄或内核引用的节点映射。
// 调用方须持有 lifecycleMu 读锁。
func (fs *RawFS) dropTmpIfUnused(ino uint64, orphan bool) {
	if fs.handles.IsOpen(ino) {
		return
	}
	tmp := fs.tmpFile(ino)
	if tmp == nil {
		return
	}
	tmp.mu.Lock()
	serverIno, lookups := tmp.serverIno, tmp.lookups
	tmp.mu.Unlock()
	if serverIno != 0 && !orphan && lookups != 0 {
		return
	}
	fs.tmpMu.Lock()
	delete(fs.tmpFiles, ino)
	if fs.serverToLocal[serverIno] == ino {
		delete(fs.serverToLocal, serverIno)
	}
	fs.tmpMu.Unlock()
	fs.cache.invalidateInode(ino)
}

// Forget 释放内核对具名匿名节点的引用。
func (fs *RawFS) Forget(nodeID, nlookup uint64) {
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	fs.caps.forget(nodeID)
	tmp := fs.tmpFile(nodeID)
	if tmp == nil {
		return
	}
	tmp.mu.Lock()
	if nlookup >= tmp.lookups {
		tmp.lookups = 0
	} else {
		tmp.lookups -= nlookup
	}
	tmp.mu.Unlock()
	fs.dropTmpIfUnused(nodeID, false)
}

// uploadTmp 创建目录项并写入数据，失败时撤销新目录项。
// 调用方须持有 tmp.mu 与 lifecycleMu 读锁。
func (fs *RawFS) uploadTmp(cancel <-chan struct{}, tmp *tmpFile, parent uint64, filename string) (*storagev1.Inode, float64, float64, fuse.Status) {
	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()
	attr := tmp.inode.Attr
	resp, err := fs.client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
		ParentIno: parent, Name: []byte(filename), Mode: attr.Mode, Uid: attr.Uid, Gid: attr.Gid, SessionId: fs.currentSession(),
	}))
	if err != nil {
		return nil, 0, 0, fs.ToStatus(err, cancel)
	}
	cleanup := func() {
		ctx, done := fs.RPCContext(nil)
		defer done()
		open := false
		_, _ = fs.client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{ParentIno: parent, Name: []byte(filename), OpenInClient: &open, SessionId: fs.currentSession()}))
	}
	inode := resp.Msg.GetInode()
	if inode == nil || inode.GetAttr() == nil {
		cleanup()
		return nil, 0, 0, fuse.EIO
	}
	indices := make([]uint64, 0, len(tmp.pages))
	for index := range tmp.pages {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })
	for _, index := range indices {
		offset := index * tmpPageSize
		length := min(uint64(tmpPageSize), attr.Size-offset)
		written, writeErr := fs.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{Ino: inode.GetIno(), Offset: offset, Data: tmp.pages[index][:length], SessionId: fs.currentSession()}))
		if writeErr != nil || written.Msg.GetSize() != uint32(length) {
			cleanup()
			if writeErr != nil {
				return nil, 0, 0, fs.ToStatusWithSession(writeErr, cancel)
			}
			return nil, 0, 0, fuse.EIO
		}
	}
	size := attr.Size
	updated, err := fs.client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
		Ino: inode.GetIno(), Size: &size, Atime: attr.Atime, Mtime: attr.Mtime, SessionId: fs.currentSession(),
	}))
	if err != nil {
		cleanup()
		return nil, 0, 0, fs.ToStatus(err, cancel)
	}
	if updated.Msg.GetInode() != nil {
		inode = updated.Msg.GetInode()
	}
	return inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds(), fuse.OK
}

func (fs *RawFS) installTmpAlias(ino uint64, tmp *tmpFile, serverIno uint64) {
	tmp.serverIno = serverIno
	tmp.pages = nil
	fs.tmpMu.Lock()
	fs.serverToLocal[serverIno] = ino
	fs.tmpMu.Unlock()
}

// linkTmp 将匿名文件具名化，已具名化的匿名节点不能再次通过此路径 Link。
func (fs *RawFS) linkTmp(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) (fuse.Status, bool) {
	tmp := fs.tmpFile(input.Oldnodeid)
	if tmp == nil {
		return fuse.OK, false
	}
	parent, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status, true
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.exclusive || tmp.named || !fs.handles.IsOpen(input.Oldnodeid) {
		return fuse.ENOENT, true
	}
	fs.cache.invalidateDir(input.NodeId)
	epoch := fs.cache.version()
	var inode *storagev1.Inode
	var entryTimeout, attrTimeout float64
	if tmp.serverIno == 0 {
		inode, entryTimeout, attrTimeout, status = fs.uploadTmp(cancel, tmp, parent, filename)
		if status != fuse.OK {
			return status, true
		}
		fs.installTmpAlias(input.Oldnodeid, tmp, inode.GetIno())
		fs.markDirty(inode.GetIno())
	} else {
		ctx, done := fs.RPCContext(cancel)
		resp, err := fs.client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{Ino: tmp.serverIno, NewParentIno: parent, NewName: []byte(filename), SessionId: fs.currentSession()}))
		done()
		if err != nil {
			return fs.ToStatus(err, cancel), true
		}
		inode, entryTimeout, attrTimeout = resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds()
		if inode == nil || inode.GetAttr() == nil {
			return fuse.EIO, true
		}
		fs.handles.ClearOrphan(input.Oldnodeid)
		fs.stateMu.Lock()
		delete(fs.orphanSessions, input.Oldnodeid)
		fs.stateMu.Unlock()
	}
	tmp.named = true
	tmp.lookups++
	fs.cache.invalidateDir(input.NodeId)
	fs.cache.putEntryAt(epoch, input.NodeId, filename, input.Oldnodeid)
	fs.cache.putInodeAt(epoch, input.Oldnodeid, inode, entryTimeout, attrTimeout)
	fs.FillEntryOut(out, inode, entryTimeout, attrTimeout)
	return fuse.OK, true
}

// syncTmpFile 将匿名文件上云并解链，供 Fsync 完成持久化。
func (fs *RawFS) syncTmpFile(cancel <-chan struct{}, ino uint64) fuse.Status {
	tmp := fs.tmpFile(ino)
	if tmp == nil {
		return fuse.OK
	}
	parent, status := fs.ToServerIno(tmp.parent)
	if status != fuse.OK {
		return status
	}
	tmp.mu.Lock()
	defer tmp.mu.Unlock()
	if tmp.serverIno != 0 {
		return fuse.OK
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return fuse.EIO
	}
	name := ".bowl-tmp-" + hex.EncodeToString(random[:])
	fs.cache.invalidateDir(tmp.parent)
	inode, _, _, status := fs.uploadTmp(cancel, tmp, parent, name)
	if status != fuse.OK {
		return status
	}
	ctx, done := fs.RPCContext(cancel)
	open := true
	sid := fs.currentSession()
	_, err := fs.client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{ParentIno: parent, Name: []byte(name), OpenInClient: &open, SessionId: sid}))
	done()
	fs.cache.invalidateDir(tmp.parent)
	if err != nil {
		cleanupCtx, cleanupDone := fs.RPCContext(nil)
		closed := false
		_, _ = fs.client.Unlink(cleanupCtx, connect.NewRequest(&storagev1.UnlinkRequest{ParentIno: parent, Name: []byte(name), OpenInClient: &closed, SessionId: fs.currentSession()}))
		cleanupDone()
		return fs.ToStatus(err, cancel)
	}
	fs.installTmpAlias(ino, tmp, inode.GetIno())
	fs.markDirty(inode.GetIno())
	fs.markOrphan(ino, sid)
	return fuse.OK
}
