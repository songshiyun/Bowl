package meta

import (
	"bytes"
	"context"
	"fmt"
	"math"
)

func validateXattrName(name []byte) error {
	if len(name) == 0 {
		return ErrInvalidArgument
	}
	if len(name) > MaxXattrNameBytes {
		return ErrRange
	}
	if bytes.IndexByte(name, 0) >= 0 {
		return ErrInvalidArgument
	}
	return nil
}

// isAllowedUserXattrFileType 判定文件模式是否允许访问 user.* 命名空间扩展属性。
// 仅允许常规文件 (S_IFREG)、目录 (S_IFDIR) 与套接字 (S_IFSOCK)。
func isAllowedUserXattrFileType(mode uint32) bool {
	fileType := mode & S_IFMT
	return fileType == S_IFREG || fileType == S_IFDIR || fileType == S_IFSOCK
}

func (m *manager) Lookup(ctx context.Context, parentIno uint64, name []byte) (*Inode, error) {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if len(name) > MaxFileNameBytes {
		return nil, ErrNameTooLong
	}
	if bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
		return nil, ErrInvalidArgument
	}

	sid, _ := SessionIDFromContext(ctx)
	return m.pebbleLookup(ctx, sid, parentIno, name)
}

func (m *manager) GetAttr(ctx context.Context, ino uint64) (*Inode, error) {
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}

	sid, _ := SessionIDFromContext(ctx)
	return m.pebbleGetAttr(ctx, sid, ino)
}

func (m *manager) Readlink(ctx context.Context, ino uint64) ([]byte, error) {
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}

	sid, _ := SessionIDFromContext(ctx)
	return m.pebbleReadlink(ctx, sid, ino)
}

func (m *manager) Mknod(ctx context.Context, params MknodParams) (*Inode, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
	}
	if params.Mode > 0o177777 {
		return nil, ErrInvalidArgument
	}

	modeType := params.Mode & S_IFMT
	switch modeType {
	case S_IFREG, S_IFCHR, S_IFBLK, S_IFIFO, S_IFSOCK:
	default:
		return nil, ErrInvalidArgument
	}

	sid, _ := SessionIDFromContext(ctx)
	return m.createPebbleNode(ctx, sid, pebbleNodeParams{parent: params.ParentIno, name: params.Name, mode: modeType | ((params.Mode & PermMask) &^ (params.Umask & PermMask)), uid: params.UID, gid: params.GID, rdev: params.Rdev})
}

func (m *manager) Mkdir(ctx context.Context, params MkdirParams) (*Inode, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
	}
	if params.Mode > 65535 {
		return nil, ErrInvalidArgument
	}

	modeType := params.Mode & S_IFMT
	if modeType != 0 && modeType != S_IFDIR {
		return nil, ErrInvalidArgument
	}

	sid, _ := SessionIDFromContext(ctx)
	return m.createPebbleNode(ctx, sid, pebbleNodeParams{parent: params.ParentIno, name: params.Name, mode: S_IFDIR | ((params.Mode & PermMask) &^ (params.Umask & PermMask)), uid: params.UID, gid: params.GID})
}

func (m *manager) Symlink(ctx context.Context, params SymlinkParams) (*Inode, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
	}
	if len(params.Target) == 0 || bytes.IndexByte(params.Target, 0) >= 0 {
		return nil, ErrInvalidArgument
	}
	if len(params.Target) > MaxSymlinkTargetBytes {
		return nil, ErrNameTooLong
	}

	sid, _ := SessionIDFromContext(ctx)
	return m.createPebbleNode(ctx, sid, pebbleNodeParams{parent: params.ParentIno, name: params.Name, mode: S_IFLNK | 0o777, uid: params.UID, gid: params.GID, target: params.Target})
}

func (m *manager) Link(ctx context.Context, params LinkParams) (*Inode, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if params.NewParentIno == 0 || params.NewParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateEntryName(params.NewName); err != nil {
		return nil, err
	}

	sid, _ := SessionIDFromContext(ctx)
	return m.linkPebbleNode(ctx, sid, params)
}

func (m *manager) Unlink(ctx context.Context, parentIno uint64, name []byte) error {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateEntryNameAllowEmpty(name); err != nil {
		return err
	}
	sid, _ := SessionIDFromContext(ctx)
	return m.unlinkPebbleNode(ctx, sid, parentIno, name)
}

func (m *manager) Rmdir(ctx context.Context, parentIno uint64, name []byte) error {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateEntryNameAllowEmpty(name); err != nil {
		return err
	}
	sid, _ := SessionIDFromContext(ctx)
	return m.rmdirPebbleNode(ctx, sid, parentIno, name)
}

func (m *manager) Rename(ctx context.Context, params RenameParams) error {
	if params.OldParentIno == 0 || params.OldParentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if params.NewParentIno == 0 || params.NewParentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if (params.Flags &^ (RenameNoReplace | RenameExchange | RenameWhiteout)) != 0 {
		return ErrInvalidArgument
	}
	if (params.Flags & (RenameNoReplace | RenameExchange)) == (RenameNoReplace | RenameExchange) {
		return ErrInvalidArgument
	}
	if (params.Flags & RenameWhiteout) != 0 {
		return ErrNotSupported
	}
	if err := validateEntryNameAllowEmpty(params.OldName); err != nil {
		return err
	}
	if err := validateEntryName(params.NewName); err != nil {
		return err
	}
	sid, _ := SessionIDFromContext(ctx)
	return m.renamePebbleNode(ctx, sid, params)
}

func (m *manager) SetXattr(ctx context.Context, params SetXattrParams) error {
	// 第 1 步：入参静态校验（不查库）
	if params.Ino == 0 || params.Ino > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if params.Flags&^(XattrCreate|XattrReplace) != 0 {
		return ErrInvalidArgument
	}
	if err := validateXattrName(params.Name); err != nil {
		return err
	}
	if len(params.Value) > MaxXattrValueBytes {
		return ErrRange
	}
	sid, _ := SessionIDFromContext(ctx)
	return m.pebbleSetXattr(ctx, sid, params)
}

func (m *manager) GetXattr(ctx context.Context, ino uint64, name []byte) ([]byte, error) {
	// 第 1 步：入参静态校验（不查库）
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateXattrName(name); err != nil {
		return nil, err
	}
	sid, _ := SessionIDFromContext(ctx)
	return m.pebbleGetXattr(ctx, sid, ino, name)
}

func (m *manager) ListXattr(ctx context.Context, ino uint64) ([][]byte, error) {
	// 第 1 步：入参静态校验（不查库）
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	sid, _ := SessionIDFromContext(ctx)
	return m.pebbleListXattr(ctx, sid, ino)
}

func (m *manager) RemoveXattr(ctx context.Context, ino uint64, name []byte) error {
	// 第 1 步：入参静态校验（不查库）
	if ino == 0 || ino > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateXattrName(name); err != nil {
		return err
	}
	sid, _ := SessionIDFromContext(ctx)
	return m.pebbleRemoveXattr(ctx, sid, ino, name)
}

func (m *manager) StatFS(ctx context.Context, ino uint64) (*StatFSResult, error) {
	// 静态参数校验（无需触达数据库）：ino 必须在 [1, math.MaxInt64] 区间内
	if ino == 0 || ino > math.MaxInt64 {
		return nil, fmt.Errorf("%w: ino must be between 1 and %d, got %d", ErrInvalidArgument, int64(math.MaxInt64), ino)
	}

	sid, _ := SessionIDFromContext(ctx)
	return m.pebbleStatFS(ctx, sid, ino)
}

func (m *manager) Create(ctx context.Context, params CreateParams) (*CreateResult, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
	}
	if params.Mode > 0xffff {
		return nil, ErrInvalidArgument
	}
	if (params.Flags & O_TMPFILE) != 0 {
		return nil, ErrInvalidArgument
	}

	modeType := params.Mode & S_IFMT
	if modeType != 0 && modeType != S_IFREG {
		return nil, ErrInvalidArgument
	}

	effectivePerm := (params.Mode & PermMask) &^ (params.Umask & (PermMask & 0o777))
	mode := S_IFREG | effectivePerm

	sid, _, err := createBranchSession(ctx, params.SessionID)
	if err != nil {
		return nil, err
	}
	return m.pebbleCreateResult(ctx, sid, params, mode)
}

func (m *manager) SetAttr(ctx context.Context, ino uint64, input SetAttrInput) (*Inode, error) {
	// 事务前入参边界防御
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}

	// 至少包含一项有效待更新属性
	if input.Mode == nil && input.UID == nil && input.GID == nil && input.Size == nil &&
		input.Atime == nil && input.Mtime == nil && !input.AtimeNow && !input.MtimeNow &&
		!input.CtimeNow && input.Flags == nil {
		return nil, ErrInvalidArgument
	}

	// 可选文件句柄与客户端会话标识静态边界防御
	if input.Fh != nil && (*input.Fh == 0 || *input.Fh > math.MaxInt64) {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(input.SessionID); err != nil {
		return nil, err
	}

	// 时间戳策略互斥防御
	if (input.Atime != nil && input.AtimeNow) || (input.Mtime != nil && input.MtimeNow) {
		return nil, ErrInvalidArgument
	}

	// Mode 范围边界防御（不超过 16 位 0o177777 = 65535）
	if input.Mode != nil && *input.Mode > 0o177777 {
		return nil, ErrInvalidArgument
	}

	// UID 范围边界防御（防止转换为 PostgreSQL INTEGER 符号位溢出）
	if input.UID != nil && *input.UID > math.MaxInt32 {
		return nil, ErrInvalidArgument
	}

	// GID 范围边界防御（防止转换为 PostgreSQL INTEGER 符号位溢出）
	if input.GID != nil && *input.GID > math.MaxInt32 {
		return nil, ErrInvalidArgument
	}

	// Size 范围边界防御（防止转换为 PostgreSQL BIGINT 符号位溢出）
	if input.Size != nil && *input.Size > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}

	// Flags 范围边界防御（防止转换为 PostgreSQL INTEGER 符号位溢出）
	if input.Flags != nil && *input.Flags > math.MaxInt32 {
		return nil, ErrInvalidArgument
	}
	sid, _, err := createBranchSession(ctx, input.SessionID)
	if err != nil {
		return nil, err
	}
	return m.pebbleSetAttr(ctx, sid, ino, input)
}

func (m *manager) Fallocate(ctx context.Context, params FallocateParams) error {
	// 1. 入参边界与整型溢出防御
	if params.Ino == 0 || params.Ino > math.MaxInt64 ||
		params.Fh > math.MaxInt64 ||
		params.Length == 0 || params.Length > math.MaxInt64 ||
		params.Offset > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if params.Offset > uint64(math.MaxInt64)-params.Length {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}
	sid, _, err := createBranchSession(ctx, params.SessionID)
	if err != nil {
		return err
	}
	return m.pebbleFallocate(ctx, sid, params)
}

func (m *manager) ReadDir(ctx context.Context, params ReadDirParams) ([]DirEntry, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 ||
		params.Fh > math.MaxInt64 ||
		params.Limit == 0 || params.Limit > MaxReadDirLimit ||
		params.Offset > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}

	sid, _, err := createBranchSession(ctx, params.SessionID)
	if err != nil {
		return nil, err
	}
	view, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	return m.pebbleReadDir(ctx, view, params.Ino, params.Offset, params.Limit)
}

func (m *manager) ReadDirPlus(ctx context.Context, params ReadDirPlusParams) ([]DirEntryPlus, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 ||
		params.Fh > math.MaxInt64 ||
		params.Limit == 0 || params.Limit > MaxReadDirLimit ||
		params.Offset > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}

	sid, _, err := createBranchSession(ctx, params.SessionID)
	if err != nil {
		return nil, err
	}
	view, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	return m.pebbleReadDirPlus(ctx, view, params.Ino, params.Offset, params.Limit)
}

func (m *manager) Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
	if req == nil {
		return nil, ErrInvalidArgument
	}
	if req.Ino == 0 || req.Ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if req.Fh > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if req.Offset > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if req.Size > BlockSize {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(req.SessionID); err != nil {
		return nil, err
	}
	sid, _, err := createBranchSession(ctx, req.SessionID)
	if err != nil {
		return nil, err
	}
	view, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	return m.pebbleRead(ctx, view, req)
}

func (m *manager) Write(ctx context.Context, req *WriteRequest) (*WriteResponse, error) {
	if req == nil {
		return nil, ErrInvalidArgument
	}
	if req.Ino == 0 || req.Ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if req.Fh > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if req.Offset > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	dataLen := len(req.Data)
	if dataLen > BlockSize {
		return nil, ErrInvalidArgument
	}
	if req.Offset > uint64(math.MaxInt64)-uint64(dataLen) {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(req.SessionID); err != nil {
		return nil, err
	}
	sid, _, err := createBranchSession(ctx, req.SessionID)
	if err != nil {
		return nil, err
	}
	return m.pebbleWrite(ctx, sid, req)
}

func (m *manager) Fsync(ctx context.Context, params FsyncParams) error {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}
	sid, _, err := createBranchSession(ctx, params.SessionID)
	if err != nil {
		return err
	}
	return m.pebbleSync(ctx, sid, params.Ino, false)
}

func (m *manager) FsyncDir(ctx context.Context, params FsyncDirParams) error {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}
	sid, _, err := createBranchSession(ctx, params.SessionID)
	if err != nil {
		return err
	}
	return m.pebbleSync(ctx, sid, params.Ino, true)
}
