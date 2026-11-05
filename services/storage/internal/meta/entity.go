package meta

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// POSIX 文件类型掩码与模式常量。
const (
	S_IFMT                uint32 = 0o170000
	S_IFIFO               uint32 = 0o010000
	S_IFCHR               uint32 = 0o020000
	S_IFDIR               uint32 = 0o040000
	S_IFBLK               uint32 = 0o060000
	S_IFREG               uint32 = 0o100000
	S_IFLNK               uint32 = 0o120000
	S_IFSOCK              uint32 = 0o140000
	S_ISUID               uint32 = 0o4000
	S_ISGID               uint32 = 0o2000
	S_ISVTX               uint32 = 0o1000
	PermMask              uint32 = 0o7777
	MaxFileNameBytes             = 255
	MaxSymlinkTargetBytes        = 4095
)

// 根节点编号。
const (
	RootIno uint64 = 1
)

// 节点链接与大小上限。
const (
	MaxNlink    uint32 = 65000
	MaxFileSize uint64 = math.MaxInt64
)

// 默认块大小与目录默认物理大小。
const (
	DefaultBlockSize uint32 = 65536
	DefaultDirSize   uint64 = 4096
)

// 单次数据请求的最大长度。
const BlockSize = 1 << 22

// 磁盘与 StatFS 常量。
const (
	// MaxDiskNameBytes 定义磁盘名称最大字节长度（255 字节）。
	MaxDiskNameBytes = 255
	// DefaultStatFSBlockSize 定义 StatFS 默认换算块大小（4096 字节）。
	DefaultStatFSBlockSize = 4096
	// DefaultListDisksPageSize 定义 ListDisks 默认单页最大返回条数（50）。
	DefaultListDisksPageSize uint32 = 50
	// MaxListDisksPageSize 定义 ListDisks 单页最大返回条数上限（1000）。
	MaxListDisksPageSize uint32 = 1000
)

// Inode 文件系统标志位常量（对齐 Linux chattr / FS_*_FL）。
const (
	FlagImmutable uint32 = 0x1 // FS_IMMUTABLE_FL
	FlagAppend    uint32 = 0x2 // FS_APPEND_FL
)

// Linux FUSE / POSIX 打开标志位常量（平台无关固定值，严禁依赖宿主机 syscall.O_*）。
const (
	O_ACCMODE = 0o3
	O_RDONLY  = 0o0
	O_WRONLY  = 0o1
	O_RDWR    = 0o2
	O_CREAT   = 0o100
	O_EXCL    = 0o200
	O_TRUNC   = 0o1000
	O_APPEND  = 0o2000
	O_TMPFILE = 0o20000000
)

// FUSE Open 响应标志常量（平台无关固定值）。
const (
	FOPEN_DIRECT_IO   uint32 = 1 << 0
	FOPEN_KEEP_CACHE  uint32 = 1 << 1
	FOPEN_NONSEEKABLE uint32 = 1 << 2
	FOPEN_CACHE_DIR   uint32 = 1 << 3
)

// Linux FUSE / POSIX fallocate 模式标志位常量（平台无关固定值）。
const (
	FallocFlKeepSize      uint32 = 0x01
	FallocFlPunchHole     uint32 = 0x02
	FallocFlCollapseRange uint32 = 0x08
	FallocFlZeroRange     uint32 = 0x10
	FallocFlInsertRange   uint32 = 0x20
)

// Linux FUSE_RENAME2 (45) 重命名标志位常量（平台无关固定值，严禁依赖宿主机 syscall）。
const (
	RenameNoReplace uint32 = 0x1
	RenameExchange  uint32 = 0x2
	RenameWhiteout  uint32 = 0x4
)

// AtimeMode 定义最后访问时间更新策略枚举。
type AtimeMode int

const (
	AtimeModeNoAtime     AtimeMode = 0 // 默认：不更新 atime
	AtimeModeRelAtime    AtimeMode = 1 // relatime：仅当 atime <= mtime 或 atime 超过 24h 时更新
	AtimeModeStrictAtime AtimeMode = 2 // strictatime：每次读取均更新 atime
)

// Linux 扩展属性（xattr）规格常量与操作标志位。
const (
	MaxXattrNameBytes  = 255
	MaxXattrValueBytes = 65536
	MaxXattrListBytes  = 65536

	XattrCreate  uint32 = 0x1
	XattrReplace uint32 = 0x2
)

// 默认客户端会话租约时长。
const DefaultSessionLeaseTTL = 15 * time.Second

// SetAttrInput 定义修改 Inode 属性的输入参数。
type SetAttrInput struct {
	Mode      *uint32
	UID       *uint32
	GID       *uint32
	Size      *uint64
	Atime     *time.Time
	Mtime     *time.Time
	AtimeNow  bool
	MtimeNow  bool
	Fh        *uint64
	SessionID *uint64
	CtimeNow  bool
	Flags     *uint32
	CallerUID uint32
	CallerGID uint32
}

// MknodParams 定义创建节点的输入参数。
type MknodParams struct {
	ParentIno uint64
	Name      []byte
	Mode      uint32
	UID       uint32
	GID       uint32
	Rdev      uint32
	Umask     uint32
}

// MkdirParams 定义创建子目录的输入参数。
type MkdirParams struct {
	ParentIno uint64
	Name      []byte
	Mode      uint32
	UID       uint32
	GID       uint32
	Umask     uint32
}

// SymlinkParams 定义创建符号链接的输入参数。
type SymlinkParams struct {
	ParentIno uint64
	Name      []byte
	Target    []byte
	UID       uint32
	GID       uint32
}

// LinkParams 定义创建硬链接的输入参数。
type LinkParams struct {
	Ino          uint64
	NewParentIno uint64
	NewName      []byte
}

// RenameParams 定义重命名、移动与交换操作的输入参数。
type RenameParams struct {
	OldParentIno uint64
	OldName      []byte
	NewParentIno uint64
	NewName      []byte
	Flags        uint32
}

// ReclaimOrphanParams 定义回收孤儿节点的输入参数。
type ReclaimOrphanParams struct {
	SessionID  uint64
	DiskID     string
	BranchName *string
	Ino        uint64
}

// CreateParams 定义原子创建常规文件并分配初始文件句柄的输入参数。
// 严禁在 Flags 中携带 O_TMPFILE 标志位（匿名临时文件由客户端管理）。
type CreateParams struct {
	ParentIno uint64
	Name      []byte
	Mode      uint32
	UID       uint32
	GID       uint32
	Flags     uint32
	SessionID *uint64
	Umask     uint32
}

// CreateResult 包含新建文件的 Inode 元数据、初始分配的 FileHandle 与打开响应标志。
type CreateResult struct {
	Inode               *Inode
	Fh                  uint64
	OpenFlags           uint32
	EntryTimeoutSeconds float64
	AttrTimeoutSeconds  float64
}

// FsyncParams 定义文件同步操作的输入参数。
type FsyncParams struct {
	Ino       uint64
	Fh        uint64
	Datasync  bool
	SessionID *uint64
}

// FsyncDirParams 定义目录句柄同步操作的输入参数。
type FsyncDirParams struct {
	Ino       uint64
	Fh        uint64
	Datasync  bool
	SessionID *uint64
}

// 目录遍历最大单次条目配额限制。
const MaxReadDirLimit uint32 = 4096

// 默认内核条目与属性缓存时效（秒）。
const (
	DefaultEntryTimeoutSeconds float64 = 1.0
	DefaultAttrTimeoutSeconds  float64 = 1.0
)

// DirEntry 定义 ReadDir 返回的基本目录项实体。
type DirEntry struct {
	Ino        uint64
	Name       []byte
	Mode       uint32
	NextOffset uint64
}

// ReadDirParams 定义目录项流式遍历输入参数。
type ReadDirParams struct {
	Ino       uint64
	Fh        uint64
	Offset    uint64
	Limit     uint32
	SessionID *uint64
}

// DirEntryPlus 定义 ReadDirPlus 返回的扩展目录项实体（包含完整 Inode 属性与内核缓存时效）。
type DirEntryPlus struct {
	Ino                 uint64
	Name                []byte
	NextOffset          uint64
	Inode               *Inode
	EntryTimeoutSeconds float64
	AttrTimeoutSeconds  float64
}

// ReadDirPlusParams 定义扩展目录项流式遍历输入参数。
type ReadDirPlusParams struct {
	Ino       uint64
	Fh        uint64
	Offset    uint64
	Limit     uint32
	SessionID *uint64
}

// FallocateParams 定义文件空间预分配与打洞操作的输入参数。
type FallocateParams struct {
	Ino       uint64
	Fh        uint64
	Offset    uint64
	Length    uint64
	Mode      uint32
	SessionID *uint64
}

// SetXattrParams 定义设置扩展属性的输入参数。
type SetXattrParams struct {
	Ino   uint64
	Name  []byte
	Value []byte
	Flags uint32
}

// 领域哨兵错误。
var (
	ErrInvalidArgument = errors.New("meta: invalid argument")
	ErrNotFound        = errors.New("meta: not found")
	ErrNotDir          = errors.New("meta: not a directory")
	ErrIsDir           = errors.New("meta: is a directory")
	ErrPerm            = errors.New("meta: operation not permitted")
	ErrAlreadyExists   = errors.New("meta: entry already exists")
	ErrNotEmpty        = errors.New("meta: directory not empty")
	ErrBadFileHandle   = errors.New("meta: bad file handle")
	ErrNotSupported    = errors.New("meta: operation not supported")
	ErrNameTooLong     = errors.New("meta: file name too long")
	ErrRange           = errors.New("meta: numerical result out of range")
	ErrTooBig          = errors.New("meta: argument list too long")
	ErrNoData          = errors.New("meta: no data available")
	ErrNoSpace         = errors.New("meta: no space left on device")
	ErrAgain           = errors.New("meta: resource temporarily unavailable")
	ErrDeadlock        = errors.New("meta: resource deadlock avoided")
	ErrInterrupted     = errors.New("meta: interrupted system call")
	ErrTooManyLinks    = errors.New("meta: too many links")
	ErrFileTooLarge    = errors.New("meta: file too large")
	ErrIllegalSeek     = errors.New("meta: illegal seek")
	ErrIO              = errors.New("meta: input/output error")
	ErrAccess          = errors.New("meta: permission denied")
	ErrInvalidShrink   = fmt.Errorf("%w: cannot shrink disk capacity below current usage", ErrInvalidArgument)
	ErrCrossDevice     = errors.New("meta: invalid cross-device link")
	ErrBusy            = errors.New("meta: device or resource busy")
)

// Inode 表示文件系统节点的领域实体。
type Inode struct {
	Ino                 uint64
	Generation          uint64
	Attr                Attribute
	EntryTimeoutSeconds float64
	AttrTimeoutSeconds  float64
}

// Attribute 表示节点的 POSIX 物理属性。
type Attribute struct {
	Mode      uint32
	Size      uint64
	Nlink     uint32
	UID       uint32
	GID       uint32
	Rdev      uint32
	Blksize   uint32
	Blocks    uint64
	Atime     time.Time
	Mtime     time.Time
	Ctime     time.Time
	Flags     uint32
	ParentIno uint64
}

// GetXattrParams 定义获取扩展属性的输入参数。
type GetXattrParams struct {
	Ino  uint64
	Name []byte
	Size uint32
}

// ListXattrParams 定义列出扩展属性名称的输入参数。
type ListXattrParams struct {
	Ino  uint64
	Size uint32
}

// RenewSessionResult 包含续期后刷新得到的会话租约截止时间与是否为新创建或过期重建标识。
type RenewSessionResult struct {
	ExpiresAt        time.Time
	SessionRecreated bool
}

// RenewBranchSessionParams 定义分支会话的绑定信息。
type RenewBranchSessionParams struct {
	SessionID  uint64
	DiskID     string
	BranchName *string
	ReadOnly   bool
	CommitID   *string
}

// Disk 描述虚拟磁盘领域实体。
type Disk struct {
	DiskID        string
	Name          string
	RootIno       uint64
	MaxBytes      uint64
	UsedBytes     uint64
	UsedInodes    uint64
	CreatedAt     time.Time
	UpdatedAt     time.Time
	DefaultBranch string
	Branches      []Branch
}

// Branch 描述磁盘中的逻辑分支。
type Branch struct {
	BranchID     string
	DiskID       string
	Name         string
	ParentBranch *string
	ForkCommitID *string
	HeadCommitID *string
	State        string
	CreatedAt    time.Time
}

// CreateBranchParams 定义分支派生参数。
type CreateBranchParams struct {
	DiskID         string
	Name           string
	SourceBranch   *string
	SourceCommitID *string
}

// DeleteBranchParams 定义分支删除参数。
type DeleteBranchParams struct {
	DiskID string
	Name   string
}

// CommitParams 定义版本快照参数。
type CommitParams struct {
	DiskID     string
	BranchName string
	Message    string
}

// CommitInfo 描述版本快照及其父版本。
type CommitInfo struct {
	CommitID            string
	DiskID              string
	BranchName          string
	ParentCommitID      *string
	MergeParentCommitID *string
	Message             string
	CreatedAt           time.Time
}

// RollbackParams 定义版本回档参数。
type RollbackParams struct {
	DiskID     string
	BranchName string
	CommitID   string
}

// ConflictStrategy 指定合并冲突的处理方式。
type ConflictStrategy int32

const (
	ConflictStrategyUnspecified ConflictStrategy = iota
	ConflictStrategyAbortOnConflict
	ConflictStrategyOurs
	ConflictStrategyTheirs
	ConflictStrategyMaterialize
)

// MergeResultType 表示合并结果。
type MergeResultType int32

const (
	MergeResultUnspecified MergeResultType = iota
	MergeResultMerged
	MergeResultConflicts
	MergeResultMaterialized
)

// MergeConflict 描述三路合并中不兼容的变更。
type MergeConflict struct {
	Path       string
	BaseAttr   *Inode
	OursAttr   *Inode
	TheirsAttr *Inode
	Reason     string
}

// MergeParams 定义分支合并参数。
type MergeParams struct {
	DiskID       string
	SourceBranch string
	TargetBranch string
	Strategy     ConflictStrategy
}

// ResolveMergeParams 指定待完成的合并。
type ResolveMergeParams struct {
	DiskID     string
	BranchName string
	Message    string
}

// AbortMergeParams 指定待取消的合并。
type AbortMergeParams struct {
	DiskID     string
	BranchName string
}

// MergeResult 描述合并后的快照或冲突。
type MergeResult struct {
	Result        MergeResultType
	MergeCommitID *string
	Conflicts     []MergeConflict
}

// DiffChangeType 表示路径的变更类型。
type DiffChangeType int32

const (
	DiffChangeTypeUnspecified DiffChangeType = 0
	DiffChangeTypeAdded       DiffChangeType = 1
	DiffChangeTypeModified    DiffChangeType = 2
	DiffChangeTypeDeleted     DiffChangeType = 3
	DiffChangeTypeRenamed     DiffChangeType = 4
)

// DiffParams 指定待比较的磁盘和版本。
type DiffParams struct {
	DiskID  string
	FromRef string
	ToRef   string
}

// DiffEntry 描述路径、属性及变更的数据块。
type DiffEntry struct {
	ChangeType           DiffChangeType
	Path                 string
	OldPath              string
	OldAttr              *Inode
	NewAttr              *Inode
	ModifiedBlockIndices []uint64
}

// CreateDiskParams 描述创建虚拟磁盘入参。
type CreateDiskParams struct {
	Name     string
	MaxBytes uint64
}

// ListDisksParams 描述分页查询虚拟磁盘列表入参。
type ListDisksParams struct {
	PageSize  uint32
	PageToken string
}

// ListDisksResult 描述分页查询虚拟磁盘列表返回结果。
type ListDisksResult struct {
	Disks         []*Disk
	NextPageToken string
}

// UpdateDiskParams 描述更新虚拟磁盘入参。
type UpdateDiskParams struct {
	DiskID   string
	Name     *string
	MaxBytes *uint64
}

// StatFSResult 描述磁盘文件系统统计状态（对接 Linux FUSE FUSE_STATFS = 17 协议）。
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

// ReadRequest 定义文件数据流读取请求参数。
type ReadRequest struct {
	Ino       uint64
	Fh        uint64
	Offset    uint64
	Size      uint32
	SessionID *uint64
}

// ReadResponse 包含文件数据流读取返回的数据载荷。
type ReadResponse struct {
	Data []byte
}

// WriteRequest 定义文件数据流写入请求参数。
type WriteRequest struct {
	Ino       uint64
	Fh        uint64
	Offset    uint64
	Data      []byte
	SessionID *uint64
}

// WriteResponse 包含文件数据流写入返回的实际写入大小与更新后的 Inode 元数据。
type WriteResponse struct {
	Size  uint32
	Inode *Inode
}
