# Bowl 分布式存储系统

Bowl 负责管理面向沙箱（Sandbox）的分布式文件系统，涵盖客户端自治的 POSIX 目录树与句柄/文件锁语义、服务端基于嵌入式键值与追加封包（Append-Only Pack）的单盘存储引擎、基于写世代号栅栏（Epoch Fencing）的无会话一写多读（SWMR）并发控制及单盘实时逻辑配额核算。

## Storage 元数据与存储引擎

**Inode (索引节点)**:
文件系统中标识文件或目录实体的唯一持久化数字标识符与元数据载体。
_Avoid_: FileID, NodeID, ObjectId, VNode

**Entry (目录项)**:
父目录下将名称映射到目标 Inode 的单向命名绑定关系。
_Avoid_: Dentry, FileItem, DirRecord, Record

**Lookup (路径查找)**:
根据目标 Disk 与父级 Inode 及目标名称，解析并获取对应 Entry 及其 Inode 属性的元数据检索操作。
_Avoid_: Find, Search, GetFile, Query

**GetAttr (属性获取)**:
根据目标 Disk 与目标 Inode 编号，直接检索并获取该 Inode 及其 Attribute 的元数据检索操作。
_Avoid_: Stat, GetFileInfo, FetchAttr, DescribeNode

**SetAttr (属性设置)**:
根据目标 Disk、有效写 Epoch 与目标 Inode 编号，按需修改该 Inode 的可变 Attribute（如权限模式、文件大小、所属用户与组、访问与修改时间戳、文件系统标志位）并刷新状态变更时间的元数据变更操作。
_Avoid_: Chmod, Chown, Truncate, UpdateAttr, ModifyNode, PatchInode

**Mknod (节点创建)**:
在指定 Disk 与父目录 Inode 下创建非目录文件节点并绑定同名 Entry 的元数据变更操作。
_Avoid_: CreateNode, MakeNode, Touch, CreateFile

**Mkdir (目录创建)**:
在指定 Disk 与父目录 Inode 下创建目录节点、绑定同名 Entry 并递增父目录硬链接计数的元数据变更操作。
_Avoid_: CreateDir, MakeDirectory, NewFolder

**Unlink (目录项解链)**:
从指定 Disk 的父目录移除非目录 Entry 并递减目标 Inode 硬链接计数的元数据变更操作；若目标文件在写端本地无打开句柄则立即连同数据块物理删除，若仍有打开句柄则转为孤儿节点并在最后一个句柄关闭时触发物理删除。
_Avoid_: DeleteFile, RemoveFile, Erase, DestroyNode

**Rmdir (空目录移除)**:
从指定 Disk 的父目录移除空目录 Entry、将目标目录硬链接计数归零并递减父目录硬链接计数的元数据变更操作。
_Avoid_: DeleteDir, RemoveFolder, PurgeDir

**Rename (目录项重命名)**:
在指定 Disk 的同一或不同父目录间原子性地移动、覆盖或交换 Entry 绑定关系（支持禁止覆盖与双向交换模式）的元数据变更操作。
_Avoid_: Move, Mv, Relocate, Rebind

**Link (硬链接创建)**:
在指定 Disk 的父目录下为已存在的非目录 Inode（或未携带独占标志的活跃匿名临时文件）绑定新 Entry 并递增其硬链接计数的元数据变更操作。
_Avoid_: HardLink, AddLink, AliasNode

**Symlink (符号链接创建)**:
在指定 Disk 的父目录下创建存储目标路径字节序列的符号链接 Inode 并绑定同名 Entry 的元数据变更操作。
_Avoid_: SoftLink, CreateShortcut, MakeAlias

**Readlink (符号链接读取)**:
根据目标 Disk 与符号链接 Inode 编号，直接读取其目标路径字节序列的元数据检索操作。
_Avoid_: GetSymlink, ResolveLink, ReadTarget

**Orphan Inode (孤儿节点)**:
在单盘实时存储中，所有 Entry 均已解链（硬链接计数为零）但因写端本地仍持有打开句柄而暂未物理删除的 Inode 实体；正常关闭最后一个句柄时由客户端通知物理删除，写端异常崩溃残留的孤儿节点在下一次可写挂载递增 Epoch 时由服务端自动清扫回收。
_Avoid_: ZombieNode, DanglingInode, DeletedFile

**Attribute (文件属性)**:
描述 Inode 的物理状态与控制参数的集合（如大小、权限模式、硬链接数、时间戳、修改序列号等）。
_Avoid_: Stat, Meta, Properties, Info

**Generation (代数)**:
用于标识 Inode 复用周期的递增版本号，防止缓存引用已释放后重新分配的 Inode。
_Avoid_: Version, Revision, InodeEpoch

**ReadDir (目录读取)**:
根据目标 Disk、目录 Inode 与游标偏移量，合成 `.` / `..` 并分页拉取子项 Entry 列表的元数据检索操作。
_Avoid_: ListDir, ReadEntries, FetchDir, ScanDir

**ReadDirPlus (增强目录读取)**:
在 ReadDir 基础之上，内联预取并返回子项完整 Inode 属性与内核缓存时效的批量元数据检索操作。
_Avoid_: ReadDirWithAttr, BatchLookup, ScanDirPlus

**Fsync (文件同步)**:
将目标文件未同步的脏数据立即刷入服务端存储引擎并确认持久化落盘的显式屏障操作。
_Avoid_: Sync, CommitFile, FlushDisk

**FsyncDir (目录同步)**:
确认目标目录的已提交目录项与状态元数据已在服务端存储引擎持久化落盘的同步屏障操作。
_Avoid_: SyncDir, CommitDir, FlushDir

**Fallocate (空间预分配与打洞)**:
针对常规文件 Inode 执行空间预留扩展或指定区间零字节打洞并联动权限与时间戳的变更操作。
_Avoid_: AllocSpace, PunchHole, Preallocate, ReserveBlocks

**Xattr (扩展属性)**:
关联于 Inode 的持久化名称-二进制值键值对元数据。
_Avoid_: ExtendedAttr, CustomMeta, Tag, Property, UserAttr

**SetXattr (扩展属性设置)**:
根据目标 Disk、有效写 Epoch、目标 Inode 编号、属性名称、二进制值与创建或替换语义标志，新增或更新目标 Inode 的 Xattr 并刷新状态变更时间的元数据变更操作。
_Avoid_: PutXattr, WriteXattr, UpdateXattr, AddTag

**GetXattr (扩展属性获取)**:
根据目标 Disk、目标 Inode 编号与属性名称，检索并获取对应 Xattr 二进制值的元数据检索操作。
_Avoid_: ReadXattr, FetchXattr, QueryXattr

**ListXattr (扩展属性列表获取)**:
根据目标 Disk 与目标 Inode 编号，检索并按名称字节序升序返回该 Inode 绑定的全部 Xattr 名称列表的元数据检索操作。
_Avoid_: GetXattrs, ScanXattr, EnumXattr

**RemoveXattr (扩展属性移除)**:
根据目标 Disk、有效写 Epoch、目标 Inode 编号与属性名称，删除指定 Xattr 并刷新目标 Inode 状态变更时间的元数据变更操作。
_Avoid_: DeleteXattr, DelXattr, UnsetXattr, DropXattr

## Storage 单盘控制面与数据引擎

**Disk (虚拟磁盘)**:
对外交付的顶层独立文件系统实例，由全局唯一 UUID 标识，底层一对一绑定独立的单盘存储引擎实例并维护自身容量配额上限、实时已用空间与写世代号。
_Avoid_: Volume, Drive, MountPoint, Partition, Bucket, Repo

**Epoch (写世代号)**:
单盘存储引擎维护的单调递增 64 位无符号整数栅栏令牌，每次可写挂载握手时原子递增并颁发给新写端；服务端通过比对写请求携带的 Epoch 与当前权威 Epoch 是否相等来实时拦截过期写端（无需后台心跳会话租约）。
_Avoid_: SessionID, LeaseToken, Term, Generation, FencingToken

**Blob (文件数据块)**:
常规文件按 `256 KiB` 固定边界编号切分的逻辑数据分片载体，按实际字节长度追加合装于所属 Disk 目录下的 `64 MiB` 只追加封包文件（`packs/<pack_id>.pack`）内，并由存储引擎维护 16 字节 `BlobRef`（`PackID`、包内偏移 `Offset` 与有效长度 `Length`）定位索引；覆写与删除产生的旧空间在 `.pack` 存活率低于水位线时通过搬迁回收。
_Avoid_: Chunk, Slice, Block, Page, Cell, Sector, ObjectFile, Delta

**Read (文件数据读取)**:
根据目标 Disk、常规文件 Inode、字节偏移量与期望长度，从单盘存储引擎检索并返回对应区间字节流的无状态读取操作。
_Avoid_: FetchData, GetBytes, ReadFile, PullStream

**Write (文件数据写入)**:
可写客户端携带有效写 Epoch 将写入改动提交至目标 Disk 存储引擎（块数据按实际长度追加写入活跃 `.pack` 封包文件，轻量 `BlobRef` 与 Inode 元数据批量提交至存储引擎），并实时校验与更新单盘逻辑配额用量的写入操作。
_Avoid_: PutData, WriteBytes, AppendFile, PushStream

**MountDisk (磁盘挂载握手)**:
客户端在挂载目标 Disk 时向服务端发起的单次握手操作：只读挂载仅校验 Disk 存在并返回根目录属性（Epoch 为 0）；可写挂载原子递增并持久化该 Disk 的权威写 Epoch、清扫上一任写端残留的 Orphan Inode 并返回新 Epoch 与根目录属性。
_Avoid_: RenewSession, OpenSession, AttachDisk, AcquireLease

**CreateDisk (磁盘创建)**:
在控制面注册 Disk 身份并在单盘存储引擎中初始化根目录 Inode、初始容量配额上限与写世代号的生命周期建立操作。
_Avoid_: NewDisk, InitVolume, ProvisionDisk, MakeDisk

**GetDisk (磁盘查询)**:
根据目标 Disk 唯一标识符，聚合控制面身份信息与单盘存储引擎中的配额上限、实时已用逻辑空间与节点数的状态检索操作。
_Avoid_: DescribeDisk, FetchDisk, StatDisk, InspectDisk

**ListDisks (磁盘列表查询)**:
按创建时间倒序与游标分页规则，批量检索 Disk 实例并聚合各盘存储引擎的配额上限与实时逻辑用量的检索操作。
_Avoid_: ScanDisks, GetAllDisks, EnumDisks, QueryDisks

**UpdateDisk (磁盘更新)**:
根据目标 Disk 唯一标识符，按需修改控制面磁盘名称或透传调整单盘存储引擎的容量配额上限（校验不低于当前已用逻辑空间）的生命周期变更操作。
_Avoid_: ResizeDisk, ExpandDisk, ShrinkDisk, ModifyDisk, PatchDisk

**DeleteDisk (磁盘删除)**:
根据目标 Disk 唯一标识符，原子关闭其单盘存储引擎实例、从控制面注销记录并清理底层物理目录以释放空间的生命周期释放操作。
_Avoid_: RemoveDisk, DestroyDisk, DropVolume, PurgeDisk

**StatFS (文件系统状态查询)**:
从目标 Disk 的单盘存储引擎直接检索总配额上限、已用逻辑空间、可用空间、Inode 节点统计及派生 POSIX 文件系统块指标的状态检索操作。
_Avoid_: GetDiskStat, StatVFS, DiskInfo, GetCapacity

## Client 客户端挂载与本地 POSIX 运行时

**FileHandle (本地文件句柄)**:
客户端在本地内存为单次文件或目录打开会话分配的不透明数字标识符，用于在客户端本地闭环维护打开引用计数及在写端驱动打开期间被解链孤儿节点的延迟回收（服务端无句柄概念）。
_Avoid_: Fd, FileDescriptor, SessionID, HandleID, StreamID

**Create (文件创建并打开)**:
客户端在指定父目录下创建常规文件、分配本地 FileHandle 并在本地权威目录树登记，携带有效写 Epoch 同步写入服务端存储引擎的操作。
_Avoid_: CreateAndOpen, TouchAndOpen, NewFile

**TmpFile (匿名临时文件创建)**:
客户端在本地创建未绑定 Entry（硬链接计数为零）的匿名常规文件并分配本地 FileHandle 的操作（若关闭前未通过 Link 转正且未触发数据上云，则全程在客户端本地销毁）。
_Avoid_: CreateTemp, MakeAnonFile, OpenTmpFile

**Open (文件打开)**:
客户端针对已存在的非目录 Inode 在本地建立打开会话、分配本地 FileHandle 并递增本地打开引用计数的纯客户端操作。
_Avoid_: OpenFile, AcquireHandle, ConnectFile

**Flush (句柄冲刷)**:
在单个文件描述符关闭时触发、在客户端本地释放对应 LockOwner 所持本地 POSIX 记录锁的会话操作（常规写入已通过 Write-Through 写入服务端 OS Page Cache 与异步 Pebble WAL，Flush 不触发服务端物理磁盘 `fsync`）。
_Avoid_: Close, SyncHandle, Drain

**Release (句柄释放)**:
客户端在本地销毁指定 FileHandle、递减目标 Inode 本地打开引用计数，并在写端引用计数归零且该节点处于已上云 Orphan Inode 状态时携带写 Epoch 通知服务端物理删除的会话终止操作。
_Avoid_: CloseFile, DestroyHandle, FreeHandle

**OpenDir (目录打开)**:
客户端针对已存在的目录 Inode 在本地建立目录读取会话并缓存完整目录项流的纯客户端操作。
_Avoid_: OpenDirectory, AcquireDirHandle, StartDirStream

**ReleaseDir (目录句柄释放)**:
客户端在本地销毁指定目录 FileHandle 并释放对应内存目录流缓存的纯客户端操作。
_Avoid_: CloseDir, DestroyDirHandle, FreeDirHandle

**FileLock (本地文件锁)**:
客户端在本地内存关联于目标 Inode 的运行期同步契约（涵盖 POSIX 字节区间记录锁与 BSD `flock` 整文件锁），在客户端本地实现零网络往返的加锁、冲突探测（GetLk）、非阻塞设置（SetLk）与阻塞等待（SetLkw）。
_Avoid_: RecordLock, RangeLock, Flock, LockEntry

**LockOwner (锁拥有者)**:
FUSE 内核为锁持有主体（POSIX 进程文件表或 BSD `flock` 打开文件表项）分配的 64 位本地不透明令牌。
_Avoid_: OwnerID, LockHolder, ClientID, HolderID

**Mount (磁盘挂载)**:
客户端通过 `MountDisk` 握手获取目标 Disk 的写 Epoch（或以只读模式无状态校验目标 Disk）、初始化本地目录树缓存并挂载至宿主机目标目录的生命周期建立操作（无后台心跳协程）。
_Avoid_: Attach, BindDisk, ConnectVolume

**Umount (磁盘卸载)**:
排空客户端本地未同步脏数据并解除宿主机目标目录上 FUSE 挂载绑定的纯客户端生命周期释放操作（无需调用服务端卸载 RPC；CLI 兼容 `unmount` 别名）。
_Avoid_: Detach, UnbindDisk, DisconnectVolume
