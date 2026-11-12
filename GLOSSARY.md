# Bowl 分布式存储系统

Bowl 是面向沙箱（Sandbox）的分布式虚拟磁盘文件系统，涵盖客户端自治的 POSIX 命名空间缓存与句柄/文件锁运行时、基于写世代号栅栏（Epoch）的无会话一写多读（SWMR）并发控制，以及按虚拟磁盘隔离的数据封包存储与实时逻辑配额管理。

## Storage 元数据与命名空间

**Inode (索引节点)**:
虚拟磁盘文件系统中标识文件、目录或符号链接等实体的唯一持久化数字标识符与元数据载体。
_Avoid_: FileID, NodeID, ObjectId, VNode

**Entry (目录项)**:
父目录下将名称映射到目标 Inode 的单向命名绑定关系。
_Avoid_: Dentry, FileItem, DirRecord, Record

**Lookup (路径查找)**:
根据目标 Disk、父目录 Inode 与目标名称，解析并获取对应 Entry 及其目标 Inode 属性的元数据检索操作。
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
从指定 Disk 的父目录移除非目录 Entry 并递减目标 Inode 硬链接计数的元数据变更操作；若目标文件在写端本地无打开句柄则立即连同数据物理删除，若仍有打开句柄则转为 Orphan Inode 延迟回收。
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
所有目录 Entry 均已解链（硬链接计数为零）但因写端本地仍持有打开 FileHandle 而暂未物理删除的 Inode 实体；正常关闭最后一个句柄时由客户端发起 ReclaimOrphan 回收，写端异常终止残留的孤儿节点在下一次可写 MountDisk 递增 Epoch 时由服务端自动清扫。
_Avoid_: ZombieNode, DanglingInode, DeletedFile

**ReclaimOrphan (孤儿节点回收)**:
可写客户端在目标 Orphan Inode 的最后一个本地 FileHandle 关闭后，携带有效写 Epoch 通知服务端彻底物理删除该节点并释放其已用逻辑空间的元数据变更操作。
_Avoid_: PurgeOrphan, CleanOrphan, DeleteOrphan, ReleaseInode

**Attribute (文件属性)**:
描述 Inode 状态与控制参数的集合（涵盖文件类型与权限模式、逻辑字节大小、硬链接计数、属主与属组、块统计、访问/修改/状态变更时间戳及文件系统标志位）。
_Avoid_: Stat, Meta, Properties, Info

**Generation (代数)**:
标识 Inode 分配复用周期的单调版本号，用于防止缓存引用已释放后重新分配的节点。
_Avoid_: Version, Revision, InodeEpoch

**ReadDir (目录读取)**:
根据目标 Disk、目录 Inode 与游标偏移量，合成当前目录与父目录特殊项并分页拉取子项 Entry 列表的元数据检索操作。
_Avoid_: ListDir, ReadEntries, FetchDir, ScanDir

**ReadDirPlus (增强目录读取)**:
在 ReadDir 基础上内联返回子项完整 Inode 属性与缓存时效的批量元数据检索操作。
_Avoid_: ReadDirWithAttr, BatchLookup, ScanDirPlus

**Fsync (文件同步)**:
排空目标文件在客户端未提交的缓冲数据并确认服务端完成数据与元数据持久化落盘的显式屏障操作。
_Avoid_: Sync, CommitFile, FlushDisk

**FsyncDir (目录同步)**:
确认目标目录已提交的目录项与状态变更已在服务端完成持久化落盘的显式屏障操作。
_Avoid_: SyncDir, CommitDir, FlushDir

**Fallocate (空间预分配与打洞)**:
针对常规文件 Inode 执行逻辑空间预留扩展或指定区间零字节打洞并联动权限与时间戳的变更操作。
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
对外交付的顶层独立文件系统实例，由全局唯一标识符标识，独占维护自身根目录 Inode、容量配额上限、实时已用逻辑空间与写世代号。
_Avoid_: Volume, Drive, MountPoint, Partition, Bucket, Repo

**Epoch (写世代号)**:
归属于单个 Disk 的单调递增写权限栅栏令牌，每次可写 MountDisk 握手时原子递增并颁发给新写端；服务端通过校验写请求携带的 Epoch 与当前权威值一致来实时拦截过期写端。
_Avoid_: SessionID, LeaseToken, Term, Generation, FencingToken

**Blob (文件数据块)**:
常规文件按固定逻辑网格边界切分的数据分片单元，按实际有效字节长度紧凑追加存储于所属 Disk 的 Pack 容器内。
_Avoid_: Chunk, Slice, Block, Page, Cell, Sector, ObjectFile, Delta

**Pack (数据封包)**:
归属于单个 Disk、承载多个 Blob 连续追加写入的只追加物理存储容器；写满封口后转为不可变只读状态，并在有效数据占比低于水位线或归零时按容器粒度搬迁与回收。
_Avoid_: Segment, ExtentFile, SSTable, Archive, VolumeFile, BlobFile

**Read (文件数据读取)**:
根据目标 Disk、常规文件 Inode、起始字节偏移量与期望长度，检索并返回对应区间字节流的无状态读取操作。
_Avoid_: FetchData, GetBytes, ReadFile, PullStream

**Write (文件数据写入)**:
可写客户端携带有效写 Epoch 向目标 Disk 的常规文件指定偏移区间提交字节数据，并实时校验与更新文件属性及单盘已用逻辑配额的变更操作。
_Avoid_: PutData, WriteBytes, AppendFile, PushStream

**MountDisk (磁盘挂载握手)**:
客户端挂载目标 Disk 时向服务端发起的单次握手操作：只读挂载仅校验 Disk 存在并返回根目录属性（Epoch 为零）；可写挂载原子递增并持久化该 Disk 的权威 Epoch、清扫上一任写端残留的 Orphan Inode 并返回新 Epoch 与根目录属性。
_Avoid_: RenewSession, OpenSession, AttachDisk, AcquireLease

**CreateDisk (磁盘创建)**:
注册新 Disk 身份并原子初始化其根目录 Inode、容量配额上限与写世代号的生命周期建立操作。
_Avoid_: NewDisk, InitVolume, ProvisionDisk, MakeDisk

**GetDisk (磁盘查询)**:
根据目标 Disk 唯一标识符，检索其身份信息、容量配额上限、实时已用逻辑空间与已用 Inode 数量的状态查询操作。
_Avoid_: DescribeDisk, FetchDisk, StatDisk, InspectDisk

**ListDisks (磁盘列表查询)**:
按创建时间倒序与游标分页规则，批量检索 Disk 实例及其配额与实时逻辑用量状态的列表查询操作。
_Avoid_: ScanDisks, GetAllDisks, EnumDisks, QueryDisks

**UpdateDisk (磁盘更新)**:
根据目标 Disk 唯一标识符，按需修改磁盘名称或调整容量配额上限（不低于当前已用逻辑空间）的生命周期变更操作。
_Avoid_: ResizeDisk, ExpandDisk, ShrinkDisk, ModifyDisk, PatchDisk

**DeleteDisk (磁盘删除)**:
根据目标 Disk 唯一标识符，关闭并注销该磁盘实例、彻底回收其全部元数据与底层物理存储空间的生命周期释放操作。
_Avoid_: RemoveDisk, DestroyDisk, DropVolume, PurgeDisk

**StatFS (文件系统状态查询)**:
检索目标 Disk 的总配额上限、已用逻辑空间、剩余可用空间、已用 Inode 统计及 POSIX 文件系统块指标的状态查询操作。
_Avoid_: GetDiskStat, StatVFS, DiskInfo, GetCapacity

## Client 客户端挂载与本地 POSIX 运行时

**FileHandle (本地文件句柄)**:
客户端在本地内存为单次文件或目录打开会话分配的不透明数字标识符，用于在客户端闭环维护打开引用计数、异步写错误游标及打开期间被解链孤儿节点的延迟回收。
_Avoid_: Fd, FileDescriptor, SessionID, HandleID, StreamID

**Create (文件创建并打开)**:
可写客户端携带有效写 Epoch 在指定父目录下原子创建常规文件并绑定 Entry，同时在本地分配 FileHandle 并更新命名空间缓存的复合操作。
_Avoid_: CreateAndOpen, TouchAndOpen, NewFile

**TmpFile (匿名临时文件创建)**:
客户端创建未绑定任何 Entry（硬链接计数为零）的匿名常规文件并分配本地 FileHandle 的操作；若关闭前未通过 Link 具名化，其生命周期随最后一个句柄关闭而终结。
_Avoid_: CreateTemp, MakeAnonFile, OpenTmpFile

**Open (文件打开)**:
客户端针对已存在的非目录 Inode 建立打开会话（按需执行截断）、分配本地 FileHandle 并递增本地打开引用计数的操作。
_Avoid_: OpenFile, AcquireHandle, ConnectFile

**Flush (句柄冲刷)**:
单个文件描述符关闭时触发的会话冲刷操作，负责排空目标文件在客户端暂存与在途的异步写入、向当前 FileHandle 兑现未消费的异步写错误，并在本地释放对应 LockOwner 持有的 POSIX 记录锁（不触发服务端持久化落盘屏障）。
_Avoid_: Close, SyncHandle, Drain

**Release (句柄释放)**:
客户端销毁指定 FileHandle、递减目标 Inode 本地打开引用计数，并在写端引用计数归零且该节点处于已持久化 Orphan Inode 状态时通过 ReclaimOrphan 通知服务端物理回收的会话终止操作。
_Avoid_: CloseFile, DestroyHandle, FreeHandle

**OpenDir (目录打开)**:
客户端校验目标节点为目录并在本地分配目录 FileHandle 以建立目录读取会话的纯客户端操作。
_Avoid_: OpenDirectory, AcquireDirHandle, StartDirStream

**ReleaseDir (目录句柄释放)**:
客户端销毁指定目录 FileHandle 以终止目录读取会话的纯客户端操作。
_Avoid_: CloseDir, DestroyDirHandle, FreeDirHandle

**FileLock (本地文件锁)**:
客户端在本地内存关联于目标 Inode 的运行期并发协调契约（涵盖 POSIX 字节区间记录锁与 BSD `flock` 整文件锁），在本地完成冲突探测（GetLk）、非阻塞设置（SetLk）与阻塞等待（SetLkw）。
_Avoid_: RecordLock, RangeLock, Flock, LockEntry

**LockOwner (锁拥有者)**:
内核为锁持有主体（POSIX 进程文件表或 BSD `flock` 打开文件表项）分配的本地不透明身份令牌。
_Avoid_: OwnerID, LockHolder, ClientID, HolderID

**Mount (磁盘挂载)**:
客户端通过 MountDisk 握手获取目标 Disk 的写 Epoch（或以只读模式校验目标 Disk）、初始化本地运行时缓存并绑定至宿主机目标挂载点的生命周期建立操作。
_Avoid_: Attach, BindDisk, ConnectVolume

**Umount (磁盘卸载)**:
排空客户端全部未同步脏数据与在途操作并解除宿主机目标挂载点绑定的纯客户端生命周期释放操作。
_Avoid_: Detach, UnbindDisk, DisconnectVolume
