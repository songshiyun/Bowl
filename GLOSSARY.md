# Bowl 分布式存储系统

Bowl 负责管理面向沙箱（Sandbox）的分布式文件系统，涵盖客户端自治的 POSIX 目录树与句柄/文件锁语义、服务端基于嵌入式键值与固定块的数据存储、单盘逻辑分支树（Branch Tree）与一写多读（SWMR）生命周期、Git 风格版本演进（Commit / Fork / Merge / Rollback / Diff）及整棵树共享的用户视角逻辑配额核算。

## Storage 元数据与存储引擎

**Inode (索引节点)**:
文件系统中标识文件或目录实体的唯一持久化数字标识符与元数据载体。
_Avoid_: FileID, NodeID, ObjectId, VNode

**Entry (目录项)**:
父目录下将名称映射到目标 Inode 的单向命名绑定关系。
_Avoid_: Dentry, FileItem, DirRecord, Record

**Lookup (路径查找)**:
根据父级 Inode 与目标名称，解析并获取对应 Entry 及其 Inode 属性的元数据检索操作。
_Avoid_: Find, Search, GetFile, Query

**GetAttr (属性获取)**:
根据目标 Inode 编号，直接检索并获取该 Inode 及其 Attribute 的元数据检索操作。
_Avoid_: Stat, GetFileInfo, FetchAttr, DescribeNode

**SetAttr (属性设置)**:
根据目标 Inode 编号，按需修改该 Inode 的可变 Attribute（如权限模式、文件大小、所属用户与组、访问与修改时间戳、文件系统标志位）并刷新状态变更时间的元数据变更操作。
_Avoid_: Chmod, Chown, Truncate, UpdateAttr, ModifyNode, PatchInode

**Mknod (节点创建)**:
在指定父目录 Inode 下创建非目录文件节点并绑定同名 Entry 的元数据变更操作。
_Avoid_: CreateNode, MakeNode, Touch, CreateFile

**Mkdir (目录创建)**:
在指定父目录 Inode 下创建目录节点、绑定同名 Entry 并递增父目录硬链接计数的元数据变更操作。
_Avoid_: CreateDir, MakeDirectory, NewFolder

**Unlink (目录项解链)**:
从指定父目录移除非目录 Entry 并递减目标 Inode 硬链接计数的元数据变更操作；若目标文件在写端本地无打开句柄则立即连同数据块物理删除，若仍有打开句柄则转为孤儿节点并在最后一个句柄关闭时触发物理删除。
_Avoid_: DeleteFile, RemoveFile, Erase, DestroyNode

**Rmdir (空目录移除)**:
从指定父目录移除空目录 Entry、将目标目录硬链接计数归零并递减父目录硬链接计数的元数据变更操作。
_Avoid_: DeleteDir, RemoveFolder, PurgeDir

**Rename (目录项重命名)**:
在同一或不同父目录间原子性地移动、覆盖或交换 Entry 绑定关系（支持禁止覆盖与双向交换模式）的元数据变更操作。
_Avoid_: Move, Mv, Relocate, Rebind

**Link (硬链接创建)**:
在指定父目录下为已存在的非目录 Inode（或未携带独占标志的活跃匿名临时文件）绑定新 Entry 并递增其硬链接计数的元数据变更操作。
_Avoid_: HardLink, AddLink, AliasNode

**Symlink (符号链接创建)**:
在指定父目录下创建存储目标路径字节序列的符号链接 Inode 并绑定同名 Entry 的元数据变更操作。
_Avoid_: SoftLink, CreateShortcut, MakeAlias

**Readlink (符号链接读取)**:
根据目标符号链接 Inode 编号，直接读取其目标路径字节序列的元数据检索操作。
_Avoid_: GetSymlink, ResolveLink, ReadTarget

**Orphan Inode (孤儿节点)**:
在写端独占的分支实时工作区中，所有 Entry 均已解链（硬链接计数为零）但因写端本地仍持有打开句柄而暂未物理删除的 Inode 实体；正常关闭最后一个句柄时当场物理删除，仅在分支无活跃写端时由后台回收。
_Avoid_: ZombieNode, DanglingInode, DeletedFile

**Attribute (文件属性)**:
描述 Inode 的物理状态与控制参数的集合（如大小、权限模式、硬链接数、时间戳、修改序列号等）。
_Avoid_: Stat, Meta, Properties, Info

**Generation (代数)**:
用于标识 Inode 复用周期的递增版本号，防止缓存引用已释放后重新分配的 Inode。
_Avoid_: Version, Revision, Epoch

**ReadDir (目录读取)**:
根据目录 Inode 与游标偏移量，合成 `.` / `..` 并分页拉取子项 Entry 列表的元数据检索操作。
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
根据目标 Inode 编号、属性名称、二进制值与创建或替换语义标志，新增或更新目标 Inode 的 Xattr 并刷新状态变更时间的元数据变更操作。
_Avoid_: PutXattr, WriteXattr, UpdateXattr, AddTag

**GetXattr (扩展属性获取)**:
根据目标 Inode 编号与属性名称，检索并获取对应 Xattr 二进制值的元数据检索操作。
_Avoid_: ReadXattr, FetchXattr, QueryXattr

**ListXattr (扩展属性列表获取)**:
根据目标 Inode 编号，检索并按名称字节序升序返回该 Inode 绑定的全部 Xattr 名称列表的元数据检索操作。
_Avoid_: GetXattrs, ScanXattr, EnumXattr

**RemoveXattr (扩展属性移除)**:
根据目标 Inode 编号与属性名称，删除指定 Xattr 并刷新目标 Inode 状态变更时间的元数据变更操作。
_Avoid_: DeleteXattr, DelXattr, UnsetXattr, DropXattr

## Storage 数据与分支版本引擎

**Disk (虚拟磁盘)**:
对外交付的顶层文件系统实例与版本树容器，由全局唯一 UUID 标识，内部承载一棵由根分支（`main`）及若干派生逻辑分支（Branch）组成的 Git 风格版本树，整棵树的所有分支与历史快照共享唯一的用户视角逻辑配额上限。
_Avoid_: Volume, Drive, MountPoint, Partition, Bucket, Repo

**Branch (逻辑分支)**:
隶属于单个 Disk 内部的可独立演进与挂载的逻辑工作区节点（默认包含根分支 `main`），每个 Branch 遵循「最多 1 个可写会话独占实时工作区、多个只读会话挂载不可变快照」的一写多读（SWMR）边界，底层对应独立的嵌入式存储实例并通过硬链接共享未修改物理数据。
_Avoid_: ChildDisk, SubVolume, Workspace, ForkDisk

**Fork (分支派生)**:
在同一个 Disk 内部，基于指定父 Branch 的 Commit 快照通过底层硬链接检查点零拷贝派生出新逻辑 Branch 的操作。
_Avoid_: CloneDisk, CopyVolume, Duplicate, Split

**Commit (版本快照)**:
在排空目标 Branch 写端未同步改动后，对该 Branch 当前存储状态生成硬链接检查点（Checkpoint）并记录父版本指针而定格成的不可变、可回档、可派生分支、可合并与可对比的时间点版本快照。
_Avoid_: Backup, Archive, Tag, Savepoint

**Rollback (版本回档)**:
在确认目标 Branch 无活跃可写会话的前提下，将该 Branch 的实时工作区原子重置回指定历史 Commit 快照的生命周期操作。
_Avoid_: Revert, Reset, Restore, Undo

**Merge (分支合并)**:
在确认目标 Branch 无活跃可写会话的前提下，由控制面基于源 Branch 与目标 Branch 的最近共同祖先（LCA）Commit 执行文件级三路合并（3-Way Merge），自动合入无冲突变更并支持冲突清单返回、策略裁决或工作区冲突物化解决后生成双亲 Commit 的版本汇聚操作。
_Avoid_: Combine, Join, SyncBranch, Rebase

**Conflict (合并冲突)**:
在三路 Merge 过程中，源 Branch 与目标 Branch 自共同祖先以来对同一文件或目录项作出了不兼容修改而需由控制面策略或工作区人工裁决的待决状态。
_Avoid_: Collision, Clash, DiffError

**Diff (版本差异对比)**:
服务端就近对比同一 Disk 内两个 Commit（或 Branch 工作区）之间的变更文件与变更数据块索引并返回结构化摘要，配合客户端按需拉取并渲染文本行级增删对比或二进制摘要的检索操作。
_Avoid_: Compare, DeltaView, ChangeLog, PatchView

**Blob (文件数据块)**:
常规文件按固定边界编号切分的数据分片载体，支持整片覆盖写入、局部字节补丁追加、区间删除与局部置零打洞，并由存储引擎后台自动合并整理。
_Avoid_: Chunk, Slice, Block, Page, Cell, Sector, ObjectFile, Delta

**Read (文件数据读取)**:
根据目标常规文件 Inode、字节偏移量与期望长度，从目标 Branch 存储引擎检索并返回对应区间字节流的读取操作。
_Avoid_: FetchData, GetBytes, ReadFile, PullStream

**Write (文件数据写入)**:
可写客户端将写入改动提交至目标 Branch 存储引擎、按整棵 Disk 树跨分支同 Inode 最大逻辑体积去重核算配额，并由存储引擎持久化与后台折叠整理的写入操作。
_Avoid_: PutData, WriteBytes, AppendFile, PushStream

**CreateDisk (磁盘创建)**:
在逻辑层注册 Disk 身份、初始配额上限与默认根分支 `main`，并初始化其底层存储目录与根目录 Inode 的生命周期建立操作。
_Avoid_: NewDisk, InitVolume, ProvisionDisk, MakeDisk

**GetDisk (磁盘查询)**:
根据目标 Disk 唯一标识符，获取其身份、逻辑分支树状态、配额上限及整棵树去重后的用户视角实时逻辑已用空间与节点数的状态检索操作。
_Avoid_: DescribeDisk, FetchDisk, StatDisk, InspectDisk

**ListDisks (磁盘列表查询)**:
按创建时间倒序与游标分页规则，批量检索 Disk 实例并返回其分支树摘要与用户视角实时逻辑用量的检索操作。
_Avoid_: ScanDisks, GetAllDisks, EnumDisks, QueryDisks

**UpdateDisk (磁盘更新)**:
根据目标 Disk 唯一标识符，按需修改其名称或调整总配额上限（校验不低于当前用户视角实时逻辑已用空间）的生命周期变更操作。
_Avoid_: ResizeDisk, ExpandDisk, ShrinkDisk, ModifyDisk, PatchDisk

**DeleteDisk (磁盘删除)**:
在确认目标 Disk 下所有 Branch 均无未超期 ClientSession 挂载会话的前提下，从逻辑层注销该 Disk 并移除其底层全部分支工作区与历史快照目录以释放物理空间的生命周期释放操作。
_Avoid_: RemoveDisk, DestroyDisk, DropVolume, PurgeDisk

**StatFS (文件系统状态查询)**:
从目标 Disk 及当前挂载 Branch 检索总配额上限、整棵树用户视角已用逻辑空间、可用空间、Inode 节点统计及派生 POSIX 文件系统块指标的状态检索操作。
_Avoid_: GetDiskStat, StatVFS, DiskInfo, GetCapacity

## Client 客户端挂载与本地 POSIX 运行时

**ClientSession (客户端会话)**:
单次客户端挂载实例针对目标 Disk 的指定 Branch（或不可变 Commit）向服务端注册并按租约周期保活的运行期身份上下文，显式区分「单 Branch 独占可写会话」与「不可变快照共享只读会话」。
_Avoid_: MountSession, Connection, ClientLease, PeerSession

**RenewSession (会话续期)**:
客户端显式注册或延长其 ClientSession 租约有效期的保活操作，由服务端强控单个 Branch 唯一写端租约独占性。
_Avoid_: Heartbeat, PingSession, KeepAlive, TouchSession

**CloseSession (会话关闭)**:
客户端在正常卸载退出时显式注销其 ClientSession 租约（若为可写会话则同步清理该 Branch 实时工作区残留的无主孤儿节点）的生命周期终止操作。
_Avoid_: EndSession, DestroySession, LogoutSession, DisconnectSession

**FileHandle (本地文件句柄)**:
客户端在本地内存为单次文件或目录打开会话分配的不透明数字标识符，用于在客户端本地闭环维护打开引用计数及在写端驱动打开期间被解链孤儿节点的延迟回收（服务端无句柄概念）。
_Avoid_: Fd, FileDescriptor, SessionID, HandleID, StreamID

**Create (文件创建并打开)**:
客户端在指定父目录下创建常规文件、分配本地 FileHandle 并在本地权威目录树登记，同步写入服务端存储后端的操作。
_Avoid_: CreateAndOpen, TouchAndOpen, NewFile

**TmpFile (匿名临时文件创建)**:
客户端在本地创建未绑定 Entry（硬链接计数为零）的匿名常规文件并分配本地 FileHandle 的操作（若关闭前未通过 Link 转正且未触发数据上云，则全程在客户端本地销毁）。
_Avoid_: CreateTemp, MakeAnonFile, OpenTmpFile

**Open (文件打开)**:
客户端针对已存在的非目录 Inode 在本地建立打开会话、分配本地 FileHandle 并递增本地打开引用计数的纯客户端操作。
_Avoid_: OpenFile, AcquireHandle, ConnectFile

**Flush (句柄冲刷)**:
在单个文件描述符关闭时触发、在客户端本地将目标文件未同步脏数据刷入服务端并释放对应 LockOwner 所持本地 POSIX 记录锁的会话操作。
_Avoid_: Close, SyncHandle, Drain

**Release (句柄释放)**:
客户端在本地销毁指定 FileHandle、递减目标 Inode 本地打开引用计数，并在写端引用计数归零且该节点处于已上云 Orphan Inode 状态时通知服务端物理删除的会话终止操作。
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
根据目标 Disk 唯一标识符与指定逻辑 Branch（默认 `main`）建立独占可写 ClientSession（绑定该 Branch 实时工作区并在重连时清扫旧会话残留孤儿节点）或共享只读 ClientSession（绑定不可变 Commit 快照）、初始化根目录与按需懒加载的本地权威目录树缓存并挂载至宿主机目标目录的生命周期建立操作。
_Avoid_: Attach, BindDisk, ConnectVolume

**Umount (磁盘卸载)**:
排空客户端本地未同步脏数据、注销 ClientSession 租约并解除宿主机目标目录上 FUSE 挂载绑定的生命周期释放操作（CLI 兼容 `unmount` 别名）。
_Avoid_: Detach, UnbindDisk, DisconnectVolume
