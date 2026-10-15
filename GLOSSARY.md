# Stronger 元数据引擎

Stronger 元数据引擎负责管理分布式文件系统的目录树、节点索引及 POSIX 文件系统协议映射。

## 术语规范

**Inode (索引节点)**:
文件系统中标识文件或目录实体的唯一持久化数字标识符与元数据载体。
_Avoid_: FileID, NodeID, ObjectId, VNode

**Entry (目录项)**:
父目录下将名称映射到目标 Inode 的单向命名绑定关系。
_Avoid_: Dentry, FileItem, DirRecord, Record

**Lookup (路径查找)**:
根据父级 Inode 与目标名称，解析并获取对应 Entry 及其 Inode 属性的元数据检索操作。
_Avoid_: Find, Search, GetFile, Query

**Attribute (文件属性)**:
描述 Inode 的物理状态与内核控制参数的集合（如大小、权限模式、硬链接数、时间戳等）。
_Avoid_: Stat, Meta, Properties, Info

**Generation (代数)**:
用于标识 Inode 复用周期的递增版本号，防止客户端缓存引用已释放后重新分配的 Inode。
_Avoid_: Version, Revision, Epoch
