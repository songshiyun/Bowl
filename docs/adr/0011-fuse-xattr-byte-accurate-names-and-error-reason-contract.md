# 0011. FUSE 扩展属性（Xattr）、文件名与软链目标改为字节流存储及具体错误码传递

## 状态
已采纳 (accepted)

## 背景
Storage 元数据引擎在完成目录遍历和 `Fallocate`（ADR-0010）后，需要支持 Linux FUSE 扩展属性的四个接口：`FUSE_SETXATTR (22)`、`FUSE_GETXATTR (23)`、`FUSE_LISTXATTR (24)` 和 `FUSE_REMOVEXATTR (25)`。在对接 Linux 内核 FUSE 协议时，遇到六个实际问题：
1. **Linux 文件名、扩展属性名和符号链接路径不一定是合法的 UTF-8 字符串**：在 Linux 内核（VFS）里，文件名（`dentry`）、扩展属性名（`xattr name`）和符号链接指向的路径（`symlink target`）可以是除了空字节 `\x00`（文件名还不能含 `/`）之外的任意字节序列，并不要求是合法的 UTF-8 文本。而我们之前在数据库里把 `entries.name` 定为 `VARCHAR(255)`（ADR-0001）、把 `inodes.symlink_target` 定为 `TEXT`（ADR-0007），在 Protobuf 里也用了 `string` 类型，遇到非 UTF-8 字节就会直接报错；而且 `protovalidate` 的 `bytes.pattern` 校验规则底层会强转成 `string(this)`，遇到非 UTF-8 字节也会直接崩溃。
2. **扩展属性允许 0 字节空值，且单个文件的属性名总长度不能超过 64KB**：POSIX `setxattr(2)` 允许写入长度为 `0` 字节的空属性值（这不等于删除属性），单个属性值最大为 `XATTR_SIZE_MAX = 65536` 字节；同时 Linux `listxattr(2)` 一次最多只能返回 `XATTR_LIST_MAX = 65536` 字节的属性名列表。如果不限制单个文件上所有扩展属性名的累计长度，一旦属性名加起来（算上结尾的 `\0`）超过 `65536` 字节，以后调 `listxattr` 就会永远返回 `E2BIG`，再也列不出这个文件的扩展属性了。
3. **FUSE 协议里的 `size` 缓冲区检查放在客户端还是服务端**：Linux FUSE 的 `FUSE_GETXATTR` 和 `FUSE_LISTXATTR` 请求里带有一个 `size` 参数（`size == 0` 时只查询需要多大的缓冲区；`size > 0` 时如果实际数据比 `size` 大就要返回 `ERANGE` 错误）。这个跟本地内存缓冲区大小紧密相关的检查，放在客户端本地做比传到服务端 RPC 里做更合适。
4. **扩展属性的权限检查（如 `user.*`、`trusted.*`）谁来管**：Linux 内核对扩展属性的检查分为两类：一类跟当前操作用户的身份权限有关（比如粘滞位目录、需要管理员 `CAP_SYS_ADMIN` 权限的 `trusted.*`、以及普通的 `rwx` 权限检查），另一类只看文件本身的类型（比如 `user.*` 属性不允许挂在符号链接或设备文件上）。我们需要明确客户端挂载参数怎么配、服务端负责检查哪一部分。
5. **要不要在服务端解析 POSIX ACL 或自动删除 `security.capability`**：扩展属性里经常存有 `system.posix_acl_access`、`system.posix_acl_default` 和 `security.capability`，需要明确服务端是只把它们当普通字节存起来，还是要去解析里面的权限结构。
6. **只靠 gRPC 的十几个标准状态码，客户端分不清具体的 POSIX `errno`**：一个 gRPC 状态码往往对应好几个不同的 POSIX 错误码（比如 `NotFound` 既可能是文件不存在 `ENOENT`，也可能是扩展属性不存在 `ENODATA`；`FailedPrecondition` 可能是 `ENOTDIR`、`EISDIR`、`ENOTEMPTY` 或 `EBADF`；`InvalidArgument` 可能是 `EINVAL`、`ERANGE`、`E2BIG` 或 `ENAMETOOLONG`）。而且文件名太长要返回 `ENAMETOOLONG`，扩展属性名太长却要返回 `ERANGE`；另外如果把 gRPC 的 `Unimplemented` 在客户端转成了 `ENOSYS`，Linux 内核会以为整个文件系统永远不支持这个功能，以后再也不调了（应该转成 `EOPNOTSUPP`）。

## 决策
1. **把所有文件名、扩展属性名和符号链接路径全部改为原始字节类型（`bytes` / `BYTEA`）**：
   - **数据库表结构与迁移**：
     - 新建 `inode_xattrs` 表：`ino BIGINT NOT NULL REFERENCES inodes(ino) ON DELETE CASCADE`、`name BYTEA NOT NULL`、`value BYTEA NOT NULL`，主键为 `(ino, name)`；加上检查约束：属性名长度在 `1..255` 字节之间且不含 `\x00`（`chk_inode_xattrs_name`），属性值长度不超过 `65536` 字节（`chk_inode_xattrs_value`）。
     - 把 `entries.name` 从 `VARCHAR(255)` 改为 `BYTEA NOT NULL`，加上检查约束：长度在 `1..255` 字节之间，且不含 `\x00` 和 `/`（更新 ADR-0001 的定义）；为了保证数据库迁移如果执行回滚（`Down`）遇到非 UTF-8 字节时不报错，回滚脚本把非 UTF-8 字节编码到 Unicode 私用区（`U+E000..U+E0FF`），重新升级（`Up`）时再还原回原始字节。
     - 把 `inodes.symlink_target` 从 `TEXT` 改为 `BYTEA`，最大长度从 `4096` 改为 `4095` 字节（对齐 Linux `PATH_MAX - 1`，给结尾预留 NUL 字节，更新 ADR-0007 的定义），迁移时先把已有的超长路径截断到 `4095` 字节；并用 `CASE WHEN (mode & 61440) = 40960 ...` 编写检查约束 `chk_inodes_symlink_target`，确保只有符号链接才有 `symlink_target` 且不含 `\x00`：
       ```sql
       ALTER TABLE inodes ADD CONSTRAINT chk_inodes_symlink_target CHECK (
           CASE
               WHEN (mode & 61440) = 40960 THEN
                   symlink_target IS NOT NULL
                   AND octet_length(symlink_target) BETWEEN 1 AND 4095
                   AND position('\x00'::bytea IN symlink_target) = 0
               ELSE
                   symlink_target IS NULL
           END
       );
       ```
   - **数据库时间戳统一用 `clock_timestamp()`**：
     - 在 `TouchInodeCtime`、`TouchInodeTimestamps` 等更新时间的 SQL 里，统一用实时变化的 `clock_timestamp()` 替代事务内固定不变的 `NOW()`，确保同一个事务或连续快速操作时 `ctime` / `mtime` 每次都会往前走。
   - **Protobuf 字段改为 `bytes` 并用 CEL 字节函数校验**：
     - 把所有请求和响应里的目录项名称（`Lookup`、`Mknod`、`Mkdir`、`Symlink`、`Link`、`Unlink`、`Rmdir`、`Rename`、`Create`、`DirEntry`、`DirEntryPlus`）、扩展属性名称（`SetXattr`、`GetXattr`、`RemoveXattr`、`ListXattrResponse.names`）和符号链接目标路径（`SymlinkRequest.target`、`ReadlinkResponse.target`）全部改为 `bytes`（Go 代码里对应 `[]byte`）。
     - 不用会强转 UTF-8 的 `bytes.pattern`，改用 `bytes.min_len` / `bytes.max_len` 搭配 CEL 字节判断 `!this.contains(b'\\x00')`（目录项名称再加一个 `&& !this.contains(b'/')`）；并在 `pkg/validator` 中把参数校验不通过的情况，转成带有具体 POSIX 错误原因（`ERANGE`、`E2BIG`、`ENAMETOOLONG`、`EINVAL`）的 gRPC `InvalidArgument` 错误。
   - **防止 0 字节空切片被存成数据库 `NULL`**：因为 `protobuf-go` 会把 0 字节的 `bytes` 解码成 `nil`，而 `pgx` 会把 `nil []byte` 当成 SQL 的 `NULL` 插入导致报错，所以 `meta.Manager` 在写入 `inode_xattrs.value` 前如果发现是 `nil`，统一替换成非空的 `[]byte{}`，确保 0 字节的空属性值能正常存进数据库。
2. **扩展属性 RPC 接口不传缓冲区 `size`，由客户端本地处理**：
   - `GetXattr` 接收 `(ino, name)` 直接返回完整的 `bytes value`；`ListXattr` 接收 `(ino)` 按字节升序（`ORDER BY name ASC`）返回 `repeated bytes names`。
   - FUSE 内核协议里的 `size == 0` 查长度、把属性名用 `\0` 拼成一串、以及当缓冲区 `size` 太小时返回 `ERANGE`，全部由客户端 FUSE 适配层在本地内存里完成，服务端 RPC 接口不掺和缓冲区大小的事。
3. **客户端开启 `default_permissions` 管用户权限，服务端负责检查 `user.*` 的文件类型和 64KB 属性名总长限制**：
   - **权限检查分工**：
     - **客户端挂载必须开启 `-o default_permissions`**：让客户端机器的 Linux 内核直接根据调用进程的用户身份检查 `rwx` 权限、粘滞位目录属主以及 `trusted.*` 所需的管理员 `CAP_SYS_ADMIN` 权限；`StorageService` 只在受信任的内网供 FUSE 客户端调用，不直接暴露给公网终端。
     - **服务端检查 `user.*` 允许的文件类型（对齐 Linux 7.1+ 规则）**：当扩展属性名以 `"user."` 开头时，只允许目标节点是常规文件（`S_IFREG`）、目录（`S_IFDIR`）或套接字（`S_IFSOCK`）；如果对符号链接、字符设备、块设备或命名管道（FIFO）设置或删除 `user.*` 属性，返回 `ErrPerm`（`EPERM`），读取 `user.*` 属性则返回 `ErrNoData`（`ENODATA`）。`ListXattr` 不做过滤。所有类型的节点以及已被删除但还打开着的孤儿节点（`nlink == 0`）都支持扩展属性操作。
   - **服务端检查顺序与加锁步骤**：
     1. **先检查输入参数（不用查数据库）**：
        - `ino == 0 || ino > math.MaxInt64` 返回 `ErrInvalidArgument`（`EINVAL`）；
        - `SetXattr` 的 `flags` 含有 `0x3` 以外的未知位时返回 `ErrInvalidArgument`（`EINVAL`）；
        - 扩展属性名为空（`len(name) == 0`）或包含 `\x00` 时返回 `ErrInvalidArgument`（`EINVAL`）；
        - 扩展属性名超过 255 字节（`len(name) > 255`）返回 `ErrRange`（`ERANGE`，如果既超长又含 `\x00` 优先返回 `ERANGE`），而文件名超 255 字节或软链路径超 4095 字节则返回 `ErrNameTooLong`（`ENAMETOOLONG`）；
        - `SetXattr` 的属性值超过 64KB（`len(value) > 65536`）返回 `ErrRange`（`ERANGE`）。
     2. **锁住或查询 `Inode`**：`SetXattr` 和 `RemoveXattr` 在写事务里调用 `GetInodeForUpdate(ino)` 加行级排他锁；`GetXattr` 和 `ListXattr` 在只读事务里调用 `GetInode(ino)`；节点不存在一律返回 `ErrNotFound`（`ENOENT`）。
     3. **检查 `user.*` 文件类型**：在查具体属性之前先检查文件类型，不符合要求时直接返回 `ErrPerm` 或 `ErrNoData`。
     4. **检查 `XATTR_CREATE (0x1)` / `XATTR_REPLACE (0x2)` 标志与单文件 64KB 属性名总长上限**：`SetXattr` 先查 `(ino, name)` 是否已经存在——如果已存在且带了只准新建的 `XATTR_CREATE` 标志，返回 `ErrAlreadyExists`（`EEXIST`）；如果不存在且带了只准替换的 `XATTR_REPLACE` 标志，返回 `ErrNoData`（`ENODATA`）。只有当要插入一个全新的属性名时，才在 `Inode` 行锁保护下算一下这个文件现有所有属性名的总长度 `Σ(octet_length(name) + 1)`；如果加上新属性名后总长超过 `65536` 字节（对齐 ext4 / tmpfs 的限制），就返回 `ErrNoSpace`（`ENOSPC`）；如果是覆盖已有的同名属性，名字长度没变，不做这个拦截。
     5. **更新时间**：`SetXattr` 和 `RemoveXattr` 成功后只调用 `TouchInodeCtime(ino)` 更新 `ctime`，不改动 `mtime` 和 `atime`。
4. **服务端不解析 POSIX ACL，也不开启 `FUSE_HANDLE_KILLPRIV`**：
   - `system.posix_acl_access` 和 `system.posix_acl_default` 只当作普通扩展属性按原样读写，服务端不解析里面的二进制内容，也不去联动修改 `Inode.mode`。
   - 挂载时不协商开启 `FUSE_HANDLE_KILLPRIV` 和 `FUSE_HANDLE_KILLPRIV_V2`：当写文件、截断文件或改属主需要清除特权位和 `security.capability` 扩展属性时，由 Linux 内核自己发 `FUSE_SETATTR` 和 `FUSE_REMOVEXATTR` 请求过来处理，服务端不需要在 `SetAttr`、`Open(O_TRUNC)` 或 `Fallocate` 里暗自去删 `security.capability`。
5. **用 `google.rpc.ErrorInfo` 把准确的 POSIX `errno` 名字传给客户端**：
   - 全项目统一在 gRPC 错误详情 `google.rpc.ErrorInfo` 里设置 `domain = "bowl.storage"`，并在 `reason` 字段里直接填入标准的 POSIX `errno` 名称（如 `"ENOENT"`、`"ENODATA"`）。FUSE 客户端优先读取 `ErrorInfo.reason` 来还原内核错误码。
   - **禁止把不支持的操作转成 `ENOSYS`**：FUSE 客户端看到 `(Unimplemented, "EOPNOTSUPP")` 时必须转成 `EOPNOTSUPP`，绝不能把 gRPC 的 `Unimplemented` 转成 `ENOSYS`，以免 Linux 内核误把该 FUSE 功能永久关闭。
   - 业务错误与 gRPC 状态码及 `reason` 的对应表如下：
     - `ErrInvalidArgument` $\rightarrow$ `InvalidArgument` + `reason: "EINVAL"`
     - `ErrNameTooLong` $\rightarrow$ `InvalidArgument` + `reason: "ENAMETOOLONG"`
     - `ErrRange` $\rightarrow$ `InvalidArgument` + `reason: "ERANGE"`
     - `ErrTooBig` $\rightarrow$ `InvalidArgument` + `reason: "E2BIG"`
     - `ErrNotFound` $\rightarrow$ `NotFound` + `reason: "ENOENT"`
     - `ErrNoData` $\rightarrow$ `NotFound` + `reason: "ENODATA"`
     - `ErrAlreadyExists` $\rightarrow$ `AlreadyExists` + `reason: "EEXIST"`
     - `ErrPerm` $\rightarrow$ `PermissionDenied` + `reason: "EPERM"`
     - `ErrAccess` $\rightarrow$ `PermissionDenied` + `reason: "EACCES"`
     - `ErrNotDir` $\rightarrow$ `FailedPrecondition` + `reason: "ENOTDIR"`
     - `ErrIsDir` $\rightarrow$ `FailedPrecondition` + `reason: "EISDIR"`
     - `ErrNotEmpty` $\rightarrow$ `FailedPrecondition` + `reason: "ENOTEMPTY"`
     - `ErrBadFileHandle` $\rightarrow$ `FailedPrecondition` + `reason: "EBADF"`
     - `ErrNoSpace` $\rightarrow$ `ResourceExhausted` + `reason: "ENOSPC"`
     - `ErrTooManyLinks` $\rightarrow$ `ResourceExhausted` + `reason: "EMLINK"`
     - `ErrFileTooLarge` $\rightarrow$ `InvalidArgument` + `reason: "EFBIG"`
     - `ErrIllegalSeek` $\rightarrow$ `InvalidArgument` + `reason: "ESPIPE"`
     - `ErrIO` $\rightarrow$ `Internal` + `reason: "EIO"`
     - `ErrNotSupported` $\rightarrow$ `Unimplemented` + `reason: "EOPNOTSUPP"`

## 结果
- **正面效果**：支持了包含非 UTF-8 原始字节的文件名、符号链接和扩展属性；通过限制单个文件的属性名总长不超过 64KB，防止了 `listxattr` 因为列表太长永远报 `E2BIG` 读不出来的问题；通过 `google.rpc.ErrorInfo` 让客户端能准确区分 `ENAMETOOLONG` 与 `ERANGE`、`ENOENT` 与 `ENODATA` 等同属一个 gRPC 大类的不同 POSIX 错误码。
- **代价与取舍**：把原来的 `entries.name` 和 `inodes.symlink_target` 从字符串改成 `BYTEA` 字节切片，需要同步修改 Protobuf、数据库查询（`sqlc`）和业务代码里的参数类型。
