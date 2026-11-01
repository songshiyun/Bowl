# 0001. FUSE Lookup 接口与 Inode 基础数据结构设计

## 状态
已采纳 (accepted)

## 背景
Storage 元数据服务需要通过 RPC 接口对接 Linux FUSE 内核的 `lookup` 操作。按照 POSIX FUSE 的 `lookup(parent, name)` 规范，查找一个文件名时，服务端必须返回符合 `fuse_entry_param` 要求的完整信息：包括节点编号（`ino`）、节点复用代数（`generation`）、文件的各项属性（大小、权限、硬链接数、时间戳等）以及内核缓存有效时间。我们需要定下这套基础的 Protobuf 数据结构，方便后面的 `GetAttr`、`SetAttr`、`Create`、`ReadDirPlus` 等接口直接共用。

## 决策
1. **Protobuf 文件位置与包名**：
   - 接口定义统一放在 `proto/storage/v1/storage.proto`，包名设为 `storage.v1`。
2. **将节点标识（`Inode`）与文件属性（`InodeAttribute`）拆成两个结构体**：
   - `Inode` 消息包含节点的唯一编号 `ino`、复用代数 `generation`、内核缓存时间以及属性 `attr`。
   - `InodeAttribute` 消息专门存放对应 POSIX `struct stat` 的各项文件属性（`mode`, `size`, `nlink`, `uid`, `gid`, `rdev`, `blksize`, `blocks`, `atime`, `mtime`, `ctime`, `flags`），方便后续在 `GetAttr`、`SetAttr`、`ReadDirPlus` 等接口中直接复用。
3. **`Lookup` 请求与响应字段**：
   - `LookupRequest` 包含父目录编号 `parent_ino` 和要查找的名称 `name`（注：名称字段在 [ADR-0011](0011-fuse-xattr-byte-accurate-names-and-error-reason-contract.md) 中从 `string` 改为 `bytes` 原始字节流），并校验名称不能为空、不能超过 255 字节、不能包含路径分隔符 `/` 或空字节 `\x00`。
   - `LookupResponse` 返回查到的 `Inode` 以及内核缓存有效时间 `entry_timeout_seconds`（目录项缓存秒数）和 `attr_timeout_seconds`（属性缓存秒数）。
4. **错误码约定**：
   - 当目标文件或父目录不存在时，返回标准状态码 `NotFound`（客户端转成 POSIX `-ENOENT`），不在响应体里加布尔标志位来表示存不存在。
   - 当父节点存在但不是目录（`(mode & S_IFMT) != S_IFDIR`）时，返回状态码 `FailedPrecondition`（客户端转成 POSIX `-ENOTDIR`）。
5. **时间戳格式**：
   - 所有时间字段统一使用 `google.protobuf.Timestamp`，保留纳秒精度。

## 结果
- **正面效果**：把 `Inode` 和 `InodeAttribute` 分开定义后，后续所有返回文件属性的接口都能直接复用同一套结构；输入参数边界和错误返回方式清晰明确。
- **代价与取舍**：Protobuf 消息多了一层 `attr` 嵌套，但换来了各个接口之间结构的统一。
