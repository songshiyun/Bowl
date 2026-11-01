# 0024. 服务端写入先落系统缓存、Fsync 按需刷盘与内存实时统计磁盘配额

## 状态
已采纳 (accepted) — 演进 [ADR-0009](0009-fuse-file-handle-lifecycle-and-orphan-reclamation.md)、[ADR-0013](0013-fuse-rename2-tmpfile-fsyncdir-and-session-orphan-reclamation.md)、[ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md)、[ADR-0019](0019-chunk-slice-block-nvme-data-layer.md) 与 [ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md)

## 背景
在完成 [ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md) 与 [ADR-0023](0023-session-touch-throttling-lockless-handle-close-and-read-pipeline.md) 后，系统在写入速度和磁盘配额统计上还有两个明显问题：

1. **每次 `Write(1 MiB)` 都强制同步刷入物理硬盘，速度被单块云盘 `180 MiB/s` 的硬件上限卡死**：
   - 在阿里云双机内网测试（[`benchmark/juicefs/REPORT.md`](../../benchmark/juicefs/REPORT.md)）中，`juicefs bench (-p 4)` 并发写入 4 个 `1 GiB` 大文件（每次写 `1 MiB`）时，JuiceFS 达到 `302.28 ~ 302.49 MiB/s`，而 Bowl 只有 `178.59 MiB/s`。
   - 原因是服务端挂载的单块 `120 GiB cloud_essd (PL1)` 云盘，其同步落盘的硬件极限就是 `180 MiB/s` 左右（本地 `fio --direct=1 --end_fsync=1` 实测为 `168.59 ~ 191.13 MiB/s`，磁盘利用率 `96.29%`）。
   - 按照 POSIX 标准，普通 `write(2)` 只需要保证后续读取立即可见（写入操作系统页缓存 Page Cache 即可），只有调用 `fsync(2)` / `fdatasync(2)` 时才需要阻塞等待物理硬盘落盘；未调用 `fsync` 的脏页由操作系统内核在后台自行回写。但之前 [ADR-0019](0019-chunk-slice-block-nvme-data-layer.md) 和 [ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md) 在每次 `Write` 请求处理过程中都强制执行 `tmpFile.Sync()`、`SyncDirs()` 和 PostgreSQL WAL `fdatasync`（写 `4 GiB` 数据要触发 4,096 次文件 `fsync` 和 4,096 次目录 `fsync`），等于把所有普通 `Write` 都当成 `O_SYNC` 执行。
   - 与此同时，[ADR-0009](0009-fuse-file-handle-lifecycle-and-orphan-reclamation.md) 和 [ADR-0013](0013-fuse-rename2-tmpfile-fsyncdir-and-session-orphan-reclamation.md) 里的 `Fsync` 与 `FsyncDir` 反而只检查了文件句柄是否有效，没有真正去触发硬盘刷盘。
2. **`disks` 表重复保存 `used_bytes` 和 `used_inodes`，既造成数据库行锁排队，又容易在崩溃时账目不一致**：
   - [ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 在 `disks` 表里额外维护了 `used_bytes`（已用字节数）和 `used_inodes`（已用节点数）两个字段。如果每次写文件都在事务里锁住 `disks` 表去更新这两个数字，同一个 `Disk` 下的并发写入和建删文件就会全部排队；如果改成先在内存里累加、再由后台协程定期写回 `disks` 表，一旦进程在写回前崩溃（如 `SIGKILL`、OOM 或断电），`inodes` 表里的文件变化已经提交了，内存里没来得及写回的增量就会丢失，重启后从 `disks` 表读出来的已用空间就彻底不准了。
   - 实际上，`inodes` 表本身已经记录了每个节点的 `root_ino`、`mode` 和 `size`，只要查 `inodes` 表就能算出真实的已用空间和节点数，`disks` 表根本不需要重复存这两个字段，也不需要后台协程定期去刷 `disks` 表。

## 决策

1. **`Write` 写入操作系统文件缓存即返回，不再每次阻塞等待硬盘 `fsync`（演进 ADR-0019 / ADR-0022）**：
   - `StorageService` 的 Go 进程不在自身堆内存里缓存任何 `Block` 数据内容。`LocalFSBlockStore` 写块文件时，把数据写入同目录的临时文件 `.tmp_<slice_id>_<block_idx>_*`，执行 `Close()` 并通过 `os.Rename(tmpPath, finalPath)` 替换成正式文件名后直接返回。此时文件已经进入服务端操作系统的页缓存（Page Cache），不再在 `Write` 请求里阻塞等待 `tmpFile.Sync()` 和目录 `SyncDirs()`，也不启动任何后台定时或定量刷盘协程，日常脏页回写完全交由操作系统内核自己调度。
   - **刚写完的数据立即可读**：`Write` 返回成功时，对应的 `Block` 文件已经出现在服务端目录中，PostgreSQL 事务也已提交。无论同机还是跨机客户端，紧接着发起 `Read`、`GetAttr` 或 `Lookup`，都能直接从操作系统缓存和数据库里读到最新写入的内容，以及最新的 `size`、`mtime`、`ctime`（包括自动清除 `S_ISUID` / `S_ISGID` 位）。
   - **写入失败时直接删除文件**：如果同批次里某个写入任务因为句柄失效（`ErrBadFileHandle`）或空间不足（`ErrNoSpace`）失败，`cleanupBlocks()` 直接把刚才生成的块文件删掉即可，不需要额外做目录 `fsync`。

2. **`Fsync` 与 `FsyncDir` 按需触发并等待硬盘刷盘完成（演进 ADR-0009 / ADR-0013）**：
   - 客户端调用 `Fsync` 或 `FsyncDir` 时，服务端先按原有规则检查 `Inode` 类型和打开的 `FileHandle` 是否有效，然后触发底层块存储刷盘：若自上次刷盘以来有新写入的数据块，在 Linux 下直接对数据目录调用 `unix.Syncfs(fd)`（非 Linux 环境或自定义测试钩子下则对未刷盘的文件和目录执行 `Sync`，遇到因回滚已被删掉的文件时忽略 `os.ErrNotExist`），将操作系统缓存中的脏数据真正刷入物理硬盘（多个并发的 `Fsync` 合并等待同一次刷盘；若期间无新写入则直接返回）。
   - 如果硬盘刷盘出错，向上层返回 `ErrIO`（对应客户端 `EIO`）。

3. **配合 PostgreSQL 异步提交，保证「数据块先落盘、数据库日志后落盘」**：
   - 为了省去每次普通 `Write` 等待 PostgreSQL WAL `fdatasync` 的磁盘开销，同时防止整机突然断电时出现「PostgreSQL 里记录了新切片，但硬盘上的 `Block` 文件还没刷下去」的损坏情况，普通 `Write` 的数据库事务设置 `SET LOCAL synchronous_commit = off`，让事务把 WAL 日志写入内存缓冲区就立即返回。
   - 当客户端调用 `Fsync` 或 `FsyncDir` 时，服务端先等待底层 `Block` 文件刷入物理硬盘，再调用 `SELECT pg_current_wal_flush_lsn()` 强制把 PostgreSQL 的 WAL 日志刷入硬盘，保证显式同步时的落盘顺序始终是「先刷物理数据块，后刷数据库元数据」。

4. **删掉 `disks` 表里的配额统计列，改为首次访问时查 `inodes` 表汇总、之后在内存里直接加减（演进 ADR-0015）**：
   - **删掉冗余列并添加覆盖索引**：新增数据库迁移脚本，执行 `ALTER TABLE disks DROP COLUMN used_bytes, DROP COLUMN used_inodes;`（`disks` 表只保留 `disk_id`、`name`、`root_ino`、`max_bytes`、`created_at`、`updated_at`），并把原来的单列索引 `idx_inodes_root_ino` 替换为包含 `mode` 和 `size` 的覆盖索引 `idx_inodes_root_ino_quota ON inodes (root_ino) INCLUDE (mode, size)`。磁盘的已用空间 `used_bytes`（同一 `root_ino` 下所有常规文件 `(mode & 61440) = 32768` 的 `size` 总和）与已用节点数 `used_inodes`（同一 `root_ino` 下的全部 `Inode` 数量，包含还没被清理的 `Orphan Inode`）统一以 `inodes` 表为准。
   - **首次访问某个 `Disk` 时从索引一次性算出初始值**：当内存里还没有某个 `Disk`（`root_ino`）的配额记录（`!st.loaded`）时，直接通过覆盖索引 `idx_inodes_root_ino_quota` 做一次仅扫索引的汇总查询（Index-Only Scan），算出当前的 `used_bytes` 和 `used_inodes`，连同 `disks` 表里的 `max_bytes` 一起存入内存结构 `diskQuotaState`，并标记为已加载（`st.loaded = true`）。
   - **运行期间直接在内存中加减配额，取消后台写回数据库**：加载到内存后，彻底删掉原来的 `AddDiskQuotaByRootIno` SQL、`FlushDiskQuotas`、`runQuotaFlushLoop` 和 `WithDiskQuotaFlushInterval` 等后台刷表代码。所有写请求在落库前先在内存里检查 `usedBytes + reservedBytes` 是否超过 `maxBytes` 并预占空间（超出上限直接返回 `ErrNoSpace` / `ENOSPC`），数据库事务提交成功后直接在内存里更新 `st.usedBytes += deltaBytes` 和 `st.usedInodes += deltaInodes`（如果事务回滚就释放预占空间）；`StatFS`、`GetDisk`、`ListDisks` 直接从内存读取当前配额；`UpdateDisk` 在锁住该盘下属 `Inode` 时实时汇总一次 `used_bytes`，检查缩容后的容量不小于当前已用空间，并同步更新内存里的 `max_bytes`。

## 结果
- **正面效果**：
  - 回归标准 POSIX 语义：普通 `Write` 写入操作系统页缓存即返回，彻底摆脱了每次 `Write(1 MiB)` 都要等待云盘物理 `fsync` 导致的 `180 MiB/s` 速度限制，也不会因为后台定时强制刷盘而打断内核回写队列；同时不在 Go 内存里囤积文件数据，完全通过 `pjdfstest` POSIX 测试。
  - `Fsync` / `FsyncDir` 真正承担起物理落盘职责，配合「先刷块文件、再刷 PG WAL」的机制，保证了显式同步后断电不丢数据。
  - 彻底消除了 `disks` 表的行锁排队、后台刷表协程以及进程崩溃导致的配额失步问题：数据库只修改 `inodes` 表，冷启动首次访问时扫一次索引即可算出已用配额，后续运行期间全部在内存里完成校验和增减。
- **代价与取舍**：
  - 当整机发生突发断电（而非服务进程重启）时，还没有调用过 `Fsync` 的写入会和对应的 PostgreSQL 异步提交事务一起回滚（这与标准 Linux 本地文件系统在未调用 `fsync` 时断电的表现一致），且不会留下错误的磁盘配额。
