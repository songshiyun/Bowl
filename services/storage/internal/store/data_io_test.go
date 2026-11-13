package store_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func gatherMetricFamilies(t *testing.T, reg *prometheus.Registry) []*dto.MetricFamily {
	t.Helper()
	fams, err := reg.Gather()
	require.NoErrorf(t, err, "采集 Prometheus 指标失败: %v", err)
	return fams
}

func findMetricByLabels(fams []*dto.MetricFamily, name string, labels map[string]string) *dto.Metric {
	for _, fam := range fams {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if matchMetricLabels(m.GetLabel(), labels) {
				return m
			}
		}
	}
	return nil
}

func matchMetricLabels(pairs []*dto.LabelPair, want map[string]string) bool {
	if len(want) == 0 {
		return true
	}
	matched := 0
	for _, lp := range pairs {
		if v, ok := want[lp.GetName()]; ok && lp.GetValue() == v {
			matched++
		}
	}
	return matched == len(want)
}

func counterValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	m := findMetricByLabels(gatherMetricFamilies(t, reg), name, labels)
	if m == nil || m.GetCounter() == nil {
		return 0
	}
	return m.GetCounter().GetValue()
}

func histogramSampleCount(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) uint64 {
	t.Helper()
	m := findMetricByLabels(gatherMetricFamilies(t, reg), name, labels)
	if m == nil || m.GetHistogram() == nil {
		return 0
	}
	return m.GetHistogram().GetSampleCount()
}

func newMeteredActor(t *testing.T, opts *store.Options) (*store.StoreActor, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	metrics := store.NewMetrics()
	reg.MustRegister(metrics)
	opts.Metrics = metrics
	return openTestActor(t, opts), reg
}

func writeRepeatedChunks(t *testing.T, actor *store.StoreActor, ino uint64, chunkSize, count int, fill byte) {
	t.Helper()
	ctx := context.Background()
	chunk := bytes.Repeat([]byte{fill}, chunkSize)
	for i := range count {
		off := uint64(i * chunkSize)
		n, _, err := actor.Write(ctx, ino, off, chunk)
		require.NoErrorf(t, err, "写入第 %d 块失败: off=%d", i, off)
		require.Equalf(t, uint32(chunkSize), n, "写入长度不符: want=%d got=%d", chunkSize, n)
	}
}

func makePatternBytes(size int, seed byte) []byte {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(int(seed) + i*31 + (i >> 8))
	}
	return buf
}

func findPackFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "packs"))
	require.NoErrorf(t, err, "读取 packs 目录失败: dir=%s", dir)
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".pack") {
			out = append(out, filepath.Join(dir, "packs", e.Name()))
		}
	}
	return out
}

func corruptAllPackFilesWithByte(t *testing.T, dir string, fill byte) {
	t.Helper()
	paths := findPackFiles(t, dir)
	require.NotEmptyf(t, paths, "预期存在 .pack 文件但目录为空: dir=%s", dir)
	for _, p := range paths {
		info, err := os.Stat(p)
		require.NoError(t, err)
		if info.Size() == 0 {
			continue
		}
		err = os.WriteFile(p, bytes.Repeat([]byte{fill}, int(info.Size())), 0o600)
		require.NoErrorf(t, err, "篡改 pack 文件失败: path=%s", p)
	}
}

func TestGivenSequentialAndAlignedWritesWhenAppliedThenWriteModesAndReadbackMatch(t *testing.T) {
	ctx := context.Background()
	actor, reg := newMeteredActor(t, &store.Options{DiskID: "write-modes"})

	// Given: 新建常规文件
	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("seq.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)

	// When: 在同一块内连续执行 64 次 4 KiB 顺序追加写（共 256 KiB）
	writeRepeatedChunks(t, actor, f.Ino, 4096, 64, 0x3A)

	// Then: 全部命中 extend_tail，无 rmw 与 full_block
	require.InDeltaf(t, 64, counterValue(t, reg, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "extend_tail"}), 0.001, "64 次 4KiB 顺序写应全部触发 extend_tail")
	require.InDeltaf(t, 0, counterValue(t, reg, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "rmw"}), 0.001, "顺序尾追加不得触发 rmw")
	require.InDeltaf(t, 0, counterValue(t, reg, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "full_block"}), 0.001, "小块顺序追加不得计为 full_block")

	got, err := actor.Read(ctx, f.Ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equalf(t, bytes.Repeat([]byte{0x3A}, store.BlobSize), got, "顺序写回读数据不一致")

	// When: 回头覆盖块内非尾部区间（破坏顺序尾），触发 rmw；再对第二块整块对齐写，触发 full_block
	_, _, err = actor.Write(ctx, f.Ino, 8192, []byte("rmw-patch"))
	require.NoError(t, err)
	fullBlock := makePatternBytes(store.BlobSize, 0x55)
	_, _, err = actor.Write(ctx, f.Ino, store.BlobSize, fullBlock)
	require.NoError(t, err)

	// Then: rmw 与 full_block 计数准确且全零块写入触发 zero_elide
	require.InDeltaf(t, 1, counterValue(t, reg, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "rmw"}), 0.001, "非尾覆盖写应触发 1 次 rmw")
	require.InDeltaf(t, 1, counterValue(t, reg, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "full_block"}), 0.001, "整块对齐非零写应触发 1 次 full_block")

	// 局部全零写进入写偏路径触发 zero_elide，不分配物理块
	_, _, err = actor.Write(ctx, f.Ino, 2*store.BlobSize, make([]byte, 4096))
	require.NoError(t, err)
	require.InDeltaf(t, 1, counterValue(t, reg, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "zero_elide"}), 0.001, "局部全零写入应触发 1 次 zero_elide")

	gotZeros, err := actor.Read(ctx, f.Ino, 2*store.BlobSize, 4096)
	require.NoError(t, err)
	require.Equalf(t, make([]byte, 4096), gotZeros, "全零省略块回读应全为 0")
}

func TestGivenColdFileAcrossBlocksWhenReadThenCoalescesDiskPreadsAndPrefersHotCache(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Given: 写入 16 个连续整块并落盘关闭以清空热缓存
	wActor := openTestActor(t, &store.Options{Dir: dir, DiskID: "coalesce-disk"})
	f, err := wActor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("cold.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	payload := makePatternBytes(16*store.BlobSize, 0x11)
	_, _, err = wActor.Write(ctx, f.Ino, 0, payload)
	require.NoError(t, err)
	require.NoError(t, wActor.Close())

	// When: 以带指标的新实例重开并一次读取全部 16 块（冷读）
	rActor, reg := newMeteredActor(t, &store.Options{Dir: dir, DiskID: "coalesce-disk"})
	gotAll, err := rActor.Read(ctx, f.Ino, 0, 16*store.BlobSize)
	require.NoError(t, err)
	require.Equalf(t, payload, gotAll, "16 块合并冷读内容不符")

	// Then: 16 个物理相邻块合并为 1 次 pread，且 16 块均记为 disk_pack 来源
	require.Equalf(t, uint64(1), histogramSampleCount(t, reg, "bowl_storage_blob_pread_duration_seconds", nil), "16 块冷读应合并为单次 pread")
	require.InDeltaf(t, 16, counterValue(t, reg, "bowl_storage_blob_read_source_total", map[string]string{"source": "disk_pack"}), 0.001, "16 块冷读应全部计入 disk_pack")

	// When: 重开并在未对齐跨 5 块范围执行单次读取
	require.NoError(t, rActor.Close())
	rActor2, reg2 := newMeteredActor(t, &store.Options{Dir: dir, DiskID: "coalesce-disk"})
	unalignedOff := uint64(store.BlobSize + 123)
	unalignedLen := uint32(3*store.BlobSize + 4096)
	gotSlice, err := rActor2.Read(ctx, f.Ino, unalignedOff, unalignedLen)
	require.NoError(t, err)
	require.Equalf(t, payload[unalignedOff:unalignedOff+uint64(unalignedLen)], gotSlice, "跨块未对齐合并冷读内容不符")
	require.Equalf(t, uint64(1), histogramSampleCount(t, reg2, "bowl_storage_blob_pread_duration_seconds", nil), "跨 5 块未对齐冷读应合并为单次 pread")

	// When: 新建热缓存块（部分写路径进入 hotShard 并 Fsync 落盘），随后物理篡改磁盘 .pack 后再次读取
	hotFile, err := rActor2.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("hot.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	hotData := makePatternBytes(4096, 0x99)
	_, _, err = rActor2.Write(ctx, hotFile.Ino, 0, hotData)
	require.NoError(t, err)
	require.NoError(t, rActor2.Fsync(ctx, hotFile.Ino))

	corruptAllPackFilesWithByte(t, dir, 0xCC)
	hotSlice, err := rActor2.Read(ctx, hotFile.Ino, 0, 4096)
	require.NoError(t, err)

	// Then: 优先命中热缓存返回原始正确字节，不发起额外磁盘 pread
	require.Equalf(t, hotData, hotSlice, "热缓存应优先于篡改后的磁盘 pack")
	require.Equalf(t, uint64(1), histogramSampleCount(t, reg2, "bowl_storage_blob_pread_duration_seconds", nil), "命中热缓存不得增加磁盘 pread 次数")
	require.GreaterOrEqualf(t, counterValue(t, reg2, "bowl_storage_blob_read_source_total", map[string]string{"source": "hot_cache"}), 1.0, "应记录 hot_cache 命中")
}

func TestGivenSparseFileWhenReadIntoDirtyBufferThenReusesSliceZeroesHolesAndKeepsLowAllocs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Given: 块 0 与块 2 写入数据，块 1 留为空洞，关闭后重开触发混合稀疏冷/热路径
	wActor := openTestActor(t, &store.Options{Dir: dir, DiskID: "sparse-readinto"})
	f, err := wActor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("sparse.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	blk0 := makePatternBytes(store.BlobSize, 0x21)
	blk2 := makePatternBytes(store.BlobSize, 0x42)
	_, _, err = wActor.Write(ctx, f.Ino, 0, blk0)
	require.NoError(t, err)
	_, _, err = wActor.Write(ctx, f.Ino, 2*store.BlobSize, blk2)
	require.NoError(t, err)
	require.NoError(t, wActor.Close())

	rActor, reg := newMeteredActor(t, &store.Options{Dir: dir, DiskID: "sparse-readinto"})

	// When: 传入预先填满 0xFE 脏字节的足够容量切片执行 ReadInto
	totalSize := 3 * store.BlobSize
	dst := bytes.Repeat([]byte{0xFE}, totalSize)
	got, err := rActor.ReadInto(ctx, f.Ino, 0, uint32(totalSize), dst)
	require.NoError(t, err)

	// Then: 原地复用调用方缓冲区、空洞区间被清零、实块数据准确
	require.Lenf(t, got, len(dst), "ReadInto 返回长度不符")
	require.Samef(t, &dst[0], &got[0], "ReadInto 应当直接复用调用方缓冲区首地址")
	require.Equalf(t, blk0, got[:store.BlobSize], "块 0 数据不符")
	require.Equalf(t, make([]byte, store.BlobSize), got[store.BlobSize:2*store.BlobSize], "中间稀疏空洞必须将脏缓冲区清零")
	require.Equalf(t, blk2, got[2*store.BlobSize:], "块 2 数据不符")

	// When: 已取消 ctx 发起 Read，不得触发磁盘 pread
	preadsBefore := histogramSampleCount(t, reg, "bowl_storage_blob_pread_duration_seconds", nil)
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = rActor.Read(canceledCtx, f.Ino, 0, 4096)
	require.ErrorIsf(t, err, context.Canceled, "已取消 context 应返回 context.Canceled: got=%v", err)
	require.Equalf(t, preadsBefore, histogramSampleCount(t, reg, "bowl_storage_blob_pread_duration_seconds", nil), "已取消 Read 不得发起磁盘 pread")

	// When: 测量 768 KiB ReadInto 复用缓冲区的稳态堆分配次数（证明未重新分配大块载荷切片）
	allocs := testing.AllocsPerRun(20, func() {
		mustReadIntoLen(t, ctx, rActor, f.Ino, uint32(totalSize), dst)
	})
	require.LessOrEqualf(t, allocs, 25.0, "ReadInto 稳态复用读分配次数超标: got=%v", allocs)
}

func mustReadIntoLen(t *testing.T, ctx context.Context, actor *store.StoreActor, ino uint64, size uint32, dst []byte) {
	out, err := actor.ReadInto(ctx, ino, 0, size, dst)
	if err != nil || len(out) != int(size) {
		t.Fatalf("ReadInto 稳态读失败: len=%d err=%v", len(out), err)
	}
}

func TestGivenRegularFileWhenFallocateModesAndErrorsThenContractsEnforced(t *testing.T) {
	ctx := context.Background()
	actor := openTestActor(t, &store.Options{DiskID: "falloc-disk", MaxBytes: 16 << 20})

	// Given: 包含 1 个实块数据的文件、不可变文件、只追加文件与目录
	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("f"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	initial := makePatternBytes(store.BlobSize, 0x77)
	_, _, err = actor.Write(ctx, f.Ino, 0, initial)
	require.NoError(t, err)

	// When: mode=0 越过 EOF 扩展文件大小并填充零区
	require.NoError(t, actor.Fallocate(ctx, f.Ino, 0, 2*store.BlobSize, 0))
	attr, err := actor.GetAttr(ctx, f.Ino)
	require.NoError(t, err)
	require.Equalf(t, uint64(2*store.BlobSize), attr.Size, "mode=0 应将文件扩展至 2*BlobSize")

	// When: FallocKeepSize 超出当前 EOF 不改变文件 Size
	require.NoError(t, actor.Fallocate(ctx, f.Ino, 2*store.BlobSize, store.BlobSize, store.FallocKeepSize))
	attr, err = actor.GetAttr(ctx, f.Ino)
	require.NoError(t, err)
	require.Equalf(t, uint64(2*store.BlobSize), attr.Size, "FallocKeepSize 不得增大文件 Size")

	// When: FallocPunchHole | FallocKeepSize 对首块内部打洞
	require.NoError(t, actor.Fallocate(ctx, f.Ino, 1024, 4096, store.FallocPunchHole|store.FallocKeepSize))
	punched, err := actor.Read(ctx, f.Ino, 0, 6144)
	require.NoError(t, err)
	require.Equalf(t, initial[:1024], punched[:1024], "打洞前段字节应保持不变")
	require.Equalf(t, make([]byte, 4096), punched[1024:5120], "打洞区间必须读回全 0")
	require.Equalf(t, initial[5120:6144], punched[5120:6144], "打洞后段字节应保持不变")

	// When: FallocZeroRange（不带 KeepSize）清零前部并越过 EOF 扩容；带 KeepSize 时不改变 Size
	require.NoError(t, actor.Fallocate(ctx, f.Ino, 0, 1024, store.FallocZeroRange|store.FallocKeepSize))
	headZero, err := actor.Read(ctx, f.Ino, 0, 1024)
	require.NoError(t, err)
	require.Equalf(t, make([]byte, 1024), headZero, "FallocZeroRange 区间应全部归零")
	require.NoError(t, actor.Fallocate(ctx, f.Ino, 2*store.BlobSize, 4096, store.FallocZeroRange))
	attr, err = actor.GetAttr(ctx, f.Ino)
	require.NoError(t, err)
	require.Equalf(t, uint64(2*store.BlobSize+4096), attr.Size, "不带 KeepSize 的 FallocZeroRange 越界时应扩展 Size")

	// Then: 非法组合、不支持标志、目录及不可变/只追加权限拒绝均按契约报错
	require.ErrorIsf(t, actor.Fallocate(ctx, f.Ino, 0, 4096, store.FallocPunchHole), store.ErrInvalid, "单独 FallocPunchHole 未带 KeepSize 应返回 ErrInvalid")
	require.ErrorIsf(t, actor.Fallocate(ctx, f.Ino, 0, 4096, 1<<30), store.ErrNotSupported, "未知 Fallocate 标志应返回 ErrNotSupported")
	require.ErrorIsf(t, actor.Fallocate(ctx, f.Ino, 0, 32<<20, 0), store.ErrNoSpace, "超额配额 Fallocate 应返回 ErrNoSpace")

	dirIno, err := actor.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("sub"), Mode: store.S_IFDIR | 0o755})
	require.NoError(t, err)
	require.ErrorIsf(t, actor.Fallocate(ctx, dirIno.Ino, 0, 4096, 0), store.ErrIsDir, "对目录执行 Fallocate 应返回 ErrIsDir")

	immFlag := store.FlagImmutable
	_, err = actor.SetAttr(ctx, f.Ino, store.SetAttrReq{Flags: &immFlag})
	require.NoError(t, err)
	require.ErrorIsf(t, actor.Fallocate(ctx, f.Ino, 0, 4096, store.FallocKeepSize), store.ErrPerm, "不可变文件 Fallocate 应返回 ErrPerm")

	appFile, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("app"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	appFlag := store.FlagAppend
	_, err = actor.SetAttr(ctx, appFile.Ino, store.SetAttrReq{Flags: &appFlag})
	require.NoError(t, err)
	require.ErrorIsf(t, actor.Fallocate(ctx, appFile.Ino, 0, 4096, store.FallocPunchHole|store.FallocKeepSize), store.ErrPerm, "只追加文件打洞应返回 ErrPerm")
}

func TestGivenWritesAndMetadataWhenFsyncAndFsyncDirThenWALFlushedAndPersisted(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "fsync-disk"})

	// Given: 写入文件数据但尚未执行 Fsync，WAL 处于脏状态
	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("dur.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	payload := []byte("durable-payload")
	_, _, err = actor.Write(ctx, f.Ino, 0, payload)
	require.NoError(t, err)
	require.Truef(t, actor.WALDirty(), "未 Fsync 的写操作应标记 WALDirty")

	// When: 执行 Fsync 与 FsyncDir
	require.NoError(t, actor.Fsync(ctx, f.Ino))
	require.Falsef(t, actor.WALDirty(), "Fsync 完成后 WALDirty 应清零")

	_, err = actor.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("d"), Mode: store.S_IFDIR | 0o755})
	require.NoError(t, err)
	require.Truef(t, actor.WALDirty(), "新建目录后应标记 WALDirty")
	require.NoError(t, actor.FsyncDir(ctx, store.RootIno))
	require.Falsef(t, actor.WALDirty(), "FsyncDir 完成后 WALDirty 应清零")

	// Then: 关闭并重开后数据与目录完整持久化
	require.NoError(t, actor.Close())
	reopened := openTestActor(t, &store.Options{Dir: dir, DiskID: "fsync-disk"})
	got, err := reopened.Read(ctx, f.Ino, 0, uint32(len(payload)))
	require.NoError(t, err)
	require.Equalf(t, payload, got, "Fsync 后重开读回内容不符")
	_, _, err = reopened.Lookup(ctx, store.RootIno, []byte("d"))
	require.NoError(t, err)
}

func TestGiven16MiBConcurrentWorkloadWhenWritingAndSnapshottingMetricsThenConsistentAndRaceFree(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	actor, reg := newMeteredActor(t, &store.Options{Dir: dir, DiskID: "stress-16m", MaxBytes: 64 << 20})

	// Given: 创建目标文件并准备 16 MiB 确定性数据（共 64 个 256 KiB 块）
	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("big.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	const totalBytes = 16 << 20
	const chunkSize = store.BlobSize

	// When: 后台并发高频采样 MetricsSnapshot，同时前台写入完整 16 MiB 数据
	stopSnap := make(chan struct{})
	var snapErr atomic.Pointer[error]
	var wg sync.WaitGroup
	wg.Go(func() {
		watchMetricsSnapshot(stopSnap, actor, &snapErr)
	})

	writePatternBlocks(t, actor, f.Ino, chunkSize, totalBytes/chunkSize)
	close(stopSnap)
	wg.Wait()
	require.Nilf(t, snapErr.Load(), "并发采样 MetricsSnapshot 检测到异常: %v", snapErr.Load())

	// Then: 快照与指标反映 16 MiB 实占容量，重开校验 16 MiB 字节逐一吻合
	snap := actor.MetricsSnapshot()
	require.Equalf(t, uint64(totalBytes), snap.LogicalBytes, "16 MiB 写入后 LogicalBytes 不符")
	require.Equalf(t, uint64(totalBytes), snap.PackLiveBytes, "16 MiB 写入后 PackLiveBytes 不符")
	require.GreaterOrEqualf(t, counterValue(t, reg, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "full_block"}), float64(totalBytes/chunkSize), "64 块整块写应计入 full_block")

	require.NoError(t, actor.Close())
	reopened := openTestActor(t, &store.Options{Dir: dir, DiskID: "stress-16m"})
	verifyPatternBlocks(t, reopened, f.Ino, chunkSize, totalBytes/chunkSize)
}

func writePatternBlocks(t *testing.T, actor *store.StoreActor, ino uint64, chunkSize, blocks int) {
	t.Helper()
	ctx := context.Background()
	for i := range blocks {
		data := makePatternBytes(chunkSize, byte(i))
		_, _, err := actor.Write(ctx, ino, uint64(i*chunkSize), data)
		require.NoErrorf(t, err, "写入第 %d 块失败", i)
	}
}

func watchMetricsSnapshot(stop <-chan struct{}, actor *store.StoreActor, firstErr *atomic.Pointer[error]) {
	for {
		select {
		case <-stop:
			return
		default:
			snap := actor.MetricsSnapshot()
			if snap.PackLiveBytes > snap.MaxBytes {
				errExceed := errors.New("PackLiveBytes 超过 MaxBytes")
				firstErr.CompareAndSwap(nil, &errExceed)
			}
		}
	}
}

func verifyPatternBlocks(t *testing.T, actor *store.StoreActor, ino uint64, chunkSize, blocks int) {
	t.Helper()
	ctx := context.Background()
	for i := range blocks {
		want := makePatternBytes(chunkSize, byte(i))
		got, err := actor.Read(ctx, ino, uint64(i*chunkSize), uint32(chunkSize))
		require.NoErrorf(t, err, "回读第 %d 块失败", i)
		require.Equalf(t, want, got, "第 %d 块重开校验数据损坏", i)
	}
}
