package store_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func packFilePathForTest(dir string, packID uint64) string {
	return filepath.Join(dir, "packs", fmt.Sprintf("%016x.pack", packID))
}

func matchMetricLabels(m *dto.Metric, labels map[string]string) bool {
	if len(labels) == 0 {
		return true
	}
	matched := 0
	for _, lp := range m.GetLabel() {
		if want, ok := labels[lp.GetName()]; ok && lp.GetValue() == want {
			matched++
		}
	}
	return matched == len(labels)
}

func gatherHistogramCount(t *testing.T, reg *prometheus.Registry, metricName string, labels map[string]string) uint64 {
	t.Helper()
	fams, err := reg.Gather()
	require.NoError(t, err)
	for _, fam := range fams {
		if fam.GetName() != metricName {
			continue
		}
		var total uint64
		for _, m := range fam.GetMetric() {
			if matchMetricLabels(m, labels) {
				total += m.GetHistogram().GetSampleCount()
			}
		}
		return total
	}
	return 0
}

func gatherCounterMetricValue(t *testing.T, reg *prometheus.Registry, metricName string, labels map[string]string) uint64 {
	t.Helper()
	fams, err := reg.Gather()
	require.NoError(t, err)
	for _, fam := range fams {
		if fam.GetName() != metricName {
			continue
		}
		for _, m := range fam.GetMetric() {
			if matchMetricLabels(m, labels) {
				return uint64(m.GetCounter().GetValue())
			}
		}
	}
	return 0
}

func TestStoreOptionsMaxPackSizeValidation(t *testing.T) {
	_, err := store.Open(store.Options{
		Dir:         t.TempDir(),
		DiskID:      "invalid-small",
		MaxPackSize: store.BlobSize - 1,
	})
	require.ErrorIs(t, err, store.ErrInvalid)

	_, err = store.Open(store.Options{
		Dir:         t.TempDir(),
		DiskID:      "invalid-large",
		MaxPackSize: store.PackMaxSize + 1,
	})
	require.ErrorIs(t, err, store.ErrInvalid)

	minActor, err := store.Open(store.Options{
		Dir:         t.TempDir(),
		DiskID:      "valid-min",
		MaxPackSize: store.BlobSize,
	})
	require.NoError(t, err)
	require.NoError(t, minActor.Close())

	maxActor, err := store.Open(store.Options{
		Dir:         t.TempDir(),
		DiskID:      "valid-max",
		MaxPackSize: store.PackMaxSize,
	})
	require.NoError(t, err)
	require.NoError(t, maxActor.Close())
}

func TestStorePackIDReopenRecovery(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	packDir := filepath.Join(base, "packs")
	require.NoError(t, os.MkdirAll(packDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(packDir, "0000000000000250.pack"), []byte("pack-data"), 0o644))

	actor, err := store.Open(store.Options{
		Dir:      base,
		DiskID:   "disk-reopen-scan",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("hello.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, []byte("hello-pack"))
	require.NoError(t, err)

	entries, err := os.ReadDir(packDir)
	require.NoError(t, err)
	var maxID uint64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".pack") {
			continue
		}
		id, parseErr := strconv.ParseUint(strings.TrimSuffix(entry.Name(), ".pack"), 16, 64)
		require.NoError(t, parseErr)
		if id > maxID {
			maxID = id
		}
	}
	require.Greater(t, maxID, uint64(0x0250), "扫描已有高编号 .pack 后新写入编号必须严格大于 0x0250")
}

func TestStorePackAutoRolloverAndColdReopenRead(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:         base,
		DiskID:      "disk-rollover",
		MaxBytes:    128 << 20,
		MaxPackSize: 3 * store.BlobSize,
	})
	require.NoError(t, err)

	f, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("rollover.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	b0 := bytes.Repeat([]byte{0x11}, store.BlobSize)
	b1 := bytes.Repeat([]byte{0x22}, store.BlobSize)
	b2 := bytes.Repeat([]byte{0x33}, 128*1024)
	fullInPack2 := bytes.Repeat([]byte{0x55}, store.BlobSize)
	b3 := bytes.Repeat([]byte{0x44}, 128*1024)
	m0 := bytes.Repeat([]byte{0x61}, store.BlobSize)
	m1 := bytes.Repeat([]byte{0x62}, store.BlobSize)
	m2 := bytes.Repeat([]byte{0x63}, store.BlobSize)
	batchPayload := bytes.Join([][]byte{m0, m1, m2}, nil)

	// Pack 1: 256 KiB + 256 KiB + 128 KiB = 640 KiB
	_, _, err = actor.Write(ctx, f.Ino, 0, b0)
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, store.BlobSize, b1)
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 2*store.BlobSize, b2)
	require.NoError(t, err)

	// Pack 1 仅剩 128 KiB，写入 256 KiB 自动封口 Pack 1 并切入 Pack 2
	_, _, err = actor.Write(ctx, f.Ino, 3*store.BlobSize, fullInPack2)
	require.NoError(t, err)
	// Pack 2 再写入 128 KiB，剩余 384 KiB
	_, _, err = actor.Write(ctx, f.Ino, 4*store.BlobSize, b3)
	require.NoError(t, err)
	// 批量写入 3 个 256 KiB 满块：第 0 块落入 Pack 2 尾部（至 640 KiB），第 1、2 块自动切入 Pack 3（512 KiB）
	_, _, err = actor.Write(ctx, f.Ino, 5*store.BlobSize, batchPayload)
	require.NoError(t, err)

	info1, err := os.Stat(packFilePathForTest(base, 1))
	require.NoError(t, err)
	require.Equal(t, int64(2*store.BlobSize+128*1024), info1.Size())

	info2, err := os.Stat(packFilePathForTest(base, 2))
	require.NoError(t, err)
	require.Equal(t, int64(2*store.BlobSize+128*1024), info2.Size())

	info3, err := os.Stat(packFilePathForTest(base, 3))
	require.NoError(t, err)
	require.Equal(t, int64(2*store.BlobSize), info3.Size())

	verifyAllRanges := func(act *store.StoreActor) {
		type readCheck struct {
			off  uint64
			want []byte
		}
		checks := []readCheck{
			{0, b0},
			{store.BlobSize, b1},
			{2 * store.BlobSize, b2},
			{3 * store.BlobSize, fullInPack2},
			{4 * store.BlobSize, b3},
			{5 * store.BlobSize, batchPayload},
		}
		for _, ck := range checks {
			got, readErr := act.Read(ctx, f.Ino, ck.off, uint32(len(ck.want)))
			require.NoError(t, readErr)
			require.Equal(t, ck.want, got)
		}
	}

	// 热读校验
	verifyAllRanges(actor)
	require.NoError(t, actor.Close())

	// 重开清空热缓存，冷读全字节一致
	reopened, err := store.Open(store.Options{
		Dir:         base,
		DiskID:      "disk-rollover",
		MaxBytes:    128 << 20,
		MaxPackSize: 3 * store.BlobSize,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	verifyAllRanges(reopened)
}

func TestStoreExtendTailAndRMWFallback(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	metrics := store.NewMetrics()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(metrics))

	actor, err := store.Open(store.Options{
		Dir:         base,
		DiskID:      "disk-extend-tail",
		MaxBytes:    64 << 20,
		MaxPackSize: store.BlobSize,
		Metrics:     metrics,
	})
	require.NoError(t, err)

	f1, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("seq.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	f2, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("other.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 1. 连续顺延写入（含跨空洞顺延）：全部命中 extend_tail，rmw == 0
	chunk0 := bytes.Repeat([]byte{0xaa}, 4096)
	_, _, err = actor.Write(ctx, f1.Ino, 0, chunk0)
	require.NoError(t, err)

	chunk1 := bytes.Repeat([]byte{0xbb}, 4096)
	_, _, err = actor.Write(ctx, f1.Ino, 4096, chunk1)
	require.NoError(t, err)

	chunk2 := bytes.Repeat([]byte{0xdd}, 2048)
	_, _, err = actor.Write(ctx, f1.Ino, 12288, chunk2)
	require.NoError(t, err)

	require.Equal(t, uint64(3), gatherCounterMetricValue(t, reg, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "extend_tail"}))
	require.Zero(t, gatherCounterMetricValue(t, reg, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "rmw"}))

	// 2. 插入另一文件写破坏 f1 尾块位置后，继续追加 f1 必须回退至 RMW
	_, _, err = actor.Write(ctx, f2.Ino, 0, []byte("tail-breaker"))
	require.NoError(t, err)

	chunk3 := bytes.Repeat([]byte{0xcc}, 4096)
	_, _, err = actor.Write(ctx, f1.Ino, 14336, chunk3)
	require.NoError(t, err)
	require.Equal(t, uint64(1), gatherCounterMetricValue(t, reg, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "rmw"}))

	// 3. 写入满块触发跨包封口后，再局部修改旧包中的块同样触发 RMW
	_, _, err = actor.Write(ctx, f2.Ino, store.BlobSize, bytes.Repeat([]byte{0xee}, store.BlobSize))
	require.NoError(t, err)
	chunk4 := bytes.Repeat([]byte{0x77}, 1024)
	_, _, err = actor.Write(ctx, f1.Ino, 2048, chunk4)
	require.NoError(t, err)
	require.Equal(t, uint64(2), gatherCounterMetricValue(t, reg, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "rmw"}))

	expected := make([]byte, 18432)
	copy(expected[0:4096], chunk0)
	copy(expected[4096:8192], chunk1)
	copy(expected[12288:14336], chunk2)
	copy(expected[14336:18432], chunk3)
	copy(expected[2048:3072], chunk4)

	gotHot, err := actor.Read(ctx, f1.Ino, 0, uint32(len(expected)))
	require.NoError(t, err)
	require.Equal(t, expected, gotHot)
	require.NoError(t, actor.Close())

	reopened, err := store.Open(store.Options{
		Dir:         base,
		DiskID:      "disk-extend-tail",
		MaxBytes:    64 << 20,
		MaxPackSize: store.BlobSize,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	gotCold, err := reopened.Read(ctx, f1.Ino, 0, uint32(len(expected)))
	require.NoError(t, err)
	require.Equal(t, expected, gotCold)
}

func TestStoreSealedPackBgSyncAndDirBarrier(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	metrics := store.NewMetrics()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(metrics))

	actor, err := store.Open(store.Options{
		Dir:         base,
		DiskID:      "disk-bgsync",
		MaxBytes:    64 << 20,
		MaxPackSize: 2 * store.BlobSize,
		Metrics:     metrics,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f1, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("f1.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	f2, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("f2.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 1. f1 写满 Pack 1（2 个 256 KiB 块）
	_, _, err = actor.Write(ctx, f1.Ino, 0, bytes.Repeat([]byte{0x11}, 2*store.BlobSize))
	require.NoError(t, err)

	// 2. f2 追加 1 个块触发 Pack 1 封口并切入新活跃包 Pack 2
	_, _, err = actor.Write(ctx, f2.Ino, 0, bytes.Repeat([]byte{0x22}, store.BlobSize))
	require.NoError(t, err)

	// 3. 后台刷盘协程自动对已封口的 Pack 1 执行异步 fdatasync
	require.Eventually(t, func() bool {
		return gatherHistogramCount(t, reg, "bowl_storage_blob_sync_duration_seconds", map[string]string{
			"target":  "pack_data",
			"trigger": "bg_rotate",
		}) == 1
	}, 2*time.Second, 5*time.Millisecond, "已封口 Pack 1 必须由后台协程自动完成预刷盘")
	require.Zero(t, gatherHistogramCount(t, reg, "bowl_storage_blob_sync_duration_seconds", map[string]string{
		"target":  "packs_dir",
		"trigger": "bg_rotate",
	}), "后台预刷盘仅刷数据文件，不提前执行目录 fsync")

	// 4. 首次对仅关联已封口新建包 Pack 1 的 f1 调用 Fsync：
	//    - Pack 1 已被后台刷完消记，fsync_barrier pack_data 不得重复递增（保持为 0）
	//    - 因 Pack 1 属于新建包，首次 Fsync 必须执行 1 次 packs/ 目录 fsync
	require.NoError(t, actor.Fsync(ctx, f1.Ino))
	require.Zero(t, gatherHistogramCount(t, reg, "bowl_storage_blob_sync_duration_seconds", map[string]string{
		"target":  "pack_data",
		"trigger": "fsync_barrier",
	}), "已被后台预刷盘的封口包不得在前台 Fsync 时重复 fdatasync")
	require.Equal(t, uint64(1), gatherHistogramCount(t, reg, "bowl_storage_blob_sync_duration_seconds", map[string]string{
		"target":  "packs_dir",
		"trigger": "fsync_barrier",
	}), "新建封口包被后台提前刷完后，首次 Fsync 必须执行 1 次 packs/ 目录 fsync")

	// 5. 对活跃包 Pack 2 关联的 f2 调用 Fsync，并在同包继续追加后验证不重复执行目录 fsync
	require.NoError(t, actor.Fsync(ctx, f2.Ino))
	require.Equal(t, uint64(1), gatherHistogramCount(t, reg, "bowl_storage_blob_sync_duration_seconds", map[string]string{
		"target":  "pack_data",
		"trigger": "fsync_barrier",
	}))
	require.Equal(t, uint64(2), gatherHistogramCount(t, reg, "bowl_storage_blob_sync_duration_seconds", map[string]string{
		"target":  "packs_dir",
		"trigger": "fsync_barrier",
	}))

	_, _, err = actor.Write(ctx, f2.Ino, store.BlobSize, bytes.Repeat([]byte{0x33}, 4096))
	require.NoError(t, err)
	require.NoError(t, actor.Fsync(ctx, f2.Ino))
	require.Equal(t, uint64(2), gatherHistogramCount(t, reg, "bowl_storage_blob_sync_duration_seconds", map[string]string{
		"target":  "pack_data",
		"trigger": "fsync_barrier",
	}))
	require.Equal(t, uint64(2), gatherHistogramCount(t, reg, "bowl_storage_blob_sync_duration_seconds", map[string]string{
		"target":  "packs_dir",
		"trigger": "fsync_barrier",
	}), "已同步过的活跃 .pack 继续追加不得重复触发目录 fsync")
}
