package meta

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// TestDeletePebbleRangePointwiseBatchShape 锁定删除策略的批次形状：
// 0 键零写入（绝不写范围墓碑），不超过 64 键逐键点删，超过 64 键回退为单个范围墓碑。
func TestDeletePebbleRangePointwiseBatchShape(t *testing.T) {
	store, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	lower, upper := []byte("xattr:1:\x00"), []byte("xattr:1:\xff")
	seed := func(n int) {
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("xattr:1:%03d", i))
			require.NoError(t, store.Set(key, []byte("v"), pebble.Sync))
		}
	}
	deleteAndCount := func() uint32 {
		batch := store.NewBatch()
		defer func() { require.NoError(t, batch.Close()) }()
		require.NoError(t, deletePebbleRangePointwise(store, batch, lower, upper))
		count := batch.Count()
		require.NoError(t, batch.Commit(pebble.Sync))
		return count
	}
	assertRangeEmpty := func() {
		iter, err := store.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		require.NoError(t, err)
		defer func() { require.NoError(t, iter.Close()) }()
		require.False(t, iter.First(), "删除后区间内不得残留任何键")
	}

	require.Zero(t, deleteAndCount(), "0 键必须零写入，不得写范围墓碑")

	seed(64)
	require.EqualValues(t, 64, deleteAndCount(), "<= 64 键必须逐键点删")
	assertRangeEmpty()

	seed(65)
	require.EqualValues(t, 1, deleteAndCount(), "> 64 键必须回退为单个范围墓碑")
	assertRangeEmpty()
}
