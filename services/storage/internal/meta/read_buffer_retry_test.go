package meta_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/readbuf"
)

type partialMissingBlockStore struct {
	meta.BlockStore
	beforeRetry func(context.Context) error
	injected    atomic.Bool
}

func (s *partialMissingBlockStore) GetBlock(ctx context.Context, sliceID uint64, blockIdx, blockSize, offset uint32, dst []byte) error {
	if s.beforeRetry != nil && s.injected.CompareAndSwap(false, true) {
		for i := range dst {
			dst[i] = 0xD3
		}
		if err := s.beforeRetry(ctx); err != nil {
			return err
		}
		return meta.ErrBlockNotFound
	}
	return s.BlockStore.GetBlock(ctx, sliceID, blockIdx, blockSize, offset, dst)
}

func TestRead_PooledRetryDoesNotExposePartialDataAfterPunchHole(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	acquireTestSliceGCAdvisoryLock(t, pool)
	store := db.NewStore(pool)
	blocks := &partialMissingBlockStore{BlockStore: meta.NewLocalFSBlockStore(t.TempDir())}
	mgr := meta.New(store, meta.WithBlockStore(blocks))
	inode, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode: int32(meta.S_IFREG | 0o600), Nlink: 1,
	})
	require.NoError(t, err)
	opened, err := mgr.Open(ctx, meta.OpenParams{Ino: uint64(inode.Ino), Flags: meta.O_RDWR})
	require.NoError(t, err)
	defer cleanupWriteTestInoAndSlices(t, store, mgr, inode.Ino, opened.Fh)

	const size = uint32(4 << 20)
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino: uint64(inode.Ino), Fh: opened.Fh, Data: bytes.Repeat([]byte{0xA5}, int(size)),
	})
	require.NoError(t, err)
	blocks.beforeRetry = func(ctx context.Context) error {
		// 首次读取留下部分旧字节后，另一次真实变更把整个区间打洞。
		return mgr.Fallocate(ctx, meta.FallocateParams{
			Ino: uint64(inode.Ino), Fh: opened.Fh, Mode: 0x01 | 0x02, Length: uint64(size),
		})
	}
	scoped, release := readbuf.NewPool().WithScope(ctx)
	defer release()
	response, err := mgr.Read(scoped, &meta.ReadRequest{
		Ino: uint64(inode.Ino), Fh: opened.Fh, Size: size,
	})
	require.NoError(t, err)
	require.True(t, blocks.injected.Load(), "故障注入必须实际命中物理读取")
	require.True(t, bytes.Equal(make([]byte, size), response.Data), "重试后的空洞不得包含首次失败读取的旧字节")
}
