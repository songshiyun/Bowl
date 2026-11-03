package meta_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
)

func inodeToRow(inode db.Inode, indx pgtype.Int8, sliceData []byte) db.GetInodeSizeAndChunksInRangeRow {
	return db.GetInodeSizeAndChunksInRangeRow{
		Ino:           inode.Ino,
		Generation:    inode.Generation,
		Mode:          inode.Mode,
		Size:          inode.Size,
		Nlink:         inode.Nlink,
		Uid:           inode.Uid,
		Gid:           inode.Gid,
		Rdev:          inode.Rdev,
		Atime:         inode.Atime,
		Mtime:         inode.Mtime,
		Ctime:         inode.Ctime,
		SymlinkTarget: inode.SymlinkTarget,
		Flags:         inode.Flags,
		ParentIno:     inode.ParentIno,
		RootIno:       inode.RootIno,
		ChunkIndx:     indx,
		ChunkSlices:   sliceData,
	}
}

func inodeToReadRow(inode db.Inode, fhFound bool, fhFlags int32, indx pgtype.Int8, sliceData []byte) db.GetInodeChunksAndHandleForReadRow {
	return db.GetInodeChunksAndHandleForReadRow{
		Ino:           inode.Ino,
		Generation:    inode.Generation,
		Mode:          inode.Mode,
		Size:          inode.Size,
		Nlink:         inode.Nlink,
		Uid:           inode.Uid,
		Gid:           inode.Gid,
		Rdev:          inode.Rdev,
		Atime:         inode.Atime,
		Mtime:         inode.Mtime,
		Ctime:         inode.Ctime,
		SymlinkTarget: inode.SymlinkTarget,
		Flags:         inode.Flags,
		ParentIno:     inode.ParentIno,
		RootIno:       inode.RootIno,
		FhFound:       fhFound,
		FhFlags:       fhFlags,
		ChunkIndx:     indx,
		ChunkSlices:   sliceData,
	}
}

// TestRead_Validation 验证 Read 的入参边界防护与合法性静态校验。
func TestRead_Validation(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mgr := meta.New(mockStore)

	tests := []struct {
		name string
		req  *meta.ReadRequest
	}{
		{
			name: "req 为 nil",
			req:  nil,
		},
		{
			name: "ino 为 0",
			req: &meta.ReadRequest{
				Ino:  0,
				Fh:   1,
				Size: 1024,
			},
		},
		{
			name: "ino 超过 MaxInt64",
			req: &meta.ReadRequest{
				Ino:  uint64(math.MaxInt64) + 1,
				Fh:   1,
				Size: 1024,
			},
		},
		{
			name: "fh 为 0",
			req: &meta.ReadRequest{
				Ino:  1,
				Fh:   0,
				Size: 1024,
			},
		},
		{
			name: "fh 超过 MaxInt64",
			req: &meta.ReadRequest{
				Ino:  1,
				Fh:   uint64(math.MaxInt64) + 1,
				Size: 1024,
			},
		},
		{
			name: "offset 超过 MaxInt64",
			req: &meta.ReadRequest{
				Ino:    1,
				Fh:     1,
				Offset: uint64(math.MaxInt64) + 1,
				Size:   1024,
			},
		},
		{
			name: "size 超过 4 MiB (BlockSize)",
			req: &meta.ReadRequest{
				Ino:  1,
				Fh:   1,
				Size: meta.BlockSize + 1,
			},
		},
		{
			name: "session_id 为 0",
			req: &meta.ReadRequest{
				Ino:       1,
				Fh:        1,
				Size:      1024,
				SessionID: ptrOf(uint64(0)),
			},
		},
		{
			name: "session_id 超过 MaxInt64",
			req: &meta.ReadRequest{
				Ino:       1,
				Fh:        1,
				Size:      1024,
				SessionID: ptrOf(uint64(math.MaxInt64) + 1),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := mgr.Read(ctx, tc.req)
			require.Nil(t, resp)
			require.ErrorIs(t, err, meta.ErrInvalidArgument)
		})
	}
}

// TestRead_HandleAndFileTypeValidation 验证 Inode 类型校验与 FileHandle 权限判定。
func TestRead_HandleAndFileTypeValidation(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(10)

	baseFileRow := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Size:  1024,
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("Inode 不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return(nil, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:  ino,
			Fh:   fh,
			Size: 100,
		})
		require.Nil(t, resp)
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("目标为目录返回 ErrIsDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		dirRow := baseFileRow
		dirRow.Mode = int32(meta.S_IFDIR | 0o755)

		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(dirRow, false, 0, pgtype.Int8{Valid: false}, nil),
			}, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:  ino,
			Fh:   fh,
			Size: 100,
		})
		require.Nil(t, resp)
		require.ErrorIs(t, err, meta.ErrIsDir)
	})

	t.Run("目标为非常规文件返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		sockRow := baseFileRow
		sockRow.Mode = int32(meta.S_IFSOCK | 0o666)

		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(sockRow, false, 0, pgtype.Int8{Valid: false}, nil),
			}, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:  ino,
			Fh:   fh,
			Size: 100,
		})
		require.Nil(t, resp)
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("句柄不存在返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(baseFileRow, false, 0, pgtype.Int8{Valid: false}, nil),
			}, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:  ino,
			Fh:   fh,
			Size: 100,
		})
		require.Nil(t, resp)
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})

	t.Run("句柄为只写模式 (O_WRONLY) 返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(baseFileRow, true, int32(meta.O_WRONLY), pgtype.Int8{Valid: false}, nil),
			}, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:  ino,
			Fh:   fh,
			Size: 100,
		})
		require.Nil(t, resp)
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})

	t.Run("句柄具备 O_RDONLY 读权限允许正常执行", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(baseFileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Valid: false}, nil),
			}, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, make([]byte, 10), resp.Data)
	})

	t.Run("句柄具备 O_RDWR 读写权限允许正常读取", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(baseFileRow, true, int32(meta.O_RDWR), pgtype.Int8{Valid: false}, nil),
			}, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, make([]byte, 10), resp.Data)
	})
}

// TestRead_EOFAndTruncation 验证 POSIX pread(2) 到达 EOF 的空流返回与越界自动截断语义。
func TestRead_EOFAndTruncation(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(200)
	fh := uint64(20)

	fileRow := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Size:  100, // 文件大小 100 字节
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("Size == 0 时直接返回空字节流且不查 Chunk 表", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(fileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Valid: false}, nil),
			}, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   0,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Data)
	})

	t.Run("Offset == Size 时 (恰好 EOF) 直接返回空字节流", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(fileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Valid: false}, nil),
			}, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 100, // 等于 inode.Size
			Size:   50,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Data)
	})

	t.Run("Offset > Size 时 (超越 EOF) 直接返回空字节流", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(fileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Valid: false}, nil),
			}, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 200, // 远大于 inode.Size
			Size:   50,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Data)
	})

	t.Run("Offset + Size > Size 时自动截断实际读取长度为 inode.Size - Offset", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		// 构造一个包含完整 100 字节的 chunk
		sliceID := uint64(555)
		sliceDesc := meta.Slice{
			Pos:  0,
			ID:   sliceID,
			Size: 100,
			Off:  0,
			Len:  100,
		}
		rawSlices := meta.EncodeSlice(sliceDesc)

		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(fileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Int64: 0, Valid: true}, rawSlices),
			}, nil).
			Once()

		// 请求 offset=80, size=50 -> 超出 100 边界，截断为 readLen = 100 - 80 = 20
		// overlapStart = 80, overlapEnd = 100, overlapLen = 20, sliceOff = 80
		expectedData := bytes.Repeat([]byte("T"), 20)
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID, uint32(0), uint32(100), uint32(80), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				require.Len(t, dst, 20)
				copy(dst, expectedData)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 80,
			Size:   50,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 20, len(resp.Data), "实际返回长度必须被截断为 20 字节")
		assert.Equal(t, expectedData, resp.Data)
	})
}

// TestRead_SparseHolesAndUnwrittenChunks 验证稀疏文件未写入 Chunk 与跳写空洞切片直接读零且不产生磁盘 IO。
func TestRead_SparseHolesAndUnwrittenChunks(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(300)
	fh := uint64(30)

	fileRow := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Size:  1000, // 逻辑大小 1000
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("目标 Chunk 在 chunks 表中不存在时直接返回全零缓冲区且不触发 BlockStore", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(fileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Valid: false}, nil),
			}, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   64,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, make([]byte, 64), resp.Data)
		// mockBS 绝未被调用
	})

	t.Run("chunks 表中的 slices 为空时直接返回全零缓冲区", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(fileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Int64: 0, Valid: true}, []byte{}),
			}, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 10,
			Size:   30,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, make([]byte, 30), resp.Data)
	})

	t.Run("稀疏跳写空洞切片 (ID == 0) 与视图末尾区域读零", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		// 仅在 pos=100 处写入了 50 字节数据，区间 [100, 150)
		sliceID := uint64(777)
		sliceDesc := meta.Slice{
			Pos:  100,
			ID:   sliceID,
			Size: 50,
			Off:  0,
			Len:  50,
		}
		rawSlices := meta.EncodeSlice(sliceDesc)

		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(fileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Int64: 0, Valid: true}, rawSlices),
			}, nil).
			Once()

		// 请求读取 [50, 200) 共 150 字节：
		// [50, 100) 为 BuildSlice 合成的空洞区间 -> 保持 0x00
		// [100, 150) 为有效切片数据 -> 从 BlockStore 读取 50 字节
		// [150, 200) 为视图末尾之外的未写入区域 -> 保持 0x00
		realData := bytes.Repeat([]byte{0xEE}, 50)
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID, uint32(0), uint32(50), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				require.Len(t, dst, 50)
				copy(dst, realData)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 50,
			Size:   150,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 150, len(resp.Data))

		// 验证结构: 50 字节 0x00 + 50 字节 0xEE + 50 字节 0x00
		expectedBuf := make([]byte, 150)
		copy(expectedBuf[50:100], realData)
		assert.Equal(t, expectedBuf, resp.Data)
	})
}

// TestRead_SequentialAndOverwrites 验证顺序写入与各种复杂重叠覆写场景下的 buildSlice 精确重组。
func TestRead_SequentialAndOverwrites(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(400)
	fh := uint64(40)

	fileRow := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Size:  1000,
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	setupMocksWithChunk := func(mockStore *dbmock.MockStore, _ *dbmock.MockQuerier, chunkSlices []byte, _ ...int64) {
		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(fileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Int64: 0, Valid: true}, chunkSlices),
			}, nil).
			Once()
	}

	t.Run("顺序单切片完整读取", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID := uint64(101)
		slices := []meta.Slice{
			{Pos: 0, ID: sliceID, Size: 100, Off: 0, Len: 100},
		}

		setupMocksWithChunk(mockStore, mockQuerier, meta.EncodeSlices(slices), int64(sliceID))

		data := bytes.Repeat([]byte("A"), 100)
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID, uint32(0), uint32(100), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, data)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   100,
		})
		require.NoError(t, err)
		assert.Equal(t, data, resp.Data)
	})

	t.Run("完全覆盖：后写切片完整遮蔽先写切片", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID1 := uint64(101)
		sliceID2 := uint64(102)
		slices := []meta.Slice{
			{Pos: 0, ID: sliceID1, Size: 100, Off: 0, Len: 100}, // 先写全 A
			{Pos: 0, ID: sliceID2, Size: 100, Off: 0, Len: 100}, // 后写全 B 完全覆盖
		}

		setupMocksWithChunk(mockStore, mockQuerier, meta.EncodeSlices(slices), int64(sliceID2))

		dataB := bytes.Repeat([]byte("B"), 100)
		// 仅从 sliceID2 读取，绝不读取 sliceID1
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID2, uint32(0), uint32(100), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataB)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   100,
		})
		require.NoError(t, err)
		assert.Equal(t, dataB, resp.Data)
	})

	t.Run("头部覆盖：后写切片覆盖前半段", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID1 := uint64(101)
		sliceID2 := uint64(102)
		slices := []meta.Slice{
			{Pos: 0, ID: sliceID1, Size: 100, Off: 0, Len: 100}, // [0, 100) A
			{Pos: 0, ID: sliceID2, Size: 30, Off: 0, Len: 30},   // [0, 30) B 头部覆盖
		}

		setupMocksWithChunk(mockStore, mockQuerier, meta.EncodeSlices(slices), int64(sliceID1), int64(sliceID2))

		dataB := bytes.Repeat([]byte("B"), 30)
		dataA := bytes.Repeat([]byte("A"), 70)

		// [0, 30) 读 sliceID2
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID2, uint32(0), uint32(30), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataB)
				return nil
			}).
			Once()

		// [30, 100) 读 sliceID1 偏移 30 处 70 字节
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID1, uint32(0), uint32(100), uint32(30), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataA)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   100,
		})
		require.NoError(t, err)
		expected := append(dataB, dataA...)
		assert.Equal(t, expected, resp.Data)
	})

	t.Run("尾部覆盖：后写切片覆盖后半段", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID1 := uint64(101)
		sliceID2 := uint64(102)
		slices := []meta.Slice{
			{Pos: 0, ID: sliceID1, Size: 100, Off: 0, Len: 100}, // [0, 100) A
			{Pos: 70, ID: sliceID2, Size: 30, Off: 0, Len: 30},  // [70, 100) B 尾部覆盖
		}

		setupMocksWithChunk(mockStore, mockQuerier, meta.EncodeSlices(slices), int64(sliceID1), int64(sliceID2))

		dataA := bytes.Repeat([]byte("A"), 70)
		dataB := bytes.Repeat([]byte("B"), 30)

		// [0, 70) 读 sliceID1 偏移 0
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID1, uint32(0), uint32(100), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataA)
				return nil
			}).
			Once()

		// [70, 100) 读 sliceID2 偏移 0
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID2, uint32(0), uint32(30), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataB)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   100,
		})
		require.NoError(t, err)
		expected := append(dataA, dataB...)
		assert.Equal(t, expected, resp.Data)
	})

	t.Run("中间劈裂覆盖：后写切片将先写切片劈裂为左右两段", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID1 := uint64(101)
		sliceID2 := uint64(102)
		slices := []meta.Slice{
			{Pos: 0, ID: sliceID1, Size: 100, Off: 0, Len: 100}, // [0, 100) A
			{Pos: 30, ID: sliceID2, Size: 40, Off: 0, Len: 40},  // [30, 70) B 中间覆写
		}

		setupMocksWithChunk(mockStore, mockQuerier, meta.EncodeSlices(slices), int64(sliceID1), int64(sliceID2))

		dataALeft := bytes.Repeat([]byte("A"), 30)
		dataB := bytes.Repeat([]byte("B"), 40)
		dataARight := bytes.Repeat([]byte("A"), 30)

		// 左段 [0, 30): sliceID1, off=0, len=30
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID1, uint32(0), uint32(100), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataALeft)
				return nil
			}).
			Once()

		// 中段 [30, 70): sliceID2, off=0, len=40
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID2, uint32(0), uint32(40), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataB)
				return nil
			}).
			Once()

		// 右段 [70, 100): sliceID1, off=70, len=30
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID1, uint32(0), uint32(100), uint32(70), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataARight)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   100,
		})
		require.NoError(t, err)
		expected := append(append(dataALeft, dataB...), dataARight...)
		assert.Equal(t, expected, resp.Data)
	})

	t.Run("中间打洞覆盖 (ID == 0)：中间空洞切片不触发 BlockStore 且填零", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID1 := uint64(101)
		slices := []meta.Slice{
			{Pos: 0, ID: sliceID1, Size: 100, Off: 0, Len: 100}, // [0, 100) A
			{Pos: 30, ID: 0, Size: 0, Off: 0, Len: 40},          // [30, 70) 打洞空洞
		}

		setupMocksWithChunk(mockStore, mockQuerier, meta.EncodeSlices(slices), int64(sliceID1))

		dataALeft := bytes.Repeat([]byte("A"), 30)
		dataARight := bytes.Repeat([]byte("A"), 30)

		// 左段 [0, 30)
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID1, uint32(0), uint32(100), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataALeft)
				return nil
			}).
			Once()

		// 中段 [30, 70) 为空洞，不调用 mockBS！

		// 右段 [70, 100)
		mockBS.EXPECT().
			GetBlock(mock.Anything, sliceID1, uint32(0), uint32(100), uint32(70), mock.Anything).
			RunAndReturn(func(ctx context.Context, sid uint64, bIdx, bSize, off uint32, dst []byte) error {
				copy(dst, dataARight)
				return nil
			}).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   100,
		})
		require.NoError(t, err)
		expected := append(append(dataALeft, make([]byte, 40)...), dataARight...)
		assert.Equal(t, expected, resp.Data)
	})
}

// TestRead_AtimeModeAndSessionRenew 验证 AtimeMode (noatime/relatime/strictatime) 判定与 session_id 校验。
func TestRead_AtimeModeAndSessionRenew(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(500)
	fh := uint64(50)

	setupFileAndChunkMocks := func(mockStore *dbmock.MockStore, fileRow db.Inode) {
		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(fileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Valid: false}, nil),
			}, nil).
			Once()
	}

	t.Run("NoAtime 模式下绝不调用 TouchInodeAtime", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore, meta.WithAtimeMode(meta.AtimeModeNoAtime))

		fileRow := db.Inode{
			Ino:   int64(ino),
			Mode:  int32(meta.S_IFREG | 0o644),
			Size:  100,
			Atime: pgtype.Timestamptz{Time: now.Add(-48 * time.Hour), Valid: true}, // 很久以前
			Mtime: pgtype.Timestamptz{Time: now, Valid: true},
			Ctime: pgtype.Timestamptz{Time: now, Valid: true},
		}
		setupFileAndChunkMocks(mockStore, fileRow)

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		// mockStore.TouchInodeAtime 未被期望，若被调用则会 panic/fail
	})

	t.Run("StrictAtime 模式下超过 1 秒触发 TouchInodeAtime", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore, meta.WithAtimeMode(meta.AtimeModeStrictAtime))

		fileRow := db.Inode{
			Ino:   int64(ino),
			Mode:  int32(meta.S_IFREG | 0o644),
			Size:  100,
			Atime: pgtype.Timestamptz{Time: now.Add(-5 * time.Second), Valid: true},
			Mtime: pgtype.Timestamptz{Time: now, Valid: true},
			Ctime: pgtype.Timestamptz{Time: now, Valid: true},
		}
		setupFileAndChunkMocks(mockStore, fileRow)

		mockStore.EXPECT().
			TouchInodeAtime(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("RelAtime 模式下 atime < mtime 触发 TouchInodeAtime", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore, meta.WithAtimeMode(meta.AtimeModeRelAtime))

		fileRow := db.Inode{
			Ino:   int64(ino),
			Mode:  int32(meta.S_IFREG | 0o644),
			Size:  100,
			Atime: pgtype.Timestamptz{Time: now.Add(-10 * time.Minute), Valid: true},
			Mtime: pgtype.Timestamptz{Time: now.Add(-5 * time.Minute), Valid: true}, // mtime 更新
			Ctime: pgtype.Timestamptz{Time: now.Add(-5 * time.Minute), Valid: true},
		}
		setupFileAndChunkMocks(mockStore, fileRow)

		mockStore.EXPECT().
			TouchInodeAtime(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("RelAtime 模式下 atime >= mtime 且距离现在不足 24h 不触发 TouchInodeAtime", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore, meta.WithAtimeMode(meta.AtimeModeRelAtime))

		fileRow := db.Inode{
			Ino:   int64(ino),
			Mode:  int32(meta.S_IFREG | 0o644),
			Size:  100,
			Atime: pgtype.Timestamptz{Time: now.Add(-1 * time.Hour), Valid: true},
			Mtime: pgtype.Timestamptz{Time: now.Add(-2 * time.Hour), Valid: true}, // atime 比 mtime 新
			Ctime: pgtype.Timestamptz{Time: now.Add(-2 * time.Hour), Valid: true},
		}
		setupFileAndChunkMocks(mockStore, fileRow)

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("携带有效 session_id 时透传至合并读取查询", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)
		sessionID := uint64(8888)

		fileRow := db.Inode{
			Ino:   int64(ino),
			Mode:  int32(meta.S_IFREG | 0o644),
			Size:  100,
			Atime: pgtype.Timestamptz{Time: now, Valid: true},
			Mtime: pgtype.Timestamptz{Time: now, Valid: true},
			Ctime: pgtype.Timestamptz{Time: now, Valid: true},
		}

		mockStore.EXPECT().
			GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
				StartIndx: 0,
				EndIndx:   0,
			}).
			Return([]db.GetInodeChunksAndHandleForReadRow{
				inodeToReadRow(fileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Valid: false}, nil),
			}, nil).
			Once()

		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:       ino,
			Fh:        fh,
			Offset:    0,
			Size:      10,
			SessionID: &sessionID,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

// TestRead_BlockStoreIOError 验证 BlockStore 发生读取错误时向上透传包装包含 ErrIO 的错误。
func TestRead_BlockStoreIOError(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(600)
	fh := uint64(60)

	fileRow := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Size:  100,
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	mockStore := dbmock.NewMockStore(t)
	mockBS := metamock.NewMockBlockStore(t)
	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

	sliceID := uint64(999)
	sliceDesc := meta.Slice{
		Pos:  0,
		ID:   sliceID,
		Size: 100,
		Off:  0,
		Len:  100,
	}

	mockStore.EXPECT().
		GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
			Ino:       int64(ino),
			Fh:        int64(fh),
			StartIndx: 0,
			EndIndx:   0,
		}).
		Return([]db.GetInodeChunksAndHandleForReadRow{
			inodeToReadRow(fileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Int64: 0, Valid: true}, meta.EncodeSlice(sliceDesc)),
		}, nil).
		Once()

	mockBS.EXPECT().
		GetBlock(mock.Anything, sliceID, uint32(0), uint32(100), uint32(0), mock.Anything).
		Return(fmt.Errorf("%w: failed to read nvme file", meta.ErrIO)).
		Once()

	resp, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   100,
	})
	require.Nil(t, resp)
	require.Error(t, err)
	assert.True(t, errors.Is(err, meta.ErrIO), "必须包含 ErrIO 哨兵错误以映射到 EIO")
}

// TestRead_LocalFSBlockStoreIntegration 验证使用真实 LocalFSBlockStore 从本地磁盘物理文件读取数据。
func TestRead_LocalFSBlockStoreIntegration(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(700)
	fh := uint64(70)

	tmpDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(tmpDir)

	mockStore := dbmock.NewMockStore(t)
	mgr := meta.New(mockStore, meta.WithBlockStore(bs))

	payload := []byte("HelloWorld-LocalFSBlockStore-ReadVerification")
	sliceID := uint64(888)

	// 先在物理介质写入真实 block
	err := bs.PutBlock(ctx, sliceID, 0, payload)
	require.NoError(t, err)

	fileRow := db.Inode{
		Ino:   int64(ino),
		Mode:  int32(meta.S_IFREG | 0o644),
		Size:  int64(len(payload)),
		Atime: pgtype.Timestamptz{Time: now, Valid: true},
		Mtime: pgtype.Timestamptz{Time: now, Valid: true},
		Ctime: pgtype.Timestamptz{Time: now, Valid: true},
	}

	sliceDesc := meta.Slice{
		Pos:  0,
		ID:   sliceID,
		Size: uint32(len(payload)),
		Off:  0,
		Len:  uint32(len(payload)),
	}

	mockStore.EXPECT().
		GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
			Ino:       int64(ino),
			Fh:        int64(fh),
			StartIndx: 0,
			EndIndx:   0,
		}).
		Return([]db.GetInodeChunksAndHandleForReadRow{
			inodeToReadRow(fileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Int64: 0, Valid: true}, meta.EncodeSlice(sliceDesc)),
		}, nil).
		Once()

	resp, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   uint32(len(payload)),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, payload, resp.Data)
}

// TestRead_SliceRefLockRetryAndExhaustion 单元测试并发 GC 删块触发重读快照与耗尽行为
func TestRead_SliceRefLockRetryAndExhaustion(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(500)
	fh := uint64(50)

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       1024,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("首次读取物理块不存在触发重读快照拿到合并后新 sliceID 成功读取", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)

		slice101 := meta.EncodeSlice(meta.Slice{Pos: 0, ID: 101, Size: 1024, Off: 0, Len: 1024})
		slice202 := meta.EncodeSlice(meta.Slice{Pos: 0, ID: 202, Size: 1024, Off: 0, Len: 1024})

		// 第一次调用 GetInodeChunksAndHandleForRead 返回 sliceID=101
		mockStore.EXPECT().GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
			Ino:       int64(ino),
			Fh:        int64(fh),
			StartIndx: 0,
			EndIndx:   0,
		}).Return([]db.GetInodeChunksAndHandleForReadRow{
			inodeToReadRow(baseFileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Int64: 0, Valid: true}, slice101),
		}, nil).Once()

		mockBS.EXPECT().GetBlock(mock.Anything, uint64(101), uint32(0), uint32(1024), uint32(0), mock.Anything).
			Return(fmt.Errorf("%w: old block removed", meta.ErrBlockNotFound)).Once()

		// attempt 1 触发重新获取快照，返回新合并的 sliceID=202
		mockStore.EXPECT().GetInodeSizeAndChunksInRange(mock.Anything, db.GetInodeSizeAndChunksInRangeParams{
			Ino:       int64(ino),
			StartIndx: 0,
			EndIndx:   0,
		}).Return([]db.GetInodeSizeAndChunksInRangeRow{
			inodeToRow(baseFileRow, pgtype.Int8{Int64: 0, Valid: true}, slice202),
		}, nil).Once()

		expectedData := bytes.Repeat([]byte("X"), 1024)
		mockBS.EXPECT().GetBlock(mock.Anything, uint64(202), uint32(0), uint32(1024), uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sliceID uint64, blockIdx, blockSize, offset uint32, dst []byte) error {
				copy(dst, expectedData)
				return nil
			}).Once()

		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   1024,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, expectedData, resp.Data)
	})

	t.Run("重读快照发现并发截断至 EOF 返回空字节流", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)

		slice101 := meta.EncodeSlice(meta.Slice{Pos: 0, ID: 101, Size: 1024, Off: 0, Len: 1024})

		// 第一次调用返回 size=1024
		mockStore.EXPECT().GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
			Ino:       int64(ino),
			Fh:        int64(fh),
			StartIndx: 0,
			EndIndx:   0,
		}).Return([]db.GetInodeChunksAndHandleForReadRow{
			inodeToReadRow(baseFileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Int64: 0, Valid: true}, slice101),
		}, nil).Once()

		mockBS.EXPECT().GetBlock(mock.Anything, uint64(101), uint32(0), uint32(1024), uint32(0), mock.Anything).
			Return(fmt.Errorf("%w: old block removed", meta.ErrBlockNotFound)).Once()

		// 重读快照：文件大小被截断为 0
		truncatedRow := baseFileRow
		truncatedRow.Size = 0
		mockStore.EXPECT().GetInodeSizeAndChunksInRange(mock.Anything, db.GetInodeSizeAndChunksInRangeParams{
			Ino:       int64(ino),
			StartIndx: 0,
			EndIndx:   0,
		}).Return([]db.GetInodeSizeAndChunksInRangeRow{
			inodeToRow(truncatedRow, pgtype.Int8{Valid: false}, nil),
		}, nil).Once()

		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   1024,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Data, "并发截断至 EOF 时安全返回空字节流")
	})

	t.Run("重读快照发现节点已被删除返回 meta.ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)

		slice101 := meta.EncodeSlice(meta.Slice{Pos: 0, ID: 101, Size: 1024, Off: 0, Len: 1024})

		mockStore.EXPECT().GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
			Ino:       int64(ino),
			Fh:        int64(fh),
			StartIndx: 0,
			EndIndx:   0,
		}).Return([]db.GetInodeChunksAndHandleForReadRow{
			inodeToReadRow(baseFileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Int64: 0, Valid: true}, slice101),
		}, nil).Once()

		mockBS.EXPECT().GetBlock(mock.Anything, uint64(101), uint32(0), uint32(1024), uint32(0), mock.Anything).
			Return(fmt.Errorf("%w: old block removed", meta.ErrBlockNotFound)).Once()

		// 重读快照：节点已被删除，返回空列表
		mockStore.EXPECT().GetInodeSizeAndChunksInRange(mock.Anything, db.GetInodeSizeAndChunksInRangeParams{
			Ino:       int64(ino),
			StartIndx: 0,
			EndIndx:   0,
		}).Return([]db.GetInodeSizeAndChunksInRangeRow{}, nil).Once()

		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   1024,
		})
		require.Nil(t, resp)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("重读快照数据库报错原样上抛", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)

		slice101 := meta.EncodeSlice(meta.Slice{Pos: 0, ID: 101, Size: 1024, Off: 0, Len: 1024})

		mockStore.EXPECT().GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
			Ino:       int64(ino),
			Fh:        int64(fh),
			StartIndx: 0,
			EndIndx:   0,
		}).Return([]db.GetInodeChunksAndHandleForReadRow{
			inodeToReadRow(baseFileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Int64: 0, Valid: true}, slice101),
		}, nil).Once()

		mockBS.EXPECT().GetBlock(mock.Anything, uint64(101), uint32(0), uint32(1024), uint32(0), mock.Anything).
			Return(fmt.Errorf("%w: old block removed", meta.ErrBlockNotFound)).Once()

		dbErr := errors.New("re-get query database failure")
		mockStore.EXPECT().GetInodeSizeAndChunksInRange(mock.Anything, db.GetInodeSizeAndChunksInRangeParams{
			Ino:       int64(ino),
			StartIndx: 0,
			EndIndx:   0,
		}).Return(nil, dbErr).Once()

		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   1024,
		})
		require.Nil(t, resp)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "re-get inode size and chunks")
	})

	t.Run("连续达最大重试上限 8 次块不存在后返回 meta.ErrIO", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)

		slice101 := meta.EncodeSlice(meta.Slice{Pos: 0, ID: 101, Size: 1024, Off: 0, Len: 1024})

		// 第 1 次调 GetInodeChunksAndHandleForRead，随后重试调 7 次 GetInodeSizeAndChunksInRange，共 8 次
		mockStore.EXPECT().GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
			Ino:       int64(ino),
			Fh:        int64(fh),
			StartIndx: 0,
			EndIndx:   0,
		}).Return([]db.GetInodeChunksAndHandleForReadRow{
			inodeToReadRow(baseFileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Int64: 0, Valid: true}, slice101),
		}, nil).Once()

		mockStore.EXPECT().GetInodeSizeAndChunksInRange(mock.Anything, db.GetInodeSizeAndChunksInRangeParams{
			Ino:       int64(ino),
			StartIndx: 0,
			EndIndx:   0,
		}).Return([]db.GetInodeSizeAndChunksInRangeRow{
			inodeToRow(baseFileRow, pgtype.Int8{Int64: 0, Valid: true}, slice101),
		}, nil).Times(7)

		mockBS.EXPECT().GetBlock(mock.Anything, uint64(101), uint32(0), uint32(1024), uint32(0), mock.Anything).
			Return(fmt.Errorf("%w: old block removed", meta.ErrBlockNotFound)).Times(8)

		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
		resp, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   1024,
		})
		require.Nil(t, resp)
		require.Error(t, err)
		assert.ErrorIs(t, err, meta.ErrIO)
	})
}

// TestRead_BlockNotFoundOptimisticRetry 验证当 GetBlock 遇到 ErrBlockNotFound 时自动重查最新 Chunk 快照完成重试读取。
func TestRead_BlockNotFoundOptimisticRetry(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(501)
	fh := uint64(51)

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       1024,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	mockStore := dbmock.NewMockStore(t)
	mockBS := metamock.NewMockBlockStore(t)

	oldSlice := meta.EncodeSlice(meta.Slice{Pos: 0, ID: 301, Size: 1024, Off: 0, Len: 1024})
	newSlice := meta.EncodeSlice(meta.Slice{Pos: 0, ID: 302, Size: 1024, Off: 0, Len: 1024})

	// 第一次查询返回旧切片 301
	mockStore.EXPECT().GetInodeChunksAndHandleForRead(mock.Anything, db.GetInodeChunksAndHandleForReadParams{
		Ino:       int64(ino),
		Fh:        int64(fh),
		StartIndx: 0,
		EndIndx:   0,
	}).Return([]db.GetInodeChunksAndHandleForReadRow{
		inodeToReadRow(baseFileRow, true, int32(meta.O_RDONLY), pgtype.Int8{Int64: 0, Valid: true}, oldSlice),
	}, nil).Once()

	// 旧切片 301 的物理块已被删除，返回 ErrBlockNotFound
	mockBS.EXPECT().GetBlock(mock.Anything, uint64(301), uint32(0), uint32(1024), uint32(0), mock.Anything).
		Return(fmt.Errorf("%w: old block removed", meta.ErrBlockNotFound)).Once()

	// 自动触发第 2 次查询拿到新合并切片 302
	mockStore.EXPECT().GetInodeSizeAndChunksInRange(mock.Anything, db.GetInodeSizeAndChunksInRangeParams{
		Ino:       int64(ino),
		StartIndx: 0,
		EndIndx:   0,
	}).Return([]db.GetInodeSizeAndChunksInRangeRow{
		inodeToRow(baseFileRow, pgtype.Int8{Int64: 0, Valid: true}, newSlice),
	}, nil).Once()

	expectedData := bytes.Repeat([]byte("Z"), 1024)
	mockBS.EXPECT().GetBlock(mock.Anything, uint64(302), uint32(0), uint32(1024), uint32(0), mock.Anything).
		RunAndReturn(func(ctx context.Context, sliceID uint64, blockIdx, blockSize, offset uint32, dst []byte) error {
			copy(dst, expectedData)
			return nil
		}).Once()

	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
	resp, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   1024,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, expectedData, resp.Data)
}

// TestRead_PostgresIntegration_MergedSingleQueryAndErrorPriority 真实 PostgreSQL + BlockStore 集成测试：
// 1. 验证旧版 Read 先识别会话，再以一条 SQL 获取节点、数据和句柄；无会话时直接读取；
// 2. 验证错误优先级严格符合 ENOENT -> EISDIR / EINVAL -> EBADF（含不存在句柄、O_WRONLY、过期会话、陈旧 generation 句柄）；
// 3. 验证未写入 chunk、空文件、EOF、稀疏空洞（全空洞与局部空洞）及跨 chunk 读取返回精确预期字节。
func TestRead_PostgresIntegration_MergedSingleQueryAndErrorPriority(t *testing.T) {
	tracer := &openFastQueryTracer{}
	tracedPool, ctx := setupMetaTestDBWithTracer(t, tracer)
	if tracedPool == nil {
		return
	}

	store := db.NewStore(tracedPool)
	bs := meta.NewLocalFSBlockStore(t.TempDir())
	mgr := meta.New(store, meta.WithBlockStore(bs))

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-read-merged-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
	})

	sessID := uint64(time.Now().UnixNano() & 0x3FFFFFFF)
	_, err = mgr.RenewSessionWithDisk(ctx, sessID, &disk.DiskID)
	require.NoError(t, err)

	otherSessID := sessID + 1
	_, err = mgr.RenewSessionWithDisk(ctx, otherSessID, &disk.DiskID)
	require.NoError(t, err)

	expiredSessID := sessID + 2
	_, err = mgr.RenewSessionWithDisk(ctx, expiredSessID, &disk.DiskID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = tracedPool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2, $3)", int64(sessID), int64(otherSessID), int64(expiredSessID))
	})

	assertSingleReadSQL := func(t *testing.T, queries []string, hasSession bool) {
		t.Helper()
		query := assertLegacyDataQuery(t, queries, hasSession)
		upper := strings.ToUpper(strings.TrimSpace(query))
		assert.False(t, strings.HasPrefix(upper, "BEGIN"), "不应开启显式事务 BEGIN: %s", query)
		assert.False(t, strings.HasPrefix(upper, "COMMIT"), "不应存在显式事务 COMMIT: %s", query)
		assert.Contains(t, upper, "FILE_HANDLES", "单条查询必须内联校验 file_handles")
	}

	// 1. 准备目录与符号链接用于错误优先级验证
	subDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
		ParentIno: disk.RootIno,
		Name:      []byte("read_err_dir"),
		Mode:      0o755,
	})
	require.NoError(t, err)

	symNode, err := mgr.Symlink(ctx, meta.SymlinkParams{
		ParentIno: disk.RootIno,
		Name:      []byte("read_err_sym"),
		Target:    []byte("target.txt"),
	})
	require.NoError(t, err)

	// 2. 准备常规文件及不同状态句柄
	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("read_merged.bin"),
		Mode:      0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessID,
	})
	require.NoError(t, err)
	fileIno := createRes.Inode.Ino
	rwFh := createRes.Fh
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for indx := int64(0); indx <= 2; indx++ {
			chunkRow, cErr := store.GetChunk(cleanupCtx, db.GetChunkParams{Ino: int64(fileIno), Indx: indx})
			if cErr == nil && len(chunkRow.Slices) > 0 {
				if sls, dErr := meta.DecodeSlices(chunkRow.Slices); dErr == nil {
					for _, s := range sls {
						if s.ID > 0 {
							_ = store.DeleteSliceRef(cleanupCtx, int64(s.ID))
						}
					}
				}
			}
		}
		_ = meta.DeleteAllChunksByInos(cleanupCtx, store, []int64{int64(fileIno)})
	})

	// 无会话句柄
	noSessOpen, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   fileIno,
		Flags: meta.O_RDONLY,
	})
	require.NoError(t, err)
	noSessFh := noSessOpen.Fh

	// O_WRONLY 句柄
	wrOnlyOpen, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_WRONLY,
		SessionID: &sessID,
	})
	require.NoError(t, err)

	// 过期会话句柄
	expOpen, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDONLY,
		SessionID: &expiredSessID,
	})
	require.NoError(t, err)
	_, err = tracedPool.Exec(ctx, "UPDATE client_sessions SET expires_at = NOW() - INTERVAL '1 hour' WHERE session_id = $1", int64(expiredSessID))
	require.NoError(t, err)

	// 另一个常规文件的句柄（用于验证 fh 与 ino 不匹配返回 EBADF）
	otherFileCreate, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("read_other.bin"),
		Mode:      0o644,
		Flags:     meta.O_RDONLY,
		SessionID: &sessID,
	})
	require.NoError(t, err)

	// Open-Unlink-Release 回收后的已删除孤儿节点（验证 ENOENT 优先于 EBADF）
	reclaimedOrphan, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("reclaimed_orphan.bin"),
		Mode:      0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessID,
	})
	require.NoError(t, err)
	reclaimedOrphanIno := reclaimedOrphan.Inode.Ino
	require.NoError(t, mgr.Unlink(ctx, disk.RootIno, []byte("reclaimed_orphan.bin")))
	require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
		Ino:       reclaimedOrphanIno,
		Fh:        reclaimedOrphan.Fh,
		SessionID: &sessID,
	}))

	// 3. 验证错误优先级 ENOENT -> EISDIR / EINVAL -> EBADF。
	for _, tc := range []struct {
		name      string
		req       *meta.ReadRequest
		expectErr error
	}{
		{
			name:      "Inode 不存在返回 ErrNotFound (ENOENT)",
			req:       &meta.ReadRequest{Ino: 999999999, Fh: 999999, Offset: 0, Size: 64, SessionID: &sessID},
			expectErr: meta.ErrNotFound,
		},
		{
			name:      "Unlink+Release回收后节点返回 ErrNotFound (ENOENT 优先于 EBADF)",
			req:       &meta.ReadRequest{Ino: reclaimedOrphanIno, Fh: reclaimedOrphan.Fh, Offset: 0, Size: 64, SessionID: &sessID},
			expectErr: meta.ErrNotFound,
		},
		{
			name:      "目录 Inode 即使句柄不存在也优先返回 ErrIsDir (EISDIR)",
			req:       &meta.ReadRequest{Ino: subDir.Ino, Fh: 999999, Offset: 0, Size: 64, SessionID: &sessID},
			expectErr: meta.ErrIsDir,
		},
		{
			name:      "符号链接 Inode 即使句柄不存在也优先返回 ErrInvalidArgument (EINVAL)",
			req:       &meta.ReadRequest{Ino: symNode.Ino, Fh: 999999, Offset: 0, Size: 64, SessionID: &sessID},
			expectErr: meta.ErrInvalidArgument,
		},
		{
			name:      "句柄不存在返回 ErrBadFileHandle (EBADF)",
			req:       &meta.ReadRequest{Ino: fileIno, Fh: 999999999, Offset: 0, Size: 64, SessionID: &sessID},
			expectErr: meta.ErrBadFileHandle,
		},
		{
			name:      "O_WRONLY 句柄返回 ErrBadFileHandle (EBADF)",
			req:       &meta.ReadRequest{Ino: fileIno, Fh: wrOnlyOpen.Fh, Offset: 0, Size: 64, SessionID: &sessID},
			expectErr: meta.ErrBadFileHandle,
		},
		{
			name:      "跨会话访问返回 ErrBadFileHandle (EBADF)",
			req:       &meta.ReadRequest{Ino: fileIno, Fh: rwFh, Offset: 0, Size: 64, SessionID: &otherSessID},
			expectErr: meta.ErrBadFileHandle,
		},
		{
			name:      "无会话请求访问绑定会话句柄返回 ErrBadFileHandle (EBADF)",
			req:       &meta.ReadRequest{Ino: fileIno, Fh: rwFh, Offset: 0, Size: 64, SessionID: nil},
			expectErr: meta.ErrBadFileHandle,
		},
		{
			name:      "有会话请求访问无会话句柄返回 ErrBadFileHandle (EBADF)",
			req:       &meta.ReadRequest{Ino: fileIno, Fh: noSessFh, Offset: 0, Size: 64, SessionID: &sessID},
			expectErr: meta.ErrBadFileHandle,
		},
		{
			name:      "过期会话句柄返回 ErrBadFileHandle (EBADF)",
			req:       &meta.ReadRequest{Ino: fileIno, Fh: expOpen.Fh, Offset: 0, Size: 64, SessionID: &expiredSessID},
			expectErr: meta.ErrBadFileHandle,
		},
		{
			name:      "跨文件句柄不匹配返回 ErrBadFileHandle (EBADF)",
			req:       &meta.ReadRequest{Ino: fileIno, Fh: otherFileCreate.Fh, Offset: 0, Size: 64, SessionID: &sessID},
			expectErr: meta.ErrBadFileHandle,
		},
	} {
		t.Run("错误契约与单语句验证_"+tc.name, func(t *testing.T) {
			tracer.Reset()
			resp, rErr := mgr.Read(ctx, tc.req)
			assert.Nil(t, resp)
			require.ErrorIs(t, rErr, tc.expectErr)
			assertSingleReadSQL(t, tracer.Snapshot(), tc.req.SessionID != nil)
		})
	}

	// 4. 空文件读取 (size == 0)：返回空字节。
	t.Run("空文件读取返回空切片", func(t *testing.T) {
		tracer.Reset()
		resp, rErr := mgr.Read(ctx, &meta.ReadRequest{
			Ino:       fileIno,
			Fh:        rwFh,
			Offset:    0,
			Size:      4096,
			SessionID: &sessID,
		})
		require.NoError(t, rErr)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Data)
		assertSingleReadSQL(t, tracer.Snapshot(), true)
	})

	// 5. 未写入 chunk 的稀疏文件 (SetAttr 扩容但无 chunk 记录)：返回全零且仅 1 条 SQL
	sparseSize := uint64(meta.ChunkSize + 128*1024)
	_, err = mgr.SetAttr(ctx, fileIno, meta.SetAttrInput{Size: &sparseSize})
	require.NoError(t, err)

	t.Run("未写入chunk的全空洞文件读取返回全零且仅执行1条SQL", func(t *testing.T) {
		tracer.Reset()
		resp, rErr := mgr.Read(ctx, &meta.ReadRequest{
			Ino:       fileIno,
			Fh:        noSessFh,
			Offset:    0,
			Size:      64 * 1024,
			SessionID: nil,
		})
		require.NoError(t, rErr)
		require.NotNil(t, resp)
		assert.Equal(t, make([]byte, 64*1024), resp.Data)
		assertSingleReadSQL(t, tracer.Snapshot(), false)
	})

	t.Run("Offset大于等于文件Size返回EOF空切片", func(t *testing.T) {
		tracer.Reset()
		resp, rErr := mgr.Read(ctx, &meta.ReadRequest{
			Ino:       fileIno,
			Fh:        rwFh,
			Offset:    sparseSize + 1024,
			Size:      4096,
			SessionID: &sessID,
		})
		require.NoError(t, rErr)
		require.NotNil(t, resp)
		assert.Empty(t, resp.Data)
		assertSingleReadSQL(t, tracer.Snapshot(), true)
	})

	// 6. 局部空洞 + 跨 Chunk 边界写入与读取：验证数据精确。
	crossOffset := uint64(meta.ChunkSize - 64*1024)
	crossPayload := bytes.Repeat([]byte("M"), 128*1024) // [ChunkSize-64KiB, ChunkSize+64KiB)
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:       fileIno,
		Fh:        rwFh,
		Offset:    crossOffset,
		Data:      crossPayload,
		SessionID: &sessID,
	})
	require.NoError(t, err)

	t.Run("局部空洞与跨Chunk读取精确拼接", func(t *testing.T) {
		// 读取 [ChunkSize-128KiB, ChunkSize+128KiB) 共 256 KiB：
		// 前 64 KiB 为空洞 0x00，中间 128 KiB 为 crossPayload，后 64 KiB 为空洞 0x00（总文件大小 ChunkSize+128KiB）
		tracer.Reset()
		resp, rErr := mgr.Read(ctx, &meta.ReadRequest{
			Ino:       fileIno,
			Fh:        rwFh,
			Offset:    uint64(meta.ChunkSize - 128*1024),
			Size:      256 * 1024,
			SessionID: &sessID,
		})
		require.NoError(t, rErr)
		require.NotNil(t, resp)

		expected := make([]byte, 256*1024)
		copy(expected[64*1024:192*1024], crossPayload)
		assert.Equal(t, expected, resp.Data)
		assertSingleReadSQL(t, tracer.Snapshot(), true)
	})
}
