package fusefs

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestWriteBehind_SmallWriteCoalescingAndCreateSeed(t *testing.T) {
	t.Run("Create新建WAL免探针预热与95帧小块写单次RPC合并刷盘", func(t *testing.T) {
		srv := newTestStorageServer(1.0, 1.0)
		inst := setupTestClientFS(t, srv, nil, 1.0, 1.0)

		var createOut fuse.CreateOut
		require.Equal(t, fuse.OK, inst.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    syscall.O_RDWR,
			Mode:     0o644,
		}, "bench.db-wal", &createOut))

		var expected bytes.Buffer
		walHeader := bytes.Repeat([]byte{0x37}, 32)
		expected.Write(walHeader)
		w, st := inst.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
			Offset:   0,
			Size:     uint32(len(walHeader)),
		}, walHeader)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, uint32(len(walHeader)), w)

		off := uint64(len(walHeader))
		for i := range 95 {
			frame := bytes.Repeat([]byte{byte(i + 1)}, 4120)
			expected.Write(frame)
			n, writeSt := inst.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: createOut.NodeId},
				Fh:       createOut.Fh,
				Offset:   off,
				Size:     uint32(len(frame)),
			}, frame)
			require.Equal(t, fuse.OK, writeSt)
			require.Equal(t, uint32(len(frame)), n)
			off += uint64(len(frame))
		}

		// Fsync 前全部 96 次写入均在客户端内存 stageBuf 合并，跨机 Write RPC 次数为 0。
		require.Zero(t, srv.writeCalls.Load(), "新建普通文件小块连续写应在内存合并，Fsync 前不发 Write RPC")

		// 本地缓存已由 OnStage 同步更新 Size，GetAttr 零 RPC 即可读到最新大小且不打断聚合槽。
		var attrOut fuse.AttrOut
		require.Equal(t, fuse.OK, inst.GetAttr(nil, &fuse.GetAttrIn{
			InHeader: fuse.InHeader{NodeId: createOut.NodeId},
		}, &attrOut))
		require.Equal(t, uint64(expected.Len()), attrOut.Size)
		require.Zero(t, srv.writeCalls.Load(), "TTL 有效期内本地缓存命中 GetAttr 不得提前刷裂 stageBuf")

		// Fsync 同步将整块 391,432B 合并为 1 次 Write RPC + 1 次 Fsync RPC 提交。
		require.Equal(t, fuse.OK, inst.Fsync(nil, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
		}))
		require.Equal(t, int64(1), srv.writeCalls.Load(), "96 次 WAL 写入在 Fsync 时应合并为单次 Write RPC")
		require.Equal(t, int64(1), srv.fsyncCalls.Load())

		readBuf := make([]byte, expected.Len())
		res, readSt := inst.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
			Offset:   0,
			Size:     uint32(len(readBuf)),
		}, readBuf)
		require.Equal(t, fuse.OK, readSt)
		got, byteSt := res.Bytes(readBuf)
		require.Equal(t, fuse.OK, byteSt)
		require.Equal(t, expected.Bytes(), got)
	})

	t.Run("Open已有文件首写同步探针后支持分段4KiB页写与前向相邻合并", func(t *testing.T) {
		srv := newTestStorageServer(1.0, 1.0)
		now := timestamppb.Now()
		initData := bytes.Repeat([]byte{0xAA}, 64*1024)
		srv.files[200] = &memFile{
			inode: &storagev1.Inode{
				Ino:        200,
				Generation: 1,
				Attr: &storagev1.InodeAttribute{
					Mode:  syscall.S_IFREG | 0o644,
					Nlink: 1,
					Size:  uint64(len(initData)),
					Mtime: now,
					Ctime: now,
				},
			},
			data: append([]byte(nil), initData...),
		}
		srv.entries[1]["main.db"] = 200

		inst := setupTestClientFS(t, srv, nil, 1.0, 1.0)
		expected := append([]byte(nil), initData...)

		var entryOut fuse.EntryOut
		require.Equal(t, fuse.OK, inst.Lookup(nil, &fuse.InHeader{NodeId: 1}, "main.db", &entryOut))

		var openOut fuse.OpenOut
		require.Equal(t, fuse.OK, inst.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: entryOut.NodeId},
			Flags:    syscall.O_RDWR,
		}, &openOut))

		writePage := func(pageIdx int, fill byte) {
			page := bytes.Repeat([]byte{fill}, 4096)
			off := pageIdx * 4096
			copy(expected[off:off+4096], page)
			w, st := inst.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: entryOut.NodeId},
				Fh:       openOut.Fh,
				Offset:   uint64(off),
				Size:     4096,
			}, page)
			require.Equal(t, fuse.OK, st)
			require.Equal(t, uint32(4096), w)
		}

		// 第 1 页：Open 打开的已有文件首笔写入走同步探针 RPC。
		writePage(0, 0x10)
		require.Equal(t, int64(1), srv.writeCalls.Load())

		// 第 1..3 页连续写入：合并进同一 stageBuf，0 新增 RPC。
		writePage(1, 0x11)
		writePage(2, 0x12)
		writePage(3, 0x13)
		require.Equal(t, int64(1), srv.writeCalls.Load())

		// 跳跃至第 10 页，再写第 11 页及前向紧邻的第 9 页：
		// 跳跃触发旧槽 [4KiB, 16KiB) 单次同步刷出，新槽合并 [36KiB, 48KiB)。
		writePage(10, 0x20)
		require.Equal(t, int64(2), srv.writeCalls.Load())
		writePage(11, 0x21)
		writePage(9, 0x1F)
		require.Equal(t, int64(2), srv.writeCalls.Load(), "前向相邻页写应原地合并进当前 stageBuf")

		require.Equal(t, fuse.OK, inst.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: entryOut.NodeId},
			Fh:       openOut.Fh,
		}))
		require.Equal(t, int64(3), srv.writeCalls.Load(), "7 次分散页写应收敛为 1 次探针 + 2 次分段合并共 3 次 RPC")

		srv.mu.Lock()
		gotServerData := append([]byte(nil), srv.files[200].data...)
		srv.mu.Unlock()
		require.Equal(t, expected, gotServerData)
	})

	t.Run("AttrTimeout为0严格直通与缓存失效后GetAttr触发BarrierInode", func(t *testing.T) {
		strictSrv := newTestStorageServer(0, 0)
		strictFS := setupTestClientFS(t, strictSrv, nil, 0, 0)

		var cStrict fuse.CreateOut
		require.Equal(t, fuse.OK, strictFS.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    syscall.O_RDWR,
			Mode:     0o644,
		}, "strict.bin", &cStrict))
		buf4K := bytes.Repeat([]byte("z"), 4096)
		for i := range 5 {
			_, st := strictFS.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: cStrict.NodeId},
				Fh:       cStrict.Fh,
				Offset:   uint64(i) * 4096,
				Size:     4096,
			}, buf4K)
			require.Equal(t, fuse.OK, st)
		}
		require.Equal(t, int64(5), strictSrv.writeCalls.Load(), "pjdfstest 0s TTL 模式下每次写入必须保持同步直通")
		require.Zero(t, strictFS.Snapshot().WriteBehind.StagedCalls)

		// 1.0s TTL 下若属性缓存因淘汰/失效未命中，GetAttr 穿透前必须先执行 BarrierInode 刷出挂起缓冲。
		prodSrv := newTestStorageServer(1.0, 1.0)
		prodFS := setupTestClientFS(t, prodSrv, nil, 1.0, 1.0)
		var cProd fuse.CreateOut
		require.Equal(t, fuse.OK, prodFS.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    syscall.O_RDWR,
			Mode:     0o644,
		}, "evicted-attr.bin", &cProd))
		_, st := prodFS.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: cProd.NodeId},
			Fh:       cProd.Fh,
			Offset:   0,
			Size:     4096,
		}, buf4K)
		require.Equal(t, fuse.OK, st)
		require.Zero(t, prodSrv.writeCalls.Load())

		// 模拟该节点属性被本地缓存淘汰。
		prodFS.Raw().cache.invalidateInode(cProd.NodeId)
		var out fuse.AttrOut
		require.Equal(t, fuse.OK, prodFS.GetAttr(nil, &fuse.GetAttrIn{
			InHeader: fuse.InHeader{NodeId: cProd.NodeId},
		}, &out))
		require.Equal(t, int64(1), prodSrv.writeCalls.Load(), "缓存未命中的 GetAttr 必须先通过 BarrierInode 刷出挂起缓冲")
		require.Equal(t, uint64(4096), out.Size)
	})

	t.Run("Create携带SUID特殊权限位时禁用免探针预热保持首写同步", func(t *testing.T) {
		srv := newTestStorageServer(1.0, 1.0)
		inst := setupTestClientFS(t, srv, nil, 1.0, 1.0)

		var cOut fuse.CreateOut
		require.Equal(t, fuse.OK, inst.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    syscall.O_RDWR,
			Mode:     syscall.S_ISUID | 0o755,
		}, "suid.bin", &cOut))
		_, st := inst.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: cOut.NodeId},
			Fh:       cOut.Fh,
			Offset:   0,
			Size:     4096,
		}, bytes.Repeat([]byte("x"), 4096))
		require.Equal(t, fuse.OK, st)
		require.Equal(t, int64(1), srv.writeCalls.Load(), "SUID 文件首写必须直通服务端执行 KILLPRIV 校验")
	})
}

func TestNamespaceCache_CompleteDirSynthesisAndStreamDemotion(t *testing.T) {
	t.Run("Mkdir加Create构建完整小目录后ReadDirPlus本地零RPC合成", func(t *testing.T) {
		srv := newTestStorageServer(1.0, 1.0)
		var readDirRPCs atomic.Int64
		srv.readDirHook = func(_ context.Context, _ *storagev1.ReadDirPlusRequest) ([]*storagev1.DirEntryPlus, error) {
			readDirRPCs.Add(1)
			return nil, nil
		}
		inst := setupTestClientFS(t, srv, nil, 1.0, 1.0)

		var dirOut fuse.EntryOut
		require.Equal(t, fuse.OK, inst.Mkdir(nil, &fuse.MkdirIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Mode:     0o755,
		}, "readdir-small", &dirOut))

		// 逆序创建 20 个子文件并掺入一次 ENOENT 负缓存项，验证合成流严格按字节序升序且过滤负缓存。
		for i := 19; i >= 0; i-- {
			var cOut fuse.CreateOut
			name := fmt.Sprintf("entry%05d", i)
			require.Equal(t, fuse.OK, inst.Create(nil, &fuse.CreateIn{
				InHeader: fuse.InHeader{NodeId: dirOut.NodeId},
				Flags:    syscall.O_RDWR,
				Mode:     0o644,
			}, name, &cOut))
			inst.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cOut.NodeId}, Fh: cOut.Fh})
		}
		inst.Raw().cache.putEntry(dirOut.NodeId, "ghost-negative", 0)

		var openDirOut fuse.OpenOut
		require.Equal(t, fuse.OK, inst.OpenDir(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: dirOut.NodeId},
		}, &openDirOut))

		entries, st := inst.Raw().readDirEntries(context.Background(), nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: dirOut.NodeId},
			Fh:       openDirOut.Fh,
			Offset:   0,
			Size:     65536,
		})
		require.Equal(t, fuse.OK, st)
		require.Zero(t, readDirRPCs.Load(), "completeDirs 且子属性齐全时 ReadDirPlus 应本地合成，0 次跨机 RPC")
		require.Len(t, entries, 22)
		require.Equal(t, ".", string(entries[0].GetName()))
		require.Equal(t, dirOut.NodeId, entries[0].GetIno())
		require.Equal(t, uint64(1), entries[0].GetNextOffset())
		require.Equal(t, "..", string(entries[1].GetName()))
		require.Equal(t, uint64(1), entries[1].GetIno())
		require.Equal(t, uint64(2), entries[1].GetNextOffset())
		for i := range 20 {
			require.Equal(t, fmt.Sprintf("entry%05d", i), string(entries[2+i].GetName()))
			require.Equal(t, uint64(3+i), entries[2+i].GetNextOffset())
		}
		inst.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: dirOut.NodeId}, Fh: openDirOut.Fh})
	})

	t.Run("任一子项属性缺失或目录跨父目录Rename后安全回退远端ReadDirPlus", func(t *testing.T) {
		srv := newTestStorageServer(1.0, 1.0)
		var readDirRPCs atomic.Int64
		srv.readDirHook = func(_ context.Context, req *storagev1.ReadDirPlusRequest) ([]*storagev1.DirEntryPlus, error) {
			readDirRPCs.Add(1)
			srv.mu.Lock()
			defer srv.mu.Unlock()
			dirFile := srv.files[req.GetIno()]
			return []*storagev1.DirEntryPlus{
				{Ino: req.GetIno(), Name: []byte("."), NextOffset: 1, Inode: cloneProtoInode(dirFile.inode), AttrTimeoutSeconds: 1.0},
				{Ino: 1, Name: []byte(".."), NextOffset: 2, Inode: cloneProtoInode(srv.files[1].inode), AttrTimeoutSeconds: 1.0},
			}, nil
		}
		inst := setupTestClientFS(t, srv, nil, 1.0, 1.0)

		var d1, d2 fuse.EntryOut
		require.Equal(t, fuse.OK, inst.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: 1}, Mode: 0o755}, "d1", &d1))
		require.Equal(t, fuse.OK, inst.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: 1}, Mode: 0o755}, "d2", &d2))

		var f1 fuse.CreateOut
		require.Equal(t, fuse.OK, inst.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: d1.NodeId}, Flags: syscall.O_RDWR, Mode: 0o644}, "f1", &f1))
		inst.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: f1.NodeId}, Fh: f1.Fh})

		// 1) 失效子项 f1 的属性项 'i' -> ReadDirPlus 必须安全回退远端 RPC。
		inst.Raw().cache.invalidateInode(f1.NodeId)
		var od1 fuse.OpenOut
		require.Equal(t, fuse.OK, inst.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: d1.NodeId}}, &od1))
		_, st := inst.Raw().readDirEntries(context.Background(), nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: d1.NodeId}, Fh: od1.Fh, Size: 4096})
		require.Equal(t, fuse.OK, st)
		require.Equal(t, int64(1), readDirRPCs.Load(), "子项属性缺失时严禁合成残缺目录流，必须回退远端 RPC")
		inst.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: d1.NodeId}, Fh: od1.Fh})

		// 2) 将空完整目录 d2 跨父目录 Rename 到 d1 下 -> parentByDir 必须被清除并回退远端 RPC。
		require.Equal(t, fuse.OK, inst.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Newdir:   d1.NodeId,
		}, "d2", "d2-moved"))
		var od2 fuse.OpenOut
		require.Equal(t, fuse.OK, inst.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: d2.NodeId}}, &od2))
		_, st = inst.Raw().readDirEntries(context.Background(), nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: d2.NodeId}, Fh: od2.Fh, Size: 4096})
		require.Equal(t, fuse.OK, st)
		require.Equal(t, int64(2), readDirRPCs.Load(), "目录跨父目录移动后必须清除旧父目录映射并回退远端 RPC")
		inst.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: d2.NodeId}, Fh: od2.Fh})
	})

	t.Run("目录流降级保留子节点属性消除后续OpenDir的GetAttr穿透", func(t *testing.T) {
		srv := newTestStorageServer(1.0, 1.0)
		now := timestamppb.Now()
		var subInos []uint64
		var stream []*storagev1.DirEntryPlus
		stream = append(stream,
			&storagev1.DirEntryPlus{Ino: 1, Name: []byte("."), NextOffset: 1, Inode: cloneProtoInode(srv.files[1].inode), AttrTimeoutSeconds: 1.0},
			&storagev1.DirEntryPlus{Ino: 1, Name: []byte(".."), NextOffset: 2, Inode: cloneProtoInode(srv.files[1].inode), AttrTimeoutSeconds: 1.0},
		)
		for i := range 8 {
			ino := uint64(300 + i)
			subInos = append(subInos, ino)
			node := &storagev1.Inode{
				Ino:        ino,
				Generation: 1,
				Attr: &storagev1.InodeAttribute{
					Mode:  syscall.S_IFDIR | 0o755,
					Nlink: 2,
					Mtime: now,
					Ctime: now,
				},
			}
			srv.files[ino] = &memFile{inode: node}
			srv.entries[ino] = make(map[string]uint64)
			name := fmt.Sprintf("sub-%02d", i)
			srv.entries[1][name] = ino
			stream = append(stream, &storagev1.DirEntryPlus{
				Ino:                 ino,
				Name:                []byte(name),
				NextOffset:          uint64(3 + i),
				Inode:               cloneProtoInode(node),
				EntryTimeoutSeconds: 1.0,
				AttrTimeoutSeconds:  1.0,
			})
		}
		srv.readDirHook = func(_ context.Context, _ *storagev1.ReadDirPlusRequest) ([]*storagev1.DirEntryPlus, error) {
			return cloneDir(stream), nil
		}

		inst := setupTestClientFS(t, srv, nil, 1.0, 1.0)
		var rootDH fuse.OpenOut
		require.Equal(t, fuse.OK, inst.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 1}}, &rootDH))
		_, st := inst.Raw().readDirEntries(context.Background(), nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 1}, Fh: rootDH.Fh, Size: 65536})
		require.Equal(t, fuse.OK, st)
		inst.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 1}, Fh: rootDH.Fh})

		// 在根目录新建文件触发根目录流降级（demoteDirStreamToEntriesLocked）。
		var cOut fuse.CreateOut
		require.Equal(t, fuse.OK, inst.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    syscall.O_RDWR,
			Mode:     0o644,
		}, "trigger-demote.txt", &cOut))
		inst.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cOut.NodeId}, Fh: cOut.Fh})

		// 逐个 OpenDir 子目录：即使服务端删除其记录（证明绝未发 GetAttr RPC），也必须全部命中降级保留的本地 'i' 缓存！
		srv.mu.Lock()
		for _, ino := range subInos {
			delete(srv.files, ino)
		}
		srv.mu.Unlock()

		for _, ino := range subInos {
			var dh fuse.OpenOut
			require.Equal(t, fuse.OK, inst.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: ino}}, &dh))
			inst.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: dh.Fh})
		}
	})
}

func TestWritevAndIoctlFastShortCircuit(t *testing.T) {
	srv := newTestStorageServer(1.0, 1.0)
	inst := setupTestClientFS(t, srv, nil, 1.0, 1.0)

	var cOut fuse.CreateOut
	require.Equal(t, fuse.OK, inst.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Flags:    syscall.O_RDWR,
		Mode:     0o644,
	}, "iov.bin", &cOut))

	// 1) 单段零拷贝 Writev + 多段拼接 Writev 均正常进入 WriteBehind 内存合并。
	w1, st := inst.Writev(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: cOut.NodeId},
		Fh:       cOut.Fh,
		Offset:   0,
		Size:     5,
	}, [][]byte{[]byte("hello")})
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint32(5), w1)

	w2, st := inst.Writev(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: cOut.NodeId},
		Fh:       cOut.Fh,
		Offset:   5,
		Size:     7,
	}, [][]byte{[]byte(", "), []byte("world"), nil})
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint32(7), w2)
	require.Zero(t, srv.writeCalls.Load())

	// 2) 非 FS_IOC_* 命令（如 TCGETS = 0x5401）前置快速返回 ENOTTY，绝不返回 ENOSYS，且不触发 BarrierInode 刷裂聚合缓冲。
	const tcgetsCmd = 0x5401
	var ioctlOut fuse.IoctlOut
	ioctlSt := inst.Ioctl(nil, &fuse.IoctlIn{
		InHeader: fuse.InHeader{NodeId: cOut.NodeId},
		Fh:       cOut.Fh,
		Cmd:      tcgetsCmd,
	}, nil, &ioctlOut, make([]byte, 64))
	require.Equal(t, fuse.Status(syscall.ENOTTY), ioctlSt)
	require.Zero(t, srv.writeCalls.Load(), "非支持 ioctl 短路不得无谓刷出挂起写缓冲")

	// 3) 合法 FS_IOC_GETFLAGS 与 FS_IOC_FSGETXATTR 正常触发 BarrierInode 并返回 OK。
	outBuf := make([]byte, 32)
	require.Equal(t, fuse.OK, inst.Ioctl(nil, &fuse.IoctlIn{
		InHeader: fuse.InHeader{NodeId: cOut.NodeId},
		Fh:       cOut.Fh,
		Cmd:      FS_IOC_GETFLAGS,
	}, nil, &ioctlOut, outBuf[:8]))
	require.Equal(t, int64(1), srv.writeCalls.Load(), "合法 FS_IOC_GETFLAGS 应触发写回屏障刷出缓冲")

	require.Equal(t, fuse.OK, inst.Ioctl(nil, &fuse.IoctlIn{
		InHeader: fuse.InHeader{NodeId: cOut.NodeId},
		Fh:       cOut.Fh,
		Cmd:      FS_IOC_FSGETXATTR,
	}, nil, &ioctlOut, outBuf[:28]))

	srv.mu.Lock()
	got := string(srv.files[cOut.NodeId].data)
	srv.mu.Unlock()
	require.Equal(t, "hello, world", got)
}

func TestLookupAndGetAttrReadOnlyFastPath_ZeroAllocAndRaceSafe(t *testing.T) {
	srv := newTestStorageServer(1.0, 1.0)
	inst := setupTestClientFS(t, srv, nil, 1.0, 1.0)
	raw := inst.Raw()

	var cOut fuse.CreateOut
	require.Equal(t, fuse.OK, inst.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Flags:    syscall.O_RDWR,
		Mode:     0o644,
	}, "hot.txt", &cOut))

	// 1) 验证单锁只读快路径在本地命中时零堆内存分配。
	var entryOut fuse.EntryOut
	lookupHeader := &fuse.InHeader{NodeId: 1}
	lookupAllocs := testing.AllocsPerRun(100, func() {
		st := raw.Lookup(nil, lookupHeader, "hot.txt", &entryOut)
		if st != fuse.OK {
			t.Fatalf("unexpected status: %v", st)
		}
	})
	require.Zero(t, lookupAllocs, "Lookup 本地缓存命中快路径必须为 0 allocs/op")

	var attrOut fuse.AttrOut
	getAttrIn := &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: cOut.NodeId}}
	getAttrAllocs := testing.AllocsPerRun(100, func() {
		st := raw.GetAttr(nil, getAttrIn, &attrOut)
		if st != fuse.OK {
			t.Fatalf("unexpected status: %v", st)
		}
	})
	require.Zero(t, getAttrAllocs, "GetAttr 本地缓存命中快路径必须为 0 allocs/op")

	// 2) 并发 -race 压力检验：多协程高频 Lookup/GetAttr 只读访问与并发写缓冲刷新（OnStage/OnCommit）零数据竞争且尺寸单调不减。
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			var eo fuse.EntryOut
			var ao fuse.AttrOut
			var prevSize uint64
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				if raw.Lookup(nil, lookupHeader, "hot.txt", &eo) == fuse.OK {
					if eo.Size < prevSize {
						t.Errorf("Lookup 观测到尺寸倒退: %d < %d", eo.Size, prevSize)
						return
					}
					prevSize = eo.Size
				}
				if raw.GetAttr(nil, getAttrIn, &ao) == fuse.OK {
					if ao.Size < prevSize {
						t.Errorf("GetAttr 观测到尺寸倒退: %d < %d", ao.Size, prevSize)
						return
					}
					prevSize = ao.Size
				}
			}
		})
	}

	chunk := bytes.Repeat([]byte("m"), 4096)
	for i := range 128 {
		_, st := inst.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: cOut.NodeId},
			Fh:       cOut.Fh,
			Offset:   uint64(i) * 4096,
			Size:     4096,
		}, chunk)
		require.Equal(t, fuse.OK, st)
		if i%32 == 31 {
			require.Equal(t, fuse.OK, inst.Flush(nil, &fuse.FlushIn{
				InHeader: fuse.InHeader{NodeId: cOut.NodeId},
				Fh:       cOut.Fh,
			}))
		}
	}
	require.Equal(t, fuse.OK, inst.Flush(nil, &fuse.FlushIn{
		InHeader: fuse.InHeader{NodeId: cOut.NodeId},
		Fh:       cOut.Fh,
	}))
	close(stopCh)
	wg.Wait()
	inst.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cOut.NodeId}, Fh: cOut.Fh})
}
