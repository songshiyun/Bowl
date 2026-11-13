package rpccodec

import (
	"bytes"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

type testPayloadMsg struct {
	Data []byte
}

func registerOrphanMsg(pool *PayloadLeasePool[testPayloadMsg], policyCheck byte) *[MaxPayloadSize]byte {
	buf := pool.AcquireBuf()
	buf[0] = policyCheck
	msg := &testPayloadMsg{Data: buf[:64:64]}
	pool.Register(msg, buf)
	runtime.KeepAlive(msg)
	return buf
}

func claimOrphanGuard(pool *PayloadLeasePool[testPayloadMsg]) {
	buf := pool.AcquireBuf()
	msg := &testPayloadMsg{Data: buf[:128:128]}
	pool.Register(msg, buf)
	msg.Data = nil
	guard := pool.Claim(msg)
	runtime.KeepAlive(guard)
}

func TestPayloadLeasePoolIsolationAndDuplicateRegister(t *testing.T) {
	poolA := NewPayloadLeasePool[testPayloadMsg](2, OrphanRecycleBuffer)
	poolB := NewPayloadLeasePool[testPayloadMsg](-3, OrphanDropEntryOnly)

	// 1. 不同池实例的缓冲通道与租约表完全隔离
	bufA := poolA.AcquireBuf()
	bufB := poolB.AcquireBuf()
	require.NotEqual(t, unsafe.Pointer(bufA), unsafe.Pointer(bufB))

	msgA := &testPayloadMsg{Data: bufA[:16:16]}
	msgB := &testPayloadMsg{Data: bufB[:16:16]}
	poolA.Register(msgA, bufA)
	poolB.Register(msgB, bufB)
	require.Equal(t, 1, poolA.LeasedCount())
	require.Equal(t, 1, poolB.LeasedCount())

	// 跨池 Release/Claim 无效，不影响各自租约
	require.False(t, poolB.Release(msgA, func(m *testPayloadMsg) { m.Data = nil }))
	require.Nil(t, poolA.Claim(msgB))
	require.Equal(t, 1, poolA.LeasedCount())
	require.Equal(t, 1, poolB.LeasedCount())

	// 2. 同一消息重复 Register 时先回收旧租约且计数保持为 1，释放后可复用原缓冲
	bufA2 := poolA.AcquireBuf()
	require.NotEqual(t, unsafe.Pointer(bufA), unsafe.Pointer(bufA2))
	msgA.Data = bufA2[:32:32]
	poolA.Register(msgA, bufA2)
	require.Equal(t, 1, poolA.LeasedCount())

	// 原 bufA 已被回收进 poolA.freeCh，下一次 AcquireBuf 必须立即取回 bufA
	reacquiredA := poolA.AcquireBuf()
	require.Equal(t, unsafe.Pointer(bufA), unsafe.Pointer(reacquiredA))
	poolA.RecycleBuf(reacquiredA)

	// 显式释放 msgA 与 msgB
	require.True(t, poolA.Release(msgA, func(m *testPayloadMsg) { m.Data = nil }))
	require.Nil(t, msgA.Data)
	require.Zero(t, poolA.LeasedCount())
	require.False(t, poolA.Release(msgA, nil))

	msgB.Data = nil
	guardB := poolB.Claim(msgB)
	require.NotNil(t, guardB)
	require.Zero(t, poolB.LeasedCount())
	guardB.Release()
	guardB.Release() // 幂等

	// 零值与 nil Guard 安全空操作
	var nilGuard *Guard
	nilGuard.Release()
	(&Guard{}).Release()

	// nil msg / nil buf 边界安全：Register(nil, buf) 立即回收缓冲，不污染租约表
	poolEmpty := NewPayloadLeasePool[testPayloadMsg](1, OrphanRecycleBuffer)
	bufNilMsg := poolEmpty.AcquireBuf()
	poolEmpty.Register(nil, bufNilMsg)
	poolEmpty.Register(msgA, nil)
	require.Zero(t, poolEmpty.LeasedCount())
	reclaimed := poolEmpty.AcquireBuf()
	require.Equal(t, unsafe.Pointer(bufNilMsg), unsafe.Pointer(reclaimed))
	poolEmpty.RecycleBuf(reclaimed)
	poolEmpty.RecycleBuf(nil)
}

func TestPayloadLeasePoolOrphanPoliciesAndGuardCleanup(t *testing.T) {
	// 1. OrphanDropEntryOnly：孤儿对象 GC 后仅清理表项，绝不回收底层缓冲（防止外部切片 UAF）
	dropPool := NewPayloadLeasePool[testPayloadMsg](2, OrphanDropEntryOnly)
	droppedBuf := registerOrphanMsg(dropPool, 0x5A)
	require.Equal(t, 1, dropPool.LeasedCount())
	require.Eventually(t, func() bool {
		runtime.GC()
		return dropPool.LeasedCount() == 0
	}, 2*time.Second, 10*time.Millisecond)

	nextFromDropPool := dropPool.AcquireBuf()
	require.NotEqual(t, unsafe.Pointer(droppedBuf), unsafe.Pointer(nextFromDropPool), "OrphanDropEntryOnly 不得回收孤儿缓冲")
	require.Equal(t, byte(0x5A), droppedBuf[0])
	dropPool.RecycleBuf(nextFromDropPool)

	// 2. OrphanRecycleBuffer：孤儿对象 GC 后清理表项并立即归还缓冲
	recyclePool := NewPayloadLeasePool[testPayloadMsg](2, OrphanRecycleBuffer)
	recycledBuf := registerOrphanMsg(recyclePool, 0xA5)
	require.Equal(t, 1, recyclePool.LeasedCount())
	require.Eventually(t, func() bool {
		runtime.GC()
		return recyclePool.LeasedCount() == 0
	}, 2*time.Second, 10*time.Millisecond)

	reacquired := recyclePool.AcquireBuf()
	require.Equal(t, unsafe.Pointer(recycledBuf), unsafe.Pointer(reacquired), "OrphanRecycleBuffer 必须在 GC 后归还孤儿缓冲")
	recyclePool.RecycleBuf(reacquired)

	// 3. 接管后遗漏 Guard.Release()，Guard 被 GC 时触发兜底回收
	guardPool := NewPayloadLeasePool[testPayloadMsg](1, OrphanDropEntryOnly)
	seedBuf := guardPool.AcquireBuf()
	guardPool.RecycleBuf(seedBuf)
	claimOrphanGuard(guardPool)
	require.Zero(t, guardPool.LeasedCount())

	var reusedAfterGuardGC bool
	require.Eventually(t, func() bool {
		runtime.GC()
		cand := guardPool.AcquireBuf()
		matched := unsafe.Pointer(cand) == unsafe.Pointer(seedBuf)
		guardPool.RecycleBuf(cand)
		if matched {
			reusedAfterGuardGC = true
			return true
		}
		return false
	}, 2*time.Second, 10*time.Millisecond)
	require.True(t, reusedAfterGuardGC, "遗漏 Guard.Release() 时必须在 Guard 被 GC 后兜底回收缓冲")
}

func TestPayloadLeasePoolAndCodecZeroAllocs(t *testing.T) {
	pool := NewPayloadLeasePool[storagev1.ReadResponse](4, OrphanRecycleBuffer)
	warm := pool.AcquireBuf()
	pool.RecycleBuf(warm)

	msg := &storagev1.ReadResponse{}
	require.Zero(t, testing.AllocsPerRun(50, func() {
		buf := pool.AcquireBuf()
		pool.RecycleBuf(buf)
	}), "预热后的 AcquireBuf/RecycleBuf 必须严格 0 allocs/op")

	require.Zero(t, testing.AllocsPerRun(50, func() {
		if pool.Release(msg, nil) {
			t.Fatal("active == 0 时 Release 必须返回 false")
		}
	}), "active == 0 时 Release 快路径必须严格 0 allocs/op")

	reusableDst := make([]byte, 0, 1024)
	readReq := &storagev1.ReadRequest{
		Ino:    42,
		Fh:     7,
		Offset: 4096,
		Size:   128 << 10,
		DiskId: "11111111-1111-4111-8111-111111111111",
	}
	codecWithHook := NewProtoBinaryCodec(CodecHooks{
		MarshalAppend: func(dst []byte, m any) ([]byte, bool, error) {
			if resp, ok := m.(*storagev1.ReadResponse); ok {
				return append(dst, resp.Data...), true, nil
			}
			return nil, false, nil
		},
		AfterMarshal: func(m any) {
			if resp, ok := m.(*storagev1.ReadResponse); ok && resp != nil {
				pool.Release(resp, func(r *storagev1.ReadResponse) { r.Data = nil })
			}
		},
	})
	resp := &storagev1.ReadResponse{Data: []byte("fast-path")}

	require.Zero(t, testing.AllocsPerRun(50, func() {
		out, err := codecWithHook.MarshalAppend(reusableDst[:0], resp)
		if err != nil || len(out) == 0 {
			t.Fatalf("unexpected hook MarshalAppend: len=%d err=%v", len(out), err)
		}
	}), "命中 MarshalAppend 钩子时必须严格 0 allocs/op")

	require.Zero(t, testing.AllocsPerRun(50, func() {
		out, err := codecWithHook.MarshalAppend(reusableDst[:0], readReq)
		if err != nil || len(out) == 0 {
			t.Fatalf("unexpected fallback MarshalAppend: len=%d err=%v", len(out), err)
		}
	}), "未命中钩子回退标准 proto.MarshalAppend 时必须严格 0 allocs/op")
}

func TestProtoBinaryCodecBehaviorAndFallbacks(t *testing.T) {
	var afterMarshalCalled int
	codec := NewProtoBinaryCodec(CodecHooks{
		AfterMarshal: func(msg any) {
			if _, ok := msg.(*storagev1.ReadResponse); ok {
				afterMarshalCalled++
			}
		},
		Unmarshal: func(data []byte, msg any) (bool, error) {
			if r, ok := msg.(*storagev1.ReadResponse); ok && bytes.Equal(data, []byte("hook-magic")) {
				r.Data = []byte("from-hook")
				return true, nil
			}
			return false, nil
		},
	})

	require.Equal(t, CodecNameProto, codec.Name())
	require.True(t, codec.IsBinary())

	// 1. 非 proto.Message 错误
	_, err := codec.Marshal(struct{}{})
	require.ErrorContains(t, err, "does not implement proto.Message")
	_, err = codec.MarshalAppend(nil, struct{}{})
	require.ErrorContains(t, err, "does not implement proto.Message")
	_, err = codec.MarshalStable(struct{}{})
	require.ErrorContains(t, err, "does not implement proto.Message")
	err = codec.Unmarshal([]byte("abc"), struct{}{})
	require.ErrorContains(t, err, "does not implement proto.Message")

	// 2. Marshal / MarshalStable 均触发 AfterMarshal
	resp := &storagev1.ReadResponse{Data: []byte("hello")}
	wire, err := codec.Marshal(resp)
	require.NoError(t, err)
	require.Equal(t, 1, afterMarshalCalled)

	stableWire, err := codec.MarshalStable(resp)
	require.NoError(t, err)
	require.Equal(t, wire, stableWire)
	require.Equal(t, 2, afterMarshalCalled)

	// 3. Unmarshal 钩子命中与回退标准 proto.Unmarshal
	var hooked storagev1.ReadResponse
	require.NoError(t, codec.Unmarshal([]byte("hook-magic"), &hooked))
	require.Equal(t, []byte("from-hook"), hooked.Data)

	var decoded storagev1.ReadResponse
	require.NoError(t, codec.Unmarshal(wire, &decoded))
	require.Equal(t, []byte("hello"), decoded.Data)

	// 4. 畸形 Wire 数据报错并保留 %T 上下文
	var malformed storagev1.ReadRequest
	err = codec.Unmarshal([]byte{0xff, 0xff, 0xff}, &malformed)
	require.Error(t, err)
	require.ErrorContains(t, err, "unmarshal into *storagev1.ReadRequest")

	// 验证标准 proto.Equal 兼容性
	var direct storagev1.ReadResponse
	require.NoError(t, proto.Unmarshal(wire, &direct))
	require.True(t, proto.Equal(resp, &direct))
}
