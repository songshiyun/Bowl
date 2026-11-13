package rpccodec_test

import (
	"bytes"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/rpccodec"
)

type testPayloadMsg struct {
	Data []byte
}

func registerOrphanMsg(pool *rpccodec.PayloadLeasePool[testPayloadMsg], marker byte) *[rpccodec.MaxPayloadSize]byte {
	buf := pool.AcquireBuf()
	buf[0] = marker
	msg := &testPayloadMsg{Data: buf[:64:64]}
	pool.Register(msg, buf)
	runtime.KeepAlive(msg)
	return buf
}

func claimOrphanGuard(pool *rpccodec.PayloadLeasePool[testPayloadMsg]) {
	buf := pool.AcquireBuf()
	msg := &testPayloadMsg{Data: buf[:128:128]}
	pool.Register(msg, buf)
	msg.Data = nil
	guard := pool.Claim(msg)
	runtime.KeepAlive(guard)
}

func waitForGCCleanup(t *testing.T, msg string, cond func() bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		runtime.GC()
		return cond()
	}, 2*time.Second, 10*time.Millisecond, "%s", msg)
}

func requireZeroAllocs(t *testing.T, msg string, fn func()) {
	t.Helper()
	got := testing.AllocsPerRun(50, fn)
	require.Zero(t, got, "%s: want 0 allocs/op, got %v", msg, got)
}

func requireMarshalAppendSuccess(t *testing.T, codec *rpccodec.ProtoBinaryCodec, dst []byte, msg any) {
	t.Helper()
	out, err := codec.MarshalAppend(dst, msg)
	if err != nil || len(out) == 0 {
		t.Fatalf("MarshalAppend 失败: len=%d err=%v", len(out), err)
	}
}

func newZeroAllocReadRespCodec(pool *rpccodec.PayloadLeasePool[storagev1.ReadResponse]) *rpccodec.ProtoBinaryCodec {
	return rpccodec.NewProtoBinaryCodec(rpccodec.CodecHooks{
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
}

func newSpyHookCodec(afterMarshalCalled *int) *rpccodec.ProtoBinaryCodec {
	return rpccodec.NewProtoBinaryCodec(rpccodec.CodecHooks{
		AfterMarshal: func(msg any) {
			if _, ok := msg.(*storagev1.ReadResponse); ok {
				*afterMarshalCalled++
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
}

func TestPayloadLeasePool_GivenTwoPools_WhenCrossPoolOperationsCalled_ThenIsolated(t *testing.T) {
	poolA := rpccodec.NewPayloadLeasePool[testPayloadMsg](2, rpccodec.OrphanRecycleBuffer)
	poolB := rpccodec.NewPayloadLeasePool[testPayloadMsg](-3, rpccodec.OrphanDropEntryOnly)

	bufA := poolA.AcquireBuf()
	bufB := poolB.AcquireBuf()
	require.NotEqual(t, unsafe.Pointer(bufA), unsafe.Pointer(bufB), "不同池实例不得共享同一块缓冲")

	msgA := &testPayloadMsg{Data: bufA[:16:16]}
	msgB := &testPayloadMsg{Data: bufB[:16:16]}
	poolA.Register(msgA, bufA)
	poolB.Register(msgB, bufB)

	require.False(t, poolB.Release(msgA, func(m *testPayloadMsg) { m.Data = nil }), "跨池 Release 不得命中租约")
	require.Nil(t, poolA.Claim(msgB), "跨池 Claim 不得命中租约")
	require.Equal(t, 1, poolA.LeasedCount(), "poolA LeasedCount want=1, got=%d", poolA.LeasedCount())
	require.Equal(t, 1, poolB.LeasedCount(), "poolB LeasedCount want=1, got=%d", poolB.LeasedCount())

	require.True(t, poolA.Release(msgA, func(m *testPayloadMsg) { m.Data = nil }), "poolA 释放自身租约应返回 true")
	require.Nil(t, msgA.Data, "Release 后消息切片引用应被清空")
	require.Zero(t, poolA.LeasedCount(), "释放后 poolA LeasedCount want=0, got=%d", poolA.LeasedCount())
	require.False(t, poolA.Release(msgA, nil), "重复 Release 应幂等返回 false")

	msgB.Data = nil
	guardB := poolB.Claim(msgB)
	require.NotNil(t, guardB, "poolB Claim 自身租约应返回有效 Guard")
	require.Zero(t, poolB.LeasedCount(), "Claim 后 poolB LeasedCount want=0, got=%d", poolB.LeasedCount())
	guardB.Release()
	guardB.Release()
}

func TestPayloadLeasePool_GivenExistingLease_WhenRegisteredAgain_ThenRecyclesPreviousBuffer(t *testing.T) {
	pool := rpccodec.NewPayloadLeasePool[testPayloadMsg](2, rpccodec.OrphanRecycleBuffer)
	buf1 := pool.AcquireBuf()
	msg := &testPayloadMsg{Data: buf1[:16:16]}
	pool.Register(msg, buf1)

	buf2 := pool.AcquireBuf()
	require.NotEqual(t, unsafe.Pointer(buf1), unsafe.Pointer(buf2))
	msg.Data = buf2[:32:32]
	pool.Register(msg, buf2)

	require.Equal(t, 1, pool.LeasedCount(), "重复注册同一消息后 LeasedCount want=1, got=%d", pool.LeasedCount())
	reacquired := pool.AcquireBuf()
	require.Equal(t, unsafe.Pointer(buf1), unsafe.Pointer(reacquired), "旧缓冲应已立即回收到池中并可被再次获取")
	pool.RecycleBuf(reacquired)
	require.True(t, pool.Release(msg, func(m *testPayloadMsg) { m.Data = nil }))
}

func TestPayloadLeasePool_GivenNilInputs_WhenMethodsCalled_ThenHandlesSafely(t *testing.T) {
	var nilGuard *rpccodec.Guard
	nilGuard.Release()
	(&rpccodec.Guard{}).Release()

	pool := rpccodec.NewPayloadLeasePool[testPayloadMsg](1, rpccodec.OrphanRecycleBuffer)
	buf := pool.AcquireBuf()
	pool.Register(nil, buf)
	pool.Register(&testPayloadMsg{}, nil)
	require.Zero(t, pool.LeasedCount(), "nil 消息或 nil 缓冲不得写入租约表")

	reclaimed := pool.AcquireBuf()
	require.Equal(t, unsafe.Pointer(buf), unsafe.Pointer(reclaimed), "Register(nil, buf) 应立即归还缓冲")
	pool.RecycleBuf(reclaimed)
	pool.RecycleBuf(nil)
}

func TestPayloadLeasePool_GivenOrphanDropPolicy_WhenMsgCollected_ThenDropsEntryWithoutRecyclingBuffer(t *testing.T) {
	dropPool := rpccodec.NewPayloadLeasePool[testPayloadMsg](2, rpccodec.OrphanDropEntryOnly)
	droppedBuf := registerOrphanMsg(dropPool, 0x5A)
	require.Equal(t, 1, dropPool.LeasedCount())

	waitForGCCleanup(t, "OrphanDropEntryOnly 应在对象被 GC 后清除租约表项", func() bool {
		return dropPool.LeasedCount() == 0
	})

	nextBuf := dropPool.AcquireBuf()
	require.NotEqual(t, unsafe.Pointer(droppedBuf), unsafe.Pointer(nextBuf), "OrphanDropEntryOnly 不得回收孤儿缓冲")
	require.Equal(t, byte(0x5A), droppedBuf[0])
	dropPool.RecycleBuf(nextBuf)
}

func TestPayloadLeasePool_GivenOrphanRecyclePolicy_WhenMsgCollected_ThenRecyclesBuffer(t *testing.T) {
	recyclePool := rpccodec.NewPayloadLeasePool[testPayloadMsg](2, rpccodec.OrphanRecycleBuffer)
	recycledBuf := registerOrphanMsg(recyclePool, 0xA5)
	require.Equal(t, 1, recyclePool.LeasedCount())

	waitForGCCleanup(t, "OrphanRecycleBuffer 应在对象被 GC 后清除租约表项", func() bool {
		return recyclePool.LeasedCount() == 0
	})

	reacquired := recyclePool.AcquireBuf()
	require.Equal(t, unsafe.Pointer(recycledBuf), unsafe.Pointer(reacquired), "OrphanRecycleBuffer 必须在 GC 后归还孤儿缓冲")
	recyclePool.RecycleBuf(reacquired)
}

func TestPayloadLeasePool_GivenUnreleasedGuard_WhenGuardCollected_ThenRecyclesBuffer(t *testing.T) {
	guardPool := rpccodec.NewPayloadLeasePool[testPayloadMsg](1, rpccodec.OrphanDropEntryOnly)
	seedBuf := guardPool.AcquireBuf()
	guardPool.RecycleBuf(seedBuf)

	claimOrphanGuard(guardPool)
	require.Zero(t, guardPool.LeasedCount())

	waitForGCCleanup(t, "遗漏 Guard.Release() 时必须在 Guard 被 GC 后兜底回收缓冲", func() bool {
		cand := guardPool.AcquireBuf()
		matched := unsafe.Pointer(cand) == unsafe.Pointer(seedBuf)
		guardPool.RecycleBuf(cand)
		return matched
	})
}

func TestPayloadLeasePool_GivenWarmedPool_WhenAcquireRecycleOrFastRelease_ThenZeroAllocs(t *testing.T) {
	pool := rpccodec.NewPayloadLeasePool[storagev1.ReadResponse](4, rpccodec.OrphanRecycleBuffer)
	pool.RecycleBuf(pool.AcquireBuf())
	msg := &storagev1.ReadResponse{}

	requireZeroAllocs(t, "预热后的 AcquireBuf/RecycleBuf", func() {
		buf := pool.AcquireBuf()
		pool.RecycleBuf(buf)
	})
	requireZeroAllocs(t, "active == 0 时 Release 快路径", func() {
		require.False(t, pool.Release(msg, nil), "active == 0 时 Release 必须返回 false")
	})
}

func TestProtoBinaryCodec_GivenMarshalAppendHook_WhenHitOrFallback_ThenZeroAllocs(t *testing.T) {
	pool := rpccodec.NewPayloadLeasePool[storagev1.ReadResponse](4, rpccodec.OrphanRecycleBuffer)
	reusableDst := make([]byte, 0, 1024)
	readReq := &storagev1.ReadRequest{
		Ino:    42,
		Fh:     7,
		Offset: 4096,
		Size:   128 << 10,
		DiskId: "11111111-1111-4111-8111-111111111111",
	}
	codecWithHook := newZeroAllocReadRespCodec(pool)
	resp := &storagev1.ReadResponse{Data: []byte("fast-path")}

	requireZeroAllocs(t, "命中 MarshalAppend 钩子", func() {
		requireMarshalAppendSuccess(t, codecWithHook, reusableDst[:0], resp)
	})
	requireZeroAllocs(t, "未命中钩子回退标准 proto.MarshalAppend", func() {
		requireMarshalAppendSuccess(t, codecWithHook, reusableDst[:0], readReq)
	})
}

func TestProtoBinaryCodec_GivenNonProtoMessage_WhenEncodedOrDecoded_ThenReturnsTypeError(t *testing.T) {
	codec := rpccodec.NewProtoBinaryCodec(rpccodec.CodecHooks{})
	require.Equal(t, rpccodec.CodecNameProto, codec.Name())
	require.True(t, codec.IsBinary())

	_, err := codec.Marshal(struct{}{})
	require.ErrorContains(t, err, "does not implement proto.Message")
	_, err = codec.MarshalAppend(nil, struct{}{})
	require.ErrorContains(t, err, "does not implement proto.Message")
	_, err = codec.MarshalStable(struct{}{})
	require.ErrorContains(t, err, "does not implement proto.Message")
	err = codec.Unmarshal([]byte("abc"), struct{}{})
	require.ErrorContains(t, err, "does not implement proto.Message")
}

func TestProtoBinaryCodec_GivenHooks_WhenMarshalAndUnmarshalInvoked_ThenAppliesHooksAndFallbacks(t *testing.T) {
	var afterMarshalCalled int
	codec := newSpyHookCodec(&afterMarshalCalled)

	resp := &storagev1.ReadResponse{Data: []byte("hello")}
	wire, err := codec.Marshal(resp)
	require.NoError(t, err)
	require.Equal(t, 1, afterMarshalCalled, "Marshal 后 AfterMarshal 调用次数 want=1, got=%d", afterMarshalCalled)

	stableWire, err := codec.MarshalStable(resp)
	require.NoError(t, err)
	require.Equal(t, wire, stableWire)
	require.Equal(t, 2, afterMarshalCalled, "MarshalStable 后 AfterMarshal 调用次数 want=2, got=%d", afterMarshalCalled)

	var hooked storagev1.ReadResponse
	require.NoError(t, codec.Unmarshal([]byte("hook-magic"), &hooked))
	require.Equal(t, []byte("from-hook"), hooked.Data)

	var decoded storagev1.ReadResponse
	require.NoError(t, codec.Unmarshal(wire, &decoded))
	require.Equal(t, []byte("hello"), decoded.Data)

	var direct storagev1.ReadResponse
	require.NoError(t, proto.Unmarshal(wire, &direct))
	require.True(t, proto.Equal(resp, &direct))

	var malformed storagev1.ReadRequest
	err = codec.Unmarshal([]byte{0xff, 0xff, 0xff}, &malformed)
	require.ErrorContains(t, err, "unmarshal into *storagev1.ReadRequest")
}
