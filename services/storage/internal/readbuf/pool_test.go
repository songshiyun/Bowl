package readbuf_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/starsrc/bowl/services/storage/internal/readbuf"
)

func requireZero(t *testing.T, data []byte) {
	t.Helper()
	if !bytes.Equal(data, make([]byte, len(data))) {
		t.Fatal("读取缓冲泄露了上一次请求或失败重试的数据")
	}
}

func TestScopeClearsPreviousFileAndRetry(t *testing.T) {
	pool := readbuf.NewPool()
	ctx, release := pool.WithScope(context.Background())
	first := readbuf.NewBuffer(ctx).Bytes(1 << 20)
	for i := range first {
		first[i] = 0xA5
	}
	release()

	ctx, release = pool.WithScope(context.Background())
	defer release()
	buffer := readbuf.NewBuffer(ctx)
	data := buffer.Bytes(1 << 20)
	requireZero(t, data)
	// 模拟一次失败读取已经写入部分数据，下一次重试仍必须从空洞全零状态开始。
	copy(data[4096:], bytes.Repeat([]byte{0xD3}, 8192))
	requireZero(t, buffer.Bytes(1<<20))
	requireZero(t, buffer.Bytes(2<<20))
}

func TestActiveResponsesKeepTheirOwnData(t *testing.T) {
	pool := readbuf.NewPool()
	ctxA, closeA := pool.WithScope(context.Background())
	defer closeA()
	dataA := readbuf.NewBuffer(ctxA).Bytes(1 << 20)
	for i := range dataA {
		dataA[i] = 0xA1
	}
	ctxB, closeB := pool.WithScope(context.Background())
	dataB := readbuf.NewBuffer(ctxB).Bytes(1 << 20)
	for i := range dataB {
		dataB[i] = 0xB2
	}
	closeB()
	closeB()

	ctxC, closeC := pool.WithScope(context.Background())
	defer closeC()
	dataC := readbuf.NewBuffer(ctxC).Bytes(1 << 20)
	ctxD, closeD := pool.WithScope(context.Background())
	defer closeD()
	dataD := readbuf.NewBuffer(ctxD).Bytes(1 << 20)
	for i := range dataC {
		dataC[i] = 0xC3
	}
	for i := range dataD {
		dataD[i] = 0xD4
	}
	if !bytes.Equal(dataA, bytes.Repeat([]byte{0xA1}, len(dataA))) ||
		!bytes.Equal(dataC, bytes.Repeat([]byte{0xC3}, len(dataC))) {
		t.Fatal("并发响应或重复释放造成缓冲别名")
	}

	// 已结束上下文上的新读取必须拥有独立数据，不能把后续活动响应重新归还池。
	late := readbuf.NewBuffer(ctxB).Bytes(1 << 20)
	copy(late, dataD)
	closeB()
	if !bytes.Equal(dataD, bytes.Repeat([]byte{0xD4}, len(dataD))) {
		t.Fatal("已结束上下文破坏了活动响应")
	}
}

func TestConcurrentScopesDoNotShareResponses(t *testing.T) {
	pool := readbuf.NewPool()
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for iteration := range 16 {
				ctx, closeScope := pool.WithScope(context.Background())
				data := readbuf.NewBuffer(ctx).Bytes(256 << 10)
				value := byte(worker*17 + iteration + 1)
				for _, b := range data {
					if b != 0 {
						t.Error("并发读取收到旧请求数据")
						break
					}
				}
				for i := range data {
					data[i] = value
				}
				for _, b := range data {
					if b != value {
						t.Error("并发读取的数据被其他请求覆盖")
						break
					}
				}
				closeScope()
			}
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("并发缓冲验证超时")
	}
}
