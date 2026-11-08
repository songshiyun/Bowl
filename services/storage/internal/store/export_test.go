package store

import "sync"

// SetBeforeCompactRelocateHookForTest 注册在搬迁活块读取追加完成、向 StoreActor 投递 opCompactRelocate 前触发的测试钩子。
func (s *StoreActor) SetBeforeCompactRelocateHookForTest(hook func()) {
	s.beforeCompactRelocateHook = hook
}

// HoldTickForTest 占住当前一轮 Actor Tick，使并发测试可将多条真实操作攒入同一 mb.active 批次并在下一轮 Tick 合并演进；
// 调用返回的 release 函数即可放行当前 Tick。
func HoldTickForTest(s *StoreActor) (release func()) {
	entered := make(chan struct{})
	unblock := make(chan struct{})
	done := make(chan struct{})
	var once sync.Once

	s.beforeTickHook = func() {
		once.Do(func() {
			close(entered)
			<-unblock
			s.beforeTickHook = nil
		})
	}
	go func() {
		cmd := s.acquireCmd()
		cmd.op = opNoop
		_ = s.submit(cmd)
		close(done)
	}()
	<-entered
	return func() {
		close(unblock)
		<-done
	}
}
