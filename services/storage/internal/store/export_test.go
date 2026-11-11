package store

// SetBeforeCompactRelocateHookForTest 注册在搬迁活块读取追加完成、向 StoreActor 投递 opCompactRelocate 前触发的测试钩子。
func (s *StoreActor) SetBeforeCompactRelocateHookForTest(hook func()) {
	s.beforeCompactRelocateHook = hook
}

// SetBeforeTickHookForTest 注册在每次 processTick 开始排空批次前触发的测试钩子。
func (s *StoreActor) SetBeforeTickHookForTest(hook func()) {
	s.beforeTickHook = hook
}

// SetSyncFileHookForTest 注册在 BlobStore 执行 .pack fdatasync 前触发的测试钩子。
func (s *BlobStore) SetSyncFileHookForTest(hook func(packID uint64)) {
	s.mu.Lock()
	s.syncFileHook = hook
	s.mu.Unlock()
}
