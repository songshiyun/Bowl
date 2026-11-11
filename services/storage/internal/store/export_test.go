package store

// SetBeforeCompactRelocateHookForTest 注册在搬迁活块读取追加完成、向 StoreActor 投递 opCompactRelocate 前触发的测试钩子。
func (s *StoreActor) SetBeforeCompactRelocateHookForTest(hook func()) {
	s.beforeCompactRelocateHook = hook
}
