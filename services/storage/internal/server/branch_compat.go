package server

import (
	"errors"

	"connectrpc.com/connect"
)

// 旧存储引擎不能执行分支会话与客户端本地句柄语义。
func branchContextUnimplemented() error {
	return connect.NewError(connect.CodeUnimplemented, errors.New("分支会话和客户端本地句柄尚未实现"))
}
