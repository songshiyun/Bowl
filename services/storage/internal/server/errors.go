// Package server 实现 StorageService 的 ConnectRPC 处理器、拦截器与错误转换。
package server

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"

	"github.com/starsrc/pkg/serverx"

	"github.com/starsrc/bowl/services/storage/internal/control"
)

// statusToConnectError 将 gRPC *status.Status（含 google.rpc.ErrorInfo 等 Details）无损转换为 *connect.Error。
// 委托给 serverx.StatusToConnectError。
func statusToConnectError(st *status.Status) error {
	return serverx.StatusToConnectError(st)
}

// StatusToConnectError 是 statusToConnectError 的导出版本，供外部包或测试直接调用。
func StatusToConnectError(st *status.Status) error {
	return serverx.StatusToConnectError(st)
}

// toConnectError 将任意错误（领域哨兵错误、上下文取消/超时或未知内部错误）转换为对应的 *connect.Error。
// 若 err == nil 返回 nil；上下文取消/超时直接保留原始错误链包装为 *connect.Error；
// 其余通过 control.ToStatus(err) 获得 *status.Status 并委托给 StatusToConnectError。
func toConnectError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return connect.NewError(connect.CodeCanceled, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return connect.NewError(connect.CodeDeadlineExceeded, err)
	}
	if conflictErr, ok := errors.AsType[*control.MergeConflictError](err); ok {
		connectErr := connect.NewError(connect.CodeAborted, conflictErr)
		if infoDetail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{
			Reason: "ECONFLICT",
			Domain: control.ErrorDomain,
		}); detailErr == nil {
			connectErr.AddDetail(infoDetail)
		}
		if reportDetail, detailErr := connect.NewErrorDetail(toProtoMergeConflictReport(conflictErr.LCACommitID, &conflictErr.Report)); detailErr == nil {
			connectErr.AddDetail(reportDetail)
		}
		return connectErr
	}
	return StatusToConnectError(control.ToStatus(err))
}

// ToConnectError 是 toConnectError 的导出版本，供外部包或后续 RPC 实现调用。
func ToConnectError(err error) error {
	return toConnectError(err)
}

// ExtractErrorInfo 从错误中提取首个附带的 google.rpc.ErrorInfo 详情。
// 委托给 serverx.ExtractErrorInfo。
func ExtractErrorInfo(err error) *errdetails.ErrorInfo {
	return serverx.ExtractErrorInfo(err)
}
