package server

import (
	"context"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// Diff 返回两个版本之间的结构化差异。
func (s *Server) Diff(ctx context.Context, req *connect.Request[storagev1.DiffRequest]) (*connect.Response[storagev1.DiffResponse], error) {
	entries, err := s.metaManager.Diff(ctx, meta.DiffParams{DiskID: req.Msg.DiskId, FromRef: req.Msg.FromRef, ToRef: req.Msg.ToRef})
	if err != nil {
		return nil, toConnectError(err)
	}
	result := make([]*storagev1.DiffEntry, 0, len(entries))
	for _, entry := range entries {
		out := &storagev1.DiffEntry{ChangeType: storagev1.DiffChangeType(entry.ChangeType), Path: entry.Path, OldPath: entry.OldPath, ModifiedBlockIndices: entry.ModifiedBlockIndices}
		if entry.OldAttr != nil {
			out.OldAttr = toProtoAttribute(&entry.OldAttr.Attr)
		}
		if entry.NewAttr != nil {
			out.NewAttr = toProtoAttribute(&entry.NewAttr.Attr)
		}
		result = append(result, out)
	}
	return connect.NewResponse(&storagev1.DiffResponse{Entries: result}), nil
}
