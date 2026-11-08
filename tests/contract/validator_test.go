package contract_test

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/validator"
)

// 该文件验证 pkg/validator 的自有业务映射，以及 storage.proto 中声明的字段边界契约；
// protovalidate 库本身的通用行为不在契约测试中重复测试。

func requireErrorInfo(t *testing.T, st *status.Status, wantReason string) {
	t.Helper()
	require.NotNil(t, st)
	require.Equal(t, codes.InvalidArgument, st.Code())
	details := st.Details()
	require.Len(t, details, 1)
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	require.Equal(t, validator.ErrorDomain, info.GetDomain())
	require.Equal(t, wantReason, info.GetReason())
}

func assertValidationReason(t *testing.T, err error, wantReason string) {
	t.Helper()
	require.Error(t, err)

	var pvErr *protovalidate.ValidationError
	require.ErrorAs(t, err, &pvErr, "应支持 errors.As 提取底层 protovalidate.ValidationError")

	reason, ok := validator.ErrnoReason(err)
	require.True(t, ok)
	require.Equal(t, wantReason, reason)

	requireErrorInfo(t, validator.ToStatus(err), wantReason)

	grpcStatus, ok := status.FromError(err)
	require.True(t, ok)
	requireErrorInfo(t, grpcStatus, wantReason)
}

func TestStorageValidatorAcceptsValidRequests(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	one := uint64(1)
	require.NoError(t, v.Validate(&storagev1.LookupRequest{ParentIno: 1, Name: []byte("test.txt")}))
	require.NoError(t, v.Validate(&storagev1.LookupRequest{ParentIno: 1, Name: []byte{0xff, 0xfe, 0x80, 0x81}}))
	require.NoError(t, v.Validate(&storagev1.MkdirRequest{ParentIno: 1, Name: []byte("subdir"), Mode: 0o755, Uid: 1000, Gid: 1000}))
	require.NoError(t, v.Validate(&storagev1.SetXattrRequest{Ino: 1, Name: []byte("user.k"), Value: []byte("v"), Flags: 3}))
	require.NoError(t, v.Validate(&storagev1.CreateRequest{ParentIno: 1, Name: []byte("created"), Mode: 0o644, Uid: math.MaxInt32, Gid: math.MaxInt32, SessionId: &one}))
	require.NoError(t, v.Validate(&storagev1.ReadRequest{Ino: 1, Offset: math.MaxInt64, Size: 4194304, SessionId: &one}))
	require.NoError(t, v.Validate(&storagev1.WriteRequest{Ino: 1, Offset: 0, Data: bytes.Repeat([]byte("w"), 4194304), SessionId: &one}))
	require.NoError(t, v.Validate(&storagev1.WriteRequest{Ino: 1, Offset: math.MaxInt64, SessionId: &one}))
	require.NoError(t, v.Validate(&storagev1.FallocateRequest{Ino: 1, Offset: 0, Length: 1, Mode: 0, SessionId: &one}))

	maxInt := uint64(math.MaxInt64)
	require.NoError(t, v.Validate(&storagev1.CreateRequest{ParentIno: maxInt, Name: []byte("created"), SessionId: &maxInt}))
	require.NoError(t, v.Validate(&storagev1.ReadRequest{Ino: maxInt, Fh: maxInt, Offset: maxInt, SessionId: &maxInt}))
	require.NoError(t, v.Validate(&storagev1.WriteRequest{Ino: maxInt, Fh: maxInt, Offset: maxInt, SessionId: &maxInt}))
	require.NoError(t, v.Validate(&storagev1.FallocateRequest{Ino: maxInt, Fh: maxInt, Offset: maxInt, Length: maxInt, SessionId: &maxInt}))
}

func TestStorageValidatorMapsErrnoReasons(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	assertValidationReason(t, v.Validate(&storagev1.LookupRequest{ParentIno: 0, Name: []byte("test.txt")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.LookupRequest{ParentIno: 1, Name: bytes.Repeat([]byte("a"), 256)}), validator.ReasonNameTooLong)
	assertValidationReason(t, v.Validate(&storagev1.LookupRequest{ParentIno: 1, Name: []byte("foo\x00bar")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.LookupRequest{ParentIno: 1, Name: []byte("foo/bar")}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.SetXattrRequest{Ino: 1, Name: []byte{}, Value: []byte("val")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.SetXattrRequest{Ino: 1, Name: []byte("user.k"), Value: []byte("v"), Flags: 4}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.SetXattrRequest{Ino: 1, Name: bytes.Repeat([]byte("a"), 256), Value: []byte("val")}), validator.ReasonRange)
	assertValidationReason(t, v.Validate(&storagev1.SetXattrRequest{Ino: 1, Name: append(bytes.Repeat([]byte("a"), 255), 0), Value: []byte("val")}), validator.ReasonRange)
	assertValidationReason(t, v.Validate(&storagev1.SetXattrRequest{Ino: 1, Name: []byte("user.a\x00b"), Value: []byte("val")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.SetXattrRequest{Ino: 1, Name: []byte("user.test"), Value: bytes.Repeat([]byte("v"), 65537)}), validator.ReasonRange)

	assertValidationReason(t, v.Validate(&storagev1.GetXattrRequest{Ino: 1, Name: nil}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.RemoveXattrRequest{Ino: 1, Name: bytes.Repeat([]byte("x"), 256)}), validator.ReasonRange)
	assertValidationReason(t, v.Validate(&storagev1.ListXattrResponse{Names: [][]byte{[]byte("user.ok"), {}}}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.GetXattrResponse{Value: bytes.Repeat([]byte("v"), 65537)}), validator.ReasonRange)

	assertValidationReason(t, v.Validate(&storagev1.MkdirRequest{ParentIno: 1, Name: bytes.Repeat([]byte("d"), 256), Mode: 0o755}), validator.ReasonNameTooLong)
	assertValidationReason(t, v.Validate(&storagev1.MkdirRequest{ParentIno: 1, Name: bytes.Repeat([]byte("/"), 256), Mode: 0o755}), validator.ReasonNameTooLong)
	assertValidationReason(t, v.Validate(&storagev1.RenameRequest{OldParentIno: 1, OldName: []byte("old"), NewParentIno: 1, NewName: bytes.Repeat([]byte("n"), 256)}), validator.ReasonNameTooLong)

	assertValidationReason(t, v.Validate(&storagev1.SymlinkRequest{ParentIno: 1, Name: []byte("link"), Target: bytes.Repeat([]byte("t"), 4096)}), validator.ReasonNameTooLong)
	assertValidationReason(t, v.Validate(&storagev1.SymlinkRequest{ParentIno: 1, Name: []byte("link"), Target: []byte{}}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.SymlinkRequest{ParentIno: 1, Name: []byte("link"), Target: append(bytes.Repeat([]byte("t"), 4095), 0)}), validator.ReasonInvalidArgument)

	zero := uint64(0)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{ParentIno: 0, Name: []byte("created")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{ParentIno: 1, Name: []byte{}}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{ParentIno: 1, Name: bytes.Repeat([]byte("c"), 256)}), validator.ReasonNameTooLong)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{ParentIno: 1, Name: []byte("bad/name")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{ParentIno: 1, Name: []byte("bad\x00name")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{ParentIno: 1, Name: []byte("created"), Uid: math.MaxInt32 + 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{ParentIno: 1, Name: []byte("created"), Gid: math.MaxInt32 + 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{ParentIno: 1, Name: []byte("created"), SessionId: &zero}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.ReadRequest{Ino: 0, Size: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReadRequest{Ino: 1, Size: 4194305}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReadRequest{Ino: 1, Size: 1, SessionId: &zero}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{Ino: 0, Data: []byte("x")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{Ino: 1, Data: bytes.Repeat([]byte("w"), 4194305)}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{Ino: 1, Offset: math.MaxInt64, Data: []byte("x")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{Ino: 1, Data: []byte("x"), SessionId: &zero}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{Ino: 0, Length: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{Ino: 1, Length: 0}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{Ino: 1, Length: 1, SessionId: &zero}), validator.ReasonInvalidArgument)

	overMax := uint64(math.MaxInt64) + 1
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{ParentIno: overMax, Name: []byte("created")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{ParentIno: 1, Name: []byte("created"), SessionId: &overMax}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReadRequest{Ino: overMax, Size: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReadRequest{Ino: 1, Fh: overMax, Size: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReadRequest{Ino: 1, Offset: overMax, Size: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReadRequest{Ino: 1, Size: 1, SessionId: &overMax}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{Ino: overMax, Data: []byte("x")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{Ino: 1, Fh: overMax, Data: []byte("x")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{Ino: 1, Offset: overMax}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{Ino: 1, Data: []byte("x"), SessionId: &overMax}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{Ino: overMax, Length: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{Ino: 1, Fh: overMax, Length: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{Ino: 1, Offset: overMax, Length: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{Ino: 1, Length: overMax}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{Ino: 1, Length: 1, SessionId: &overMax}), validator.ReasonInvalidArgument)
}

func TestStorageValidatorRawProtovalidateStatus(t *testing.T) {
	rawValidator, err := protovalidate.New()
	require.NoError(t, err)

	rawErr := rawValidator.Validate(&storagev1.SetXattrRequest{Ino: 1, Name: bytes.Repeat([]byte("a"), 256), Value: []byte("v")})
	require.Error(t, rawErr)

	requireErrorInfo(t, validator.ToStatus(rawErr), validator.ReasonRange)
}

func TestStorageValidatorErrnoReasonRejectsNonValidationErrors(t *testing.T) {
	reason, ok := validator.ErrnoReason(errors.New("not a validation error"))
	require.False(t, ok)
	require.Empty(t, reason)

	require.Nil(t, validator.ToStatus(nil))
	require.NoError(t, validator.ToGRPCError(nil))
}

const (
	validDiskID   = "11111111-1111-7111-8111-111111111111"
	validCommitID = "22222222-2222-7222-8222-222222222222"
	nilUUID       = "00000000-0000-0000-0000-000000000000"
)

func TestStorageValidatorAcceptsValidVersionAndDiskRequests(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	mainBranch := "main"
	featureBranch := "feature"
	renamed := "renamed"
	maxBytes := uint64(1 << 30)

	require.NoError(t, v.Validate(&storagev1.CreateBranchRequest{DiskId: validDiskID, Name: featureBranch}))
	require.NoError(t, v.Validate(&storagev1.CreateBranchRequest{DiskId: validDiskID, Name: featureBranch, SourceBranch: &mainBranch, SourceCommitId: new(validCommitID)}))
	require.NoError(t, v.Validate(&storagev1.ListBranchesRequest{DiskId: validDiskID}))
	require.NoError(t, v.Validate(&storagev1.DeleteBranchRequest{DiskId: validDiskID, Name: featureBranch}))
	require.NoError(t, v.Validate(&storagev1.CommitRequest{DiskId: validDiskID, BranchName: mainBranch, Message: "snapshot"}))
	require.NoError(t, v.Validate(&storagev1.RollbackRequest{DiskId: validDiskID, BranchName: mainBranch, CommitId: validCommitID}))
	require.NoError(t, v.Validate(&storagev1.MergeRequest{
		DiskId: validDiskID, SourceBranch: featureBranch, TargetBranch: mainBranch,
		Strategy: storagev1.ConflictStrategy_CONFLICT_STRATEGY_ABORT_ON_CONFLICT,
	}))
	require.NoError(t, v.Validate(&storagev1.ResolveMergeRequest{DiskId: validDiskID, BranchName: mainBranch, Message: "resolved"}))
	require.NoError(t, v.Validate(&storagev1.AbortMergeRequest{DiskId: validDiskID, BranchName: mainBranch}))
	require.NoError(t, v.Validate(&storagev1.DiffRequest{DiskId: validDiskID, FromRef: mainBranch, ToRef: featureBranch}))

	require.NoError(t, v.Validate(&storagev1.CreateDiskRequest{Name: "disk", MaxBytes: maxBytes}))
	require.NoError(t, v.Validate(&storagev1.GetDiskRequest{DiskId: validDiskID}))
	require.NoError(t, v.Validate(&storagev1.ListDisksRequest{PageSize: 1000, PageToken: "next"}))
	require.NoError(t, v.Validate(&storagev1.UpdateDiskRequest{DiskId: validDiskID, Name: &renamed}))
	require.NoError(t, v.Validate(&storagev1.UpdateDiskRequest{DiskId: validDiskID, MaxBytes: &maxBytes}))
	require.NoError(t, v.Validate(&storagev1.DeleteDiskRequest{DiskId: validDiskID}))

	require.NoError(t, v.Validate(&storagev1.RenewSessionRequest{SessionId: 1, DiskId: new(validDiskID), BranchName: &mainBranch}))
	require.NoError(t, v.Validate(&storagev1.RenewSessionRequest{
		SessionId: 1, DiskId: new(validDiskID), BranchName: &mainBranch, ReadOnly: true, CommitId: new(validCommitID),
	}))
	require.NoError(t, v.Validate(&storagev1.CloseSessionRequest{SessionId: 1}))
	require.NoError(t, v.Validate(&storagev1.ReclaimOrphanRequest{SessionId: 1, DiskId: validDiskID, BranchName: &mainBranch, Ino: 1}))
}

func TestStorageValidatorMapsVersionAndDiskReasons(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	mainBranch := "main"
	featureBranch := "feature"
	longName := strings.Repeat("a", 256)
	overMax := uint64(math.MaxInt64) + 1

	assertValidationReason(t, v.Validate(&storagev1.CreateBranchRequest{DiskId: "bad", Name: featureBranch}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateBranchRequest{DiskId: nilUUID, Name: featureBranch}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateBranchRequest{DiskId: validDiskID, Name: ""}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateBranchRequest{DiskId: validDiskID, Name: "a/b"}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateBranchRequest{DiskId: validDiskID, Name: "a\x00b"}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateBranchRequest{DiskId: validDiskID, Name: ".."}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateBranchRequest{DiskId: validDiskID, Name: longName}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateBranchRequest{DiskId: validDiskID, Name: featureBranch, SourceBranch: new("a/b")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateBranchRequest{DiskId: validDiskID, Name: featureBranch, SourceCommitId: new("bad")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateBranchRequest{DiskId: validDiskID, Name: featureBranch, SourceCommitId: new(nilUUID)}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.ListBranchesRequest{DiskId: nilUUID}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.DeleteBranchRequest{DiskId: nilUUID, Name: featureBranch}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.DeleteBranchRequest{DiskId: validDiskID, Name: ""}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.DeleteBranchRequest{DiskId: validDiskID, Name: "a/b"}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.CommitRequest{DiskId: "bad", BranchName: mainBranch}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CommitRequest{DiskId: validDiskID, BranchName: ""}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CommitRequest{DiskId: validDiskID, BranchName: "a/b"}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CommitRequest{DiskId: validDiskID, BranchName: ".."}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.RollbackRequest{DiskId: nilUUID, BranchName: mainBranch, CommitId: validCommitID}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.RollbackRequest{DiskId: validDiskID, BranchName: "", CommitId: validCommitID}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.RollbackRequest{DiskId: validDiskID, BranchName: mainBranch, CommitId: "bad"}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.RollbackRequest{DiskId: validDiskID, BranchName: mainBranch, CommitId: nilUUID}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.MergeRequest{DiskId: "bad", SourceBranch: featureBranch, TargetBranch: mainBranch}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.MergeRequest{DiskId: validDiskID, SourceBranch: "", TargetBranch: mainBranch}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.MergeRequest{DiskId: validDiskID, SourceBranch: featureBranch, TargetBranch: "a/b"}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.MergeRequest{DiskId: validDiskID, SourceBranch: featureBranch, TargetBranch: mainBranch, Strategy: storagev1.ConflictStrategy(99)}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.ResolveMergeRequest{DiskId: nilUUID, BranchName: mainBranch}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ResolveMergeRequest{DiskId: validDiskID, BranchName: ".."}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.AbortMergeRequest{DiskId: "bad", BranchName: mainBranch}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.AbortMergeRequest{DiskId: validDiskID, BranchName: "a/b"}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.DiffRequest{DiskId: "bad", FromRef: mainBranch, ToRef: featureBranch}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.DiffRequest{DiskId: validDiskID, FromRef: "", ToRef: featureBranch}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.DiffRequest{DiskId: validDiskID, FromRef: mainBranch, ToRef: "a/b"}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.DiffRequest{DiskId: validDiskID, FromRef: mainBranch, ToRef: ".."}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.CreateDiskRequest{Name: longName, MaxBytes: 1}), validator.ReasonNameTooLong)
	assertValidationReason(t, v.Validate(&storagev1.CreateDiskRequest{Name: "a\x00b", MaxBytes: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateDiskRequest{Name: "disk", MaxBytes: 0}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateDiskRequest{Name: "disk", MaxBytes: overMax}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.GetDiskRequest{DiskId: "bad"}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.GetDiskRequest{DiskId: nilUUID}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ListDisksRequest{PageSize: 1001}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.UpdateDiskRequest{DiskId: validDiskID}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.UpdateDiskRequest{DiskId: "bad", Name: new("renamed")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.UpdateDiskRequest{DiskId: validDiskID, Name: new(longName)}), validator.ReasonNameTooLong)
	assertValidationReason(t, v.Validate(&storagev1.UpdateDiskRequest{DiskId: validDiskID, Name: new("a\x00b")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.UpdateDiskRequest{DiskId: validDiskID, MaxBytes: new(uint64(0))}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.UpdateDiskRequest{DiskId: validDiskID, MaxBytes: new(overMax)}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.DeleteDiskRequest{DiskId: nilUUID}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.RenewSessionRequest{SessionId: 0, DiskId: new(validDiskID)}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.RenewSessionRequest{SessionId: overMax, DiskId: new(validDiskID)}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.RenewSessionRequest{SessionId: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.RenewSessionRequest{SessionId: 1, DiskId: new("bad")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.RenewSessionRequest{SessionId: 1, DiskId: new(nilUUID)}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.RenewSessionRequest{SessionId: 1, DiskId: new(validDiskID), BranchName: new("")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.RenewSessionRequest{SessionId: 1, DiskId: new(validDiskID), BranchName: new("a/b")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.RenewSessionRequest{SessionId: 1, DiskId: new(validDiskID), CommitId: new("bad"), ReadOnly: true}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.RenewSessionRequest{SessionId: 1, DiskId: new(validDiskID), CommitId: new(nilUUID), ReadOnly: true}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.RenewSessionRequest{SessionId: 1, DiskId: new(validDiskID), CommitId: new(validCommitID)}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CloseSessionRequest{SessionId: 0}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CloseSessionRequest{SessionId: overMax}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.ReclaimOrphanRequest{SessionId: 0, DiskId: validDiskID, Ino: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReclaimOrphanRequest{SessionId: overMax, DiskId: validDiskID, Ino: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReclaimOrphanRequest{SessionId: 1, DiskId: "bad", Ino: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReclaimOrphanRequest{SessionId: 1, DiskId: nilUUID, Ino: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReclaimOrphanRequest{SessionId: 1, DiskId: validDiskID, BranchName: new(""), Ino: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReclaimOrphanRequest{SessionId: 1, DiskId: validDiskID, BranchName: new("a/b"), Ino: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReclaimOrphanRequest{SessionId: 1, DiskId: validDiskID, BranchName: new(".."), Ino: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReclaimOrphanRequest{SessionId: 1, DiskId: validDiskID, Ino: 0}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReclaimOrphanRequest{SessionId: 1, DiskId: validDiskID, Ino: overMax}), validator.ReasonInvalidArgument)
}
