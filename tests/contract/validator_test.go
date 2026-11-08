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
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/validator"
)

// 该文件验证 pkg/validator 的自有业务映射，以及 storage.proto 中声明的字段边界契约；
// protovalidate 库本身的通用行为不在契约测试中重复测试。

const (
	validDiskID = "11111111-1111-7111-8111-111111111111"
	nilUUID     = "00000000-0000-0000-0000-000000000000"
)

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

	require.NoError(t, v.Validate(&storagev1.LookupRequest{DiskId: validDiskID, ParentIno: 1, Name: []byte("test.txt")}))
	require.NoError(t, v.Validate(&storagev1.LookupRequest{DiskId: validDiskID, ParentIno: 1, Name: []byte{0xff, 0xfe, 0x80, 0x81}}))
	require.NoError(t, v.Validate(&storagev1.MkdirRequest{DiskId: validDiskID, Epoch: 1, ParentIno: 1, Name: []byte("subdir"), Mode: 0o755, Uid: 1000, Gid: 1000}))
	require.NoError(t, v.Validate(&storagev1.SetXattrRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Name: []byte("user.k"), Value: []byte("v"), Flags: 3}))
	require.NoError(t, v.Validate(&storagev1.CreateRequest{DiskId: validDiskID, Epoch: 1, ParentIno: 1, Name: []byte("created"), Mode: 0o644, Uid: math.MaxInt32, Gid: math.MaxInt32}))
	require.NoError(t, v.Validate(&storagev1.ReadRequest{DiskId: validDiskID, Ino: 1, Offset: math.MaxInt64, Size: 4194304}))
	require.NoError(t, v.Validate(&storagev1.WriteRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Offset: 0, Data: bytes.Repeat([]byte("w"), 4194304)}))
	require.NoError(t, v.Validate(&storagev1.WriteRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Offset: math.MaxInt64}))
	require.NoError(t, v.Validate(&storagev1.FallocateRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Offset: 0, Length: 1, Mode: 0}))

	maxInt := uint64(math.MaxInt64)
	require.NoError(t, v.Validate(&storagev1.CreateRequest{DiskId: validDiskID, Epoch: maxInt, ParentIno: maxInt, Name: []byte("created")}))
	require.NoError(t, v.Validate(&storagev1.ReadRequest{DiskId: validDiskID, Ino: maxInt, Fh: maxInt, Offset: maxInt}))
	require.NoError(t, v.Validate(&storagev1.WriteRequest{DiskId: validDiskID, Epoch: maxInt, Ino: maxInt, Fh: maxInt, Offset: maxInt}))
	require.NoError(t, v.Validate(&storagev1.FallocateRequest{DiskId: validDiskID, Epoch: maxInt, Ino: maxInt, Fh: maxInt, Offset: maxInt, Length: maxInt}))
}

func TestStorageValidatorMapsErrnoReasons(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	assertValidationReason(t, v.Validate(&storagev1.LookupRequest{DiskId: validDiskID, ParentIno: 0, Name: []byte("test.txt")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.LookupRequest{DiskId: validDiskID, ParentIno: 1, Name: bytes.Repeat([]byte("a"), 256)}), validator.ReasonNameTooLong)
	assertValidationReason(t, v.Validate(&storagev1.LookupRequest{DiskId: validDiskID, ParentIno: 1, Name: []byte("foo\x00bar")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.LookupRequest{DiskId: validDiskID, ParentIno: 1, Name: []byte("foo/bar")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.LookupRequest{DiskId: "bad", ParentIno: 1, Name: bytes.Repeat([]byte("a"), 256)}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.SetXattrRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Name: []byte{}, Value: []byte("val")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.SetXattrRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Name: []byte("user.k"), Value: []byte("v"), Flags: 4}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.SetXattrRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Name: bytes.Repeat([]byte("a"), 256), Value: []byte("val")}), validator.ReasonRange)
	assertValidationReason(t, v.Validate(&storagev1.SetXattrRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Name: append(bytes.Repeat([]byte("a"), 255), 0), Value: []byte("val")}), validator.ReasonRange)
	assertValidationReason(t, v.Validate(&storagev1.SetXattrRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Name: []byte("user.a\x00b"), Value: []byte("val")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.SetXattrRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Name: []byte("user.test"), Value: bytes.Repeat([]byte("v"), 65537)}), validator.ReasonRange)
	assertValidationReason(t, v.Validate(&storagev1.SetXattrRequest{DiskId: "bad", Epoch: 1, Ino: 1, Name: bytes.Repeat([]byte("a"), 256), Value: []byte("val")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.SetXattrRequest{DiskId: validDiskID, Epoch: 0, Ino: 1, Name: bytes.Repeat([]byte("a"), 256), Value: []byte("val")}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.GetXattrRequest{DiskId: validDiskID, Ino: 1, Name: nil}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.GetXattrRequest{DiskId: nilUUID, Ino: 1, Name: bytes.Repeat([]byte("x"), 256)}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.RemoveXattrRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Name: bytes.Repeat([]byte("x"), 256)}), validator.ReasonRange)
	assertValidationReason(t, v.Validate(&storagev1.RemoveXattrRequest{DiskId: validDiskID, Epoch: 0, Ino: 1, Name: bytes.Repeat([]byte("x"), 256)}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ListXattrResponse{Names: [][]byte{[]byte("user.ok"), {}}}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.GetXattrResponse{Value: bytes.Repeat([]byte("v"), 65537)}), validator.ReasonRange)

	assertValidationReason(t, v.Validate(&storagev1.MkdirRequest{DiskId: validDiskID, Epoch: 1, ParentIno: 1, Name: bytes.Repeat([]byte("d"), 256), Mode: 0o755}), validator.ReasonNameTooLong)
	assertValidationReason(t, v.Validate(&storagev1.MkdirRequest{DiskId: validDiskID, Epoch: 1, ParentIno: 1, Name: bytes.Repeat([]byte("/"), 256), Mode: 0o755}), validator.ReasonNameTooLong)
	assertValidationReason(t, v.Validate(&storagev1.MkdirRequest{DiskId: validDiskID, Epoch: 0, ParentIno: 1, Name: bytes.Repeat([]byte("d"), 256), Mode: 0o755}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.RenameRequest{DiskId: validDiskID, Epoch: 1, OldParentIno: 1, OldName: []byte("old"), NewParentIno: 1, NewName: bytes.Repeat([]byte("n"), 256)}), validator.ReasonNameTooLong)
	assertValidationReason(t, v.Validate(&storagev1.RenameRequest{DiskId: nilUUID, Epoch: 1, OldParentIno: 1, OldName: []byte("old"), NewParentIno: 1, NewName: bytes.Repeat([]byte("n"), 256)}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.SymlinkRequest{DiskId: validDiskID, Epoch: 1, ParentIno: 1, Name: []byte("link"), Target: bytes.Repeat([]byte("t"), 4096)}), validator.ReasonNameTooLong)
	assertValidationReason(t, v.Validate(&storagev1.SymlinkRequest{DiskId: validDiskID, Epoch: 1, ParentIno: 1, Name: []byte("link"), Target: []byte{}}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.SymlinkRequest{DiskId: validDiskID, Epoch: 1, ParentIno: 1, Name: []byte("link"), Target: append(bytes.Repeat([]byte("t"), 4095), 0)}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.SymlinkRequest{DiskId: validDiskID, Epoch: 0, ParentIno: 1, Name: []byte("link"), Target: bytes.Repeat([]byte("t"), 4096)}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{DiskId: validDiskID, Epoch: 1, ParentIno: 0, Name: []byte("created")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{DiskId: validDiskID, Epoch: 1, ParentIno: 1, Name: []byte{}}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{DiskId: validDiskID, Epoch: 1, ParentIno: 1, Name: bytes.Repeat([]byte("c"), 256)}), validator.ReasonNameTooLong)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{DiskId: validDiskID, Epoch: 1, ParentIno: 1, Name: []byte("bad/name")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{DiskId: validDiskID, Epoch: 1, ParentIno: 1, Name: []byte("bad\x00name")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{DiskId: validDiskID, Epoch: 1, ParentIno: 1, Name: []byte("created"), Uid: math.MaxInt32 + 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{DiskId: validDiskID, Epoch: 1, ParentIno: 1, Name: []byte("created"), Gid: math.MaxInt32 + 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{DiskId: validDiskID, Epoch: 0, ParentIno: 1, Name: []byte("created")}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.ReadRequest{DiskId: validDiskID, Ino: 0, Size: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReadRequest{DiskId: validDiskID, Ino: 1, Size: 4194305}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReadRequest{DiskId: nilUUID, Ino: 1, Size: 1}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{DiskId: validDiskID, Epoch: 1, Ino: 0, Data: []byte("x")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Data: bytes.Repeat([]byte("w"), 4194305)}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Offset: math.MaxInt64, Data: []byte("x")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{DiskId: validDiskID, Epoch: 0, Ino: 1, Data: []byte("x")}), validator.ReasonInvalidArgument)

	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{DiskId: validDiskID, Epoch: 1, Ino: 0, Length: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Length: 0}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{DiskId: validDiskID, Epoch: 0, Ino: 1, Length: 1}), validator.ReasonInvalidArgument)

	overMax := uint64(math.MaxInt64) + 1
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{DiskId: validDiskID, Epoch: 1, ParentIno: overMax, Name: []byte("created")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.CreateRequest{DiskId: validDiskID, Epoch: overMax, ParentIno: 1, Name: []byte("created")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReadRequest{DiskId: validDiskID, Ino: overMax, Size: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReadRequest{DiskId: validDiskID, Ino: 1, Fh: overMax, Size: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReadRequest{DiskId: validDiskID, Ino: 1, Offset: overMax, Size: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{DiskId: validDiskID, Epoch: 1, Ino: overMax, Data: []byte("x")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Fh: overMax, Data: []byte("x")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Offset: overMax}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.WriteRequest{DiskId: validDiskID, Epoch: overMax, Ino: 1, Data: []byte("x")}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{DiskId: validDiskID, Epoch: 1, Ino: overMax, Length: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Fh: overMax, Length: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Offset: overMax, Length: 1}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Length: overMax}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.FallocateRequest{DiskId: validDiskID, Epoch: overMax, Ino: 1, Length: 1}), validator.ReasonInvalidArgument)
}

func TestStorageValidatorRawProtovalidateStatus(t *testing.T) {
	rawValidator, err := protovalidate.New()
	require.NoError(t, err)

	rawErr := rawValidator.Validate(&storagev1.SetXattrRequest{DiskId: validDiskID, Epoch: 1, Ino: 1, Name: bytes.Repeat([]byte("a"), 256), Value: []byte("v")})
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

func TestStorageValidatorControlPlaneAndMountDiskRequests(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	renamed := "renamed"
	maxBytes := uint64(1 << 30)
	longName := strings.Repeat("a", 256)
	overMax := uint64(math.MaxInt64) + 1

	require.NoError(t, v.Validate(&storagev1.MountDiskRequest{DiskId: validDiskID, ReadOnly: false}))
	require.NoError(t, v.Validate(&storagev1.MountDiskRequest{DiskId: validDiskID, ReadOnly: true}))
	assertValidationReason(t, v.Validate(&storagev1.MountDiskRequest{}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.MountDiskRequest{DiskId: "bad"}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.MountDiskRequest{DiskId: nilUUID}), validator.ReasonInvalidArgument)

	require.NoError(t, v.Validate(&storagev1.CreateDiskRequest{Name: "disk", MaxBytes: maxBytes}))
	require.NoError(t, v.Validate(&storagev1.GetDiskRequest{DiskId: validDiskID}))
	require.NoError(t, v.Validate(&storagev1.ListDisksRequest{PageSize: 1000, PageToken: "next"}))
	require.NoError(t, v.Validate(&storagev1.UpdateDiskRequest{DiskId: validDiskID, Name: &renamed}))
	require.NoError(t, v.Validate(&storagev1.UpdateDiskRequest{DiskId: validDiskID, MaxBytes: &maxBytes}))
	require.NoError(t, v.Validate(&storagev1.DeleteDiskRequest{DiskId: validDiskID}))

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
}

func TestStorageValidatorReadDataPlaneDiskIDContract(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	readCases := []struct {
		name  string
		build func(diskID string) proto.Message
	}{
		{
			name: "LookupRequest",
			build: func(diskID string) proto.Message {
				return &storagev1.LookupRequest{DiskId: diskID, ParentIno: 1, Name: []byte("f")}
			},
		},
		{
			name: "GetAttrRequest",
			build: func(diskID string) proto.Message {
				return &storagev1.GetAttrRequest{DiskId: diskID, Ino: 1}
			},
		},
		{
			name: "ReadlinkRequest",
			build: func(diskID string) proto.Message {
				return &storagev1.ReadlinkRequest{DiskId: diskID, Ino: 1}
			},
		},
		{
			name: "ReadRequest",
			build: func(diskID string) proto.Message {
				return &storagev1.ReadRequest{DiskId: diskID, Ino: 1, Size: 64}
			},
		},
		{
			name: "ReadDirRequest",
			build: func(diskID string) proto.Message {
				return &storagev1.ReadDirRequest{DiskId: diskID, Ino: 1, Limit: 16}
			},
		},
		{
			name: "ReadDirPlusRequest",
			build: func(diskID string) proto.Message {
				return &storagev1.ReadDirPlusRequest{DiskId: diskID, Ino: 1, Limit: 16}
			},
		},
		{
			name: "GetXattrRequest",
			build: func(diskID string) proto.Message {
				return &storagev1.GetXattrRequest{DiskId: diskID, Ino: 1, Name: []byte("user.k")}
			},
		},
		{
			name: "ListXattrRequest",
			build: func(diskID string) proto.Message {
				return &storagev1.ListXattrRequest{DiskId: diskID, Ino: 1}
			},
		},
		{
			name: "StatFSRequest",
			build: func(diskID string) proto.Message {
				return &storagev1.StatFSRequest{DiskId: diskID, Ino: 1}
			},
		},
	}

	require.Len(t, readCases, 9)
	for _, tc := range readCases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, v.Validate(tc.build(validDiskID)))
			assertValidationReason(t, v.Validate(tc.build("")), validator.ReasonInvalidArgument)
			assertValidationReason(t, v.Validate(tc.build("not-a-uuid")), validator.ReasonInvalidArgument)
			assertValidationReason(t, v.Validate(tc.build(nilUUID)), validator.ReasonInvalidArgument)
		})
	}
}

func TestStorageValidatorWriteDataPlaneDiskIDAndEpochContract(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	overMax := uint64(math.MaxInt64) + 1
	mode := uint32(0o644)

	writeCases := []struct {
		name  string
		build func(diskID string, epoch uint64) proto.Message
	}{
		{
			name: "SetAttrRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.SetAttrRequest{DiskId: diskID, Epoch: epoch, Ino: 1, Mode: &mode}
			},
		},
		{
			name: "MknodRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.MknodRequest{DiskId: diskID, Epoch: epoch, ParentIno: 1, Name: []byte("node"), Mode: 0o644}
			},
		},
		{
			name: "MkdirRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.MkdirRequest{DiskId: diskID, Epoch: epoch, ParentIno: 1, Name: []byte("dir"), Mode: 0o755}
			},
		},
		{
			name: "SymlinkRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.SymlinkRequest{DiskId: diskID, Epoch: epoch, ParentIno: 1, Name: []byte("sym"), Target: []byte("tgt")}
			},
		},
		{
			name: "LinkRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.LinkRequest{DiskId: diskID, Epoch: epoch, Ino: 2, NewParentIno: 1, NewName: []byte("hard")}
			},
		},
		{
			name: "UnlinkRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.UnlinkRequest{DiskId: diskID, Epoch: epoch, ParentIno: 1, Name: []byte("f")}
			},
		},
		{
			name: "RmdirRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.RmdirRequest{DiskId: diskID, Epoch: epoch, ParentIno: 1, Name: []byte("d")}
			},
		},
		{
			name: "RenameRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.RenameRequest{DiskId: diskID, Epoch: epoch, OldParentIno: 1, OldName: []byte("a"), NewParentIno: 1, NewName: []byte("b")}
			},
		},
		{
			name: "CreateRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.CreateRequest{DiskId: diskID, Epoch: epoch, ParentIno: 1, Name: []byte("f"), Mode: 0o644}
			},
		},
		{
			name: "WriteRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.WriteRequest{DiskId: diskID, Epoch: epoch, Ino: 1, Data: []byte("hello")}
			},
		},
		{
			name: "FsyncRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.FsyncRequest{DiskId: diskID, Epoch: epoch, Ino: 1}
			},
		},
		{
			name: "FsyncDirRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.FsyncDirRequest{DiskId: diskID, Epoch: epoch, Ino: 1}
			},
		},
		{
			name: "FallocateRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.FallocateRequest{DiskId: diskID, Epoch: epoch, Ino: 1, Length: 4096}
			},
		},
		{
			name: "SetXattrRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.SetXattrRequest{DiskId: diskID, Epoch: epoch, Ino: 1, Name: []byte("user.k"), Value: []byte("v")}
			},
		},
		{
			name: "RemoveXattrRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.RemoveXattrRequest{DiskId: diskID, Epoch: epoch, Ino: 1, Name: []byte("user.k")}
			},
		},
		{
			name: "ReclaimOrphanRequest",
			build: func(diskID string, epoch uint64) proto.Message {
				return &storagev1.ReclaimOrphanRequest{DiskId: diskID, Epoch: epoch, Ino: 1}
			},
		},
	}

	require.Len(t, writeCases, 16)
	for _, tc := range writeCases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, v.Validate(tc.build(validDiskID, 1)))
			require.NoError(t, v.Validate(tc.build(validDiskID, math.MaxInt64)))
			assertValidationReason(t, v.Validate(tc.build("", 1)), validator.ReasonInvalidArgument)
			assertValidationReason(t, v.Validate(tc.build("not-a-uuid", 1)), validator.ReasonInvalidArgument)
			assertValidationReason(t, v.Validate(tc.build(nilUUID, 1)), validator.ReasonInvalidArgument)
			assertValidationReason(t, v.Validate(tc.build(validDiskID, 0)), validator.ReasonInvalidArgument)
			assertValidationReason(t, v.Validate(tc.build(validDiskID, overMax)), validator.ReasonInvalidArgument)
		})
	}

	assertValidationReason(t, v.Validate(&storagev1.ReclaimOrphanRequest{DiskId: validDiskID, Epoch: 1, Ino: 0}), validator.ReasonInvalidArgument)
	assertValidationReason(t, v.Validate(&storagev1.ReclaimOrphanRequest{DiskId: validDiskID, Epoch: 1, Ino: overMax}), validator.ReasonInvalidArgument)
}
