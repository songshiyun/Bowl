package contract_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/validator"
)

func TestCreateBranchContract(t *testing.T) {
	service := storagev1.File_storage_v1_storage_proto.Services().ByName("StorageService")
	method := service.Methods().ByName("CreateBranch")
	require.NotNil(t, method)
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)
	for _, tc := range []struct {
		name, diskID, branch string
		valid                bool
	}{
		{"合法分支", "11111111-1111-7111-8111-111111111111", "feature-一", true},
		{"磁盘标识非法", "invalid", "feature", false},
		{"磁盘标识为零", "00000000-0000-0000-0000-000000000000", "feature", false},
		{"分支名为空", "11111111-1111-7111-8111-111111111111", "", false},
		{"分支名含斜线", "11111111-1111-7111-8111-111111111111", "a/b", false},
		{"分支名含空字节", "11111111-1111-7111-8111-111111111111", "a\x00b", false},
		{"分支名为上级目录", "11111111-1111-7111-8111-111111111111", "..", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := dynamicpb.NewMessage(method.Input())
			msg.Set(method.Input().Fields().ByName("disk_id"), protoreflect.ValueOfString(tc.diskID))
			msg.Set(method.Input().Fields().ByName("name"), protoreflect.ValueOfString(tc.branch))
			if tc.valid {
				require.NoError(t, v.Validate(msg))
			} else {
				assertErrorInfoReason(t, v.Validate(msg), validator.ReasonInvalidArgument)
			}
		})
	}
}

func TestRenewSessionBranchContract(t *testing.T) {
	desc := (&storagev1.RenewSessionRequest{}).ProtoReflect().Descriptor()
	for _, name := range []protoreflect.Name{"disk_id", "branch_name", "read_only", "commit_id"} {
		require.NotNil(t, desc.Fields().ByName(name), string(name))
	}
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)
	const disk = "11111111-1111-7111-8111-111111111111"
	const commit = "22222222-2222-7222-8222-222222222222"
	for _, tc := range []struct {
		name                 string
		disk, branch, commit *string
		readOnly, valid      bool
	}{
		{name: "旧会话", valid: true},
		{name: "默认分支", disk: ptrString(disk), valid: true},
		{name: "只读快照", disk: ptrString(disk), branch: ptrString("main"), commit: ptrString(commit), readOnly: true, valid: true},
		{name: "分支名为空", disk: ptrString(disk), branch: ptrString(""), valid: false},
		{name: "分支名含斜线", disk: ptrString(disk), branch: ptrString("a/b"), valid: false},
		{name: "快照不是 UUID", disk: ptrString(disk), commit: ptrString("bad"), readOnly: true, valid: false},
		{name: "快照为零 UUID", disk: ptrString(disk), commit: ptrString("00000000-0000-0000-0000-000000000000"), readOnly: true, valid: false},
		{name: "写会话不能挂载快照", disk: ptrString(disk), commit: ptrString(commit), valid: false},
		{name: "分支挂载需要磁盘", branch: ptrString("main"), valid: false},
		{name: "只读会话需要磁盘", readOnly: true, valid: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := dynamicpb.NewMessage(desc)
			msg.Set(desc.Fields().ByName("session_id"), protoreflect.ValueOfUint64(1))
			for name, value := range map[protoreflect.Name]*string{"disk_id": tc.disk, "branch_name": tc.branch, "commit_id": tc.commit} {
				if value != nil {
					msg.Set(desc.Fields().ByName(name), protoreflect.ValueOfString(*value))
				}
			}
			msg.Set(desc.Fields().ByName("read_only"), protoreflect.ValueOfBool(tc.readOnly))
			if tc.valid {
				require.NoError(t, v.Validate(msg))
			} else {
				assertErrorInfoReason(t, v.Validate(msg), validator.ReasonInvalidArgument)
			}
		})
	}
}

func TestFilesystemRequestsCarrySession(t *testing.T) {
	for _, msg := range []proto.Message{
		&storagev1.LookupRequest{}, &storagev1.GetAttrRequest{}, &storagev1.SetAttrRequest{},
		&storagev1.MknodRequest{}, &storagev1.MkdirRequest{}, &storagev1.SymlinkRequest{},
		&storagev1.ReadlinkRequest{}, &storagev1.LinkRequest{}, &storagev1.UnlinkRequest{},
		&storagev1.RmdirRequest{}, &storagev1.RenameRequest{}, &storagev1.ReadRequest{},
		&storagev1.WriteRequest{}, &storagev1.FsyncRequest{}, &storagev1.FsyncDirRequest{},
		&storagev1.ReadDirRequest{}, &storagev1.ReadDirPlusRequest{}, &storagev1.FallocateRequest{},
		&storagev1.SetXattrRequest{}, &storagev1.GetXattrRequest{}, &storagev1.ListXattrRequest{},
		&storagev1.RemoveXattrRequest{}, &storagev1.StatFSRequest{},
	} {
		t.Run(string(msg.ProtoReflect().Descriptor().Name()), func(t *testing.T) {
			field := msg.ProtoReflect().Descriptor().Fields().ByName("session_id")
			require.NotNil(t, field)
			require.True(t, field.HasPresence())
		})
	}
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)
	msg := &storagev1.GetAttrRequest{Ino: 1}
	field := msg.ProtoReflect().Descriptor().Fields().ByName("session_id")
	require.NotNil(t, field)
	msg.ProtoReflect().Set(field, protoreflect.ValueOfUint64(1))
	require.NoError(t, v.Validate(msg))
	msg.ProtoReflect().Set(field, protoreflect.ValueOfUint64(0))
	assertErrorInfoReason(t, v.Validate(msg), validator.ReasonInvalidArgument)
}

func TestVersionManagementRPCContract(t *testing.T) {
	service := storagev1.File_storage_v1_storage_proto.Services().ByName("StorageService")
	for method, fields := range map[protoreflect.Name][]protoreflect.Name{
		"ReclaimOrphan": {"session_id", "disk_id", "branch_name", "ino"},
		"ListBranches":  {"disk_id"},
		"DeleteBranch":  {"disk_id", "name"},
		"Commit":        {"disk_id", "branch_name", "message"},
		"Rollback":      {"disk_id", "branch_name", "commit_id"},
		"Merge":         {"disk_id", "source_branch", "target_branch", "strategy"},
		"ResolveMerge":  {"disk_id", "branch_name", "message"},
		"AbortMerge":    {"disk_id", "branch_name"},
		"Diff":          {"disk_id", "from_ref", "to_ref"},
	} {
		t.Run(string(method), func(t *testing.T) {
			m := service.Methods().ByName(method)
			require.NotNil(t, m)
			for _, field := range fields {
				require.NotNil(t, m.Input().Fields().ByName(field), string(field))
			}
		})
	}
}
