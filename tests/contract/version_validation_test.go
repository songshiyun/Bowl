package contract_test

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/validator"
)

const (
	versionDiskID   = "11111111-1111-7111-8111-111111111111"
	versionCommitID = "22222222-2222-7222-8222-222222222222"
	versionBranchID = "33333333-3333-7333-8333-333333333333"
)

func TestVersionRequestValidation(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)
	cases := []struct {
		name          string
		message       proto.Message
		branches, ids []protoreflect.Name
	}{
		{"创建分支", &storagev1.CreateBranchRequest{DiskId: versionDiskID, Name: "dev"}, []protoreflect.Name{"name", "source_branch"}, []protoreflect.Name{"disk_id", "source_commit_id"}},
		{"列出分支", &storagev1.ListBranchesRequest{DiskId: versionDiskID}, nil, []protoreflect.Name{"disk_id"}},
		{"删除分支", &storagev1.DeleteBranchRequest{DiskId: versionDiskID, Name: "dev"}, []protoreflect.Name{"name"}, []protoreflect.Name{"disk_id"}},
		{"提交", &storagev1.CommitRequest{DiskId: versionDiskID, BranchName: "main"}, []protoreflect.Name{"branch_name"}, []protoreflect.Name{"disk_id"}},
		{"回档", &storagev1.RollbackRequest{DiskId: versionDiskID, BranchName: "main", CommitId: versionCommitID}, []protoreflect.Name{"branch_name"}, []protoreflect.Name{"disk_id", "commit_id"}},
		{"合并", &storagev1.MergeRequest{DiskId: versionDiskID, SourceBranch: "dev", TargetBranch: "main"}, []protoreflect.Name{"source_branch", "target_branch"}, []protoreflect.Name{"disk_id"}},
		{"完成合并", &storagev1.ResolveMergeRequest{DiskId: versionDiskID, BranchName: "main"}, []protoreflect.Name{"branch_name"}, []protoreflect.Name{"disk_id"}},
		{"放弃合并", &storagev1.AbortMergeRequest{DiskId: versionDiskID, BranchName: "main"}, []protoreflect.Name{"branch_name"}, []protoreflect.Name{"disk_id"}},
		{"差异", &storagev1.DiffRequest{DiskId: versionDiskID, FromRef: "main", ToRef: versionCommitID}, []protoreflect.Name{"from_ref", "to_ref"}, []protoreflect.Name{"disk_id"}},
		{"回收孤儿", &storagev1.ReclaimOrphanRequest{DiskId: versionDiskID, SessionId: 1, Ino: 1}, []protoreflect.Name{"branch_name"}, []protoreflect.Name{"disk_id"}},
		{"会话", &storagev1.RenewSessionRequest{SessionId: 1, DiskId: proto.String(versionDiskID), ReadOnly: true}, []protoreflect.Name{"branch_name"}, []protoreflect.Name{"disk_id", "commit_id"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, v.Validate(tc.message))
			for _, field := range tc.branches {
				for _, value := range []string{"", ".", "..", "a/b", "a\x00b", "a\nb", strings.Repeat("a", 256)} {
					bad := proto.Clone(tc.message).ProtoReflect()
					bad.Set(bad.Descriptor().Fields().ByName(field), protoreflect.ValueOfString(value))
					assertErrorInfoReason(t, v.Validate(bad.Interface()), validator.ReasonInvalidArgument)
				}
				for _, value := range []string{"main", "中文", strings.Repeat("a", 255)} {
					good := proto.Clone(tc.message).ProtoReflect()
					good.Set(good.Descriptor().Fields().ByName(field), protoreflect.ValueOfString(value))
					require.NoError(t, v.Validate(good.Interface()), "%s: %q", field, value)
				}
			}
			for _, field := range tc.ids {
				for _, value := range []string{"", "invalid", "00000000-0000-0000-0000-000000000000"} {
					bad := proto.Clone(tc.message).ProtoReflect()
					bad.Set(bad.Descriptor().Fields().ByName(field), protoreflect.ValueOfString(value))
					assertErrorInfoReason(t, v.Validate(bad.Interface()), validator.ReasonInvalidArgument)
				}
				good := proto.Clone(tc.message).ProtoReflect()
				good.Set(good.Descriptor().Fields().ByName(field), protoreflect.ValueOfString(versionCommitID))
				require.NoError(t, v.Validate(good.Interface()), "%s", field)
			}
		})
	}
}

func TestReclaimOrphanBoundsAndMergeStrategy(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)
	for _, field := range []protoreflect.Name{"ino", "session_id"} {
		for _, value := range []uint64{1, math.MaxInt64} {
			msg := (&storagev1.ReclaimOrphanRequest{SessionId: 1, DiskId: versionDiskID, Ino: 1}).ProtoReflect()
			msg.Set(msg.Descriptor().Fields().ByName(field), protoreflect.ValueOfUint64(value))
			require.NoError(t, v.Validate(msg.Interface()))
		}
		for _, value := range []uint64{0, math.MaxInt64 + 1, math.MaxUint64} {
			msg := (&storagev1.ReclaimOrphanRequest{SessionId: 1, DiskId: versionDiskID, Ino: 1}).ProtoReflect()
			msg.Set(msg.Descriptor().Fields().ByName(field), protoreflect.ValueOfUint64(value))
			assertErrorInfoReason(t, v.Validate(msg.Interface()), validator.ReasonInvalidArgument)
		}
	}
	for _, strategy := range []storagev1.ConflictStrategy{
		storagev1.ConflictStrategy_CONFLICT_STRATEGY_UNSPECIFIED,
		storagev1.ConflictStrategy_CONFLICT_STRATEGY_ABORT_ON_CONFLICT,
		storagev1.ConflictStrategy_CONFLICT_STRATEGY_OURS,
		storagev1.ConflictStrategy_CONFLICT_STRATEGY_THEIRS,
		storagev1.ConflictStrategy_CONFLICT_STRATEGY_MATERIALIZE,
	} {
		require.NoError(t, v.Validate(&storagev1.MergeRequest{DiskId: versionDiskID, SourceBranch: "dev", TargetBranch: "main", Strategy: strategy}))
	}
	assertErrorInfoReason(t, v.Validate(&storagev1.MergeRequest{DiskId: versionDiskID, SourceBranch: "dev", TargetBranch: "main", Strategy: storagev1.ConflictStrategy(99)}), validator.ReasonInvalidArgument)
}

func TestVersionEntitiesAndResponses(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)
	now := timestamppb.New(time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC))
	branch := &storagev1.Branch{BranchId: versionBranchID, DiskId: versionDiskID, Name: "dev", State: storagev1.BranchState_BRANCH_STATE_NORMAL, CreatedAt: now, ParentBranch: proto.String("main"), ForkCommitId: proto.String(versionCommitID), HeadCommitId: proto.String(versionCommitID)}
	commit := &storagev1.CommitInfo{CommitId: versionCommitID, DiskId: versionDiskID, BranchName: "main", CreatedAt: now, ParentCommitId: proto.String(versionCommitID), MergeParentCommitId: proto.String(versionCommitID)}
	disk := &storagev1.Disk{DiskId: versionDiskID, RootIno: 1, MaxBytes: 1000, UsedBytes: 42, UsedInodes: 2, CreatedAt: now, UpdatedAt: now, Branches: []*storagev1.Branch{branch}, DefaultBranch: proto.String("main")}
	encodedDisk, err := proto.Marshal(disk)
	require.NoError(t, err)
	decodedDisk := &storagev1.Disk{}
	require.NoError(t, proto.Unmarshal(encodedDisk, decodedDisk))
	require.True(t, proto.Equal(disk, decodedDisk))
	for _, msg := range []proto.Message{branch, commit, disk, &storagev1.CreateBranchResponse{Branch: branch}, &storagev1.ListBranchesResponse{Branches: []*storagev1.Branch{branch}}, &storagev1.CommitResponse{Commit: commit}, &storagev1.ResolveMergeResponse{Commit: commit}} {
		require.NoError(t, v.Validate(msg))
	}
	for _, field := range []protoreflect.Name{"name", "parent_branch"} {
		bad := proto.Clone(branch).ProtoReflect()
		bad.Set(bad.Descriptor().Fields().ByName(field), protoreflect.ValueOfString("../invalid"))
		assertErrorInfoReason(t, v.Validate(bad.Interface()), validator.ReasonInvalidArgument)
	}
	badDisk := proto.Clone(disk).(*storagev1.Disk)
	badDisk.DefaultBranch = proto.String("a/b")
	assertErrorInfoReason(t, v.Validate(badDisk), validator.ReasonInvalidArgument)
	badCommit := proto.Clone(commit).(*storagev1.CommitInfo)
	badCommit.BranchName = ""
	assertErrorInfoReason(t, v.Validate(badCommit), validator.ReasonInvalidArgument)
	for _, state := range []storagev1.BranchState{0, 99} {
		bad := proto.Clone(branch).(*storagev1.Branch)
		bad.State = state
		assertErrorInfoReason(t, v.Validate(bad), validator.ReasonInvalidArgument)
	}
	branch.State = storagev1.BranchState_BRANCH_STATE_MERGING
	require.NoError(t, v.Validate(branch))
	for _, field := range []protoreflect.Name{"branch_id", "disk_id", "fork_commit_id", "head_commit_id"} {
		bad := proto.Clone(branch).ProtoReflect()
		bad.Set(bad.Descriptor().Fields().ByName(field), protoreflect.ValueOfString("00000000-0000-0000-0000-000000000000"))
		assertErrorInfoReason(t, v.Validate(bad.Interface()), validator.ReasonInvalidArgument)
	}
	for _, field := range []protoreflect.Name{"commit_id", "disk_id", "parent_commit_id", "merge_parent_commit_id"} {
		bad := proto.Clone(commit).ProtoReflect()
		bad.Set(bad.Descriptor().Fields().ByName(field), protoreflect.ValueOfString("invalid"))
		assertErrorInfoReason(t, v.Validate(bad.Interface()), validator.ReasonInvalidArgument)
	}
	attr := &storagev1.InodeAttribute{Mode: 0100644, Atime: now, Mtime: now, Ctime: now}
	merge := &storagev1.MergeResponse{Result: storagev1.MergeResult_MERGE_RESULT_CONFLICTS, Conflicts: []*storagev1.MergeConflict{{Path: "/file", BaseAttr: attr, OursAttr: attr, TheirsAttr: attr, Reason: "内容冲突"}}}
	require.NoError(t, v.Validate(merge))
	for _, result := range []storagev1.MergeResult{storagev1.MergeResult_MERGE_RESULT_MERGED, storagev1.MergeResult_MERGE_RESULT_CONFLICTS, storagev1.MergeResult_MERGE_RESULT_MATERIALIZED} {
		merge.Result = result
		require.NoError(t, v.Validate(merge))
	}
	merge.MergeCommitId = proto.String("not-a-uuid")
	assertErrorInfoReason(t, v.Validate(merge), validator.ReasonInvalidArgument)
	merge.MergeCommitId = nil
	merge.Result = storagev1.MergeResult(99)
	assertErrorInfoReason(t, v.Validate(merge), validator.ReasonInvalidArgument)
	for _, change := range []storagev1.DiffChangeType{storagev1.DiffChangeType_DIFF_CHANGE_TYPE_ADDED, storagev1.DiffChangeType_DIFF_CHANGE_TYPE_MODIFIED, storagev1.DiffChangeType_DIFF_CHANGE_TYPE_DELETED, storagev1.DiffChangeType_DIFF_CHANGE_TYPE_RENAMED} {
		entry := &storagev1.DiffEntry{ChangeType: change, Path: "/file", OldPath: "/old", OldAttr: attr, NewAttr: attr, ModifiedBlockIndices: []uint64{0, 3}}
		require.NoError(t, v.Validate(&storagev1.DiffResponse{Entries: []*storagev1.DiffEntry{entry}}))
		bytes, err := proto.Marshal(entry)
		require.NoError(t, err)
		decoded := &storagev1.DiffEntry{}
		require.NoError(t, proto.Unmarshal(bytes, decoded))
		require.True(t, proto.Equal(entry, decoded))
	}
	assertErrorInfoReason(t, v.Validate(&storagev1.DiffEntry{ChangeType: 99, Path: "/file"}), validator.ReasonInvalidArgument)
	assertErrorInfoReason(t, v.Validate(&storagev1.DiffEntry{Path: "/file"}), validator.ReasonInvalidArgument)
}

func TestFilesystemSessionBounds(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)
	for _, msg := range []proto.Message{
		&storagev1.LookupRequest{}, &storagev1.GetAttrRequest{}, &storagev1.MknodRequest{},
		&storagev1.MkdirRequest{}, &storagev1.SymlinkRequest{}, &storagev1.ReadlinkRequest{},
		&storagev1.LinkRequest{}, &storagev1.UnlinkRequest{}, &storagev1.RmdirRequest{},
		&storagev1.RenameRequest{}, &storagev1.SetXattrRequest{}, &storagev1.GetXattrRequest{},
		&storagev1.ListXattrRequest{}, &storagev1.RemoveXattrRequest{}, &storagev1.StatFSRequest{},
	} {
		t.Run(string(msg.ProtoReflect().Descriptor().Name()), func(t *testing.T) {
			for _, tc := range []struct {
				value   uint64
				invalid bool
			}{{0, true}, {1, false}, {math.MaxInt64, false}, {math.MaxInt64 + 1, true}} {
				copy := proto.Clone(msg).ProtoReflect()
				field := copy.Descriptor().Fields().ByName("session_id")
				copy.Set(field, protoreflect.ValueOfUint64(tc.value))
				err := v.Validate(copy.Interface())
				var validationErr *protovalidate.ValidationError
				if err != nil {
					require.True(t, errors.As(err, &validationErr))
				}
				found := false
				if validationErr != nil {
					for _, violation := range validationErr.Violations {
						if violation.FieldDescriptor != nil && violation.FieldDescriptor.Name() == field.Name() {
							found = true
						}
					}
				}
				require.Equal(t, tc.invalid, found, "session_id=%d", tc.value)
			}
		})
	}
}
