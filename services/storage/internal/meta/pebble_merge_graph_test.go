package meta

import (
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestMergeBaseCommitDAG(t *testing.T) {
	id := func() pgtype.UUID { return pgtype.UUID{Bytes: uuid.New(), Valid: true} }
	root, fork, c, mergedC, d, mergedD := id(), id(), id(), id(), id(), id()
	graph := commitGraph{
		root:    {CommitID: root},
		fork:    {CommitID: fork, ParentCommitID: root},
		c:       {CommitID: c, ParentCommitID: fork},
		mergedC: {CommitID: mergedC, ParentCommitID: fork, MergeParentCommitID: c},
		d:       {CommitID: d, ParentCommitID: fork},
		mergedD: {CommitID: mergedD, ParentCommitID: mergedC, MergeParentCommitID: d},
	}
	for _, tc := range []struct{ source, target, base pgtype.UUID }{
		{c, fork, fork},
		{d, mergedC, fork},
		{mergedD, fork, fork},
		{d, mergedD, d},
		{mergedC, mergedD, mergedC},
	} {
		actual, err := graph.lowestMergeBase(tc.source, tc.target)
		require.NoError(t, err)
		require.Equal(t, tc.base, actual)
	}
	// 即使旧祖先经额外的合并边更近，也不能代替其共同后代。
	old, newer, left1, left2, right1, right2, left, right := id(), id(), id(), id(), id(), id(), id(), id()
	graph = commitGraph{
		old: {CommitID: old}, newer: {CommitID: newer, ParentCommitID: old},
		left1: {CommitID: left1, ParentCommitID: newer}, left2: {CommitID: left2, ParentCommitID: left1},
		right1: {CommitID: right1, ParentCommitID: newer}, right2: {CommitID: right2, ParentCommitID: right1},
		left:  {CommitID: left, ParentCommitID: left2, MergeParentCommitID: old},
		right: {CommitID: right, ParentCommitID: right2, MergeParentCommitID: old},
	}
	actual, err := graph.lowestMergeBase(left, right)
	require.NoError(t, err)
	require.Equal(t, newer, actual)
}
