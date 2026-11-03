package meta

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble/v2"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

type commitGraph map[pgtype.UUID]db.Commit

func loadMergeGraph(ctx context.Context, q db.Querier, disk, source, target pgtype.UUID) (commitGraph, map[pgtype.UUID]bool, map[pgtype.UUID]bool, error) {
	graph := make(commitGraph)
	sets := [2]map[pgtype.UUID]bool{make(map[pgtype.UUID]bool), make(map[pgtype.UUID]bool)}
	for i, head := range []pgtype.UUID{source, target} {
		rows, err := q.ListCommitAncestors(ctx, db.ListCommitAncestorsParams{DiskID: disk, CommitID: head})
		if err != nil {
			return nil, nil, nil, err
		}
		for _, row := range rows {
			graph[row.CommitID] = row
			sets[i][row.CommitID] = true
		}
		if !sets[i][head] {
			return nil, nil, nil, ErrNotFound
		}
	}
	return graph, sets[0], sets[1], nil
}

func (g commitGraph) distances(head pgtype.UUID) map[pgtype.UUID]int {
	distance := map[pgtype.UUID]int{head: 0}
	queue := []pgtype.UUID{head}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		commit := g[id]
		for _, parent := range []pgtype.UUID{commit.ParentCommitID, commit.MergeParentCommitID} {
			if !parent.Valid {
				continue
			}
			if _, ok := distance[parent]; !ok {
				distance[parent] = distance[id] + 1
				queue = append(queue, parent)
			}
		}
	}
	return distance
}

// lowestMergeBase 排除所有有其他共同后代的共同祖先；多个候选时确定性选取最近者。
func (g commitGraph) lowestMergeBase(source, target pgtype.UUID) (pgtype.UUID, error) {
	sourceDistance, targetDistance := g.distances(source), g.distances(target)
	common := make(map[pgtype.UUID]bool)
	for id := range sourceDistance {
		if _, ok := targetDistance[id]; ok {
			common[id] = true
		}
	}
	dominated := make(map[pgtype.UUID]bool)
	for id := range common {
		commit := g[id]
		for _, parent := range []pgtype.UUID{commit.ParentCommitID, commit.MergeParentCommitID} {
			if parent.Valid && common[parent] {
				dominated[parent] = true
			}
		}
	}
	var candidates []pgtype.UUID
	for id := range common {
		if !dominated[id] {
			candidates = append(candidates, id)
		}
	}
	if len(candidates) == 0 {
		return pgtype.UUID{}, fmt.Errorf("合并分支没有共同祖先: %w", ErrInvalidArgument)
	}
	sort.Slice(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		da, db := sourceDistance[a]+targetDistance[a], sourceDistance[b]+targetDistance[b]
		if da != db {
			return da < db
		}
		return a.String() < b.String()
	})
	return candidates[0], nil
}

func (m *manager) mergeDirtyInodes(ctx context.Context, disk string, graph commitGraph, base pgtype.UUID, sides ...map[pgtype.UUID]bool) (map[uint64]bool, error) {
	baseAncestors := graph.distances(base)
	commits := make(map[pgtype.UUID]bool)
	for _, side := range sides {
		for id := range side {
			if _, included := baseAncestors[id]; !included {
				commits[id] = true
			}
		}
	}
	dirty := make(map[uint64]bool)
	for id := range commits {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		commit, err := m.pebble.OpenCommit(disk, id.String())
		if err != nil {
			return nil, err
		}
		lower, upper := pebblekv.DirtyRange()
		iter, err := commit.DB().NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		if err == nil {
			for iter.First(); iter.Valid(); iter.Next() {
				ino, decodeErr := pebblekv.DecodeDirtyKey(iter.Key())
				if decodeErr != nil {
					err = decodeErr
					break
				}
				dirty[ino] = true
			}
			err = errors.Join(err, iter.Error(), iter.Close())
		}
		err = errors.Join(err, commit.Close())
		if err != nil {
			return nil, err
		}
	}
	return dirty, nil
}
