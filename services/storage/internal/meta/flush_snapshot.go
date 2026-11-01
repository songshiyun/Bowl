package meta

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

type flushSnapshot struct {
	mode            int32
	hasHandle       bool
	handleCreatedAt pgtype.Timestamptz
	hasPosixLocks   bool
}

// queryFlushSnapshot 在同一语句快照内读取节点、活跃句柄与持久化 POSIX 锁。
// 锁表不按会话过滤，确保进程重启后仍能发现需要事务清理的锁。
func queryFlushSnapshot(ctx context.Context, q db.DBTX, params FlushParams) (flushSnapshot, error) {
	const query = `-- name: flushSnapshot
		SELECT i.mode, h.fh IS NOT NULL, h.created_at,
			CASE WHEN $4::boolean THEN EXISTS (
				SELECT 1 FROM file_locks fl WHERE fl.ino = i.ino AND fl.lk_flags = 0
			) ELSE false END
		FROM inodes i
		LEFT JOIN (
			SELECT fh.fh, fh.ino, fh.created_at
			FROM file_handles fh
			LEFT JOIN client_sessions s ON fh.session_id = s.session_id
			WHERE fh.fh = $2
			  AND fh.session_id IS NOT DISTINCT FROM $3::bigint
			  AND (fh.session_id IS NULL OR (s.expires_at > clock_timestamp() AND fh.created_at >= s.created_at))
		) h ON h.ino = i.ino
		WHERE i.ino = $1
	`
	var snapshot flushSnapshot
	err := q.QueryRow(ctx, query, int64(params.Ino), int64(params.Fh), toPgSessionID(params.SessionID), params.LockOwner != nil).
		Scan(&snapshot.mode, &snapshot.hasHandle, &snapshot.handleCreatedAt, &snapshot.hasPosixLocks)
	if errors.Is(err, pgx.ErrNoRows) {
		return flushSnapshot{}, ErrNotFound
	}
	if err != nil {
		return flushSnapshot{}, fmt.Errorf("flush snapshot for inode %d: %w", params.Ino, err)
	}
	return snapshot, nil
}
