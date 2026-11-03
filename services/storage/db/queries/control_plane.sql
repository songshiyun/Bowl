-- name: GetDiskControlPlane :one
SELECT disk_id, storage_dir, branches FROM disks WHERE disk_id = $1;

-- name: GetDiskControlPlaneForUpdate :one
SELECT disk_id, storage_dir, branches FROM disks WHERE disk_id = $1 FOR UPDATE;

-- name: SetDiskStorageDir :one
UPDATE disks SET storage_dir = $2, updated_at = clock_timestamp()
WHERE disk_id = $1
RETURNING disk_id, storage_dir, branches;

-- name: CompareAndSetDiskBranches :one
UPDATE disks SET branches = sqlc.arg(branches)::jsonb, updated_at = clock_timestamp()
WHERE disk_id = sqlc.arg(disk_id)::uuid
  AND branches = sqlc.arg(expected_branches)::jsonb
RETURNING branches;

-- name: CreateCommit :one
INSERT INTO commits (commit_id, disk_id, branch_name, parent_commit_id, merge_parent_commit_id, message, checkpoint_dir)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING *;

-- name: GetCommit :one
SELECT * FROM commits WHERE commit_id = $1;

-- name: ListCommitsByBranch :many
SELECT * FROM commits WHERE disk_id = $1 AND branch_name = $2 ORDER BY created_at, commit_id;

-- name: ListCommitAncestors :many
WITH RECURSIVE ancestors AS (
    SELECT root.commit_id, root.parent_commit_id, root.merge_parent_commit_id
    FROM commits root WHERE root.disk_id = sqlc.arg(disk_id) AND root.commit_id = sqlc.arg(commit_id)
    UNION
    SELECT c.commit_id, c.parent_commit_id, c.merge_parent_commit_id
    FROM commits c
    JOIN ancestors a ON c.commit_id IN (a.parent_commit_id, a.merge_parent_commit_id)
    WHERE c.disk_id = sqlc.arg(disk_id)
)
SELECT c.* FROM commits c
WHERE c.disk_id = sqlc.arg(disk_id) AND c.commit_id IN (SELECT a.commit_id FROM ancestors a)
ORDER BY c.created_at, c.commit_id;

-- 写端冲突返回 23505（uq_client_sessions_managed_writer）；租期单位为微秒。
-- 活跃会话不能更换绑定，错误绑定或非正租期返回零行。
-- name: RenewControlPlaneSession :one
SELECT * FROM renew_control_plane_session(
    sqlc.arg(session_id)::bigint, sqlc.arg(disk_id)::uuid, sqlc.arg(branch_name)::text,
    sqlc.arg(read_only)::boolean, sqlc.narg(commit_id)::uuid, sqlc.arg(lease_micros)::bigint
);

-- name: ListActiveCommitSessions :many
SELECT * FROM client_sessions
WHERE lease_managed AND read_only AND disk_id = $1 AND commit_id = $2
  AND expires_at > clock_timestamp()
ORDER BY session_id;

-- name: GetActiveBranchWriter :one
SELECT * FROM client_sessions
WHERE lease_managed AND NOT read_only AND disk_id = $1 AND branch_name = $2
  AND expires_at > clock_timestamp();
