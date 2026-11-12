-- name: CreateDiskLineage :one
INSERT INTO disk_lineages (lineage_id, next_ino, next_pack_id, created_at)
VALUES ($1, 2, 1, clock_timestamp())
RETURNING lineage_id, next_ino, next_pack_id, created_at;

-- name: LockDiskLineage :one
SELECT lineage_id, next_ino, next_pack_id, created_at
FROM disk_lineages
WHERE lineage_id = $1
FOR UPDATE;

-- name: ReserveInodeSegment :one
UPDATE disk_lineages
SET next_ino = GREATEST(next_ino, @min_next::bigint) + @count::bigint
WHERE lineage_id = @lineage_id::uuid
  AND @count::bigint > 0
RETURNING
    (next_ino - @count::bigint)::bigint AS first_ino,
    (next_ino - 1)::bigint AS last_ino;

-- name: ReservePackSegment :one
UPDATE disk_lineages
SET next_pack_id = GREATEST(next_pack_id, @min_next::bigint) + @count::bigint
WHERE lineage_id = @lineage_id::uuid
  AND @count::bigint > 0
RETURNING
    (next_pack_id - @count::bigint)::bigint AS first_pack_id,
    (next_pack_id - 1)::bigint AS last_pack_id;

-- name: CreateDiskCommit :one
INSERT INTO disk_commits (
    commit_id,
    lineage_id,
    source_disk_id,
    parent_commit_id,
    merge_parent_commit_id,
    created_at
)
VALUES ($1, $2, $3, $4, $5, clock_timestamp())
RETURNING commit_id, lineage_id, source_disk_id, parent_commit_id, merge_parent_commit_id, created_at;

-- name: CreateDisk :one
INSERT INTO disks (
    disk_id,
    name,
    lineage_id,
    parent_disk_id,
    base_commit_id,
    created_at,
    updated_at
)
VALUES ($1, $2, $3, $4, $5, clock_timestamp(), clock_timestamp())
RETURNING disk_id, name, created_at, updated_at, lineage_id, parent_disk_id, base_commit_id;

-- name: GetDisk :one
SELECT disk_id, name, created_at, updated_at, lineage_id, parent_disk_id, base_commit_id
FROM disks
WHERE disk_id = $1;

-- name: ListDisks :many
SELECT disk_id, name, created_at, updated_at, lineage_id, parent_disk_id, base_commit_id
FROM disks
WHERE (NOT @has_cursor::boolean)
   OR (created_at, disk_id) < (@cursor_created_at::timestamptz, @cursor_disk_id::uuid)
ORDER BY created_at DESC, disk_id DESC
LIMIT @page_limit::int;

-- name: GetDiskForUpdate :one
SELECT disk_id, name, created_at, updated_at, lineage_id, parent_disk_id, base_commit_id
FROM disks
WHERE disk_id = $1
FOR UPDATE;

-- name: UpdateDisk :one
UPDATE disks
SET name = $2, updated_at = clock_timestamp()
WHERE disk_id = $1
RETURNING disk_id, name, created_at, updated_at, lineage_id, parent_disk_id, base_commit_id;

-- name: SetDiskBaseCommit :exec
UPDATE disks
SET base_commit_id = $2
WHERE disk_id = $1;

-- name: ClearLineageBaseCommits :exec
UPDATE disks
SET base_commit_id = NULL
WHERE lineage_id = $1
  AND base_commit_id IS NOT NULL;

-- name: DeleteLineageCommits :many
DELETE FROM disk_commits
WHERE lineage_id = $1
RETURNING commit_id;

-- name: CountDisksByLineage :one
SELECT COUNT(*)
FROM disks
WHERE lineage_id = $1;

-- name: DeleteDiskLineage :exec
DELETE FROM disk_lineages
WHERE lineage_id = $1;

-- name: DeleteDisk :execrows
DELETE FROM disks
WHERE disk_id = $1;
