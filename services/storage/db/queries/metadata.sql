-- name: CreateDisk :one
INSERT INTO disks (disk_id, name, created_at, updated_at)
VALUES ($1, $2, clock_timestamp(), clock_timestamp())
RETURNING disk_id, name, created_at, updated_at;

-- name: GetDisk :one
SELECT disk_id, name, created_at, updated_at
FROM disks WHERE disk_id = $1;

-- name: ListDisks :many
SELECT disk_id, name, created_at, updated_at
FROM disks
WHERE (NOT @has_cursor::boolean)
   OR (created_at, disk_id) < (@cursor_created_at::timestamptz, @cursor_disk_id::uuid)
ORDER BY created_at DESC, disk_id DESC
LIMIT @page_limit::int;

-- name: GetDiskForUpdate :one
SELECT disk_id, name, created_at, updated_at
FROM disks WHERE disk_id = $1 FOR UPDATE;

-- name: UpdateDisk :one
UPDATE disks SET name = $2, updated_at = clock_timestamp()
WHERE disk_id = $1
RETURNING disk_id, name, created_at, updated_at;

-- name: DeleteDisk :execrows
DELETE FROM disks WHERE disk_id = $1;
