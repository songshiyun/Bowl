-- name: NextSliceID :one
SELECT nextval('slice_id_seq')::bigint;

-- name: GetChunk :one
SELECT ino, indx, slices
FROM chunks
WHERE ino = @ino::bigint AND indx = @indx::bigint;

-- name: GetChunkForUpdate :one
SELECT ino, indx, slices
FROM chunks
WHERE ino = @ino::bigint AND indx = @indx::bigint
FOR UPDATE;

-- name: GetChunksInRange :many
SELECT ino, indx, slices
FROM chunks
WHERE ino = @ino::bigint AND indx >= @start_indx::bigint AND indx <= @end_indx::bigint
ORDER BY indx ASC;

-- name: AppendChunkSlice :one
INSERT INTO chunks (ino, indx, slices)
VALUES (@ino::bigint, @indx::bigint, @slices::bytea)
ON CONFLICT (ino, indx)
DO UPDATE SET slices = chunks.slices || EXCLUDED.slices
RETURNING octet_length(slices)::bigint;

-- name: AppendExistingChunkSlice :one
WITH updated AS (
    UPDATE chunks
    SET slices = slices || @slices::bytea
    WHERE ino = @ino::bigint AND indx = @indx::bigint
    RETURNING octet_length(slices)::bigint AS octet_len
)
SELECT COALESCE((SELECT octet_len FROM updated), 0)::bigint AS octet_len;

-- name: UpdateChunkSlices :exec
UPDATE chunks
SET slices = @slices::bytea
WHERE ino = @ino::bigint AND indx = @indx::bigint;

-- name: DeleteChunk :many
DELETE FROM chunks
WHERE ino = @ino::bigint AND indx = @indx::bigint
RETURNING slices;

-- name: DeleteChunksByIno :many
DELETE FROM chunks
WHERE ino = @ino::bigint
RETURNING slices;

-- name: DeleteChunksByInos :many
DELETE FROM chunks
WHERE ino = ANY(@inos::bigint[])
RETURNING slices;

-- name: DeleteChunksFromIndx :many
DELETE FROM chunks
WHERE ino = @ino::bigint AND indx >= @from_indx::bigint
RETURNING slices;

-- name: DeleteChunksAfterIndx :many
DELETE FROM chunks
WHERE ino = @ino::bigint AND indx > @after_indx::bigint
RETURNING slices;

-- name: InsertSliceRef :exec
INSERT INTO slice_refs (slice_id, size, refs)
VALUES (@slice_id::bigint, @size::integer, @refs::integer);

-- name: AddSliceRefCount :exec
UPDATE slice_refs
SET refs = refs + @delta::integer
WHERE slice_id = @slice_id::bigint;

-- name: GetSliceRef :one
SELECT slice_id, size, refs, created_at
FROM slice_refs
WHERE slice_id = @slice_id::bigint;

-- name: ListGarbageSliceRefs :many
SELECT slice_id, size, refs, created_at
FROM slice_refs
WHERE refs <= 0
ORDER BY slice_id ASC
LIMIT @limit_count::integer
FOR UPDATE SKIP LOCKED;

-- name: DeleteSliceRef :exec
DELETE FROM slice_refs
WHERE slice_id = @slice_id::bigint;

-- name: UpdateInodeSizeAndTouchForWrite :one
UPDATE inodes
SET
    size = $2,
    mode = $3,
    mtime = ts.now,
    ctime = ts.now
FROM (SELECT clock_timestamp() AS now) AS ts
WHERE ino = $1
RETURNING ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime, symlink_target, flags, parent_ino, root_ino;

-- name: GetInodeSizeAndChunksInRange :many
SELECT
    i.ino,
    i.generation,
    i.mode,
    i.size,
    i.nlink,
    i.uid,
    i.gid,
    i.rdev,
    i.atime,
    i.mtime,
    i.ctime,
    i.symlink_target,
    i.flags,
    i.parent_ino,
    i.root_ino,
    c.indx AS chunk_indx,
    c.slices AS chunk_slices
FROM inodes i
LEFT JOIN chunks c ON c.ino = i.ino AND c.indx >= @start_indx::bigint AND c.indx <= @end_indx::bigint
WHERE i.ino = @ino::bigint
ORDER BY c.indx ASC;

-- name: LockValidSliceRefsKeyShare :many
SELECT slice_id
FROM slice_refs
WHERE slice_id = ANY(@slice_ids::bigint[])
  AND refs > 0
ORDER BY slice_id ASC
FOR KEY SHARE;

-- name: WriteFileAtomic :one
WITH async_wal AS (
    SELECT set_config('synchronous_commit', 'off', true)
),
locked_inode AS (
    SELECT ino, generation, mode, size, nlink, uid, gid, rdev, atime, mtime, ctime, symlink_target, flags, parent_ino, root_ino
    FROM inodes
    WHERE inodes.ino = @ino::bigint
    FOR UPDATE
),
valid_handle AS (
    SELECT fh.fh, fh.flags
    FROM file_handles fh
    CROSS JOIN locked_inode li
    WHERE fh.fh = @fh::bigint
      AND fh.ino = li.ino
      AND (li.mode & 61440) = 32768
      AND (fh.flags & 3) IN (1, 2)
      AND (
          (sqlc.narg('session_id')::bigint IS NULL AND fh.session_id IS NULL)
          OR (
              sqlc.narg('session_id')::bigint IS NOT NULL
              AND fh.session_id = sqlc.narg('session_id')::bigint
              AND EXISTS (
                  SELECT 1
                  FROM client_sessions s
                  WHERE s.session_id = fh.session_id
                    AND s.expires_at > clock_timestamp()
                    AND fh.created_at >= s.created_at
                    AND (
                        s.disk_id IS NULL
                        OR li.root_ino <= 0
                        OR EXISTS (
                            SELECT 1
                            FROM disks d
                            WHERE d.root_ino = li.root_ino
                              AND d.disk_id = s.disk_id
                        )
                    )
              )
          )
      )
),
write_calc AS (
    SELECT
        li.ino,
        li.mode AS old_mode,
        li.size AS old_size,
        li.root_ino,
        CASE WHEN (vh.flags & 1024) <> 0 THEN li.size ELSE sqlc.arg('req_offset')::bigint END AS actual_offset,
        ((vh.flags & 1024) <> 0 AND li.size > 9223372036854775807::bigint - sqlc.arg('data_len')::bigint) AS offset_overflow,
        CASE
            WHEN sqlc.arg('quota_avail_bytes')::bigint >= 0 THEN sqlc.arg('quota_avail_bytes')::bigint
            WHEN li.root_ino <= 0 THEN 9223372036854775807::bigint
            ELSE COALESCE((
                SELECT GREATEST(0::bigint, d.max_bytes - COALESCE((
                    SELECT SUM(i2.size)::bigint
                    FROM inodes i2
                    WHERE i2.root_ino = li.root_ino
                      AND (i2.mode & 61440) = 32768
                ), 0::bigint))
                FROM disks d
                WHERE d.root_ino = li.root_ino
            ), 9223372036854775807::bigint)
        END AS effective_avail_bytes
    FROM locked_inode li
    JOIN valid_handle vh ON true
),
write_plan AS (
    SELECT
        wc.ino,
        wc.old_mode,
        wc.old_size,
        wc.root_ino,
        wc.actual_offset,
        wc.offset_overflow,
        CASE
            WHEN wc.offset_overflow THEN wc.old_size
            ELSE GREATEST(wc.old_size, wc.actual_offset + sqlc.arg('data_len')::bigint)
        END AS new_size,
        CASE
            WHEN wc.offset_overflow THEN 0::bigint
            ELSE GREATEST(0::bigint, GREATEST(wc.old_size, wc.actual_offset + sqlc.arg('data_len')::bigint) - wc.old_size)
        END AS delta_bytes,
        wc.effective_avail_bytes,
        CASE
            WHEN wc.offset_overflow THEN false
            ELSE GREATEST(0::bigint, GREATEST(wc.old_size, wc.actual_offset + sqlc.arg('data_len')::bigint) - wc.old_size) > wc.effective_avail_bytes
        END AS quota_exceeded,
        CASE WHEN wc.offset_overflow THEN 0::bigint ELSE (wc.actual_offset / 4194304)::bigint END AS start_indx,
        CASE WHEN wc.offset_overflow THEN 0::bigint ELSE ((wc.actual_offset + sqlc.arg('data_len')::bigint - 1) / 4194304)::bigint END AS end_indx,
        CASE WHEN wc.offset_overflow THEN 0::int4 ELSE (wc.actual_offset % 4194304)::int4 END AS pos_1,
        CASE WHEN wc.offset_overflow THEN 0::int4 ELSE LEAST(sqlc.arg('data_len')::bigint, 4194304 - (wc.actual_offset % 4194304))::int4 END AS len_1,
        CASE WHEN wc.offset_overflow THEN 0::int4 ELSE GREATEST(0::bigint, sqlc.arg('data_len')::bigint - (4194304 - (wc.actual_offset % 4194304)))::int4 END AS len_2
    FROM write_calc wc
),
appended_chunk_1 AS (
    INSERT INTO chunks (ino, indx, slices)
    SELECT
        wp.ino,
        wp.start_indx,
        int4send(wp.pos_1) || int8send(sqlc.arg('slice_id')::bigint) || int4send(sqlc.arg('data_len')::int4) || int4send(0::int4) || int4send(wp.len_1)
    FROM write_plan wp
    WHERE NOT wp.offset_overflow AND NOT wp.quota_exceeded
    ON CONFLICT (ino, indx) DO UPDATE
    SET slices = chunks.slices || EXCLUDED.slices
    RETURNING octet_length(slices)::bigint AS octet_len_1
),
appended_chunk_2 AS (
    INSERT INTO chunks (ino, indx, slices)
    SELECT
        wp.ino,
        wp.end_indx,
        int4send(0::int4) || int8send(sqlc.arg('slice_id')::bigint) || int4send(sqlc.arg('data_len')::int4) || int4send(wp.len_1) || int4send(wp.len_2)
    FROM write_plan wp
    WHERE NOT wp.offset_overflow AND NOT wp.quota_exceeded AND wp.end_indx > wp.start_indx
    ON CONFLICT (ino, indx) DO UPDATE
    SET slices = chunks.slices || EXCLUDED.slices
    RETURNING octet_length(slices)::bigint AS octet_len_2
),
inserted_slice_ref AS (
    INSERT INTO slice_refs (slice_id, size, refs, created_at)
    SELECT
        sqlc.arg('slice_id')::bigint,
        sqlc.arg('data_len')::int4,
        CASE WHEN wp.end_indx > wp.start_indx THEN 2 ELSE 1 END,
        clock_timestamp()
    FROM write_plan wp
    WHERE NOT wp.offset_overflow AND NOT wp.quota_exceeded
    RETURNING slice_id
),
updated_inode AS (
    UPDATE inodes i
    SET size  = wp.new_size,
        mode  = wp.old_mode & ~3072,
        mtime = clock_timestamp(),
        ctime = clock_timestamp()
    FROM write_plan wp
    WHERE i.ino = wp.ino
      AND NOT wp.offset_overflow
      AND NOT wp.quota_exceeded
    RETURNING i.ino, i.generation, i.mode, i.size, i.nlink, i.uid, i.gid, i.rdev, i.atime, i.mtime, i.ctime, i.symlink_target, i.flags, i.parent_ino, i.root_ino
)
SELECT
    CASE
        WHEN li.ino IS NULL THEN 'not_found'
        WHEN (li.mode & 61440) = 16384 THEN 'is_dir'
        WHEN (li.mode & 61440) <> 32768 THEN 'invalid_arg'
        WHEN vh.fh IS NULL THEN 'bad_handle'
        WHEN wp.offset_overflow THEN 'invalid_arg'
        WHEN wp.quota_exceeded THEN 'quota_exceeded'
        ELSE 'ok'
    END::text AS status,
    COALESCE(wp.quota_exceeded, false)::boolean AS quota_exceeded,
    COALESCE(li.size, 0)::bigint AS old_size,
    COALESCE(ui.size, COALESCE(li.size, 0))::bigint AS new_size,
    COALESCE(wp.start_indx, 0)::bigint AS start_indx,
    COALESCE(wp.end_indx, 0)::bigint AS end_indx,
    COALESCE(ac1.octet_len_1, 0)::bigint AS octet_len_1,
    COALESCE(ac2.octet_len_2, 0)::bigint AS octet_len_2,
    COALESCE(ui.ino, COALESCE(li.ino, 0))::bigint AS ino,
    COALESCE(ui.generation, COALESCE(li.generation, 0))::bigint AS generation,
    COALESCE(ui.mode, COALESCE(li.mode, 0))::int4 AS mode,
    COALESCE(ui.nlink, COALESCE(li.nlink, 0))::int4 AS nlink,
    COALESCE(ui.uid, COALESCE(li.uid, 0))::int4 AS uid,
    COALESCE(ui.gid, COALESCE(li.gid, 0))::int4 AS gid,
    COALESCE(ui.rdev, COALESCE(li.rdev, 0))::bigint AS rdev,
    COALESCE(ui.atime, li.atime)::timestamptz AS atime,
    COALESCE(ui.mtime, li.mtime)::timestamptz AS mtime,
    COALESCE(ui.ctime, li.ctime)::timestamptz AS ctime,
    ui.symlink_target AS symlink_target,
    COALESCE(ui.flags, COALESCE(li.flags, 0))::int4 AS flags,
    COALESCE(ui.parent_ino, COALESCE(li.parent_ino, 0))::bigint AS parent_ino,
    COALESCE(ui.root_ino, COALESCE(li.root_ino, 0))::bigint AS root_ino
FROM async_wal
LEFT JOIN locked_inode li ON true
LEFT JOIN valid_handle vh ON true
LEFT JOIN write_plan wp ON true
LEFT JOIN appended_chunk_1 ac1 ON true
LEFT JOIN appended_chunk_2 ac2 ON true
LEFT JOIN inserted_slice_ref isr ON true
LEFT JOIN updated_inode ui ON true;


