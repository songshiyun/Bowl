-- +goose Up
-- +goose StatementBegin
DROP FUNCTION IF EXISTS renew_control_plane_session(BIGINT, UUID, TEXT, BOOLEAN, UUID, BIGINT);
DROP TABLE IF EXISTS client_sessions;
DROP TABLE IF EXISTS commits;

ALTER TABLE disks
    DROP CONSTRAINT IF EXISTS chk_disks_main_branch,
    DROP CONSTRAINT IF EXISTS chk_disks_capacity,
    DROP CONSTRAINT IF EXISTS chk_disks_root_ino,
    DROP COLUMN IF EXISTS branches,
    DROP COLUMN IF EXISTS storage_dir,
    DROP COLUMN IF EXISTS max_bytes,
    DROP COLUMN IF EXISTS root_ino,
    ALTER COLUMN name TYPE TEXT;

CREATE INDEX IF NOT EXISTS idx_disks_created_at_id ON disks (created_at DESC, disk_id DESC);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_disks_created_at_id;

ALTER TABLE disks
    ALTER COLUMN name TYPE VARCHAR(255),
    ADD COLUMN root_ino BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN max_bytes BIGINT NOT NULL DEFAULT 1073741824,
    ADD COLUMN storage_dir TEXT NOT NULL DEFAULT '',
    ADD COLUMN branches JSONB NOT NULL DEFAULT jsonb_build_array(jsonb_build_object(
        'branch_id', gen_random_uuid(),
        'name', 'main',
        'parent_branch', NULL,
        'fork_commit_id', NULL,
        'head_commit_id', NULL,
        'state', 'NORMAL',
        'merge_state', NULL
    )),
    ADD CONSTRAINT chk_disks_root_ino CHECK (root_ino > 0),
    ADD CONSTRAINT chk_disks_capacity CHECK (max_bytes > 0),
    ADD CONSTRAINT chk_disks_main_branch CHECK (
        jsonb_typeof(branches) = 'array'
        AND branches @> '[{"name":"main","parent_branch":null}]'::jsonb
    );

ALTER TABLE disks ALTER COLUMN max_bytes DROP DEFAULT;

CREATE TABLE commits (
    commit_id UUID PRIMARY KEY,
    disk_id UUID NOT NULL REFERENCES disks(disk_id) ON DELETE CASCADE,
    branch_name TEXT NOT NULL,
    parent_commit_id UUID,
    merge_parent_commit_id UUID,
    message TEXT NOT NULL DEFAULT '',
    checkpoint_dir TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_commits_disk_commit UNIQUE (disk_id, commit_id),
    CONSTRAINT fk_commits_parent FOREIGN KEY (disk_id, parent_commit_id)
        REFERENCES commits(disk_id, commit_id),
    CONSTRAINT fk_commits_merge_parent FOREIGN KEY (disk_id, merge_parent_commit_id)
        REFERENCES commits(disk_id, commit_id),
    CONSTRAINT chk_commits_not_own_parent CHECK (
        commit_id IS DISTINCT FROM parent_commit_id
        AND commit_id IS DISTINCT FROM merge_parent_commit_id
    )
);
CREATE INDEX idx_commits_disk_branch_created ON commits (disk_id, branch_name, created_at, commit_id);

CREATE TABLE client_sessions (
    session_id BIGINT PRIMARY KEY,
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    disk_id UUID REFERENCES disks(disk_id) ON DELETE CASCADE,
    branch_name TEXT NOT NULL DEFAULT 'main',
    read_only BOOLEAN NOT NULL DEFAULT false,
    commit_id UUID,
    lease_managed BOOLEAN NOT NULL DEFAULT false,
    CONSTRAINT fk_client_sessions_commit FOREIGN KEY (disk_id, commit_id)
        REFERENCES commits(disk_id, commit_id),
    CONSTRAINT chk_client_sessions_control_plane CHECK (
        NOT lease_managed OR (
            disk_id IS NOT NULL AND branch_name <> ''
            AND ((read_only AND commit_id IS NOT NULL) OR (NOT read_only AND commit_id IS NULL))
        )
    )
);
CREATE INDEX idx_client_sessions_expires_at ON client_sessions (expires_at);
CREATE INDEX idx_client_sessions_disk_expires ON client_sessions (disk_id, expires_at);
CREATE UNIQUE INDEX uq_client_sessions_managed_writer
    ON client_sessions (disk_id, branch_name)
    WHERE lease_managed AND NOT read_only;
CREATE INDEX idx_client_sessions_commit_readers
    ON client_sessions (disk_id, commit_id, expires_at)
    WHERE lease_managed AND read_only;

CREATE FUNCTION renew_control_plane_session(
    p_session_id BIGINT, p_disk_id UUID, p_branch_name TEXT,
    p_read_only BOOLEAN, p_commit_id UUID, p_lease_micros BIGINT
) RETURNS SETOF client_sessions AS $$
BEGIN
    IF p_lease_micros <= 0 THEN
        RETURN;
    END IF;

    PERFORM pg_advisory_xact_lock(hashtextextended('bowl:session:' || p_session_id::text, 0));
    IF NOT p_read_only THEN
        PERFORM pg_advisory_xact_lock(hashtextextended('bowl:writer:' || p_disk_id::text || ':' || p_branch_name, 0));
    END IF;

    DELETE FROM client_sessions
    WHERE lease_managed AND expires_at <= clock_timestamp()
      AND disk_id = p_disk_id AND branch_name = p_branch_name
      AND (
          (session_id = p_session_id AND read_only = p_read_only
           AND commit_id IS NOT DISTINCT FROM p_commit_id)
          OR (NOT read_only AND NOT p_read_only)
      );

    RETURN QUERY
    INSERT INTO client_sessions AS s
        (session_id, expires_at, updated_at, created_at, disk_id, branch_name, read_only, commit_id, lease_managed)
    VALUES
        (p_session_id, clock_timestamp() + p_lease_micros * interval '1 microsecond',
         clock_timestamp(), clock_timestamp(), p_disk_id, p_branch_name, p_read_only, p_commit_id, true)
    ON CONFLICT (session_id) DO UPDATE
    SET expires_at = clock_timestamp() + p_lease_micros * interval '1 microsecond',
        updated_at = clock_timestamp()
    WHERE s.lease_managed AND s.disk_id = EXCLUDED.disk_id
      AND s.branch_name = EXCLUDED.branch_name AND s.read_only = EXCLUDED.read_only
      AND s.commit_id IS NOT DISTINCT FROM EXCLUDED.commit_id
      AND s.expires_at > clock_timestamp()
    RETURNING s.*;
END;
$$ LANGUAGE plpgsql VOLATILE;
-- +goose StatementEnd
