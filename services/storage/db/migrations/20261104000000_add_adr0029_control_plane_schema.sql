-- +goose Up
-- +goose StatementBegin
ALTER TABLE disks
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
    ADD CONSTRAINT chk_disks_main_branch CHECK (
        jsonb_typeof(branches) = 'array'
        AND branches @> '[{"name":"main","parent_branch":null}]'::jsonb
    );

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

ALTER TABLE client_sessions
    ADD COLUMN branch_name TEXT NOT NULL DEFAULT 'main',
    ADD COLUMN read_only BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN commit_id UUID,
    ADD COLUMN lease_managed BOOLEAN NOT NULL DEFAULT false,
    ADD CONSTRAINT fk_client_sessions_commit FOREIGN KEY (disk_id, commit_id)
        REFERENCES commits(disk_id, commit_id),
    ADD CONSTRAINT chk_client_sessions_control_plane CHECK (
        NOT lease_managed OR (
            disk_id IS NOT NULL AND branch_name <> ''
            AND ((read_only AND commit_id IS NOT NULL) OR (NOT read_only AND commit_id IS NULL))
        )
    );

-- 扩展期保留旧会话语义，写端唯一约束只覆盖新控制面会话。
-- 启用分支写端独占前，须停用旧接口并清理存量旧会话。
CREATE UNIQUE INDEX uq_client_sessions_managed_writer
    ON client_sessions (disk_id, branch_name)
    WHERE lease_managed AND NOT read_only;
CREATE INDEX idx_client_sessions_commit_readers
    ON client_sessions (disk_id, commit_id, expires_at)
    WHERE lease_managed AND read_only;

-- 先取得会话和分支锁，再以新的语句快照清理过期租约并读取数据库时钟。
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

-- +goose Down
-- +goose StatementBegin
DROP FUNCTION renew_control_plane_session(BIGINT, UUID, TEXT, BOOLEAN, UUID, BIGINT);
DROP INDEX idx_client_sessions_commit_readers;
DROP INDEX uq_client_sessions_managed_writer;
ALTER TABLE client_sessions DROP CONSTRAINT fk_client_sessions_commit,
    DROP CONSTRAINT chk_client_sessions_control_plane,
    DROP COLUMN lease_managed, DROP COLUMN commit_id, DROP COLUMN read_only, DROP COLUMN branch_name;
DROP TABLE commits;
ALTER TABLE disks DROP COLUMN branches, DROP COLUMN storage_dir;
-- +goose StatementEnd
