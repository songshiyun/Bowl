-- +goose Up
-- +goose StatementBegin
CREATE TABLE disk_lineages (
    lineage_id UUID PRIMARY KEY,
    next_ino BIGINT NOT NULL DEFAULT 2 CHECK (next_ino >= 2),
    next_pack_id BIGINT NOT NULL DEFAULT 1 CHECK (next_pack_id >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE disk_commits (
    commit_id UUID PRIMARY KEY,
    lineage_id UUID NOT NULL REFERENCES disk_lineages(lineage_id) ON DELETE CASCADE,
    source_disk_id UUID NOT NULL,
    parent_commit_id UUID,
    merge_parent_commit_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_disk_commits_lineage_commit UNIQUE (lineage_id, commit_id),
    CONSTRAINT fk_disk_commits_parent FOREIGN KEY (lineage_id, parent_commit_id)
        REFERENCES disk_commits(lineage_id, commit_id),
    CONSTRAINT fk_disk_commits_merge_parent FOREIGN KEY (lineage_id, merge_parent_commit_id)
        REFERENCES disk_commits(lineage_id, commit_id),
    CONSTRAINT chk_disk_commits_not_self_parent CHECK (
        commit_id IS DISTINCT FROM parent_commit_id
        AND commit_id IS DISTINCT FROM merge_parent_commit_id
    )
);
CREATE INDEX idx_disk_commits_lineage_id ON disk_commits (lineage_id);

-- 兼容存量盘数据：为已有每块独立盘补齐专属血缘记录
INSERT INTO disk_lineages (lineage_id, created_at)
SELECT disk_id, created_at FROM disks
ON CONFLICT (lineage_id) DO NOTHING;

ALTER TABLE disks
    ADD COLUMN lineage_id UUID,
    ADD COLUMN parent_disk_id UUID,
    ADD COLUMN base_commit_id UUID;

UPDATE disks SET lineage_id = disk_id WHERE lineage_id IS NULL;

ALTER TABLE disks
    ALTER COLUMN lineage_id SET NOT NULL,
    ADD CONSTRAINT fk_disks_lineage FOREIGN KEY (lineage_id)
        REFERENCES disk_lineages(lineage_id),
    ADD CONSTRAINT fk_disks_base_commit FOREIGN KEY (lineage_id, base_commit_id)
        REFERENCES disk_commits(lineage_id, commit_id),
    ADD CONSTRAINT chk_disks_not_own_parent CHECK (disk_id IS DISTINCT FROM parent_disk_id);

CREATE INDEX idx_disks_lineage_id ON disks (lineage_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_disks_lineage_id;

ALTER TABLE disks
    DROP CONSTRAINT IF EXISTS chk_disks_not_own_parent,
    DROP CONSTRAINT IF EXISTS fk_disks_base_commit,
    DROP CONSTRAINT IF EXISTS fk_disks_lineage,
    DROP COLUMN IF EXISTS base_commit_id,
    DROP COLUMN IF EXISTS parent_disk_id,
    DROP COLUMN IF EXISTS lineage_id;

DROP INDEX IF EXISTS idx_disk_commits_lineage_id;
DROP TABLE IF EXISTS disk_commits;
DROP TABLE IF EXISTS disk_lineages;
-- +goose StatementEnd
