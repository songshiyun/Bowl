-- +goose Up
-- +goose StatementBegin
CREATE SEQUENCE IF NOT EXISTS inode_id_seq START WITH 2;
-- +goose StatementEnd

CREATE TABLE IF NOT EXISTS inodes (
    ino BIGINT PRIMARY KEY DEFAULT nextval('inode_id_seq'),
    generation BIGINT NOT NULL DEFAULT 0,
    mode INTEGER NOT NULL,
    size BIGINT NOT NULL DEFAULT 0,
    nlink INTEGER NOT NULL DEFAULT 1,
    uid INTEGER NOT NULL DEFAULT 0,
    gid INTEGER NOT NULL DEFAULT 0,
    rdev BIGINT NOT NULL DEFAULT 0,
    atime TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    mtime TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ctime TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS entries (
    parent_ino BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    ino BIGINT NOT NULL,
    CONSTRAINT pk_entries PRIMARY KEY (parent_ino, name),
    CONSTRAINT fk_entries_parent_ino FOREIGN KEY (parent_ino) REFERENCES inodes(ino) ON DELETE CASCADE,
    CONSTRAINT fk_entries_ino FOREIGN KEY (ino) REFERENCES inodes(ino) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_entries_ino ON entries(ino);

-- +goose Down
DROP INDEX IF EXISTS idx_entries_ino;
DROP TABLE IF EXISTS entries;
DROP TABLE IF EXISTS inodes;
DROP SEQUENCE IF EXISTS inode_id_seq;
