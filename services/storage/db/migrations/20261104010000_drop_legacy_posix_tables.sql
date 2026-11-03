-- +goose Up
-- +goose StatementBegin
-- 会话、句柄和锁只存在于客户端；旧会话不能再访问分支。
DELETE FROM client_sessions WHERE NOT lease_managed;
DROP TABLE IF EXISTS file_lock_waiters CASCADE;
DROP TABLE IF EXISTS file_locks CASCADE;
DROP TABLE IF EXISTS file_handles CASCADE;
DROP TABLE IF EXISTS slice_refs CASCADE;
DROP TABLE IF EXISTS chunks CASCADE;
DROP TABLE IF EXISTS inode_xattrs CASCADE;
DROP TABLE IF EXISTS entries CASCADE;
DROP TABLE IF EXISTS inodes CASCADE;
DROP TABLE IF EXISTS _rollback_entries_name_backup CASCADE;
DROP TABLE IF EXISTS _rollback_symlink_target_backup CASCADE;
DROP FUNCTION IF EXISTS alloc_ino(UUID) CASCADE;
DROP SEQUENCE IF EXISTS file_handle_id_seq CASCADE;
DROP SEQUENCE IF EXISTS file_locks_id_seq CASCADE;
DROP SEQUENCE IF EXISTS inode_id_seq CASCADE;
DROP SEQUENCE IF EXISTS slice_id_seq CASCADE;
-- 每个 Pebble 分支的根节点均为 1，不再要求跨磁盘唯一。
ALTER TABLE disks DROP CONSTRAINT IF EXISTS disks_root_ino_key;
UPDATE disks SET root_ino = 1;
ALTER TABLE disks ALTER COLUMN root_ino SET DEFAULT 1;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- 仅恢复旧表结构；被删除的 PostgreSQL 数据须从备份恢复。
CREATE TABLE public.chunks (
    ino bigint NOT NULL,
    indx bigint NOT NULL,
    slices bytea NOT NULL,
    CONSTRAINT chk_chunks_indx CHECK ((indx >= 0)),
    CONSTRAINT chk_chunks_ino CHECK ((ino > 0)),
    CONSTRAINT chk_chunks_slices_len CHECK (((octet_length(slices) > 0) AND ((octet_length(slices) % 24) = 0)))
);

CREATE TABLE public.entries (
    parent_ino bigint NOT NULL,
    name bytea NOT NULL,
    ino bigint NOT NULL,
    CONSTRAINT chk_entries_name CHECK ((((octet_length(name) >= 1) AND (octet_length(name) <= 255)) AND (POSITION(('\x00'::bytea) IN (name)) = 0) AND (POSITION(('\x2f'::bytea) IN (name)) = 0)))
);

CREATE SEQUENCE public.file_handle_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE public.file_handles (
    fh bigint DEFAULT nextval('public.file_handle_id_seq'::regclass) NOT NULL,
    ino bigint NOT NULL,
    flags integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    session_id bigint
);

CREATE TABLE public.file_lock_waiters (
    session_id bigint NOT NULL,
    request_id bigint NOT NULL,
    ino bigint NOT NULL,
    fh bigint NOT NULL,
    lock_owner bigint NOT NULL,
    lk_flags integer DEFAULT 0 NOT NULL,
    start_offset bigint NOT NULL,
    end_offset bigint NOT NULL,
    lock_type smallint NOT NULL,
    pid integer DEFAULT 0 NOT NULL,
    status smallint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT chk_file_lock_waiters_flags CHECK ((lk_flags = ANY (ARRAY[0, 1]))),
    CONSTRAINT chk_file_lock_waiters_range CHECK (((start_offset >= 0) AND (end_offset >= start_offset))),
    CONSTRAINT chk_file_lock_waiters_status CHECK ((status = ANY (ARRAY[1, 2, 3]))),
    CONSTRAINT chk_file_lock_waiters_type CHECK ((lock_type = ANY (ARRAY[0, 1])))
);

CREATE TABLE public.file_locks (
    id bigint NOT NULL,
    session_id bigint NOT NULL,
    ino bigint NOT NULL,
    fh bigint NOT NULL,
    lock_owner bigint NOT NULL,
    lk_flags integer DEFAULT 0 NOT NULL,
    start_offset bigint NOT NULL,
    end_offset bigint NOT NULL,
    lock_type smallint NOT NULL,
    pid integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT chk_file_locks_flags CHECK ((lk_flags = ANY (ARRAY[0, 1]))),
    CONSTRAINT chk_file_locks_range CHECK (((start_offset >= 0) AND (end_offset >= start_offset))),
    CONSTRAINT chk_file_locks_type CHECK ((lock_type = ANY (ARRAY[0, 1])))
);

CREATE SEQUENCE public.file_locks_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.file_locks_id_seq OWNED BY public.file_locks.id;

CREATE SEQUENCE public.inode_id_seq
    START WITH 2
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE public.inode_xattrs (
    ino bigint NOT NULL,
    name bytea NOT NULL,
    value bytea NOT NULL,
    CONSTRAINT chk_inode_xattrs_name CHECK ((((octet_length(name) >= 1) AND (octet_length(name) <= 255)) AND (POSITION(('\x00'::bytea) IN (name)) = 0))),
    CONSTRAINT chk_inode_xattrs_value CHECK ((octet_length(value) <= 65536))
);

CREATE TABLE public.inodes (
    ino bigint DEFAULT nextval('public.inode_id_seq'::regclass) NOT NULL,
    generation bigint DEFAULT 0 NOT NULL,
    mode integer NOT NULL,
    size bigint DEFAULT 0 NOT NULL,
    nlink integer DEFAULT 1 NOT NULL,
    uid integer DEFAULT 0 NOT NULL,
    gid integer DEFAULT 0 NOT NULL,
    rdev bigint DEFAULT 0 NOT NULL,
    atime timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    mtime timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    ctime timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    symlink_target bytea,
    flags integer DEFAULT 0 NOT NULL,
    parent_ino bigint DEFAULT 0 NOT NULL,
    root_ino bigint DEFAULT 0 NOT NULL,
    CONSTRAINT chk_inodes_rdev CHECK ((((mode & 61440) = ANY (ARRAY[8192, 24576])) OR (rdev = 0))),
    CONSTRAINT chk_inodes_size CHECK (((size >= 0) AND (((mode & 61440) <> ALL (ARRAY[4096, 8192, 24576, 49152])) OR (size = 0)))),
    CONSTRAINT chk_inodes_symlink_target CHECK (
CASE
    WHEN ((mode & 61440) = 40960) THEN ((symlink_target IS NULL) OR (((octet_length(symlink_target) >= 1) AND (octet_length(symlink_target) <= 4095)) AND (POSITION(('\x00'::bytea) IN (symlink_target)) = 0)))
    ELSE (symlink_target IS NULL)
END)
);

CREATE SEQUENCE public.slice_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 64;

CREATE TABLE public.slice_refs (
    slice_id bigint NOT NULL,
    size integer NOT NULL,
    refs integer DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT chk_slice_refs_id CHECK ((slice_id > 0)),
    CONSTRAINT chk_slice_refs_size CHECK ((size > 0))
);

ALTER TABLE ONLY public.file_locks ALTER COLUMN id SET DEFAULT nextval('public.file_locks_id_seq'::regclass);

ALTER TABLE ONLY public.chunks
    ADD CONSTRAINT chunks_pkey PRIMARY KEY (ino, indx);

ALTER TABLE ONLY public.file_handles
    ADD CONSTRAINT file_handles_pkey PRIMARY KEY (fh);

ALTER TABLE ONLY public.file_lock_waiters
    ADD CONSTRAINT file_lock_waiters_pkey PRIMARY KEY (session_id, request_id);

ALTER TABLE ONLY public.file_locks
    ADD CONSTRAINT file_locks_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.inodes
    ADD CONSTRAINT inodes_pkey PRIMARY KEY (ino);

ALTER TABLE ONLY public.entries
    ADD CONSTRAINT pk_entries PRIMARY KEY (parent_ino, name);

ALTER TABLE ONLY public.inode_xattrs
    ADD CONSTRAINT pk_inode_xattrs PRIMARY KEY (ino, name);

ALTER TABLE ONLY public.slice_refs
    ADD CONSTRAINT slice_refs_pkey PRIMARY KEY (slice_id);

CREATE INDEX idx_entries_ino ON public.entries USING btree (ino);

CREATE INDEX idx_file_handles_ino ON public.file_handles USING btree (ino);

CREATE INDEX idx_file_handles_session_id ON public.file_handles USING btree (session_id) WHERE (session_id IS NOT NULL);

CREATE INDEX idx_file_lock_waiters_ino_flags_status ON public.file_lock_waiters USING btree (ino, lk_flags, status, created_at);

CREATE INDEX idx_file_locks_ino_flags_range ON public.file_locks USING btree (ino, lk_flags, start_offset, end_offset);

CREATE INDEX idx_file_locks_session_ino_fh ON public.file_locks USING btree (session_id, ino, fh);

CREATE INDEX idx_file_locks_session_ino_owner ON public.file_locks USING btree (session_id, ino, lock_owner);

CREATE INDEX idx_inodes_orphan ON public.inodes USING btree (ino) WHERE (nlink = 0);

CREATE INDEX idx_inodes_root_ino_quota ON public.inodes USING btree (root_ino) INCLUDE (mode, size);

CREATE INDEX idx_slice_refs_garbage ON public.slice_refs USING btree (slice_id) WHERE (refs <= 0);

ALTER TABLE ONLY public.file_handles
    ADD CONSTRAINT file_handles_ino_fkey FOREIGN KEY (ino) REFERENCES public.inodes(ino) ON DELETE CASCADE;

ALTER TABLE ONLY public.file_handles
    ADD CONSTRAINT file_handles_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.client_sessions(session_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.file_lock_waiters
    ADD CONSTRAINT file_lock_waiters_ino_fkey FOREIGN KEY (ino) REFERENCES public.inodes(ino) ON DELETE CASCADE;

ALTER TABLE ONLY public.file_lock_waiters
    ADD CONSTRAINT file_lock_waiters_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.client_sessions(session_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.file_locks
    ADD CONSTRAINT file_locks_ino_fkey FOREIGN KEY (ino) REFERENCES public.inodes(ino) ON DELETE CASCADE;

ALTER TABLE ONLY public.file_locks
    ADD CONSTRAINT file_locks_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.client_sessions(session_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.entries
    ADD CONSTRAINT fk_entries_ino FOREIGN KEY (ino) REFERENCES public.inodes(ino) ON DELETE CASCADE;

ALTER TABLE ONLY public.entries
    ADD CONSTRAINT fk_entries_parent_ino FOREIGN KEY (parent_ino) REFERENCES public.inodes(ino) ON DELETE CASCADE;

ALTER TABLE ONLY public.inode_xattrs
    ADD CONSTRAINT fk_inode_xattrs_ino FOREIGN KEY (ino) REFERENCES public.inodes(ino) ON DELETE CASCADE;

-- 为恢复的唯一约束重新编号；旧文件数据仍需从备份恢复。
WITH numbered AS (SELECT disk_id, row_number() OVER (ORDER BY disk_id) AS ino FROM disks)
UPDATE disks SET root_ino = numbered.ino FROM numbered WHERE disks.disk_id = numbered.disk_id;
ALTER TABLE disks ALTER COLUMN root_ino DROP DEFAULT;
ALTER TABLE disks ADD CONSTRAINT disks_root_ino_key UNIQUE (root_ino);
INSERT INTO inodes (ino, mode, nlink, size, parent_ino, root_ino)
SELECT root_ino, 16877, 2, 4096, root_ino, root_ino FROM disks;
SELECT setval('inode_id_seq', GREATEST(COALESCE((SELECT MAX(ino) FROM inodes), 1), 1), true);
-- +goose StatementEnd
