package store

func init() {
	registerOp(opFsync, (*StoreActor).validateFsyncCmd, (*StoreActor).mutateFsyncCmd)
	registerOp(opFsyncDir, (*StoreActor).validateFsyncDirCmd, (*StoreActor).mutateFsyncDirCmd)
}

type fsyncOp struct {
	ino uint64
}

// Fsync 将目标非目录节点的挂起脏访问时间、元数据 WAL 与该文件关联的脏数据块持久化落盘（ADR-0030 §3.5）。
func (s *StoreActor) Fsync(ino uint64) error {
	if s.readOnly {
		return ErrReadOnly
	}
	if err := validateNodeIno(ino); err != nil {
		return err
	}

	cmd := s.acquireCmd()
	cmd.op = opFsync
	cmd.req = &fsyncOp{ino: ino}
	return s.submit(cmd)
}

// FsyncDir 将目标目录节点的挂起脏访问时间与已提交目录项/属性元数据 WAL 持久化落盘（ADR-0030 §3.5）。
func (s *StoreActor) FsyncDir(ino uint64) error {
	if s.readOnly {
		return ErrReadOnly
	}
	if err := validateNodeIno(ino); err != nil {
		return err
	}

	cmd := s.acquireCmd()
	cmd.op = opFsyncDir
	cmd.req = &fsyncOp{ino: ino}
	return s.submit(cmd)
}

func (s *StoreActor) validateFsyncCmd(cmd *writeCmd) error {
	op := cmd.req.(*fsyncOp)
	inode, err := s.readInode(s.batch, op.ino)
	if err != nil {
		return err
	}
	if err := s.checkInodeLive(s.batch, inode); err != nil {
		return err
	}
	if inode.Mode&S_IFMT == S_IFDIR {
		return ErrIsDir
	}
	return nil
}

func (s *StoreActor) mutateFsyncCmd(cmd *writeCmd) {
	op := cmd.req.(*fsyncOp)
	cmd.syncBarrier = true
	cmd.syncIno = op.ino
}

func (s *StoreActor) validateFsyncDirCmd(cmd *writeCmd) error {
	op := cmd.req.(*fsyncOp)
	inode, err := s.readInode(s.batch, op.ino)
	if err != nil {
		return err
	}
	if err := s.checkInodeLive(s.batch, inode); err != nil {
		return err
	}
	if inode.Mode&S_IFMT != S_IFDIR {
		return ErrNotDir
	}
	return nil
}

func (s *StoreActor) mutateFsyncDirCmd(cmd *writeCmd) {
	cmd.syncBarrier = true
	cmd.syncIno = 0
}
