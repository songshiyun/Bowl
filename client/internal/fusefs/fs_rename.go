package fusefs

import (
	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	// renameNoReplace 对应 Linux RENAME_NOREPLACE (0x1)，要求目标目录项不存在。
	renameNoReplace = 0x1
	// renameExchange 对应 Linux RENAME_EXCHANGE (0x2)，原子双向交换两个目录项。
	renameExchange = 0x2
	// renameWhiteout 对应 Linux RENAME_WHITEOUT (0x4)，在源项位置创建 whiteout 特殊节点。
	renameWhiteout = 0x4
)

// Rename 实现目录项重命名、跨目录移动与双向原子交换操作（支持 Linux FUSE_RENAME2 协议标志位）。
func (fs *RawFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	if st := fs.CheckWritable(); st != fuse.OK {
		return st
	}

	oldParentIno, st := fs.ToServerIno(input.NodeId)
	if st != fuse.OK {
		return st
	}

	newParentIno, st := fs.ToServerIno(input.Newdir)
	if st != fuse.OK {
		return st
	}

	fs.namespaceMu.Lock()
	defer fs.namespaceMu.Unlock()
	var source, target uint64
	var targetAttr *storagev1.Inode
	if fs.handles.AnyOpen() {
		old, oldStatus := fs.lookupInode(cancel, input.NodeId, oldName)
		if oldStatus != fuse.OK {
			return oldStatus
		}
		source = fs.ToKernelIno(old.inode.GetIno())
		cached, targetStatus := fs.lookupInode(cancel, input.Newdir, newName)
		if targetStatus != fuse.OK && targetStatus != fuse.ENOENT {
			return targetStatus
		}
		if targetStatus == fuse.OK {
			targetAttr = cached.inode
			target = fs.ToKernelIno(targetAttr.GetIno())
		}
	} else {
		source, _ = fs.cache.entry(input.NodeId, oldName)
		target, _ = fs.cache.entry(input.Newdir, newName)
	}
	targetOpen := target != 0 && fs.handles.IsOpen(target)
	sid := fs.currentSession()

	var protoFlags uint32
	if (input.Flags & renameNoReplace) != 0 {
		protoFlags |= 0x1
	}
	if (input.Flags & renameExchange) != 0 {
		protoFlags |= 0x2
	}
	if (input.Flags & renameWhiteout) != 0 {
		protoFlags |= 0x4
	}
	// 保留未知位透传给服务端，由服务端契约校验返回 EINVAL
	if unknown := input.Flags &^ (renameNoReplace | renameExchange | renameWhiteout); unknown != 0 {
		protoFlags |= unknown
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	_, err := fs.client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
		OldParentIno:       oldParentIno,
		OldName:            []byte(oldName),
		NewParentIno:       newParentIno,
		NewName:            []byte(newName),
		Flags:              protoFlags,
		SessionId:          sid,
		TargetOpenInClient: &targetOpen,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	fs.cache.invalidateDir(input.NodeId)
	fs.cache.invalidateDir(input.Newdir)
	if source != 0 {
		fs.cache.invalidateInode(source)
	}
	if target != 0 && target != source {
		fs.cache.invalidateInode(target)
	}
	if targetOpen && target != source && input.Flags&renameExchange == 0 && targetAttr != nil && targetAttr.GetAttr().GetNlink() == 1 {
		fs.markOrphan(target, sid)
	}
	return fuse.OK
}
