package pebblekv_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

func TestMetadataValuesRoundTrip(t *testing.T) {
	inode := pebblekv.Inode{Ino: 123, ParentIno: 8, Mode: 0o120777, Nlink: 2, UID: 100, GID: 200, Rdev: 7, Size: 901, Blksize: 4096, Blocks: 9, AtimeSec: -12, AtimeNsec: 34, MtimeSec: 56, MtimeNsec: 78, CtimeSec: 90, CtimeNsec: 123, Flags: 4, Generation: 55, WriteSeq: 888, SymlinkTarget: []byte{'a', 0, 0xff}}
	encoded, err := pebblekv.EncodeInode(inode)
	require.NoError(t, err)
	decoded, err := pebblekv.DecodeInode(encoded)
	require.NoError(t, err)
	require.Equal(t, inode, decoded)
	_, err = pebblekv.DecodeInode(encoded[:len(encoded)-1])
	require.ErrorIs(t, err, pebblekv.ErrInvalidValue)

	stat := pebblekv.BranchStat{LogicalBytes: 200, Inodes: 3, NextWriteSeq: 17}
	decodedStat, err := pebblekv.DecodeBranchStat(pebblekv.EncodeBranchStat(stat))
	require.NoError(t, err)
	require.Equal(t, stat, decodedStat)

	entry := pebblekv.Entry{Ino: 55, Mode: 0o100644}
	decodedEntry, err := pebblekv.DecodeEntry(pebblekv.EncodeEntry(entry))
	require.NoError(t, err)
	require.Equal(t, entry, decodedEntry)
}
