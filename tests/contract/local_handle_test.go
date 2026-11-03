package contract_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/validator"
)

func TestLocalHandleRequests(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	requests := []struct {
		name       string
		newRequest func(uint64) proto.Message
	}{
		{"Read", func(fh uint64) proto.Message { return &storagev1.ReadRequest{Ino: 1, Fh: fh} }},
		{"Write", func(fh uint64) proto.Message { return &storagev1.WriteRequest{Ino: 1, Fh: fh} }},
		{"Fsync", func(fh uint64) proto.Message { return &storagev1.FsyncRequest{Ino: 1, Fh: fh} }},
		{"FsyncDir", func(fh uint64) proto.Message { return &storagev1.FsyncDirRequest{Ino: 1, Fh: fh} }},
		{"ReadDir", func(fh uint64) proto.Message { return &storagev1.ReadDirRequest{Ino: 1, Fh: fh, Limit: 1} }},
		{"ReadDirPlus", func(fh uint64) proto.Message { return &storagev1.ReadDirPlusRequest{Ino: 1, Fh: fh, Limit: 1} }},
		{"Fallocate", func(fh uint64) proto.Message { return &storagev1.FallocateRequest{Ino: 1, Fh: fh, Length: 1} }},
	}
	for _, tc := range requests {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, v.Validate(tc.newRequest(0)))
			require.NoError(t, v.Validate(tc.newRequest(math.MaxInt64)))
			assertErrorInfoReason(t, v.Validate(tc.newRequest(math.MaxInt64+1)), validator.ReasonInvalidArgument)
		})
	}
}
