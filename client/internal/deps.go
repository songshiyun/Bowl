// Package internal 锁定 client 子模块的核心第三方依赖。
package internal

import (
	// 锁定 client 子模块的核心依赖项。
	_ "connectrpc.com/connect"
	_ "github.com/google/uuid"
	_ "github.com/hanwen/go-fuse/v2/fuse"
	_ "golang.org/x/net/http2"
	_ "golang.org/x/sys/unix"

	// 锁定生成的 Protobuf 与 Connect 契约包依赖。
	_ "github.com/starsrc/bowl/gen/go/storage/v1"
	_ "github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)
