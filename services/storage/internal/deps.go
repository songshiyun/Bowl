// Package internal 固定并行开发阶段所需的公共依赖模块，避免子包修改 go.mod 产生冲突。
package internal

import (
	_ "github.com/prometheus/client_golang/prometheus"
	_ "github.com/prometheus/client_model/go"
)
