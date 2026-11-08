// Package db 封装 Storage 控制面 PostgreSQL 连接池、事务与 sqlc 查询。
package db

import (
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/observability/telemetry"
	"github.com/starsrc/Aether/pkg/tx"

	"github.com/starsrc/bowl/services/storage/internal/config"
)

// Store 组合控制面查询与事务执行器。
type Store interface {
	Querier
	tx.Transactor[Querier]
}

type txQueries struct {
	*Queries
	db DBTX
}

// RawDBTX 返回事务连接，用于控制面咨询锁。
func (q *txQueries) RawDBTX() DBTX { return q.db }

// SQLStore 提供 PostgreSQL 控制面访问。
type SQLStore struct {
	*Queries
	tx.Transactor[Querier]
	pool *pgxpool.Pool
}

// NewStore 基于连接池创建控制面访问器。
func NewStore(pool *pgxpool.Pool) Store {
	return &SQLStore{
		Queries: New(pool),
		Transactor: tx.NewPGXTransactor(pool, func(rawTx pgx.Tx) Querier {
			return &txQueries{Queries: New(rawTx), db: rawTx}
		}),
		pool: pool,
	}
}

// Close 关闭连接池。
func (s *SQLStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

// Pool 返回连接池。
func (s *SQLStore) Pool() *pgxpool.Pool { return s.pool }

// RawDBTX 返回底层数据库访问器。
func (s *SQLStore) RawDBTX() DBTX { return s.pool }

// OpenPool 根据配置创建连接池。
func OpenPool(cfg config.DBPoolConfig) (*pgxpool.Pool, error) {
	pool, err := tx.OpenPool(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dbpool config: %w", err)
	}
	return pool, nil
}

// PoolParams 包含连接池所需依赖。
type PoolParams struct {
	fx.In
	Ready     telemetry.Ready `optional:"true"`
	Lifecycle fx.Lifecycle
	Config    config.DBPoolConfig
}

// ProvidePool 创建连接池并注册生命周期。
func ProvidePool(params PoolParams) (*pgxpool.Pool, error) {
	pool, err := OpenPool(params.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize dbpool: %w", err)
	}
	tx.RegisterPoolLifecycle(params.Lifecycle, pool)
	return pool, nil
}

// Module 提供控制面数据库依赖。
var Module = fx.Module("db", fx.Provide(ProvidePool, NewStore))
