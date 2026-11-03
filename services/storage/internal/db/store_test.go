package db_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/starsrc/Aether/pkg/observability/telemetry"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

// recordingLifecycle 用于离线捕获 Fx 生命周期钩子。
type recordingLifecycle struct {
	hooks []fx.Hook
}

func (r *recordingLifecycle) Append(hook fx.Hook) {
	r.hooks = append(r.hooks, hook)
}

func TestOpenPool_InvalidDSN(t *testing.T) {
	cfg := config.DBPoolConfig{
		DSN: "postgres://%invalid-url",
	}

	pool, err := db.OpenPool(cfg)
	require.Error(t, err)
	assert.Nil(t, pool)
	assert.Contains(t, err.Error(), "failed to parse dbpool config")
}

func TestOpenPool_TracingEnabled(t *testing.T) {
	t.Run("TracingEnabled 为 true 时挂载 OTel Tracer", func(t *testing.T) {
		cfg := config.DBPoolConfig{
			DSN:            "postgres://user:pass@127.0.0.1:5432/testdb",
			TracingEnabled: true,
		}

		pool, err := db.OpenPool(cfg)
		require.NoError(t, err)
		require.NotNil(t, pool)
		defer pool.Close()

		assert.NotNil(t, pool.Config().ConnConfig.Tracer)
	})

	t.Run("TracingEnabled 为 false 时不挂载 OTel Tracer", func(t *testing.T) {
		cfg := config.DBPoolConfig{
			DSN:            "postgres://user:pass@127.0.0.1:5432/testdb",
			TracingEnabled: false,
		}

		pool, err := db.OpenPool(cfg)
		require.NoError(t, err)
		require.NotNil(t, pool)
		defer pool.Close()

		assert.Nil(t, pool.Config().ConnConfig.Tracer)
	})
}

func TestOpenPool_MinConnsClamped(t *testing.T) {
	t.Run("MinConns 大于 MaxConns 时触发尺寸保护钳制为 MaxConns", func(t *testing.T) {
		cfg := config.DBPoolConfig{
			DSN:      "postgres://user:pass@127.0.0.1:5432/testdb",
			MaxConns: 10,
			MinConns: 20,
		}

		pool, err := db.OpenPool(cfg)
		require.NoError(t, err)
		require.NotNil(t, pool)
		defer pool.Close()

		assert.Equal(t, int32(10), pool.Config().MaxConns)
		assert.Equal(t, int32(10), pool.Config().MinConns)
	})

	t.Run("MinConns 小于等于 MaxConns 时保持原样", func(t *testing.T) {
		cfg := config.DBPoolConfig{
			DSN:      "postgres://user:pass@127.0.0.1:5432/testdb",
			MaxConns: 20,
			MinConns: 5,
		}

		pool, err := db.OpenPool(cfg)
		require.NoError(t, err)
		require.NotNil(t, pool)
		defer pool.Close()

		assert.Equal(t, int32(20), pool.Config().MaxConns)
		assert.Equal(t, int32(5), pool.Config().MinConns)
	})
}

func TestOpenPool_PoolConfigOptions(t *testing.T) {
	cfg := config.DBPoolConfig{
		DSN:               "postgres://user:pass@127.0.0.1:5432/testdb?connect_timeout=5",
		MaxConns:          15,
		MinConns:          3,
		MaxConnLifetime:   45 * time.Minute,
		MaxConnIdleTime:   15 * time.Minute,
		HealthCheckPeriod: 30 * time.Second,
		TracingEnabled:    false,
	}

	pool, err := db.OpenPool(cfg)
	require.NoError(t, err)
	require.NotNil(t, pool)
	defer pool.Close()

	poolCfg := pool.Config()
	assert.Equal(t, int32(15), poolCfg.MaxConns)
	assert.Equal(t, int32(3), poolCfg.MinConns)
	assert.Equal(t, 45*time.Minute, poolCfg.MaxConnLifetime)
	assert.Equal(t, 15*time.Minute, poolCfg.MaxConnIdleTime)
	assert.Equal(t, 30*time.Second, poolCfg.HealthCheckPeriod)
	assert.Equal(t, 5*time.Second, poolCfg.ConnConfig.ConnectTimeout)
}

func TestNewStore(t *testing.T) {
	store := db.NewStore(nil)
	assert.NotNil(t, store)

	var _ db.Store = store
}

func TestNewStore_PoolAndClose(t *testing.T) {
	cfg := config.DBPoolConfig{
		DSN: "postgres://user:pass@127.0.0.1:5432/testdb",
	}
	pool, err := db.OpenPool(cfg)
	require.NoError(t, err)
	require.NotNil(t, pool)

	store := db.NewStore(pool)
	require.NotNil(t, store)

	sqlStore, ok := store.(*db.SQLStore)
	require.True(t, ok)
	assert.Same(t, pool, sqlStore.Pool())

	// 调用 Close 关闭连接池
	sqlStore.Close()

	// 验证 pool 已关闭
	_, acquireErr := pool.Acquire(context.Background())
	require.Error(t, acquireErr)
	assert.Contains(t, acquireErr.Error(), "closed pool")

	// 幂等重复调用 Close 不 panic
	assert.NotPanics(t, func() {
		sqlStore.Close()
	})

	// pool 为 nil 时的 SQLStore.Close() 不 panic
	nilStore := db.NewStore(nil)
	assert.NotPanics(t, func() {
		nilStore.(*db.SQLStore).Close()
	})
	assert.Nil(t, nilStore.(*db.SQLStore).Pool())
}

func TestModule_FxProvidesStoreAndPool(t *testing.T) {
	cfg := config.DBPoolConfig{
		DSN: "postgres://user:pass@127.0.0.1:5432/testdb",
	}

	t.Run("同时提供 *pgxpool.Pool 与 db.Store (携带 telemetry.Ready)", func(t *testing.T) {
		var (
			store db.Store
			pool  *pgxpool.Pool
		)
		app := fxtest.New(
			t,
			fx.Supply(cfg),
			fx.Supply(telemetry.Ready{}),
			db.Module,
			fx.Populate(&store, &pool),
		)
		require.NoError(t, app.Err())
		assert.NotNil(t, store)
		assert.NotNil(t, pool)
	})

	t.Run("未显式提供 telemetry.Ready 时通过 optional 正常装配", func(t *testing.T) {
		var (
			store db.Store
			pool  *pgxpool.Pool
		)
		app := fxtest.New(
			t,
			fx.Supply(cfg),
			db.Module,
			fx.Populate(&store, &pool),
		)
		require.NoError(t, app.Err())
		assert.NotNil(t, store)
		assert.NotNil(t, pool)
	})

	t.Run("非法 DSN 时 Fx 初始化报错", func(t *testing.T) {
		invalidCfg := config.DBPoolConfig{
			DSN: "postgres://%invalid-url",
		}
		var store db.Store
		app := fx.New(
			fx.NopLogger,
			fx.Supply(invalidCfg),
			db.Module,
			fx.Populate(&store),
		)
		require.Error(t, app.Err())
		assert.Contains(t, app.Err().Error(), "failed to initialize dbpool")
	})
}

func TestModule_Lifecycle_PingFailure(t *testing.T) {
	cfg := config.DBPoolConfig{
		DSN: "postgres://user:pass@127.0.0.1:59999/testdb?connect_timeout=1",
	}

	var (
		store db.Store
		pool  *pgxpool.Pool
	)
	app := fxtest.New(
		t,
		fx.Supply(cfg),
		db.Module,
		fx.Populate(&store, &pool),
	)
	require.NoError(t, app.Err())
	require.NotNil(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := app.Start(ctx)
	require.Error(t, err, "不可达数据库端口在 OnStart 探活必须报错")
	assert.Contains(t, err.Error(), "failed to ping database")

	// 探活失败后连接池必须已被自动关闭
	_, acquireErr := pool.Acquire(context.Background())
	require.Error(t, acquireErr)
	assert.Contains(t, acquireErr.Error(), "closed pool")
}

func TestModule_Lifecycle_OnStop(t *testing.T) {
	cfg := config.DBPoolConfig{
		DSN: "postgres://user:pass@127.0.0.1:5432/testdb",
	}

	t.Run("OnStop 钩子成功执行并关闭底层连接池", func(t *testing.T) {
		lc := &recordingLifecycle{}
		pool, err := db.ProvidePool(db.PoolParams{
			Lifecycle: lc,
			Config:    cfg,
		})
		require.NoError(t, err)
		require.NotNil(t, pool)
		require.Len(t, lc.hooks, 1, "应注册单个 Fx 生命周期钩子")

		hook := lc.hooks[0]
		require.NotNil(t, hook.OnStop, "OnStop 回调函数不得为空")

		// 执行 OnStop 关闭钩子
		ctx := context.Background()
		err = hook.OnStop(ctx)
		assert.NoError(t, err, "OnStop 钩子应成功执行")

		// 验证连接池已关闭
		_, acquireErr := pool.Acquire(ctx)
		require.Error(t, acquireErr, "已关闭的连接池调用 Acquire 应返回错误")
		assert.Contains(t, acquireErr.Error(), "closed pool")

		// 验证 OnStop 重复调用具备幂等性
		repeatErr := hook.OnStop(ctx)
		assert.NoError(t, repeatErr, "重复调用 OnStop 不应报错")
	})

	t.Run("fxtest.Lifecycle 正常接收并触发 OnStop 钩子", func(t *testing.T) {
		lc := fxtest.NewLifecycle(t)
		pool, err := db.ProvidePool(db.PoolParams{
			Lifecycle: lc,
			Config:    cfg,
		})
		require.NoError(t, err)
		require.NotNil(t, pool)

		// 触发 Stop 阶段验证 Fx 集成正常
		lc.RequireStop()
	})
}

// resolveAndConnectTestDB 统一解析并连接 PostgreSQL 测试库，严格遵循显式环境变量配置故障硬失败契约。
func resolveAndConnectTestDB(ctx context.Context, fatalf func(string, ...any), skipf func(string, ...any)) (*pgxpool.Pool, string) {
	for _, envName := range []string{"STORAGE_TEST_DSN", "STORAGE_DB_DSN", "DATABASE_URL", "BOWL_TEST_POSTGRES_DSN", "TEST_DB_DSN"} {
		rawVal := strings.TrimSpace(os.Getenv(envName))
		if rawVal != "" {
			probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			poolCfg, err := pgxpool.ParseConfig(rawVal)
			if err != nil {
				fatalf("显式指定的数据库 DSN (%s) 配置解析失败: %v", envName, err)
				return nil, ""
			}
			pool, err := pgxpool.NewWithConfig(probeCtx, poolCfg)
			if err != nil {
				fatalf("显式指定的数据库 DSN (%s) 连接失败: %v", envName, err)
				return nil, ""
			}
			if err := pool.Ping(probeCtx); err != nil {
				pool.Close()
				fatalf("显式指定的数据库 DSN (%s) 连接失败: %v", envName, err)
				return nil, ""
			}
			return pool, rawVal
		}
	}

	candidates := []string{
		"postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable",
		"postgres://postgres:postgres@127.0.0.1:5432/bowl?sslmode=disable",
	}

	for _, cand := range candidates {
		probeCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		poolCfg, err := pgxpool.ParseConfig(cand)
		if err == nil {
			pool, err := pgxpool.NewWithConfig(probeCtx, poolCfg)
			if err == nil {
				if pingErr := pool.Ping(probeCtx); pingErr == nil {
					cancel()
					return pool, cand
				}
				pool.Close()
			}
		}
		cancel()
	}

	if strings.TrimSpace(os.Getenv("BOWL_FORMAL_REQUIRE_DB")) == "1" {
		fatalf("未探测到可用 PostgreSQL 实例 (9432/5432 均不可达) 且设置了 BOWL_FORMAL_REQUIRE_DB=1")
		return nil, ""
	}

	skipf("跳过 PostgreSQL 测试 (未显式配置 DSN 且 127.0.0.1:9432 和 127.0.0.1:5432 均不可达)")
	return nil, ""
}

func TestExplicitInvalidDSNFailsFast(t *testing.T) {
	for _, env := range []string{"STORAGE_TEST_DSN", "STORAGE_DB_DSN", "DATABASE_URL", "BOWL_TEST_POSTGRES_DSN", "TEST_DB_DSN"} {
		t.Setenv(env, "")
	}
	t.Setenv("STORAGE_TEST_DSN", "postgres://postgres:postgres@127.0.0.1:1/bowl?sslmode=disable&connect_timeout=1")

	var fatalCalls, skipCalls int
	fatalf := func(format string, args ...any) {
		fatalCalls++
	}
	skipf := func(format string, args ...any) {
		skipCalls++
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, _ := resolveAndConnectTestDB(ctx, fatalf, skipf)
	if pool != nil {
		pool.Close()
	}

	assert.Equal(t, 1, fatalCalls, "显式指定的非法 DSN 必须触发且仅触发 1 次 fatalf")
	assert.Equal(t, 0, skipCalls, "显式指定的非法 DSN 严禁触发 skipf")
}

func TestModule_Lifecycle_PingSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	probePool, dsn := resolveAndConnectTestDB(ctx, t.Fatalf, t.Skipf)
	if probePool == nil {
		return
	}
	probePool.Close()

	cfg := config.DBPoolConfig{
		DSN: dsn,
	}

	var (
		store db.Store
		pool  *pgxpool.Pool
	)
	app := fxtest.New(
		t,
		fx.Supply(cfg),
		db.Module,
		fx.Populate(&store, &pool),
	)
	require.NoError(t, app.Err())

	startCtx, startCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer startCancel()

	err := app.Start(startCtx)
	require.NoError(t, err)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()

	err = app.Stop(stopCtx)
	require.NoError(t, err)
}

func TestQuerierInterface(t *testing.T) {
	var _ db.Querier = (*db.Queries)(nil)
	var _ db.Querier = (db.Store)(nil)
}
