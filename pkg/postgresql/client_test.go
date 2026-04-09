package postgresql

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/jackc/pgx/v4"
	"github.com/prometheus/common/promlog"
)

type testConfig struct {
	writer PGWriter
	client *Client
}

func setupEnv(t *testing.T) *testConfig {
	tc := testConfig{}

	err := os.Setenv(
		"DATABASE_URL",
		"user=localuser password=password host=localhost port=5432 database=prometheus sslmode=disable")
	require.Nil(t, err, "failed to set DATABASE_URL")

	tc.writer = PGWriter{}
	tc.writer.logger = promlog.New(&promlog.Config{})
	tc.writer.DB, err = pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	require.Nil(t, err, "failed to connect to database")
	tc.writer.setupPgPrometheus()

	return &tc
}

func (tc *testConfig) teardownEnv(t *testing.T) {
	err := os.Unsetenv("DATABASE_URL")
	require.Nil(t, err, "failed to unset DATABASE_URL")

	partitions, err := tc.client.getPgPartitionNames(0)
	require.Nil(t, err, "failed to get partition names")

	tc.client.logger.Log("msg", "teardownEnv: removing remaining partitions")
	for _, partition := range partitions {
		err = tc.client.dropPgPartition(partition)
		require.Nil(t, err, "failed to drop partition: %s", partition)
	}
	partitions, err = tc.client.getPgPartitionNames(0)
	require.Equal(t, 0, len(partitions), "failed to delete all the partitions")
}

// Cull one partition over the count to keep.
func TestCullPgPartitionsDailySingle(t *testing.T) {
	tc := setupEnv(t)
	defer tc.teardownEnv(t)

	cfg := Config{
		PartitionScheme: "daily",
		KeepDays: 5,
	}
	logger := promlog.New(&promlog.Config{})

        tc.client = NewClient(logger, &cfg)

	// Setup 6 days of partitions
	now := time.Now()
	for i:=0 ; i<6 ; i++ {
		tc.writer.setupPgPartitions("daily", now)
		now = now.AddDate(0, 0, -1)
	}
	partitions, err := tc.client.getPgPartitionNames(0)
	require.Nil(t, err, "failed to get partition names")
	require.Equal(t, 6, len(partitions), "incorrect partition count on setup")

	cullCtx := context.Background()
	cullCancelCtx, _ := context.WithCancel(cullCtx)
	tc.client.CullPgPartitions(true, cullCancelCtx)

	partitions, err = tc.client.getPgPartitionNames(0)
	require.Nil(t, err, "failed to get partition names")
	require.Equal(t, 5, len(partitions), "incorrect partition count on culling")
}

// Cull max of 2 partitions over the count to keep.
func TestCullPgPartitionsDailyTwo(t *testing.T) {
	tc := setupEnv(t)
	defer tc.teardownEnv(t)

	cfg := Config{
		PartitionScheme: "daily",
		KeepDays: 5,
	}
	logger := promlog.New(&promlog.Config{})

        tc.client = NewClient(logger, &cfg)

	// Setup 6 days of partitions
	now := time.Now()
	for i:=0 ; i<8 ; i++ {
		tc.writer.setupPgPartitions("daily", now)
		now = now.AddDate(0, 0, -1)
	}
	partitions, err := tc.client.getPgPartitionNames(0)
	require.Nil(t, err, "failed to get partition names")
	require.Equal(t, 8, len(partitions), "incorrect partition count on setup")

	cullCtx := context.Background()
	cullCancelCtx, _ := context.WithCancel(cullCtx)
	tc.client.CullPgPartitions(true, cullCancelCtx)

	// We keep 5 max, but since there are 8, and we cull max of 2
	// per iteration, we'll end up with 6.
	partitions, err = tc.client.getPgPartitionNames(0)
	require.Nil(t, err, "failed to get partition names")
	require.Equal(t, 6, len(partitions), "incorrect partition count on culling")
}

// Cull zero if not over limit.
func TestCullPgPartitionsDailyNone(t *testing.T) {
	tc := setupEnv(t)
	defer tc.teardownEnv(t)

	cfg := Config{
		PartitionScheme: "daily",
		KeepDays: 5,
	}
	logger := promlog.New(&promlog.Config{})

        tc.client = NewClient(logger, &cfg)

	// Setup 4 days of partitions
	now := time.Now()
	for i:=0 ; i<4 ; i++ {
		tc.writer.setupPgPartitions("daily", now)
		now = now.AddDate(0, 0, -1)
	}
	partitions, err := tc.client.getPgPartitionNames(0)
	require.Nil(t, err, "failed to get partition names")
	require.Equal(t, 4, len(partitions), "incorrect partition count on setup")

	cullCtx := context.Background()
	cullCancelCtx, _ := context.WithCancel(cullCtx)
	tc.client.CullPgPartitions(true, cullCancelCtx)

	// We keep 5 max, but since there are only 4, nothing happens.
	partitions, err = tc.client.getPgPartitionNames(0)
	require.Nil(t, err, "failed to get partition names")
	require.Equal(t, 4, len(partitions), "incorrect partition count on culling")
}

// Cull a really old one.
func TestCullPgPartitionsDailyVeryOld(t *testing.T) {
	tc := setupEnv(t)
	defer tc.teardownEnv(t)

	cfg := Config{
		PartitionScheme: "daily",
		KeepDays: 5,
	}
	logger := promlog.New(&promlog.Config{})

        tc.client = NewClient(logger, &cfg)

	now := time.Now()
	for i:=0 ; i<5 ; i++ {
		tc.writer.setupPgPartitions("daily", now)
		now = now.AddDate(0, 0, -1)
	}
	now = now.AddDate(0, 0, -20)
	tc.writer.setupPgPartitions("daily", now)

	partitions, err := tc.client.getPgPartitionNames(0)
	require.Nil(t, err, "failed to get partition names")
	require.Equal(t, 6, len(partitions), "incorrect partition count on setup")

	cullCtx := context.Background()
	cullCancelCtx, _ := context.WithCancel(cullCtx)
	tc.client.CullPgPartitions(true, cullCancelCtx)

	partitions, err = tc.client.getPgPartitionNames(0)
	require.Nil(t, err, "failed to get partition names")
	require.Equal(t, 5, len(partitions), "incorrect partition count on culling")
}

// What happens when we get one in the future?
func TestCullPgPartitionsDailyFuture(t *testing.T) {
	tc := setupEnv(t)
	defer tc.teardownEnv(t)

	cfg := Config{
		PartitionScheme: "daily",
		KeepDays: 5,
	}
	logger := promlog.New(&promlog.Config{})

        tc.client = NewClient(logger, &cfg)

	now := time.Now()
	for i:=0 ; i<5 ; i++ {
		tc.writer.setupPgPartitions("daily", now)
		now = now.AddDate(0, 0, -1)
	}
	future := time.Now().AddDate(0, 0, 2)
	tc.writer.setupPgPartitions("daily", future)

	partitions, err := tc.client.getPgPartitionNames(-2)
	require.Nil(t, err, "failed to get partition names")
	require.Equal(t, 6, len(partitions), "incorrect partition count on setup")

	// It won't see the partitions in the future and will only
	// operate on partitions from now into the past.
	cullCtx := context.Background()
	cullCancelCtx, _ := context.WithCancel(cullCtx)
	tc.client.CullPgPartitions(true, cullCancelCtx)

	partitions, err = tc.client.getPgPartitionNames(-2)
	require.Nil(t, err, "failed to get partition names")
	require.Equal(t, 6, len(partitions), "incorrect partition count on culling")
}

func TestCullPgPartitionsCancel(t *testing.T) {
	tc := setupEnv(t)
	defer tc.teardownEnv(t)

	cfg := Config{
		PartitionScheme: "daily",
		KeepDays: 5,
	}
	logger := promlog.New(&promlog.Config{})

        tc.client = NewClient(logger, &cfg)

	now := time.Now()
	for i:=0 ; i<6 ; i++ {
		tc.writer.setupPgPartitions("daily", now)
		now = now.AddDate(0, 0, -1)
	}

	partitions, err := tc.client.getPgPartitionNames(0)
	require.Nil(t, err, "failed to get partition names")
	require.Equal(t, 6, len(partitions), "incorrect partition count on setup")

	cullCtx := context.Background()
	cullCancelCtx, cancel := context.WithCancel(cullCtx)
	go tc.client.CullPgPartitions(false, cullCancelCtx) // run forever

	// Assumes it will work - waits until the partition is removed then cancels the goroutine.
	partitions, err = tc.client.getPgPartitionNames(0)
	require.Nil(t, err, "failed to get partition names")
	for ; len(partitions) == 6 ; {
		partitions, err = tc.client.getPgPartitionNames(0)
		require.Nil(t, err, "failed to get partition names")
	}
	cancel()

	partitions, err = tc.client.getPgPartitionNames(0)
	require.Nil(t, err, "failed to get partition names")
	require.Equal(t, 5, len(partitions), "incorrect partition count on culling")
}
