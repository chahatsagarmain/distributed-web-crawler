package db

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	ActiveJobKey     = "crawler:active_job"
	PendingCountsKey = "crawler:pending_urls"
)

func IsJobActive(rdb *redis.Client) (bool, error) {
	ctx := context.Background()
	exists, err := rdb.Exists(ctx, ActiveJobKey).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check active job in redis: %w", err)
	}
	return exists > 0, nil
}

func StartJob(rdb *redis.Client, urlStr string) (bool, error) {
	ctx := context.Background()
	success, err := rdb.SetNX(ctx, ActiveJobKey, urlStr, 1*time.Hour).Result()
	if err != nil {
		return false, fmt.Errorf("failed to set active job in redis: %w", err)
	}
	if !success {
		return false, nil
	}

	err = rdb.Set(ctx, PendingCountsKey, 1, 1*time.Hour).Err()
	if err != nil {
		rdb.Del(ctx, ActiveJobKey)
		return false, fmt.Errorf("failed to initialize pending count in redis: %w", err)
	}

	return true, nil
}

func IncrementPending(rdb *redis.Client, count int64) (int64, error) {
	ctx := context.Background()
	val, err := rdb.IncrBy(ctx, PendingCountsKey, count).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to increment pending count: %w", err)
	}
	return val, nil
}

func DecrementPending(rdb *redis.Client) (int64, error) {
	ctx := context.Background()
	val, err := rdb.Decr(ctx, PendingCountsKey).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to decrement pending count: %w", err)
	}

	if val <= 0 {
		rdb.Del(ctx, ActiveJobKey)
		rdb.Del(ctx, PendingCountsKey)
	}

	return val, nil
}
