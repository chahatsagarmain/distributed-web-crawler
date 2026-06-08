package db

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	ActiveJobKey = "crawler:active_job"
	MaxDepthKey  = "crawler:max_depth"
)

func IsJobActive(rdb *redis.Client) (bool, error) {
	ctx := context.Background()
	exists, err := rdb.Exists(ctx, ActiveJobKey).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check active job in redis: %w", err)
	}
	return exists > 0, nil
}

func StartJob(rdb *redis.Client, urlStr string, maxDepth int) (bool, error) {
	ctx := context.Background()
	success, err := rdb.SetNX(ctx, ActiveJobKey, urlStr, 1*time.Hour).Result()
	if err != nil {
		return false, fmt.Errorf("failed to set active job in redis: %w", err)
	}
	if !success {
		return false, nil
	}

	err = rdb.Set(ctx, MaxDepthKey, maxDepth, 1*time.Hour).Err()
	if err != nil {
		rdb.Del(ctx, ActiveJobKey)
		return false, fmt.Errorf("failed to initialize max depth in redis: %w", err)
	}

	err = rdb.Set(ctx, "crawler:last_activity_time", time.Now().Unix(), 1*time.Hour).Err()
	if err != nil {
		rdb.Del(ctx, ActiveJobKey)
		rdb.Del(ctx, MaxDepthKey)
		return false, fmt.Errorf("failed to initialize last activity time in redis: %w", err)
	}

	return true, nil
}

func GetMaxDepth(rdb *redis.Client) (int, error) {
	ctx := context.Background()
	val, err := rdb.Get(ctx, MaxDepthKey).Int()
	if err != nil {
		return 0, err
	}
	return val, nil
}

func ForceCleanupJob(rdb *redis.Client) {
	ctx := context.Background()
	rdb.Del(ctx, ActiveJobKey)
	rdb.Del(ctx, MaxDepthKey)
	rdb.Del(ctx, "crawler:last_activity_time")
}
