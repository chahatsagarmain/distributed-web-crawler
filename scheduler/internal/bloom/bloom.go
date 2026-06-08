package cache

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

const (
	BloomFilterKey = "url_bloom"
)

func SetupBloomFilter(rdb *redis.Client) error {
	if err := rdb.BFReserve(context.Background(), BloomFilterKey, 0.01, 1000000).Err(); err != nil {
		if strings.Contains(err.Error(), "item exists") {
			return nil
		}
		fmt.Printf("ERROR: setting up bloom filter in redis %v\n", err.Error())
		return err
	}
	return nil
}

func CheckUrlDuplicate(r *redis.Client, url string) (bool, error) {
	res, err := r.BFExists(context.Background(), BloomFilterKey, url).Result()
	if err != nil {
		fmt.Printf("ERROR: error checking element to bloom in redis %v", err)
		return false, err
	}
	return res, nil
}

func AddToBloom(r *redis.Client, url string) (bool, error) {
	res, err := r.BFAdd(context.Background(), BloomFilterKey, url).Result()
	if err != nil {
		fmt.Printf("ERROR: error adding element to bloom in redis %v", err)
		return false, err
	}
	return res, err
}
