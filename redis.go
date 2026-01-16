package redbuckets

import (
	"context"
	"errors"
	"time"
)

var ErrRedisKeyExists = errors.New("ErrRedisKeyExists")

type Redis interface {
	ZAdd(ctx context.Context, key, member string, score float64) error
	ZRem(ctx context.Context, key, member string) error
	ZRangeByScore(ctx context.Context, key string, minScore string, maxScore string) ([]string, error)
	SetNxEx(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)
	Rem(ctx context.Context, key string) error
}
