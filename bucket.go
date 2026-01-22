package redbuckets

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"
)

type Bucket struct {
	id           uint16
	instanceID   string
	debug        func(string)
	errorHandler func(string)
	unlockCh     chan struct{}
	unlockedCh   chan struct{}
	redis        Redis
	lockTTL      time.Duration
	redisPrefix  string
	locked       bool
	mutex        sync.Mutex
}

func NewBucket(redis Redis, redisPrefix string, instanceID string, id uint16, ttl time.Duration, debug func(string), errorHandler func(string)) *Bucket {
	return &Bucket{
		id:           id,
		redis:        redis,
		debug:        debug,
		errorHandler: errorHandler,
		unlockCh:     make(chan struct{}, 1),
		unlockedCh:   make(chan struct{}, 1),
		lockTTL:      ttl,
		instanceID:   instanceID,
		redisPrefix:  redisPrefix,
	}
}

func (b *Bucket) LockAndKeep(ctx context.Context) error {
	b.lock(true)

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b.keep(ctx)
				ticker.Reset(time.Second)
			}
		}
	}()

	return nil
}
func (b *Bucket) lock(internalLock bool) {
	if internalLock {
		b.mutex.Lock()
		defer b.mutex.Unlock()
	}
	if b.locked {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := b.redis.SetNxEx(ctx, b.getRedisKey(), b.instanceID, b.lockTTL)
	if err != nil {
		b.errorHandler(err.Error())
		return
	}
	if !success {
		b.debug(fmt.Sprintf("bucket %d already locked", b.id))
	}
	b.locked = true

	return
}
func (b *Bucket) keep(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// try to lock in case of late bucket availability
	if !b.locked {
		b.lock(false)
		return
	}

	if err := b.redis.Expire(ctx, b.getRedisKey(), b.lockTTL); err != nil {
		b.errorHandler(err.Error())
		b.lock(false) // try to lock in case of network loss
		return
	}

	return
}

func (b *Bucket) getRedisKey() string {
	return b.redisPrefix + strconv.Itoa(int(b.id))
}

func (b *Bucket) Unlock() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.redis.Delete(ctx, b.getRedisKey()); err != nil {
		return fmt.Errorf("delete bucket lock key: %w", err)
	}
	b.locked = false
	return nil
}

func (b *Bucket) IsLocked() bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.locked
}
