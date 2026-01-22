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
	redis        Redis
	lockTTL      time.Duration
	redisPrefix  string
	locked       bool
	mutex        sync.Mutex
	unlockCh     chan struct{}
	unlockedCh   chan struct{}
}

func NewBucket(redis Redis, redisPrefix string, instanceID string, id uint16, ttl time.Duration,
	debug func(string), errorHandler func(string)) *Bucket {
	return &Bucket{
		id:           id,
		redis:        redis,
		debug:        debug,
		errorHandler: errorHandler,
		lockTTL:      ttl,
		instanceID:   instanceID,
		redisPrefix:  redisPrefix,
		unlockCh:     make(chan struct{}, 1),
		unlockedCh:   make(chan struct{}, 1),
	}
}

func (b *Bucket) LockAndKeep(ctx context.Context) error {
	b.mutex.Lock()
	if b.locked {
		b.mutex.Unlock()
		return nil
	}
	b.lock()
	b.mutex.Unlock()

	go func() {
		defer b.debug("stop keep goroutine")
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-b.unlockCh:
				ticker.Stop()
				return
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				b.keep(ctx)
				ticker.Reset(time.Second)
			}
		}
	}()
	b.unlockedCh <- struct{}{}

	return nil
}
func (b *Bucket) lock() {
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
		return
	}
	b.debug("locked")
	b.locked = true

	return
}
func (b *Bucket) keep(ctx context.Context) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// try to lock in case of late bucket availability
	if !b.locked {
		b.lock()
		return
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := b.redis.Expire(ctx, b.getRedisKey(), b.lockTTL); err != nil {
		b.errorHandler(err.Error())
		b.lock() // try to lock in case of network loss
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
	b.locked = false
	b.unlockCh <- struct{}{}
	<-b.unlockedCh
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.redis.Delete(ctx, b.getRedisKey()); err != nil {
		return fmt.Errorf("delete bucket lock key: %w", err)
	}
	return nil
}

func (b *Bucket) IsLocked() bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.locked
}
