package redbuckets

import (
	"context"
	"errors"
	"strconv"
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
	instance     string
}

func NewBucket(redis Redis, redisPrefix string, instance string, id uint16, ttl time.Duration, debug func(string), errorHandler func(string)) *Bucket {
	return &Bucket{
		id:           id,
		redis:        redis,
		debug:        debug,
		errorHandler: errorHandler,
		unlockCh:     make(chan struct{}, 1),
		unlockedCh:   make(chan struct{}, 1),
		lockTTL:      ttl,
		instance:     instance,
		redisPrefix:  redisPrefix,
	}
}

func (b *Bucket) Unlock() error {
	b.unlockCh <- struct{}{}
	<-b.unlockedCh
	return nil
}

func (b *Bucket) Lock() error {
	b.lock()

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-b.unlockCh:
				b.unlock()
				b.unlockedCh <- struct{}{}
				return
			case <-ticker.C:
				b.lock()
			}
		}
	}()

	return nil
}
func (b *Bucket) lock() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.redis.SetNxEx(ctx, b.redisPrefix+strconv.Itoa(int(b.id)), b.instanceID, b.lockTTL); err != nil {
		// still not unlocked by another bucket
		if errors.Is(err, ErrRedisKeyExists) {
			return
		}
		b.errorHandler(err.Error())
	}

	return
}

func (b *Bucket) unlock() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.redis.Rem(ctx, b.redisPrefix+strconv.Itoa(int(b.id))); err != nil {
		b.errorHandler(err.Error())
	}
}
