package redbuckets

import (
	"context"
	"fmt"
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
				b.refreshExpiration()
				ticker.Reset(time.Second)
			}
		}
	}()

	return nil
}
func (b *Bucket) lock() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := b.redis.SetNxEx(ctx, b.redisPrefix+strconv.Itoa(int(b.id)), b.instanceID, b.lockTTL)
	if err != nil {
		b.errorHandler(err.Error())
		return
	}
	if !success {
		b.errorHandler(fmt.Sprintf("bucket %d already locked", b.id))
	}

	return
}
func (b *Bucket) refreshExpiration() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := b.redis.Expire(ctx, b.redisPrefix+strconv.Itoa(int(b.id)), b.lockTTL); err != nil {
		b.errorHandler(err.Error())
		b.lock() // try to lock
		return
	}

	return
}

func (b *Bucket) unlock() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.redis.Delete(ctx, b.redisPrefix+strconv.Itoa(int(b.id))); err != nil {
		b.errorHandler(err.Error())
	}
}
