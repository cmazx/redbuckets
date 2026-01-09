package redbuckets

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Instance struct {
	redis              Redis
	id                 string
	buckets            map[uint16]*Bucket
	bucketsMux         sync.RWMutex
	listKey            string
	registerTimeout    time.Duration
	registerPeriod     time.Duration
	errorHandler       func(string)
	debug              func(string)
	lastInstanceIndex  int
	lastInstancesCount int
	bucketsTotalCount  int
	wg                 *sync.WaitGroup
	stop               chan struct{}
	bucketLockTTL      time.Duration
	redisPrefix        string
}
type Options func(*Instance)

func WithDebug(debug func(string)) Options {
	return func(i *Instance) {
		i.debug = debug
	}
}
func WithErrorHandler(errorHandler func(string)) Options {
	return func(i *Instance) {
		i.errorHandler = errorHandler
	}
}
func WithRegisterPeriod(registerPeriod time.Duration) Options {
	return func(i *Instance) {
		i.registerPeriod = registerPeriod
	}
}
func WithRegisterTimeout(registerTimeout time.Duration) Options {
	return func(i *Instance) {
		i.registerTimeout = registerTimeout
	}
}
func WithBucketLockTTL(ttl time.Duration) Options {
	return func(i *Instance) {
		i.bucketLockTTL = ttl
	}
}
func WithBuckets(count uint16) Options {
	return func(i *Instance) {
		i.bucketsTotalCount = int(count)
	}
}
func WithId(id string) Options {
	return func(i *Instance) {
		i.id = id
	}
}
func WithRedisPrefix(prefix string) Options {
	return func(i *Instance) {
		i.redisPrefix = prefix
	}
}

func NewInstance(redis Redis, listKey string, options ...Options) (*Instance, error) {
	if listKey == "" {
		return nil, errors.New("list key couldn't be empty")
	}
	if redis == nil {
		return nil, errors.New("redis couldn't be nil")
	}
	inst := &Instance{
		redis:             redis,
		listKey:           listKey,
		id:                uuid.New().String(),
		buckets:           make(map[uint16]*Bucket),
		wg:                &sync.WaitGroup{},
		stop:              make(chan struct{}, 1),
		errorHandler:      func(str string) { log.Println("red buckets error: " + str) },
		debug:             func(str string) {},
		registerPeriod:    time.Second,
		registerTimeout:   time.Second * 2,
		bucketLockTTL:     time.Second * 10,
		bucketsTotalCount: 1024,
		redisPrefix:       "red-buckets",
	}
	for _, option := range options {
		option(inst)
	}

	if inst.bucketsTotalCount < 1 {
		return nil, errors.New("bucket count couldn't be less than 1")
	}

	return inst, nil
}

func (i *Instance) ID() string {
	return i.id
}
func (i *Instance) Buckets() iter.Seq[uint16] {
	i.bucketsMux.RLock()
	defer i.bucketsMux.RUnlock()
	return maps.Keys(i.buckets)
}

func (i *Instance) registerInstance(ctx context.Context) error {
	if err := i.redis.ZAdd(ctx, i.listKey, i.id, float64(time.Now().Unix())); err != nil {
		return fmt.Errorf("register instance: %w", err)
	}
	return nil
}
func (i *Instance) deRegisterInstance(ctx context.Context) error {
	if len(i.buckets) > 0 {
		for _, bucket := range i.buckets {
			if err := bucket.Unlock(); err != nil {
				i.errorHandler(err.Error())
			}
		}
	}
	if err := i.redis.ZRem(ctx, i.listKey, i.id); err != nil {
		return fmt.Errorf("register instance: %w", err)
	}
	return nil
}
func (i *Instance) Run(ctx context.Context) error {
	// try to register to find problems earlier
	ctx, cancel := context.WithTimeout(ctx, i.registerTimeout)
	defer cancel()
	if err := i.registerInstance(ctx); err != nil {
		return fmt.Errorf("register instance: %w", err)
	}

	i.refreshInstances(ctx)
	i.rebalance(ctx, i.targetBuckets())
	// register/refresh cycle
	i.wg.Go(func() {
		t := time.NewTicker(i.registerPeriod)
		defer t.Stop()
		for {
			select {
			case <-i.stop:
				t.Stop()
				ctx, cancel := context.WithTimeout(ctx, i.registerTimeout*2)
				if err := i.deRegisterInstance(ctx); err != nil {
					i.errorHandler(fmt.Sprintf("de-register instance: %s", err.Error()))
				}
				cancel()
				return
			case <-t.C:
				ctx, cancel := context.WithTimeout(ctx, i.registerTimeout)
				if err := i.registerInstance(ctx); err != nil {
					i.errorHandler(fmt.Sprintf("register instance: %s", err.Error()))
				}
				cancel()
				if i.refreshInstances(ctx) {
					i.rebalance(ctx, i.targetBuckets())
					t.Reset(i.registerPeriod)
				}
			}
		}
	})

	return nil
}

func (i *Instance) Stop() {
	i.debug("stopping instance")
	i.stop <- struct{}{}
	i.debug("waiting for instance to stop")
	i.wg.Wait()
	i.debug("stopped instance")
}

func (i *Instance) rebalance(ctx context.Context, targetBuckets []uint16) {
	i.debug("*rebalance to " + fmt.Sprintf("%v", targetBuckets))
	defer i.debug("*rebalance completed*")

	i.bucketsMux.Lock()
	defer i.bucketsMux.Unlock()

	wg := sync.WaitGroup{}

	newBuckets := make(map[uint16]*Bucket, len(targetBuckets))
	for _, bucket := range targetBuckets {
		newBuckets[bucket] = nil
	}

	if len(i.buckets) == 0 {
		for _, id := range targetBuckets {
			newBuckets[id] = NewBucket(i.redis, i.redisPrefix, i.id, id, i.bucketLockTTL, i.debug, i.errorHandler)
			wg.Go(func() {
				i.lockBucket(ctx, newBuckets[id])
			})
		}

		i.buckets = newBuckets
		wg.Wait()
		return
	}

	if len(newBuckets) == 10 {
		i.debug("final")
	}
	for id := range i.buckets {
		if _, ok := newBuckets[id]; !ok {
			b := i.buckets[id]
			wg.Go(func() {
				i.unlockBucket(b)
			})
		}
	}
	for id := range newBuckets {
		if _, ok := i.buckets[id]; !ok {
			newBuckets[id] = NewBucket(i.redis, i.redisPrefix, i.id, id, i.bucketLockTTL, i.debug, i.errorHandler)
			wg.Go(func() {
				i.lockBucket(ctx, newBuckets[id])
			})
		} else {
			newBuckets[id] = i.buckets[id]
		}
	}

	wg.Wait()
	i.buckets = newBuckets
}

func (i *Instance) targetBuckets() []uint16 {
	if i.bucketsTotalCount < i.lastInstancesCount {
		// buckets are not enough for the current instance
		if i.lastInstanceIndex >= i.bucketsTotalCount {
			return nil
		}
		return []uint16{uint16(i.lastInstanceIndex)}
	}

	var bucketRangeStart, bucketRangeEnd int
	bucketBatch := i.bucketsTotalCount / i.lastInstancesCount
	bucketRangeStart = (i.lastInstanceIndex) * bucketBatch
	bucketRangeEnd = (i.lastInstanceIndex+1)*bucketBatch - 1

	if bucketRangeEnd > i.bucketsTotalCount-1 {
		bucketRangeEnd = i.bucketsTotalCount - 1
	}

	targetBuckets := make([]uint16, bucketBatch)
	pos := 0
	for bucket := range bucketBatch {
		targetBuckets[pos] = uint16(bucketRangeStart + bucket)
		pos++
	}
	return targetBuckets
}
func (i *Instance) refreshInstances(ctx context.Context) bool {
	instances, err := i.redis.ZRangeByScore(ctx, i.listKey, "now-5s", "+inf")
	if err != nil {
		i.errorHandler(fmt.Sprintf("check instance rebalance: %s", err.Error()))
		return false
	}
	currentIndex := slices.Index(instances, i.id)
	if currentIndex == -1 {
		i.errorHandler(fmt.Sprintf("check instance rebalance: current instance id not found! %s", i.id))
		return false
	}
	// nothing changed in my position
	if currentIndex == i.lastInstanceIndex && len(instances) == i.lastInstancesCount {
		return false
	}
	i.lastInstanceIndex = currentIndex
	i.lastInstancesCount = len(instances)

	return true
}

func (i *Instance) lockBucket(ctx context.Context, bucket *Bucket) {
	if err := bucket.Lock(ctx); err != nil {
		i.errorHandler(fmt.Sprintf("remove bucket: unlock bucket %d: %s", bucket.id, err.Error()))
	}
}

func (i *Instance) unlockBucket(bucket *Bucket) {
	if err := bucket.Unlock(); err != nil {
		i.errorHandler(fmt.Sprintf("remove bucket: unlock bucket %d: %s", bucket.id, err.Error()))
	}
}
