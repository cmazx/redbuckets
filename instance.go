package redbuckets

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Instance struct {
	redis              Redis
	id                 string
	buckets            map[uint16]*Bucket
	bucketsMux         sync.RWMutex
	registerMux        sync.Mutex
	listKey            string
	registerTimeout    time.Duration
	unRegisterTimeout  time.Duration
	rangeTimeout       time.Duration
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
		i.errorHandler = func(s string) {
			errorHandler("i" + i.id + " " + s)
		}
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
func WithUnRegisterTimeout(timeout time.Duration) Options {
	return func(i *Instance) {
		i.unRegisterTimeout = timeout
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
		errorHandler:      func(str string) {},
		debug:             func(str string) {},
		registerPeriod:    time.Second,
		registerTimeout:   time.Second * 3,
		unRegisterTimeout: time.Second * 10,
		rangeTimeout:      time.Second * 3,
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
	if inst.bucketsTotalCount%2 != 0 {
		return nil, errors.New("bucket count must be even")
	}
	if inst.bucketLockTTL < time.Second*2 {
		return nil, errors.New("bucket lock ttl couldn't be less than 2 seconds")
	}

	return inst, nil
}

func (i *Instance) ID() string {
	return i.id
}
func (i *Instance) Buckets() ([]uint16, func()) {
	i.bucketsMux.RLock()

	lockedBuckets := make([]uint16, 0, len(i.buckets))
	for _, bucket := range i.buckets {
		if bucket.IsLocked() {
			lockedBuckets = append(lockedBuckets, bucket.id)
		}
	}
	return lockedBuckets, func() { i.bucketsMux.RUnlock() }
}
func (i *Instance) BucketFulfillment() int {
	i.bucketsMux.RLock()
	defer i.bucketsMux.RUnlock()
	bucketsCount := len(i.buckets)
	if bucketsCount == 0 {
		return 100
	}
	buckets, freeLock := i.Buckets()
	defer freeLock()
	return (len(buckets) * 100) / bucketsCount
}

func (i *Instance) registerInstance(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, i.registerTimeout)
	defer cancel()
	i.registerMux.Lock()
	defer i.registerMux.Unlock()
	if err := i.redis.ZAdd(ctx, i.listKey, i.id, float64(time.Now().Unix())); err != nil {
		return fmt.Errorf("register instance: %w", err)
	}
	return nil
}
func (i *Instance) unRegisterInstance(ctx context.Context) error {
	i.bucketsMux.Lock()
	defer i.bucketsMux.Unlock()
	if len(i.buckets) > 0 {
		for _, bucket := range i.buckets {
			if err := bucket.Unlock(); err != nil {
				i.errorHandler(fmt.Sprintf("remove bucket: unlock bucket %d: %s", bucket.id, err.Error()))
			}
		}
		i.buckets = nil
	}
	if err := i.redis.ZRem(ctx, i.listKey, i.id); err != nil {
		return fmt.Errorf("register instance: %w", err)
	}
	return nil
}
func (i *Instance) Run(ctx context.Context) error {
	// first registration and rebalance
	if err := i.registerInstance(ctx); err != nil {
		return fmt.Errorf("register instance: %w", err)
	}
	i.refreshInstances(ctx)
	i.rebalance(ctx, i.targetBuckets(0, 0))

	go func() {
		if err := i.registerInstance(ctx); err != nil {
			i.errorHandler(fmt.Sprintf("register instance: %s", err.Error()))
		}
	}()

	t := time.NewTicker(i.registerPeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Stop()
			registerCtx, cancel := context.WithTimeout(context.Background(), i.unRegisterTimeout)
			if err := i.unRegisterInstance(registerCtx); err != nil {
				i.errorHandler(fmt.Sprintf("de-register instance: %s", err.Error()))
			}
			cancel()
			return nil
		case <-t.C:
			if err := i.registerInstance(ctx); err != nil {
				i.errorHandler(fmt.Sprintf("register instance: %s", err.Error()))
			}
			rebalance, instanceIndex, instanceCount := i.refreshInstances(ctx)
			if rebalance {
				i.rebalance(ctx, i.targetBuckets(instanceIndex, instanceCount))
			}
			i.lastInstanceIndex = instanceIndex
			i.lastInstancesCount = instanceCount
			t.Reset(i.registerPeriod)
		}
	}
}

func (i *Instance) rebalance(ctx context.Context, targetBuckets []uint16) {
	i.debug("*rebalance to " + fmt.Sprintf("%v", targetBuckets))
	defer i.debug("*rebalance completed*")

	i.bucketsMux.Lock()
	defer i.bucketsMux.Unlock()
	// [2,3]
	newBuckets := make(map[uint16]*Bucket, len(targetBuckets))
	for _, id := range targetBuckets {
		newBuckets[id] = nil
	}
	if len(targetBuckets) > 0 {
		i.debug("rebalance new buckets " + fmt.Sprintf("%d - %d", targetBuckets[0], targetBuckets[len(targetBuckets)-1]))
	} else {
		i.debug("rebalance new buckets empty")
	}

	if len(i.buckets) == 0 {
		for _, id := range targetBuckets {
			id := id
			b := NewBucket(i.redis, i.redisPrefix, i.id, id, i.bucketLockTTL, func(s string) {
				i.debug("b" + strconv.Itoa(int(id)) + ": " + s)
			}, func(s string) {
				i.errorHandler("b" + strconv.Itoa(int(id)) + ": " + s)
			})
			newBuckets[id] = b
			// allow to get error due bucket could not be freed by other instance yet
			if err := b.LockAndKeep(ctx); err != nil {
				i.errorHandler(fmt.Sprintf("remove bucket: unlock bucket %d: %s", b.id, err.Error()))
			}
		}

		i.buckets = newBuckets
		return
	}

	// 1,2
	for id := range i.buckets {
		if _, ok := newBuckets[id]; !ok { // 1
			id := id
			bucket := i.buckets[id]
			i.debug("unlock bucket " + strconv.Itoa(int(bucket.id)))
			// in case unlock failed it just expire after some time
			if err := bucket.Unlock(); err != nil {
				i.errorHandler(fmt.Sprintf("remove bucket: unlock bucket %d: %s", bucket.id, err.Error()))
			}
		}
	}

	// 2,3
	for id := range newBuckets {
		// this bucket is new
		if _, ok := i.buckets[id]; !ok { // 3
			id := id
			b := NewBucket(i.redis, i.redisPrefix, i.id, id, i.bucketLockTTL, func(s string) {
				i.debug("b" + strconv.Itoa(int(id)) + ": " + s)
			}, func(s string) {
				i.errorHandler("b" + strconv.Itoa(int(id)) + ": " + s)
			})
			newBuckets[id] = b
			if err := b.LockAndKeep(ctx); err != nil {
				i.errorHandler(fmt.Sprintf("remove bucket: unlock bucket %d: %s", b.id, err.Error()))
			}
		} else { // 2
			newBuckets[id] = i.buckets[id]
		}
	}

	i.buckets = newBuckets
}

func (i *Instance) BucketsState() map[uint16]bool {
	i.bucketsMux.RLock()
	defer i.bucketsMux.RUnlock()
	list := make(map[uint16]bool)
	for _, bucket := range i.buckets {
		list[bucket.id] = bucket.locked
	}
	return list
}

func (i *Instance) targetBuckets(instanceIndex, instanceCount int) []uint16 {
	// not yet registered
	if instanceCount == 0 {
		return nil
	}
	if i.bucketsTotalCount < instanceCount {
		// buckets are not enough for the current instance
		if instanceIndex >= i.bucketsTotalCount {
			return nil
		}
		return []uint16{uint16(instanceIndex)}
	}

	perInstance := i.bucketsTotalCount / instanceCount
	targetBuckets := make([]uint16, perInstance)
	pos := 0
	for n := range perInstance {
		targetBuckets[pos] = uint16(instanceIndex*perInstance + n)
		pos++
	}
	return targetBuckets
}
func (i *Instance) refreshInstances(ctx context.Context) (bool, int, int) {
	ctx, cancel := context.WithTimeout(ctx, i.rangeTimeout)
	defer cancel()
	instances, err := i.redis.ZRangeByScore(
		ctx,
		i.listKey,
		strconv.FormatInt(time.Now().Add(-i.registerPeriod).Unix(), 10),
		"+inf")
	if err != nil {
		i.errorHandler(fmt.Sprintf("check instance rebalance: %s", err.Error()))
		return false, i.lastInstanceIndex, i.lastInstancesCount
	}
	slices.Sort(instances)
	currentIndex := slices.Index(instances, i.id)
	if currentIndex == -1 {
		i.errorHandler(fmt.Sprintf("check instance rebalance: current instance id not found! %s", i.id))
		return false, i.lastInstanceIndex, i.lastInstancesCount
	}
	// nothing changed in my position
	if currentIndex == i.lastInstanceIndex && len(instances) == i.lastInstancesCount {
		return false, i.lastInstanceIndex, i.lastInstancesCount
	}

	return true, currentIndex, len(instances)
}
