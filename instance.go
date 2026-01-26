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
	heartBeatPeriod    time.Duration
	errorHandler       func(string)
	debug              func(string)
	lastInstanceIndex  int
	lastInstancesCount int
	bucketsTotalCount  int
	wg                 *sync.WaitGroup
	stop               chan struct{}
	bucketLockTTL      time.Duration
	redisPrefix        string
	lastRegistration   time.Time
	instanceMux        *sync.Mutex
	initialUpdateDelay time.Duration
}
type Options func(*Instance)

func WithDebug(debug func(string)) Options {
	return func(i *Instance) {
		i.debug = debug
	}
}
func WithInitialUpdateDelay(d time.Duration) Options {
	return func(i *Instance) {
		i.initialUpdateDelay = d
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
		i.heartBeatPeriod = registerPeriod
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

const defaultRedisPrefix = "red-buckets"
const defaultBucketsAmount = 1024

func NewInstance(redis Redis, listKey string, options ...Options) (*Instance, error) {
	if listKey == "" {
		return nil, errors.New("list key couldn't be empty")
	}
	if redis == nil {
		return nil, errors.New("redis couldn't be nil")
	}
	inst := &Instance{
		redis:              redis,
		listKey:            listKey,
		id:                 uuid.New().String(),
		buckets:            make(map[uint16]*Bucket),
		wg:                 &sync.WaitGroup{},
		errorHandler:       func(str string) {},
		debug:              func(str string) {},
		heartBeatPeriod:    time.Second,
		registerTimeout:    time.Second * 3, // timeout for registration request
		initialUpdateDelay: time.Second,     // pause before update buckets to avoid a rebalance storm
		unRegisterTimeout:  time.Second * 10,
		rangeTimeout:       time.Second * 3, // redis zrange request timeout
		bucketLockTTL:      time.Second * 10,
		bucketsTotalCount:  defaultBucketsAmount,
		redisPrefix:        defaultRedisPrefix,
		instanceMux:        &sync.Mutex{},
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
	if inst.bucketsTotalCount < 1 {
		return nil, errors.New("bucket count couldn't be less than 1")
	}
	if inst.bucketLockTTL < time.Second*2 {
		return nil, errors.New("bucket lock ttl couldn't be less than 2 seconds")
	}

	return inst, nil
}

func (i *Instance) ID() string {
	return i.id
}
func (i *Instance) LastRegistration() time.Time {
	i.instanceMux.Lock()
	defer i.instanceMux.Unlock()
	return i.lastRegistration
}

// Buckets returns a list of IDs for lockedAt buckets and a function to release the read lock on the buckets map.
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

// BucketFulfillment returns the percentage of lockedAt buckets
// in case of no buckets assigned to instance returns -1
func (i *Instance) BucketFulfillment() int {
	i.bucketsMux.RLock()
	defer i.bucketsMux.RUnlock()
	bucketsCount := len(i.buckets)
	if bucketsCount == 0 {
		return -1
	}

	lockedCount := 0
	for _, bucket := range i.buckets {
		if bucket != nil && bucket.IsLocked() {
			lockedCount++
		}
	}
	return (lockedCount * 100) / bucketsCount
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
	if len(i.buckets) > 0 {
		for _, bucket := range i.buckets {
			if err := bucket.Unlock(); err != nil {
				i.errorHandler(fmt.Sprintf("unregister: unlock bucket %d: %s", bucket.id, err.Error()))
			}
		}
		clear(i.buckets)
	}
	i.bucketsMux.Unlock()
	if err := i.redis.ZRem(ctx, i.listKey, i.id); err != nil {
		return fmt.Errorf("unregister instance: %w", err)
	}
	return nil
}
func (i *Instance) Run(ctx context.Context) error {
	// First registration attempt
	if err := i.registerInstance(ctx); err != nil {
		return fmt.Errorf("register instance: %w", err)
	}
	i.instanceMux.Lock()
	i.lastRegistration = time.Now()
	i.instanceMux.Unlock()

	time.Sleep(i.initialUpdateDelay)

	// Initial rebalance
	i.update(ctx)
	i.wg.Add(2)
	go func() {
		defer i.wg.Done()
		i.intanceRegistrationRoutine(ctx)
	}()
	go func() {
		defer i.wg.Done()
		i.updateRoutine(ctx)
	}()
	i.wg.Wait()
	return nil
}

func (i *Instance) updateRoutine(ctx context.Context) {
	t := time.NewTicker(i.heartBeatPeriod)
	defer t.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-t.C:
			i.update(ctx)
		}
	}
}

func (i *Instance) intanceRegistrationRoutine(ctx context.Context) {
	t := time.NewTicker(i.heartBeatPeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			i.instanceMux.Lock()
			unRegisterCtx, cancel := context.WithTimeout(context.Background(), i.unRegisterTimeout)
			if err := i.unRegisterInstance(unRegisterCtx); err != nil {
				i.errorHandler(fmt.Sprintf("de-register instance: %s", err.Error()))
			}
			cancel()
			i.instanceMux.Unlock()
			return
		case <-t.C:
			i.instanceMux.Lock()
			if err := i.registerInstance(ctx); err != nil {
				i.errorHandler(fmt.Sprintf("register instance: %s", err.Error()))
			} else {
				i.lastRegistration = time.Now()
			}
			i.instanceMux.Unlock()
		}
	}
}
func (i *Instance) update(ctx context.Context) {
	i.instanceMux.Lock()
	defer i.instanceMux.Unlock()
	if i.lastRegistration.Before(time.Now().Add(-i.unRegisterTimeout)) {
		i.errorHandler("update: last registration is too old, skip rebalance")
		return
	}

	rebalance, instanceIndex, instanceCount := i.refreshInstances(ctx, i.lastInstancesCount, i.lastInstancesCount)
	if rebalance {
		i.rebalance(ctx, i.targetBuckets(instanceIndex, instanceCount))
	}
	i.lastInstanceIndex = instanceIndex
	i.lastInstancesCount = instanceCount
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
		i.debug("rebalance new buckets " + fmt.Sprintf("%d - %d",
			targetBuckets[0], targetBuckets[len(targetBuckets)-1]))
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
				i.errorHandler(fmt.Sprintf("add bucket: lock bucket %d: %s", b.id, err.Error()))
			}
		}

		i.buckets = newBuckets
		return
	}

	// 1,2
	for id, bucket := range i.buckets {
		if _, ok := newBuckets[id]; !ok { // 1=
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
				i.errorHandler(fmt.Sprintf("add bucket: lock bucket %d: %s", b.id, err.Error()))
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
		list[bucket.id] = bucket.StillLocked()
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
	remainder := i.bucketsTotalCount % instanceCount

	start := instanceIndex*perInstance + min(instanceIndex, remainder)
	count := perInstance
	if instanceIndex < remainder {
		count++
	}

	targetBuckets := make([]uint16, 0, count)
	for n := range count {
		targetBuckets = append(targetBuckets, uint16(start+n))
	}
	return targetBuckets
}
func (i *Instance) refreshInstances(ctx context.Context, lastInstanceIndex, lastInstancesCount int) (bool, int, int) {
	ctx, cancel := context.WithTimeout(ctx, i.rangeTimeout)
	defer cancel()
	instances, err := i.redis.ZRangeByScore(
		ctx,
		i.listKey,
		strconv.FormatInt(time.Now().Add(-i.heartBeatPeriod).Unix(), 10),
		"+inf")
	if err != nil {
		i.errorHandler(fmt.Sprintf("check instance rebalance: %s", err.Error()))
		return false, lastInstanceIndex, lastInstancesCount
	}
	slices.Sort(instances)
	currentIndex := slices.Index(instances, i.id)
	if currentIndex == -1 {
		i.errorHandler(fmt.Sprintf("check instance rebalance: current instance id not found! %s", i.id))
		return false, lastInstanceIndex, lastInstancesCount
	}
	// nothing changed in my position
	if currentIndex == lastInstanceIndex && len(instances) == lastInstancesCount {
		return false, lastInstanceIndex, lastInstancesCount
	}

	return true, currentIndex, len(instances)
}
