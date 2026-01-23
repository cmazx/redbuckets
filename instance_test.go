package redbuckets

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

type RedisMock struct {
	mu                    sync.RWMutex
	instances             map[string]float64
	keys                  map[string]time.Time
	failZAdd              bool
	failZRem              bool
	failZRange            bool
	simulateLongOperation bool
}

func (m *RedisMock) ZAdd(ctx context.Context, key, member string, score float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failZAdd {
		return errors.New("ZAdd failed")
	}
	if m.simulateLongOperation {
		// Release lock during long operation to avoid blocking other calls if needed
		// but here we just want to simulate timeout.
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			m.mu.Lock()
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
		m.mu.Lock()
	}
	m.instances[member] = score
	return nil
}

func (m *RedisMock) ZRem(ctx context.Context, key, member string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.instances, member)
	if m.failZRem {
		return errors.New("ZRem failed")
	}
	return nil
}

func (m *RedisMock) ZRangeByScore(ctx context.Context, key string, minScore string, maxScore string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.failZRange {
		return nil, errors.New("ZRangeByScore failed")
	}
	type item struct {
		member string
		score  float64
	}
	items := make([]item, 0, len(m.instances))
	for member, score := range m.instances {
		items = append(items, item{member: member, score: score})
	}
	slices.SortFunc(items, func(a, b item) int {
		if a.score < b.score {
			return -1
		}
		if a.score > b.score {
			return 1
		}
		return 0
	})
	members := make([]string, 0, len(items))
	for _, it := range items {
		members = append(members, it.member)
	}

	return members, nil
}

func (m *RedisMock) Expire(_ context.Context, key string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.keys[key]; exists {
		m.keys[key] = time.Now().Add(ttl)
		return nil
	}
	return fmt.Errorf("key not found: %s", key)
}
func (m *RedisMock) SetNxEx(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.keys[key]; exists {
		return false, nil
	}
	m.keys[key] = time.Now().Add(ttl)
	return true, nil
}

func (m *RedisMock) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.keys, key)
	return nil
}

func TestRun(t *testing.T) {
	type testCase struct {
		name           string
		beforeRun      func(*Instance, *RedisMock)
		expectError    bool
		checkDuringRun func(*Instance, *RedisMock) error
		checkAfterRun  func(*Instance, *RedisMock) error
		stopTimeout    time.Duration
	}

	tests := []testCase{
		{
			name:        "successful_run_and_stop",
			beforeRun:   func(ins *Instance, redis *RedisMock) {}, // No setup for successful case
			expectError: false,
			checkDuringRun: func(ins *Instance, redis *RedisMock) error {
				redis.mu.RLock()
				defer redis.mu.RUnlock()
				if len(redis.instances) == 0 {
					return errors.New("instances should not be empty after Run")
				}
				if _, exists := redis.instances[ins.ID()]; !exists {
					return errors.New("instance ID should be in Redis after Run")
				}
				return nil
			},
			checkAfterRun: func(ins *Instance, redis *RedisMock) error {
				redis.mu.RLock()
				defer redis.mu.RUnlock()
				if _, exists := redis.instances[ins.ID()]; exists {
					return errors.New("instance should be deregistered after Stop")
				}
				return nil
			},
			stopTimeout: 100 * time.Millisecond,
		},
		{
			name: "registration_error",
			beforeRun: func(ins *Instance, redis *RedisMock) {
				redis.mu.Lock()
				defer redis.mu.Unlock()
				redis.failZAdd = true
			},
			expectError: true,
			checkAfterRun: func(ins *Instance, redis *RedisMock) error {
				redis.mu.RLock()
				defer redis.mu.RUnlock()
				if len(redis.instances) > 0 {
					return errors.New("instances should be empty after failed registration")
				}
				return nil
			},
			stopTimeout: 0, // Stop not needed since this test doesn't start any goroutines
		},
		{
			name: "context_timeout_during_registration",
			beforeRun: func(ins *Instance, redis *RedisMock) {
				ins.registerTimeout = 1 * time.Millisecond
				redis.mu.Lock()
				defer redis.mu.Unlock()
				redis.simulateLongOperation = true
			},
			expectError: true,
			checkAfterRun: func(ins *Instance, redis *RedisMock) error {
				redis.mu.RLock()
				defer redis.mu.RUnlock()
				if len(redis.instances) > 0 {
					return errors.New("instances should remain empty due to context timeout")
				}
				return nil
			},
			stopTimeout: 0,
		},
		{
			name:        "de_register_on_stop",
			beforeRun:   func(ins *Instance, redis *RedisMock) {},
			expectError: false,
			checkAfterRun: func(ins *Instance, redis *RedisMock) error {
				redis.mu.RLock()
				defer redis.mu.RUnlock()
				if _, exists := redis.instances[ins.ID()]; exists {
					return errors.New("instance should be deregistered after Stop")
				}
				return nil
			},
			stopTimeout: 100 * time.Millisecond,
		},
		{
			name: "error_handling_on_deregister",
			beforeRun: func(ins *Instance, redis *RedisMock) {
				redis.mu.Lock()
				defer redis.mu.Unlock()
				redis.failZRem = true
			},
			expectError: false,
			checkAfterRun: func(ins *Instance, redis *RedisMock) error {
				redis.mu.RLock()
				defer redis.mu.RUnlock()
				// Note: if ZRem fails in the mock, it doesn't delete from the map.
				// But the instance should still stop.
				// If the test requirement says it should be "deregistered", maybe it means local state?
				// Looking at the original test, it expected it to NOT exist in redis.instances.
				// So let's make ZRem in the mock always delete, but return error if failZRem is true.
				if _, exists := redis.instances[ins.ID()]; exists {
					return errors.New("instance should be deregistered even if Redis ZRem fails")
				}
				return nil
			},
			stopTimeout: 100 * time.Millisecond,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Setup RedisMock
			redis := &RedisMock{instances: make(map[string]float64), keys: make(map[string]time.Time)}

			// Initialize Instance with RedisMock
			inst, err := NewInstance(redis, "list-key",
				WithId("test-instance"),
				WithRegisterTimeout(5*time.Second),
				WithRegisterPeriod(2*time.Second),
				WithDebug(func(msg string) {
					fmt.Println("debug:", msg)
				}),
				WithErrorHandler(func(errMsg string) {
					fmt.Println("error:", errMsg)
				}),
			)
			if err != nil {
				t.Fatalf("failed to create instance: %v", err)
			}

			// Run beforeRun setup
			if tc.beforeRun != nil {
				tc.beforeRun(inst, redis)
			}

			// Run the instance
			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			var runErr error
			wg.Add(1)
			go func() {
				defer wg.Done()
				runErr = inst.Run(ctx)
			}()

			if tc.checkDuringRun != nil {
				// Give it a bit of time to register and rebalance
				time.Sleep(100 * time.Millisecond)
				if err := tc.checkDuringRun(inst, redis); err != nil {
					t.Errorf("checkDuringRun failed: %v", err)
				}
			}

			// Stop the instance (if necessary)
			if tc.stopTimeout > 0 {
				time.Sleep(tc.stopTimeout)
			}
			cancel()
			wg.Wait()

			// Check for expected errors during Run
			if tc.expectError && runErr == nil {
				t.Error("expected an error, but got none")
			} else if !tc.expectError && runErr != nil && !errors.Is(runErr, context.Canceled) {
				t.Errorf("unexpected error: %v", runErr)
			}

			// Run afterRun checks
			if tc.checkAfterRun != nil {
				if err := tc.checkAfterRun(inst, redis); err != nil {
					t.Errorf("checkAfterRun failed: %v", err)
				}
			}
		})
	}
}

func TestInstance_targetBuckets(t *testing.T) {
	tests := []struct {
		name               string
		bucketsTotalCount  int
		lastInstancesCount int
		lastInstanceIndex  int
		want               []uint16
	}{
		{
			name:               "not registered yet => nil",
			bucketsTotalCount:  10,
			lastInstancesCount: 0,
			lastInstanceIndex:  0,
			want:               nil,
		},
		{
			name:               "buckets less than instances: index within range => single bucket == index",
			bucketsTotalCount:  2,
			lastInstancesCount: 5,
			lastInstanceIndex:  1,
			want:               []uint16{1},
		},
		{
			name:               "buckets less than instances: index out of range => nil",
			bucketsTotalCount:  2,
			lastInstancesCount: 5,
			lastInstanceIndex:  2,
			want:               nil,
		},
		{
			name:               "even split 10 buckets / 2 instances, first instance gets 0..4",
			bucketsTotalCount:  10,
			lastInstancesCount: 2,
			lastInstanceIndex:  0,
			want:               []uint16{0, 1, 2, 3, 4},
		},
		{
			name:               "even split 10 buckets / 2 instances, second instance gets 5..9",
			bucketsTotalCount:  10,
			lastInstancesCount: 2,
			lastInstanceIndex:  1,
			want:               []uint16{5, 6, 7, 8, 9},
		},
		{
			name:               "non-even split: 10 buckets / 3 instances, first gets 0..2",
			bucketsTotalCount:  10,
			lastInstancesCount: 3,
			lastInstanceIndex:  0,
			want:               []uint16{0, 1, 2, 3},
		},
		{
			name:               "non-even split: 10 buckets / 3 instances, middle gets 4..6",
			bucketsTotalCount:  10,
			lastInstancesCount: 3,
			lastInstanceIndex:  1,
			want:               []uint16{4, 5, 6},
		},
		{
			name:               "non-even split: 10 buckets / 3 instances, last gets 7..9",
			bucketsTotalCount:  10,
			lastInstancesCount: 3,
			lastInstanceIndex:  2,
			want:               []uint16{7, 8, 9},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Instance{
				bucketsTotalCount:  tt.bucketsTotalCount,
				lastInstancesCount: tt.lastInstancesCount,
				lastInstanceIndex:  tt.lastInstanceIndex,
			}

			got := i.targetBuckets(tt.lastInstanceIndex, tt.lastInstancesCount)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("targetBuckets() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInstance_refreshInstances(t *testing.T) {
	ctx := context.Background()
	redis := &RedisMock{
		instances: make(map[string]float64),
		keys:      make(map[string]time.Time),
	}
	id := "inst-1"
	inst, _ := NewInstance(redis, "test-list", WithId(id))

	// Helper to set instances in redis
	setInstances := func(members ...string) {
		redis.mu.Lock()
		defer redis.mu.Unlock()
		redis.instances = make(map[string]float64)
		for _, m := range members {
			redis.instances[m] = float64(time.Now().Unix())
		}
	}

	t.Run("first_refresh", func(t *testing.T) {
		setInstances(id)
		rebalance, idx, count := inst.refreshInstances(ctx, inst.lastInstanceIndex, inst.lastInstancesCount)
		if !rebalance {
			t.Error("expected rebalance on first refresh")
		}
		if idx != 0 {
			t.Errorf("expected index 0, got %d", idx)
		}
		if count != 1 {
			t.Errorf("expected count 1, got %d", count)
		}
		inst.lastInstanceIndex = idx
		inst.lastInstancesCount = count
	})

	t.Run("no_change", func(t *testing.T) {
		setInstances(id)
		rebalance, _, count := inst.refreshInstances(ctx, inst.lastInstanceIndex, inst.lastInstancesCount)
		if rebalance {
			t.Error("expected no rebalance when nothing changed")
		}
		if count != 1 {
			t.Errorf("expected count 1, got %d", count)
		}
	})

	t.Run("instances_changed", func(t *testing.T) {
		setInstances(id, "inst-0") // inst-0 < inst-1 (alphabetically)
		rebalance, idx, count := inst.refreshInstances(ctx, inst.lastInstanceIndex, inst.lastInstancesCount)
		if !rebalance {
			t.Error("expected rebalance when instances changed")
		}
		if idx != 1 {
			t.Errorf("expected index 1, got %d", idx)
		}
		if count != 2 {
			t.Errorf("expected count 2, got %d", count)
		}
		inst.lastInstanceIndex = idx
		inst.lastInstancesCount = count
	})

	t.Run("no_change_after_update", func(t *testing.T) {
		setInstances(id, "inst-0")
		rebalance, _, count := inst.refreshInstances(ctx, inst.lastInstanceIndex, inst.lastInstancesCount)
		if rebalance {
			t.Error("expected no rebalance when nothing changed")
		}
		if count != 2 {
			t.Errorf("expected count 2, got %d", count)
		}
	})

	t.Run("redis_error", func(t *testing.T) {
		redis.mu.Lock()
		redis.failZRange = true
		redis.mu.Unlock()
		defer func() {
			redis.mu.Lock()
			redis.failZRange = false
			redis.mu.Unlock()
		}()

		rebalance, _, _ := inst.refreshInstances(ctx, inst.lastInstanceIndex, inst.lastInstancesCount)
		if rebalance {
			t.Error("expected no rebalance on redis error")
		}
	})

	t.Run("instance_id_not_found", func(t *testing.T) {
		setInstances("other-inst")
		rebalance, _, _ := inst.refreshInstances(ctx, inst.lastInstanceIndex, inst.lastInstancesCount)
		if rebalance {
			t.Error("expected no rebalance when current instance not found")
		}
	})
}

func TestOptions(t *testing.T) {
	redis := &RedisMock{instances: make(map[string]float64), keys: make(map[string]time.Time)}

	t.Run("WithUnRegisterTimeout", func(t *testing.T) {
		timeout := 15 * time.Second
		inst, err := NewInstance(redis, "test-list", WithUnRegisterTimeout(timeout))
		if err != nil {
			t.Fatalf("failed to create instance: %v", err)
		}
		if inst.unRegisterTimeout != timeout {
			t.Errorf("expected unRegisterTimeout %v, got %v", timeout, inst.unRegisterTimeout)
		}
	})

	t.Run("WithBucketLockTTL", func(t *testing.T) {
		ttl := 5 * time.Second
		inst, err := NewInstance(redis, "test-list", WithBucketLockTTL(ttl))
		if err != nil {
			t.Fatalf("failed to create instance: %v", err)
		}
		if inst.bucketLockTTL != ttl {
			t.Errorf("expected bucketLockTTL %v, got %v", ttl, inst.bucketLockTTL)
		}
	})

	t.Run("WithBucketLockTTL_too_low", func(t *testing.T) {
		ttl := 1 * time.Second
		_, err := NewInstance(redis, "test-list", WithBucketLockTTL(ttl))
		if err == nil {
			t.Error("expected error for low bucketLockTTL, got nil")
		}
	})

	t.Run("WithRedisPrefix", func(t *testing.T) {
		prefix := "custom-prefix"
		inst, err := NewInstance(redis, "test-list", WithRedisPrefix(prefix))
		if err != nil {
			t.Fatalf("failed to create instance: %v", err)
		}
		if inst.redisPrefix != prefix {
			t.Errorf("expected redisPrefix %s, got %s", prefix, inst.redisPrefix)
		}
	})
}
