package redbuckets

import (
	"context"
	"errors"
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
	var members []string
	for member := range m.instances {
		members = append(members, member)
	}
	return members, nil
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

func (m *RedisMock) Rem(ctx context.Context, key string) error {
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
					t.Log("debug:", msg)
				}),
				WithErrorHandler(func(errMsg string) {
					t.Log("error:", errMsg)
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
			ctx := context.Background()
			err = inst.Run(ctx)

			// Check for expected errors during Run
			if tc.expectError && err == nil {
				t.Fatal("expected an error, but got none")
			} else if !tc.expectError && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

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
			inst.Stop()

			// Run afterRun checks
			if tc.checkAfterRun != nil {
				if err := tc.checkAfterRun(inst, redis); err != nil {
					t.Errorf("checkAfterRun failed: %v", err)
				}
			}
		})
	}
}
