package redbuckets

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type redisValue struct {
	value    string
	expireAt time.Time
}
type mockRedis struct {
	keyStore      map[string]*redisValue
	mutex         sync.RWMutex
	setNxExErr    error
	remErr        error
	setNxExCalled int
	remCalled     int
}

func (m *mockRedis) GetKey(key string) redisValue {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return *m.keyStore[key]
}
func (m *mockRedis) ZAdd(ctx context.Context, key, member string, score float64) error {
	panic("implement me")
}

func (m *mockRedis) ZRem(ctx context.Context, key, member string) error {
	panic("implement me")
}

func (m *mockRedis) ZRangeByScore(ctx context.Context, key string, minScore string, maxScore string) ([]string, error) {
	panic("implement me")
}

func newMockRedis() *mockRedis {
	return &mockRedis{
		keyStore: make(map[string]*redisValue),
	}
}

func (m *mockRedis) SetNxEx(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.setNxExCalled++
	if m.setNxExErr != nil {
		return false, m.setNxExErr
	}
	if v, exists := m.keyStore[key]; exists {
		if v.expireAt.After(time.Now()) {
			return false, nil
		}
	}
	m.keyStore[key] = &redisValue{value: value, expireAt: time.Now().Add(ttl)}
	return true, nil
}

func (m *mockRedis) Expire(ctx context.Context, key string, ttl time.Duration) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if v, ok := m.keyStore[key]; ok {
		v.expireAt = time.Now().Add(ttl)
		return nil
	}
	return errors.New("key not found " + key)
}
func (m *mockRedis) Delete(ctx context.Context, key string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.remCalled++
	if m.remErr != nil {
		return m.remErr
	}
	delete(m.keyStore, key)
	return nil
}

func TestBucketLock(t *testing.T) {
	tests := []struct {
		name          string
		mockSetup     func(m *mockRedis)
		expectedError error
		verify        func(t *testing.T, mock *mockRedis)
	}{
		{
			name: "successful lock",
			mockSetup: func(m *mockRedis) {
				m.setNxExErr = nil
			},
			expectedError: nil,
			verify: func(t *testing.T, mock *mockRedis) {
				if mock.setNxExCalled != 1 {
					t.Errorf("expected SetNxEx to be called once, got %d", mock.setNxExCalled)
				}
				if len(mock.keyStore) != 1 {
					t.Errorf("expected 1 key in store, got %d", len(mock.keyStore))
				}
			},
		},
		{
			name: "lock already exists (retry)",
			mockSetup: func(m *mockRedis) {
				m.keyStore["red-bucket1"] = &redisValue{value: "red-bucket1", expireAt: time.Now().Add(time.Second * 10)}
				m.setNxExErr = nil
			},
			expectedError: nil,
			verify: func(t *testing.T, mock *mockRedis) {
				if mock.setNxExCalled != 1 {
					t.Errorf("expected SetNxEx to be called once, got %d", mock.setNxExCalled)
				}
			},
		},
		{
			name: "failure due to redis error",
			mockSetup: func(m *mockRedis) {
				m.setNxExErr = errors.New("redis failure")
			},
			expectedError: nil,
			verify: func(t *testing.T, mock *mockRedis) {
				if mock.setNxExCalled != 1 {
					t.Errorf("expected SetNxEx to be called once, got %d", mock.setNxExCalled)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockRedis := newMockRedis()
			if tt.mockSetup != nil {
				tt.mockSetup(mockRedis)
			}

			bucket := NewBucket(mockRedis, "red", "instance1", 1, time.Second, func(s string) {}, func(s string) {})
			err := bucket.LockAndKeep(t.Context())
			if !errors.Is(err, tt.expectedError) {
				t.Fatalf("expected error %v, got %v", tt.expectedError, err)
			}
			if tt.verify != nil {
				tt.verify(t, mockRedis)
			}
		})
	}
}

func TestBucketUnlockOnContextCancel(t *testing.T) {

	mockRedis := newMockRedis()
	bucket := NewBucket(mockRedis, "red", "instance1", 1, time.Second*2,
		func(s string) {}, func(s string) {})
	ctx, cancel := context.WithCancel(context.Background())

	if err := bucket.LockAndKeep(ctx); err != nil {
		t.Fatal(err)
	}
	if "instance1" != mockRedis.GetKey("red1").value {
		t.Fatalf("expected red-bucket1, got %s", mockRedis.keyStore["red-bucket1"].value)
	}
	time.Sleep(time.Second * 3)

	v := mockRedis.GetKey("red1")
	if "instance1" != v.value {
		t.Fatalf("expected red-bucket1, got %s", v.value)
	}
	if v.expireAt.Before(time.Now()) {
		t.Fatalf("expired key %v > %v", time.Now(), v.expireAt)
	}
	cancel()
	time.Sleep(time.Second * 3)
	v = mockRedis.GetKey("red1")
	if !v.expireAt.Before(time.Now()) {
		t.Fatalf("expired key %v > %v", time.Now(), v.expireAt)
	}

}
