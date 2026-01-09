package redbuckets

import (
	"context"
	"errors"
	"testing"
	"time"
)

type mockRedis struct {
	keyStore      map[string]string
	setNxExErr    error
	remErr        error
	setNxExCalled int
	remCalled     int
}

func (m *mockRedis) ZAdd(ctx context.Context, key, member string, score float64) error {
	// TODO implement me
	panic("implement me")
}

func (m *mockRedis) ZRem(ctx context.Context, key, member string) error {
	// TODO implement me
	panic("implement me")
}

func (m *mockRedis) ZRangeByScore(ctx context.Context, key string, minScore string, maxScore string) ([]string, error) {
	// TODO implement me
	panic("implement me")
}

func newMockRedis() *mockRedis {
	return &mockRedis{
		keyStore: make(map[string]string),
	}
}

func (m *mockRedis) SetNxEx(ctx context.Context, key string, value string, ttl time.Duration) error {
	m.setNxExCalled++
	if m.setNxExErr != nil {
		return m.setNxExErr
	}
	if _, exists := m.keyStore[key]; exists {
		return ErrRedisKeyExists
	}
	m.keyStore[key] = value
	return nil
}

func (m *mockRedis) Rem(ctx context.Context, key string) error {
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
		ctxTimeout    time.Duration
		expectedError error
		verify        func(t *testing.T, mock *mockRedis)
	}{
		{
			name: "successful lock",
			mockSetup: func(m *mockRedis) {
				m.setNxExErr = nil
			},
			ctxTimeout:    time.Second,
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
				m.keyStore["red-bucket1"] = "instance1"
				m.setNxExErr = nil
			},
			ctxTimeout:    time.Second,
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
			ctxTimeout:    time.Second,
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
			ctx, cancel := context.WithTimeout(context.Background(), tt.ctxTimeout)
			defer cancel()

			err := bucket.Lock(ctx)
			if !errors.Is(err, tt.expectedError) {
				t.Fatalf("expected error %v, got %v", tt.expectedError, err)
			}
			if tt.verify != nil {
				tt.verify(t, mockRedis)
			}
		})
	}
}
