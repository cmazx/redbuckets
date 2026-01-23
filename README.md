# Red Buckets

`redbuckets` is a Go library for distributed bucket management using Redis. It allows you to distribute a set of buckets (tasks, shards, etc.) across multiple service instances, automatically rebalancing them as instances join or leave.

<img width="611" height="412" alt="group-proccessing" src="https://github.com/user-attachments/assets/8139cd32-5cf5-4972-bbbe-8e52b7ce4119" />

### Features

- **Distributed Locking**: Ensures each bucket is owned by only one instance at a time using Redis.
- **Automatic Rebalancing**: When instances are added or removed, buckets are automatically redistributed.
- **Heartbeat & TTL**: Instances regularly refresh their presence in Redis, and bucket locks have a TTL to handle instance crashes.
- **Customizable**: Configure bucket count, refresh periods, lock TTLs, and more.

### Installation

```bash
go get github.com/cmazx/redbuckets
```

### Basic Usage

To use `redbuckets`, you need to implement the `Redis` interface or use a compatible client.

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/cmazx/redbuckets"
)

func main() {
    // Initialize your Redis client that implements redbuckets.Redis interface
    var redis redbuckets.Redis = yourRedisClient 

    // Create a new instance
    svc, err := redbuckets.NewInstance(redis, "my-service-shards",
        redbuckets.WithBuckets(1024),          // Total number of buckets
        redbuckets.WithRegisterPeriod(5*time.Second),
    )
    if err != nil {
        log.Fatal(err)
    }

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Start the instance management
    // This will block until the context is canceled or a fatal error occurs.
    go func() {
        if err := svc.Run(ctx); err != nil {
            log.Printf("Service run failed: %v", err)
        }
    }()

    // Periodically check which buckets this instance owns
    for {
        buckets, unlock := svc.Buckets()
        for _, bucketID := range buckets {
            // Process tasks for this bucket
            _ = bucketID
        }
        unlock() // Important: release the read lock on the buckets map
        
        select {
        case <-ctx.Done():
            return
        case <-time.After(time.Second):
        }
    }
}
```

### Configuration Options

The `NewInstance` function accepts several functional options:

- `WithId(id string)`: Sets a custom unique ID for the instance. Default is a random UUID.
- `WithBuckets(count uint16)`: Sets the total number of buckets to distribute. Default is 1024.
- `WithBucketLockTTL(ttl time.Duration)`: Sets the TTL for bucket locks in Redis. Default is 10s.
- `WithRegisterPeriod(period time.Duration)`: Sets how often the instance refreshes its registration in Redis. Default is 1s.
- `WithRegisterTimeout(timeout time.Duration)`: Sets the timeout for registration operations. Default is 3s.
- `WithUnRegisterTimeout(timeout time.Duration)`: Sets the timeout for unregistration operations. Default is 10s.
- `WithRedisPrefix(prefix string)`: Sets the prefix for Redis keys used for bucket locking. Default is `red-buckets`.
- `WithDebug(debug func(string))`: Sets a debug logger.
- `WithErrorHandler(handler func(string))`: Sets an error handler.

### How it Works

1. **Registration**: Each instance registers itself in a Redis Sorted Set (`listKey`) with a timestamp as its score.
2. **Discovery**: Instances periodically fetch the list of active instances from the Sorted Set (filtering out stale ones).
3. **Allocation**: Each instance determines its range of buckets based on its position in the sorted list of instances.
4. **Locking**: The instance attempts to acquire locks in Redis for the buckets it should own. It also periodically refreshes these locks.
5. **Rebalancing**: If the set of active instances changes, each instance recalculates its assigned buckets and releases or acquires locks accordingly.

### Redis Interface

You need to provide an implementation of the following interface:

```go
type Redis interface {
	ZAdd(ctx context.Context, key, member string, score float64) error
	ZRem(ctx context.Context, key, member string) error
	ZRangeByScore(ctx context.Context, key string, minScore string, maxScore string) ([]string, error)
	SetNxEx(ctx context.Context, key string, value string, ttl time.Duration) (bool, error)
	Delete(ctx context.Context, key string) error
	Expire(ctx context.Context, s string, ttl time.Duration) error
}
```
