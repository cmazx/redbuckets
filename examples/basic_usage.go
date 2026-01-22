package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cmazx/redbuckets"
)

type RedisMock struct {
	log       []string
	instances map[string]float64
	keys      map[string]time.Time
}

func (r RedisMock) SetNxEx(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	val, ok := r.keys[key]
	if !ok || val.Before(time.Now()) {
		r.keys[key] = time.Now().Add(ttl)
		return true, nil
	}
	return false, nil
}
func (r RedisMock) Expire(_ context.Context, s string, ttl time.Duration) error {
	_, ok := r.keys[s]
	if ok {
		r.keys[s] = time.Now().Add(ttl)
	}
	return nil
}

func (r RedisMock) Delete(ctx context.Context, key string) error {
	delete(r.keys, key)
	return nil
}

func (r RedisMock) ZAdd(_ context.Context, key, member string, score float64) error {
	r.log = append(r.log, "ZAdd "+key+" "+member+fmt.Sprintf(" %f", score))
	r.instances[member] = score
	return nil
}

func (r RedisMock) ZRem(_ context.Context, key, member string) error {
	r.log = append(r.log, "ZRem "+key+" "+member)
	delete(r.instances, member)
	return nil
}

func (r RedisMock) ZRangeByScore(_ context.Context, key string, minScore string, maxScore string) ([]string, error) {
	r.log = append(r.log, "ZRange "+key+" "+minScore+" "+maxScore)
	var list []string
	for instance := range r.instances {
		list = append(list, instance)
	}
	return list, nil
}

func main() {
	redis := &RedisMock{instances: make(map[string]float64), keys: make(map[string]time.Time)}
	svc := initService(redis, "first")
	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}
	wg.Go(func() {
		if err := svc.Run(ctx); err != nil {
			log.Fatal(err)
		}
	})
	<-time.After(time.Second * 10)

	log.Println("Starting second service.....")

	// second service
	svc2 := initService(redis, "second")
	ctx2, cancel2 := context.WithCancel(context.Background())
	wg2 := sync.WaitGroup{}
	wg2.Go(func() {
		if err := svc2.Run(ctx2); err != nil {
			log.Fatal(err)
		}
	})
	<-time.After(time.Second * 10)

	log.Println("Stopping first service.....")
	cancel()
	wg.Wait()
	<-time.After(time.Second * 10)
	cancel2()
	wg2.Wait()
	<-time.After(time.Second * 10)
	log.Println("Stopping second service.....")
}

func initService(redis *RedisMock, serviceName string) *redbuckets.Instance {
	svc, err := redbuckets.NewInstance(redis, "list-key",
		redbuckets.WithId(serviceName),
		redbuckets.WithBuckets(2),
		redbuckets.WithRegisterPeriod(5*time.Second),
		redbuckets.WithDebug(func(s string) {
			log.Println(serviceName + " debug: " + s)
		}))
	if err != nil {
		log.Fatal(err)
	}
	return svc
}
