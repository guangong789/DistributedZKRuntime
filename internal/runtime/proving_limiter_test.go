package runtime

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProvingLimiterAdmission(t *testing.T) {
	for _, limit := range []int{0, -1} {
		if limiter, err := NewProvingLimiter(limit); err == nil || limiter != nil {
			t.Fatalf("invalid capacity %d: limiter=%v error=%v", limit, limiter, err)
		}
	}
	limiter, err := NewProvingLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	if !limiter.TryAcquire() {
		t.Fatal("first admission failed")
	}
	if limiter.TryAcquire() {
		t.Fatal("admitted more than capacity")
	}
	limiter.Release()
	if !limiter.TryAcquire() {
		t.Fatal("released slot was not reusable")
	}
	limiter.Release()
}

func TestProvingLimiterConcurrentAdmission(t *testing.T) {
	const capacity, contenders = 3, 32
	limiter, err := NewProvingLimiter(capacity)
	if err != nil {
		t.Fatal(err)
	}
	start, release := make(chan struct{}), make(chan struct{})
	results := make(chan bool, contenders)
	var wg sync.WaitGroup
	var active atomic.Int32
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }); wg.Wait() })
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			admitted := limiter.TryAcquire()
			if admitted {
				active.Add(1)
				defer limiter.Release()
				defer active.Add(-1)
			}
			results <- admitted
			if admitted {
				<-release
			}
		}()
	}
	close(start)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	admitted := 0
	for i := 0; i < contenders; i++ {
		select {
		case ok := <-results:
			if ok {
				admitted++
			}
		case <-deadline.C:
			t.Fatal("TryAcquire blocked instead of rejecting full capacity")
		}
	}
	if admitted != capacity || active.Load() != capacity {
		t.Fatalf("admitted=%d active=%d, want %d", admitted, active.Load(), capacity)
	}
	releaseOnce.Do(func() { close(release) })
	wg.Wait()
	for i := 0; i < capacity; i++ {
		if !limiter.TryAcquire() {
			t.Fatal("concurrent admissions leaked capacity")
		}
	}
	if limiter.TryAcquire() {
		t.Fatal("capacity increased after releases")
	}
	for i := 0; i < capacity; i++ {
		limiter.Release()
	}
}
