package paxos

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// SafeTimer is a somewhat safe timer wrapper for paxos algorithm
type SafeTimer struct {
	mutex     sync.Mutex
	timer     *time.Timer
	timeout   time.Duration
	running   bool
	waitCount int64
	ctx       context.Context
	cancel    context.CancelFunc
	TimeoutCh chan bool
}

// IncrementWaitCountOrStart is used to increment the wait count and start the timer if it is not running
func (t *SafeTimer) IncrementWaitCountOrStart() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// If timer is not running, start it
	if !t.running {
		t.timer.Reset(t.timeout)
		t.running = true
	}
	t.waitCount++
	log.Infof("[Timer] Incremented wait count: %d, running: %v", t.waitCount, t.running)
}

// DecrementWaitCountAndResetOrStopIfZero is used to decrement the wait count and reset the timer if it is not zero
// and stop the timer if the wait count is zero
func (t *SafeTimer) DecrementWaitCountAndResetOrStopIfZero() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Decrement the wait count
	if t.waitCount > 0 {
		t.waitCount--
	}

	// Stop the timer and reset if wait count is not zero
	t.timer.Stop()
	if t.waitCount == 0 {
		t.running = false
	} else {
		t.timer.Reset(t.timeout)
	}
	log.Infof("[Timer] Decremented wait count: %d, running: %v", t.waitCount, t.running)
}

// Cleanup resets the timer and clears the wait count
func (t *SafeTimer) Cleanup() bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	stopped := t.timer.Stop()
	t.running = false
	t.waitCount = 0
	t.cancel()
	t.ctx, t.cancel = context.WithCancel(context.Background())
	log.Infof("[Timer] Cleanup wait count: %d, running: %t at %d", t.waitCount, t.running, time.Now().UnixMilli())
	return !stopped
}

// run is the internal goroutine that handles timeout events.
func (t *SafeTimer) run() {
	for range t.timer.C {
		log.Infof("[Timer] Timer expired at %d", time.Now().UnixMilli())
		t.cancel()
		t.Cleanup()
		t.TimeoutCh <- true
		log.Infof("[Timer] Timeout channel signaled at %d", time.Now().UnixMilli())
	}
}

// GetContext returns the timer's context for cancellation signaling.
func (t *SafeTimer) GetContext() context.Context {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.ctx
}

// CreateSafeTimer creates and initializes a SafeTimer instance.
func CreateSafeTimer(i int64, n int64) *SafeTimer {

	// Staggered timeout
	timeout := minBackupTimeout + (maxBackupTimeout-minBackupTimeout)*time.Duration(i)/time.Duration(n)

	t := &SafeTimer{
		mutex:     sync.Mutex{},
		timer:     time.NewTimer(timeout),
		timeout:   timeout,
		running:   false,
		waitCount: 0,
		TimeoutCh: make(chan bool),
	}
	t.timer.Stop()
	t.ctx, t.cancel = context.WithCancel(context.Background())
	go t.run()
	return t
}
