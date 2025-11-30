package paxos

import (
	"crypto/rand"
	"math/big"
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
	TimeoutCh chan time.Time
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
	log.Infof("[Timer] Incremented wait count: %d, running: %v at %d", t.waitCount, t.running, time.Now().UnixMilli())
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
	log.Infof("[Timer] Decremented wait count: %d, running: %v at %d", t.waitCount, t.running, time.Now().UnixMilli())
}

// Stop stops the timer
func (t *SafeTimer) Stop() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	stopped := t.timer.Stop()
	t.running = false
	t.waitCount = 0
	log.Infof("[Timer] Stop timer at %d, stopped: %t", time.Now().UnixMilli(), stopped)
}

// run is the internal goroutine that handles timeout events.
func (t *SafeTimer) run() {
	for expiredTime := range t.timer.C {
		log.Infof("[Timer] Timer expired at %d", expiredTime.UnixMilli())
		t.mutex.Lock()

		stopped := t.timer.Stop()
		t.running = false
		t.waitCount = 0
		log.Infof("[Timer] run: Timer reset to wait count %d, running: %t, stopped: %t at %d", t.waitCount, t.running, stopped, time.Now().UnixMilli())
		t.mutex.Unlock()
		t.TimeoutCh <- expiredTime
	}
	log.Infof("[Timer] Timer goroutine ended at %d", time.Now().UnixMilli())
}

// randomTimeout returns a random timeout between min and max
func randomTimeout(min time.Duration, max time.Duration) (time.Duration, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(max.Milliseconds()-min.Milliseconds()))
	return time.Duration(n.Int64()+min.Milliseconds()) * time.Millisecond, err
}

// CreateSafeTimer creates and initializes a SafeTimer instance.
func CreateSafeTimer(i int64, n int64) *SafeTimer {
	// Staggered timeout
	timeout := minBackupTimeout + (maxBackupTimeout-minBackupTimeout)*time.Duration(i)/time.Duration(n)
	// Random timeout
	// timeout, err := randomTimeout(minBackupTimeout, maxBackupTimeout)
	// if err != nil {
	// 	log.Fatalf("[Timer] Failed to create safe timer: %v", err)
	// }
	log.Infof("[Timer] Created safe timer with timeout: %s", timeout)

	t := &SafeTimer{
		mutex:     sync.Mutex{},
		timer:     time.NewTimer(timeout),
		timeout:   timeout,
		running:   false,
		waitCount: 0,
		TimeoutCh: make(chan time.Time),
	}
	t.timer.Stop()
	go t.run()
	return t
}
