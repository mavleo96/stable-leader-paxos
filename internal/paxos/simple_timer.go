package paxos

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// SimpleTimer is a simple timer wrapper for consensus algorithm
type SimpleTimer struct {
	mutex     sync.Mutex
	timer     *time.Timer
	timeout   time.Duration
	running   bool
	TimeoutCh chan time.Time
}

// StartIfNotRunning is used to start the timer if it is not running
func (t *SimpleTimer) StartIfNotRunning() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// If timer is not running, start it
	if !t.running {
		t.timer.Reset(t.timeout)
		t.running = true
	}
	log.Infof("[SimpleTimer] Started timer at %d", time.Now().UnixMilli())
}

// Reset resets the timer if it is running
func (t *SimpleTimer) Reset() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	active := t.timer.Reset(t.timeout)
	t.running = true
	log.Infof("[SimpleTimer] Reset timer at %d, active: %t", time.Now().UnixMilli(), active)
}

// StopIfRunning stops the timer if it is running
func (t *SimpleTimer) StopIfRunning() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	stopped := t.timer.Stop()
	t.running = false
	log.Infof("[SimpleTimer] Stopped timer at %d, stopped: %t", time.Now().UnixMilli(), stopped)
}

// run is the internal goroutine that handles timeout events.
func (t *SimpleTimer) run() {
	for expiredTime := range t.timer.C {
		log.Infof("[SimpleTimer] Timer expired at %d", expiredTime.UnixMilli())
		t.mutex.Lock()

		stopped := t.timer.Stop()
		t.running = false
		log.Infof("[SimpleTimer] run: Timer reset to running: %t, stopped: %t at %d", t.running, stopped, time.Now().UnixMilli())
		t.mutex.Unlock()
		t.TimeoutCh <- expiredTime
	}
	log.Infof("[SimpleTimer] Timer goroutine ended at %d", time.Now().UnixMilli())
}

// CreateSimpleTimer creates and initializes a SimpleTimer instance.
func CreateSimpleTimer(minTimeout time.Duration, maxTimeout time.Duration) *SimpleTimer {
	timeout, err := randomTimeout(minTimeout, maxTimeout)
	if err != nil {
		log.Fatalf("[SimpleTimer] Failed to create simple timer: %v", err)
	}
	log.Infof("[SimpleTimer] Created simple timer with timeout: %s", timeout)

	t := &SimpleTimer{
		mutex:     sync.Mutex{},
		timer:     time.NewTimer(timeout),
		timeout:   timeout,
		running:   false,
		TimeoutCh: make(chan time.Time),
	}
	t.timer.Stop()
	go t.run()
	return t
}
