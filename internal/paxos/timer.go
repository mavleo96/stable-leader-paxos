package paxos

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// TODO: need to remove redundant log statements

// SafeTimer is a somewhat safe timer wrapper for paxos algorithm
type SafeTimer struct {
	mutex            sync.Mutex
	timer            *time.Timer
	timeout          time.Duration
	running          bool
	waitCount        int64
	contextWaitCount int64
	timerContext     context.Context
	timerCtxCancel   context.CancelFunc
	TimeoutChannel   chan bool
}

// IncrementWaitCountOrStart is used to increment the wait count and start the timer if it is not running
func (t *SafeTimer) IncrementWaitCountOrStart(message string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// If timer is not running, start it
	if !t.running {
		t.timer.Reset(t.timeout)
		t.waitCount++
		log.Infof("Timer started, Wait count: %d -> %d %d %s", t.waitCount-1, t.waitCount, time.Now().UnixMilli(), message)
		t.running = true
	} else {
		// If timer is running, increment the wait count
		t.waitCount++
		log.Infof("Timer wait count incremented, Wait count: %d -> %d %s", t.waitCount-1, t.waitCount, message)
	}
}

// IncrementWaitCountOrStartAndGetContext is used to increment the wait count and start the timer if it is not running
// and return the timer context which will be cancelled by the timer routine after it expires
func (t *SafeTimer) IncrementWaitCountOrStartAndGetContext(message string) context.Context {
	t.mutex.Lock()
	t.contextWaitCount++
	t.mutex.Unlock()
	t.IncrementWaitCountOrStart(message)
	return t.timerContext
}

// DecrementWaitCountAndResetOrStopIfZero is used to decrement the wait count and reset the timer if it is not zero
// and stop the timer if the wait count is zero
func (t *SafeTimer) DecrementWaitCountAndResetOrStopIfZero(message string) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Stop the timer
	t.timer.Stop()

	// Decrement the wait count
	t.waitCount--
	if t.waitCount < 0 {
		log.Fatalf("Wait count is negative: %d %s", t.waitCount, message)
	} else if t.waitCount == 0 {
		t.running = false
		log.Infof("Timer stopped, Wait count: %d -> %d %d %s", t.waitCount+1, 0, time.Now().UnixMilli(), message)
	} else {
		if !t.running {
			log.Warn("Wait count positive but timer was not running")
		}
		t.running = true
		t.timer.Reset(t.timeout)
		log.Infof("Timer reset, Wait count: %d -> %d %d %s", t.waitCount+1, t.waitCount, time.Now().UnixMilli(), message)
	}
}

func (t *SafeTimer) DecrementContextWaitCountAndResetOrStopIfZero(message string) {
	t.mutex.Lock()
	t.contextWaitCount--
	if t.contextWaitCount < 0 {
		log.Fatal("Context wait count is negative")
	}
	t.mutex.Unlock()
	t.DecrementWaitCountAndResetOrStopIfZero(message)
}

// timerCleanup resets the timer and clears the wait count
func (t *SafeTimer) TimerCleanup() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Create a new timer
	t.timer = time.NewTimer(t.timeout)
	if !t.timer.Stop() {
		<-t.timer.C
	}
	// Reset the timer state
	t.running = false
	t.waitCount -= t.contextWaitCount
	t.contextWaitCount = 0
	t.timerContext, t.timerCtxCancel = context.WithCancel(context.Background())
	log.Infof("Timer cleaned up; current wait count -> (%d, %d)", t.waitCount, t.contextWaitCount)
}

// timerRoutine is a persistent routine that waits for the timer to expire and then cancels the timer and cleans up the timer
// and sends a message on the timeout channel
func (t *SafeTimer) timerRoutine() {
	log.Info("Timer routine started is active")
	for {
		<-t.timer.C
		t.timerCtxCancel()
		t.TimerCleanup()
		t.TimeoutChannel <- true
	}
}

// GetTimerState returns the timer state for debugging
func (t *SafeTimer) GetTimerState() (int64, bool, time.Duration) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.waitCount, t.running, t.timeout
}

// CreateSafeTimer creates a new safe timer
func CreateSafeTimer(i int, n int) *SafeTimer {
	// Randomize timeout
	// timeout, err := RandomTimeout(minBackupTimeout, maxBackupTimeout)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// Staggered timeout
	timeout := minBackupTimeout + (maxBackupTimeout-minBackupTimeout)*time.Duration(i)/time.Duration(n)

	// Create a timer and stop/drain it
	timer := time.NewTimer(timeout)
	if !timer.Stop() {
		<-timer.C
	}
	log.Infof("Backup timer set to %d ms", timeout.Milliseconds())

	// Create a timer context and cancel function
	timerContext, timerCtxCancel := context.WithCancel(context.Background())

	// Create a timer instance
	timerInstance := &SafeTimer{
		mutex:            sync.Mutex{},
		timer:            timer,
		timeout:          timeout,
		running:          false,
		waitCount:        0,
		contextWaitCount: 0,
		timerContext:     timerContext,
		timerCtxCancel:   timerCtxCancel,
		TimeoutChannel:   make(chan bool),
	}

	// Start timer routine
	go timerInstance.timerRoutine()

	return timerInstance
}
