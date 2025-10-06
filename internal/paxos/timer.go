package paxos

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type SafeTimer struct {
	Mutex     sync.Mutex
	Timer     *time.Timer
	WaitCount int64
	Running   bool
}

func (t *SafeTimer) IncrementWaitCountOrStart() {
	t.Mutex.Lock()
	if !t.Running {
		t.Timer.Reset(mainTimeout)
		t.Running = true
		t.WaitCount = 1
	} else {
		t.WaitCount++
	}
	t.Mutex.Unlock()
}

func (t *SafeTimer) DecrementWaitCountAndResetOrStopIfZero() {
	t.Mutex.Lock()
	t.Timer.Stop()
	t.WaitCount--
	if t.WaitCount < 0 {
		log.Fatalf("Wait count is negative")
	} else if t.WaitCount == 0 {
		t.Running = false
	} else {
		t.Timer.Reset(mainTimeout)
	}
	t.Mutex.Unlock()
}

func CreateSafeTimer() *SafeTimer {
	return &SafeTimer{
		Mutex:     sync.Mutex{},
		Timer:     time.NewTimer(mainTimeout),
		WaitCount: 0,
		Running:   false,
	}
}
