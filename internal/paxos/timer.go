package paxos

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type SafeTimer struct {
	Mutex     sync.RWMutex
	Timer     *time.Timer
	WaitCount int64
	Running   bool
}

func (t *SafeTimer) IncrementWaitCountOrStart() {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	if !t.Running {
		log.Infof("Timer started, Wait count: %d -> %d", t.WaitCount, 1)
		t.Timer.Reset(mainTimeout)
		t.Running = true
		t.WaitCount = 1
	} else {
		log.Infof("Timer wait count incremented, Wait count: %d -> %d", t.WaitCount, t.WaitCount+1)
		t.WaitCount++
	}
}

func (t *SafeTimer) DecrementWaitCountAndResetOrStopIfZero() {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	t.Timer.Stop()
	t.WaitCount--
	if t.WaitCount < 0 {
		log.Fatal("Wait count is negative")
	} else if t.WaitCount == 0 {
		t.Running = false
		log.Infof("Timer stopped, Wait count: %d -> %d", t.WaitCount+1, 0)
	} else {
		if !t.Running {
			log.Fatal("Wait count positive but timer was not running")
		}
		t.Running = true
		t.Timer.Reset(mainTimeout)
		log.Infof("Timer reset, Wait count: %d -> %d", t.WaitCount+1, t.WaitCount)
	}
}

func (t *SafeTimer) GetTimerState() (int64, bool) {
	t.Mutex.RLock()
	defer t.Mutex.RUnlock()
	return t.WaitCount, t.Running
}

func CreateSafeTimer() *SafeTimer {
	return &SafeTimer{
		Mutex:     sync.RWMutex{},
		Timer:     time.NewTimer(mainTimeout),
		WaitCount: 0,
		Running:   false,
	}
}
