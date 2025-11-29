package paxos

import (
	"sync"
	"time"
)

const (
	minBackupTimeout = time.Duration(250 * time.Millisecond)
	maxBackupTimeout = time.Duration(400 * time.Millisecond)
	prepareTimeout   = time.Duration(250 * time.Millisecond)
	K                = 10 // check point interval
)

// ServerConfig is the configuration for the server
type ServerConfig struct {
	mutex sync.RWMutex
	Alive bool
	F     int64
	K     int64
}

// CreateServerConfig creates a new server config
func CreateServerConfig(n int64, k int64) *ServerConfig {
	return &ServerConfig{
		mutex: sync.RWMutex{},
		Alive: true,
		F:     (n - 1) / 2,
		K:     k,
	}
}
