package crypto

import (
	"crypto/sha256"
	"encoding/json"
)

// DigestAny hashes any message
func DigestAny(msg any) []byte {
	msgBytes, _ := json.Marshal(msg)
	digest := sha256.Sum256(msgBytes)
	return digest[:]
}
