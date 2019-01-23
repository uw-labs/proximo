package id

import (
	"crypto/rand"
	"encoding/hex"
)

// Generate generates a new random string id. It panics on error.
func Generate() string {
	random := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	_, err := rand.Read(random)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(random)
}
