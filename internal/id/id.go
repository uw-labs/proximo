package id

import (
	"crypto/rand"
	"encoding/hex"
)

// Generate generates random string id and panics in case it fails.
func Generate() string {
	random := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	_, err := rand.Read(random)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(random)
}
