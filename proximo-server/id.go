package main

import (
	"crypto/rand"
	"encoding/base64"
)

func generateID() string {
	random := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	_, err := rand.Read(random)
	if err != nil {
		panic(err)
	}
	return base64.URLEncoding.EncodeToString(random)
}
