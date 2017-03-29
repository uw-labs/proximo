package main

import (
	"context"
	"fmt"
	"log"
)

func main() {
	clientId := "dotnetc-client"

	if err := proximoc.ConsumeContext(
		context.Background(),
		"127.0.0.1:6868",
		clientId,   // consumer id
		"go-topic", // topic name
		func(m *proximoc.Message) error {
			fmt.Printf("%s\n", m.GetData())
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}
