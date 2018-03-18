package main

import (
	"context"
	"fmt"
	"log"

	"github.com/uw-labs/proximo"
)

func main() {
	if err := proximo.ConsumeContext(
		context.Background(),
		"127.0.0.1:6868",
		"example-client", // consumer id
		"example-topic",  // topic name
		func(m *proximo.Message) error {
			fmt.Printf("%s\n", m.GetData())
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}
