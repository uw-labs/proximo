package main

import (
	"context"
	"fmt"
	"log"

	"github.com/uw-labs/proximo/proximoc-go"
)

func main() {
	if err := proximoc.ConsumeContext(
		context.Background(),
		"127.0.0.1:6868",
		"example-client", // consumer id
		"example-topic",  // topic name
		func(m *proximoc.Message) error {
			fmt.Printf("%s\n", m.GetData())
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}
