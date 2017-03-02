package main

import (
	"context"
	"fmt"
	"log"

	"github.com/utilitywarehouse/proximo/go-proximo"
	"github.com/utilitywarehouse/proximo/proximoc-go"
)

func main() {
	if err := proximoc.ConsumeContext(
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
