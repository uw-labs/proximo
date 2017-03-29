package main

import (
	"context"
	"fmt"
	"log"

	"github.com/utilitywarehouse/proximo/proximoc-go"
)

func main() {
	clientId := "dotnetc-client"
	
	if err := proximoc.ConsumeContext(
		context.Background(),
		"127.0.0.1:6868",
		clientId, // consumer id
		"example-topic",  // topic name
		func(m *proximoc.Message) error {
			fmt.Printf("%s\n", m.GetData())
			return nil
		}); err != nil {
		log.Fatal(err)
	}
}
