package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/utilitywarehouse/proximo/proximoc-go"
)

func main() {
	id :=time.Now()
	clientId := "example-client-"+ id.Format("20060102150405")
	
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
