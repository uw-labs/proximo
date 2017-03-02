package main

import (
	"context"
	"log"

	proximoc "github.com/utilitywarehouse/proximo/proximoc-go"
)

func main() {
	c, err := proximoc.DialProducer(
		context.Background(),
		"127.0.0.1:6868",
		"example-topic", // topic name
	)
	if err != nil {
		log.Fatal(err)
	}

	err = c.Produce([]byte("hello world"))
	if err != nil {
		log.Fatal(err)
	}

	err = c.Close()
	if err != nil {
		log.Fatal(err)
	}

}
