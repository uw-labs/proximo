package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	proximoc "github.com/utilitywarehouse/proximo/proximoc-go"
)

func main() {
	var payloadSize = 1000
	var noOfMessages = 100

	c, err := proximoc.DialProducer(
		context.Background(),
		"127.0.0.1:6868",
		"go-topic", // topic name
	)
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	for index := 0; index < noOfMessages; index++ {
		payload := make([]byte, payloadSize)
		rand.Read(payload)

		err = c.Produce(payload)
		if err != nil {
			log.Fatal(err)
		}

		//fmt.Println(len(payload))
	}

	err = c.Close()
	if err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(start)
	log.Printf("Publishing %v messages in %s", noOfMessages, elapsed)
	log.Printf("(%f msgs/second).", float32(noOfMessages)/float32(elapsed))
}
