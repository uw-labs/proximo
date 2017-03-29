package main

import (
	"context"
	"log"
	//"math/rand" 
	"time"

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

	start := time.Now()
	for index := 0; index < 100; index++ {
		payload := make([]byte, 1000)
		//rand.Read(payload)

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
    log.Printf("Publishing 100 messages in %s", elapsed)
}
