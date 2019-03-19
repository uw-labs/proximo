package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/proximo"
)

func main() {
	app := cli.NewApp()
	app.Name = "proximo client"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name: "endpoint, e",
		},
		cli.StringFlag{
			Name: "topic, t",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:  "produce",
			Usage: "produce a message. Reads from STDIN",
			Action: func(c *cli.Context) error {
				return produce(c.GlobalString("endpoint"), c.GlobalString("topic"))
			},
		},
		{
			Name: "consume",
			Action: func(c *cli.Context) error {
				return consume(c.GlobalString("endpoint"), c.GlobalString("topic"), c.String("consumer-id"))
			},
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "consumer-id",
					Usage: "The consumer ID to use.  Empty means use a random unique ID",
				},
			},
		},
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func consume(endpoint string, topic string, consumerID string) error {
	source, err := proximo.NewAsyncMessageSource(proximo.AsyncMessageSourceConfig{
		Broker:        endpoint,
		Topic:         topic,
		Offset:        proximo.OffsetOldest,
		ConsumerGroup: consumerID,
		Insecure:      true,
	})
	if err != nil {
		return err
	}
	client := substrate.NewSynchronousMessageSource(source)
	defer client.Close()

	err = client.ConsumeMessages(
		newContext(),
		func(_ context.Context, msg substrate.Message) error {
			fmt.Printf("%s", msg.Data())
			return nil
		},
	)
	if err != nil {
		return err
	}

	return client.Close()
}

func produce(endpoint string, topic string) error {
	sink, err := proximo.NewAsyncMessageSink(proximo.AsyncMessageSinkConfig{
		Broker:   endpoint,
		Topic:    topic,
		Insecure: true,
	})
	if err != nil {
		return err
	}
	client := substrate.NewSynchronousMessageSink(sink)
	defer client.Close()

	all, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return err
	}

	if err = client.PublishMessage(newContext(), &substrateMsg{data: all}); err != nil {
		return err
	}

	return client.Close()
}

func newContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sCh := make(chan os.Signal)
		signal.Notify(sCh, os.Interrupt, syscall.SIGTERM)
		<-sCh
		cancel()
	}()
	return ctx
}

type substrateMsg struct {
	data []byte
}

func (msg *substrateMsg) Data() []byte {
	return msg.data
}
