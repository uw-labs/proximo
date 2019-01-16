package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/urfave/cli"
	"github.com/uw-labs/proximo/proximoc-go"
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

func consume(endpoint string, topic string, consumerid string) error {
	if err := proximoc.ConsumeContext(
		context.Background(),
		endpoint,
		consumerid,
		topic,
		func(m *proximoc.Message) error {
			fmt.Printf("%s", m.GetData())
			return nil
		}); err != nil {
		return err
	}
	return nil
}

func produce(endpoint string, topic string) error {
	c, err := proximoc.DialProducer(
		context.Background(),
		endpoint,
		topic,
	)
	if err != nil {
		log.Fatal(err)
	}

	all, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return err
	}

	err = c.Produce(all)
	if err != nil {
		return err
	}

	err = c.Close()
	if err != nil {
		return err
	}
	return nil
}
