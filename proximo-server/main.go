package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/jawher/mow.cli"
	"google.golang.org/grpc"
)

func main() {
	app := cli.App("proximo", "GRPC Proxy gateway for message queue systems")

	port := app.Int(cli.IntOpt{
		Name:   "port",
		Value:  6868,
		Desc:   "Port to listen on",
		EnvVar: "PROXIMO_PORT",
	})
	topicMap := app.Strings(cli.StringsOpt{
		Name:   "topic-map",
		Desc:   "map one topic to another",
		EnvVar: "PROXIMO_TOPIC_MAP",
	})

	app.Command("kafka", "Use kafka backend", func(cmd *cli.Cmd) {
		brokerString := cmd.String(cli.StringOpt{
			Name:   "brokers",
			Value:  "localhost:9092",
			Desc:   "Broker addresses e.g., \"server1:9092,server2:9092\"",
			EnvVar: "PROXIMO_KAFKA_BROKERS",
		})
		cmd.Action = func() {
			brokers := strings.Split(*brokerString, ",")

			lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
			if err != nil {
				log.Fatalf("failed to listen: %v", err)
			}

			grpcServer := grpc.NewServer()
			s := newServer(&kafkaHandler{
				brokers: brokers,
			}, *topicMap)

			log.Printf("Using kafka at %s\n", brokers)
			RegisterMessageSourceServer(grpcServer, s)
			RegisterMessageSinkServer(grpcServer, s)
			log.Fatal(grpcServer.Serve(lis))
		}
	})

	app.Command("amqp", "Use AMQP backend", func(cmd *cli.Cmd) {
		address := cmd.String(cli.StringOpt{
			Name:   "address",
			Value:  "amqp://localhost:5672",
			Desc:   "Broker address",
			EnvVar: "PROXIMO_AMQP_ADDRESS",
		})
		cmd.Action = func() {
			lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
			if err != nil {
				log.Fatalf("failed to listen: %v", err)
			}

			grpcServer := grpc.NewServer()
			s := newServer(&amqpHandler{
				address: *address,
			}, *topicMap)

			log.Printf("Using AMQP at %s\n", *address)
			RegisterMessageSourceServer(grpcServer, s)
			RegisterMessageSinkServer(grpcServer, s)
			log.Fatal(grpcServer.Serve(lis))
		}
	})

	app.Command("nats-streaming", "Use NATS streaming backend", func(cmd *cli.Cmd) {
		url := cmd.String(cli.StringOpt{
			Name:   "url",
			Value:  "nats://localhost:4222",
			Desc:   "NATS url",
			EnvVar: "PROXIMO_NATS_URL",
		})
		cid := cmd.String(cli.StringOpt{
			Name:   "cid",
			Value:  "test-cluster",
			Desc:   "cluster id",
			EnvVar: "PROXIMO_NATS_CLUSTER_ID",
		})
		cmd.Action = func() {
			lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
			if err != nil {
				log.Fatalf("failed to listen: %v", err)
			}

			grpcServer := grpc.NewServer()
			s := newServer(&natsStreamingHandler{
				url:       *url,
				clusterID: *cid,
			}, *topicMap)

			log.Printf("Using NATS streaming server at %s with cluster id %s\n", *url, *cid)
			RegisterMessageSourceServer(grpcServer, s)
			RegisterMessageSinkServer(grpcServer, s)
			log.Fatal(grpcServer.Serve(lis))
		}
	})

	app.Command("nats", "Use NATS backend", func(cmd *cli.Cmd) {
		url := cmd.String(cli.StringOpt{
			Name:   "url",
			Value:  "nats://localhost:4222",
			Desc:   "NATS url",
			EnvVar: "PROXIMO_NATS_URL",
		})
		cmd.Action = func() {

			lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
			if err != nil {
				log.Fatalf("failed to listen: %v", err)
			}

			grpcServer := grpc.NewServer()
			s := newServer(&natsHandler{
				url: *url,
			}, *topicMap)

			log.Printf("Using NATS at %s\n", *url)
			RegisterMessageSourceServer(grpcServer, s)
			RegisterMessageSinkServer(grpcServer, s)
			log.Fatal(grpcServer.Serve(lis))
		}
	})

	app.Command("mem", "Use in-memory testing backend", func(cmd *cli.Cmd) {
		cmd.Action = func() {

			lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
			if err != nil {
				log.Fatalf("failed to listen: %v", err)
			}

			grpcServer := grpc.NewServer()
			s := newServer(newMemHandler(), *topicMap)

			log.Printf("Using in memory testing backend")
			RegisterMessageSourceServer(grpcServer, s)
			RegisterMessageSinkServer(grpcServer, s)
			log.Fatal(grpcServer.Serve(lis))
		}
	})

	log.Fatal(app.Run(os.Args))
}
