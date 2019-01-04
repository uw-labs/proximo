package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	cli "github.com/jawher/mow.cli"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func main() {
	app := cli.App("proximo", "GRPC Proxy gateway for message queue systems")

	port := app.Int(cli.IntOpt{
		Name:   "port",
		Value:  6868,
		Desc:   "Port to listen on",
		EnvVar: "PROXIMO_PORT",
	})

	endpoints := app.String(cli.StringOpt{
		Name:   "endpoints",
		Value:  "consume,publish",
		Desc:   "The proximo endpoints to expose (consume, publish)",
		EnvVar: "PROXIMO_ENDPOINTS",
	})

	app.Command("kafka", "Use kafka backend", func(cmd *cli.Cmd) {
		brokerString := cmd.String(cli.StringOpt{
			Name:   "brokers",
			Value:  "localhost:9092",
			Desc:   "Broker addresses e.g., \"server1:9092,server2:9092\"",
			EnvVar: "PROXIMO_KAFKA_BROKERS",
		})
		kafkaVersion := cmd.String(cli.StringOpt{
			Name:   "version",
			Desc:   "Kafka Version e.g. 1.1.1, 0.10.2.0",
			EnvVar: "PROXIMO_KAFKA_VERSION",
		})

		cmd.Action = func() {
			brokers := strings.Split(*brokerString, ",")

			var version *sarama.KafkaVersion
			if kafkaVersion != nil && *kafkaVersion != "" {
				kv, err := sarama.ParseKafkaVersion(*kafkaVersion)
				if err != nil {
					log.Fatalf("failed to parse kafka version: %v ", err)
				}
				version = &kv
			}

			lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
			if err != nil {
				log.Fatalf("failed to listen: %v", err)
			}
			var opts []grpc.ServerOption
			grpcServer := grpc.NewServer(opts...)
			kh := &kafkaHandler{
				brokers: brokers,
				version: version,
			}
			log.Printf("Using kafka at %s\n", brokers)
			registerGRPCServers(grpcServer, &server{kh}, *endpoints)
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
			var opts []grpc.ServerOption
			grpcServer := grpc.NewServer(opts...)
			kh := &amqpHandler{
				address: *address,
			}
			log.Printf("Using AMQP at %s\n", *address)
			registerGRPCServers(grpcServer, &server{kh}, *endpoints)
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
			var opts []grpc.ServerOption
			grpcServer := grpc.NewServer(opts...)
			kh, err := newNatsStreamingHandler(*url, *cid)
			if err != nil {
				log.Fatalf("failed to connect to nats streaming: %v", err)
			}
			defer kh.Close()
			log.Printf("Using NATS streaming server at %s with cluster id %s\n", *url, *cid)
			registerGRPCServers(grpcServer, &server{kh}, *endpoints)
			log.Fatal(grpcServer.Serve(lis))
		}
	})

	app.Command("mem", "Use in-memory testing backend", func(cmd *cli.Cmd) {
		cmd.Action = func() {

			lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
			if err != nil {
				log.Fatalf("failed to listen: %v", err)
			}
			opts := []grpc.ServerOption{
				grpc.KeepaliveParams(keepalive.ServerParameters{
					Time: 5 * time.Minute,
				}),
			}
			grpcServer := grpc.NewServer(opts...)
			kh := newMemHandler()
			log.Printf("Using in memory testing backend")
			registerGRPCServers(grpcServer, &server{kh}, *endpoints)
			log.Fatal(grpcServer.Serve(lis))
		}
	})

	log.Fatal(app.Run(os.Args))
}

func registerGRPCServers(grpcServer *grpc.Server, proximoServer *server, endpoints string) {
	for _, endpoint := range strings.Split(endpoints, ",") {
		switch endpoint {
		case "consume":
			RegisterMessageSourceServer(grpcServer, proximoServer)
		case "publish":
			RegisterMessageSinkServer(grpcServer, proximoServer)
		default:
			log.Fatalf("invalid expose-endpoint flag: %s", endpoint)
		}
	}
}
