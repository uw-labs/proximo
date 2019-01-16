package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	cli "github.com/jawher/mow.cli"
	"github.com/nats-io/go-nats-streaming"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/uw-labs/proximo/pkg/backend"
	"github.com/uw-labs/proximo/pkg/backend/amqp"
	"github.com/uw-labs/proximo/pkg/backend/kafka"
	"github.com/uw-labs/proximo/pkg/backend/mem"
	"github.com/uw-labs/proximo/pkg/backend/natsstreaming"
	"github.com/uw-labs/proximo/pkg/proto"
	"github.com/uw-labs/proximo/pkg/server"
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
					log.Fatalf("failed to parse kafka version: %v", err)
				}
				version = &kv
			}

			kh, err := kafka.NewHandler(version, brokers)
			if err != nil {
				log.Fatalf("failed to initialise kafka backend: %v", err)
			}

			log.Printf("Using kafka at %s\n", brokers)
			log.Fatal(listenAndServe(kh, *port, *endpoints))
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
			h, err := amqp.NewHandler(*address)
			if err != nil {
				log.Fatalf("failed to initialise amqp backend: %v", err)
			}

			log.Printf("Using AMQP at %s\n", *address)
			log.Fatal(listenAndServe(h, *port, *endpoints))
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
		maxInflight := cmd.Int(cli.IntOpt{
			Name:   "max-inflight",
			Value:  stan.DefaultMaxInflight,
			Desc:   "maximum number of unacknowledged messages",
			EnvVar: "PROXIMO_NATS_MAX_INFLIGHT",
		})
		cmd.Action = func() {
			kh, err := natsstreaming.NewHandler(*url, *cid, *maxInflight)
			if err != nil {
				log.Fatalf("failed to initialise nats streaming backend: %v ", err)
			}
			log.Printf("Using NATS streaming server at %s with cluster id %s and max inflight %v\n", *url, *cid, *maxInflight)

			log.Fatal(listenAndServe(kh, *port, *endpoints))
		}
	})

	app.Command("mem", "Use in-memory testing backend", func(cmd *cli.Cmd) {
		cmd.Action = func() {
			kh, err := mem.NewHandler()
			if err != nil {
				log.Fatalf("failed to initialise in memory backend: %v ", err)
			}

			log.Printf("Using in memory testing backend")
			log.Fatal(listenAndServe(kh, *port, *endpoints))
		}
	})

	log.Fatal(app.Run(os.Args))
}

func registerGRPCServers(grpcServer *grpc.Server, proximoServer proto.MessageServer, endpoints string) {
	for _, endpoint := range strings.Split(endpoints, ",") {
		switch endpoint {
		case "consume":
			proto.RegisterMessageSourceServer(grpcServer, proximoServer)
		case "publish":
			proto.RegisterMessageSinkServer(grpcServer, proximoServer)
		default:
			log.Fatalf("invalid expose-endpoint flag: %s", endpoint)
		}
	}
}

func listenAndServe(handler backend.Handler, port int, endpoints string) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}
	defer lis.Close()

	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 5 * time.Minute,
		}),
	}
	grpcServer := grpc.NewServer(opts...)
	registerGRPCServers(grpcServer, server.New(handler), endpoints)
	defer grpcServer.Stop()

	errCh := make(chan error, 1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() { errCh <- grpcServer.Serve(lis) }()
	select {
	case err := <-errCh:
		return errors.Wrap(err, "failed to serve grpc")
	case <-sigCh:
		return nil
	}
}
