package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/jawher/mow.cli"
	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/backends/amqp"
	"github.com/uw-labs/proximo/backends/gcm"
	"github.com/uw-labs/proximo/backends/kafka"
	"github.com/uw-labs/proximo/backends/mem"
	"github.com/uw-labs/proximo/backends/nats"
	"github.com/uw-labs/proximo/backends/nats-streaming"
	"github.com/uw-labs/proximo/backends/nsq"
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
			var opts []grpc.ServerOption
			grpcServer := grpc.NewServer(opts...)
			kh := &kafka.KafkaHandler{
				Brokers: brokers,
			}
			log.Printf("Using kafka at %s\n", brokers)
			proximo.RegisterMessageSourceServer(grpcServer, &proximo.Server{kh})
			proximo.RegisterMessageSinkServer(grpcServer, &proximo.Server{kh})
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
			kh := &amqp.AmqpHandler{
				Address: *address,
			}
			log.Printf("Using AMQP at %s\n", *address)
			proximo.RegisterMessageSourceServer(grpcServer, &proximo.Server{kh})
			proximo.RegisterMessageSinkServer(grpcServer, &proximo.Server{kh})
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
			kh := &nats_streaming.NatsStreamingHandler{
				Url:       *url,
				ClusterID: *cid,
			}
			log.Printf("Using NATS streaming server at %s with cluster id %s\n", *url, *cid)
			proximo.RegisterMessageSourceServer(grpcServer, &proximo.Server{kh})
			proximo.RegisterMessageSinkServer(grpcServer, &proximo.Server{kh})
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
			var opts []grpc.ServerOption
			grpcServer := grpc.NewServer(opts...)
			kh := &nats.NatsHandler{
				Url: *url,
			}
			log.Printf("Using NATS at %s\n", *url)
			proximo.RegisterMessageSourceServer(grpcServer, &proximo.Server{kh})
			proximo.RegisterMessageSinkServer(grpcServer, &proximo.Server{kh})
			log.Fatal(grpcServer.Serve(lis))
		}
	})

	app.Command("nsq", "Use NSQ backend", func(cmd *cli.Cmd) {
		lookupdHTTPAddrs := cmd.Strings(cli.StringsOpt{
			Name:  "lookupaddrs",
			Value: []string{},
			Desc:  "NSD lookup HTTP Addrs",
		})
		tcpAddress := cmd.Strings(cli.StringsOpt{
			Name:  "tcpaddress",
			Value: []string{"localhost:4150"},
			Desc:  "NSD tcp Addrs",
		})
		cmd.Action = func() {

			lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
			if err != nil {
				log.Fatalf("failed to listen: %v", err)
			}
			var opts []grpc.ServerOption
			grpcServer := grpc.NewServer(opts...)
			kh := &nsq.NSQHandler{
				Channel:          "teste",
				LookupdHTTPAddrs: *lookupdHTTPAddrs,
				NsqdTcpAddress:   *tcpAddress,
			}
			log.Printf("Using nsq  backend")
			proximo.RegisterMessageSourceServer(grpcServer, &proximo.Server{kh})
			proximo.RegisterMessageSinkServer(grpcServer, &proximo.Server{kh})
			log.Fatal(grpcServer.Serve(lis))
		}
	})

	app.Command("gcm", "Use GCM pub/sub backend", func(cmd *cli.Cmd) {
		project := cmd.String(cli.StringOpt{
			Name:   "project",
			Value:  "test-project",
			Desc:   "GCM cloud project",
			EnvVar: "GOOGLE_CLOUD_PROJECT",
		})
		credentials := cmd.String(cli.StringOpt{
			Name:   "credentials",
			Value:  "credentials.json",
			Desc:   "GCM cloud credentials file",
			EnvVar: "GOOGLE_APPLICATION_CREDENTIALS",
		})
		cmd.Action = func() {

			lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
			if err != nil {
				log.Fatalf("failed to listen: %v", err)
			}

			var opts []grpc.ServerOption
			grpcServer := grpc.NewServer(opts...)
			kh := &gcm.GCMHandler{
				GOOGLE_CLOUD_PROJECT: *project,
			}
			if *credentials != "" {
				os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", *credentials)
			}
			log.Printf("Using GCM pub/sub backend")
			proximo.RegisterMessageSourceServer(grpcServer, &proximo.Server{kh})
			proximo.RegisterMessageSinkServer(grpcServer, &proximo.Server{kh})
			log.Fatal(grpcServer.Serve(lis))
		}
	})

	app.Command("mem", "Use in-memory testing backend", func(cmd *cli.Cmd) {
		cmd.Action = func() {

			lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
			if err != nil {
				log.Fatalf("failed to listen: %v", err)
			}
			var opts []grpc.ServerOption
			grpcServer := grpc.NewServer(opts...)
			kh := mem.NewMemHandler()
			log.Printf("Using in memory testing backend")
			proximo.RegisterMessageSourceServer(grpcServer, &proximo.Server{kh})
			proximo.RegisterMessageSinkServer(grpcServer, &proximo.Server{kh})
			log.Fatal(grpcServer.Serve(lis))
		}
	})

	log.Fatal(app.Run(os.Args))
}
