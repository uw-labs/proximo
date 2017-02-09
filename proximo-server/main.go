package main

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/jawher/mow.cli"
	"github.com/utilitywarehouse/proximo/go-proximo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
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
				grpclog.Fatalf("failed to listen: %v", err)
			}
			var opts []grpc.ServerOption
			grpcServer := grpc.NewServer(opts...)
			kh := &kafkaHandler{
				brokers: brokers,
			}
			grpclog.Printf("%#v\n", brokers)
			proximo.RegisterMessageSourceServer(grpcServer, &consumeServer{kh.handle})
			grpclog.Fatal(grpcServer.Serve(lis))
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
				grpclog.Fatalf("failed to listen: %v", err)
			}
			var opts []grpc.ServerOption
			grpcServer := grpc.NewServer(opts...)
			kh := &amqpHandler{
				address: *address,
			}
			grpclog.Printf("%#v\n", address)
			proximo.RegisterMessageSourceServer(grpcServer, &consumeServer{kh.handle})
			grpclog.Fatal(grpcServer.Serve(lis))
		}
	})

	grpclog.Fatal(app.Run(os.Args))
}
