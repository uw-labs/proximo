package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"github.com/pkg/errors"
	"github.com/utilitywarehouse/go-operational/op"
	"google.golang.org/grpc"
)

var gitHash string

type link struct {
	description string
	url         string
}

var appMeta = struct {
	name        string
	description string
	owner       string
	slack       string
	links       []link
}{
	name:        "proximo",
	description: "Interoperable GRPC based publish/subscribe",
	owner:       "energy",
	slack:       "#industryparticipation",
	links: []link{
		{"vcs", "https://github.com/utilitywarehouse/proximo"},
	},
}

func main() {
	app := cli.App(appMeta.name, appMeta.description)

	port := app.Int(cli.IntOpt{
		Name:   "port",
		Value:  6868,
		Desc:   "Port to listen on",
		EnvVar: "PROXIMO_PORT",
	})

	probePort := app.Int(cli.IntOpt{
		Name:   "probe-port",
		Value:  8080,
		Desc:   "Port to listen for healtcheck requests",
		EnvVar: "PROXIMO_PROBE_PORT",
	})

	cr := &CommandReceiver{
		counters: NewCounters(),
	}
	app.Command("kafka", "Use kafka backend", func(cmd *cli.Cmd) {
		log.Printf("Using kafka testing backend")
		brokers := *cmd.Strings(cli.StringsOpt{
			Name: "brokers",
			Value: []string{
				"localhost:9092",
			},
			Desc:   "Broker addresses e.g., \"server1:9092,server2:9092\"",
			EnvVar: "PROXIMO_KAFKA_BROKERS",
		})
		cmd.Action = func() {
			cr.handler = &kafkaHandler{
				brokers: brokers,
			}
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
			cr.handler = &amqpHandler{
				address: *address,
			}
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
			cr.handler = &natsStreamingHandler{
				url:       *url,
				clusterID: *cid,
			}
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
			cr.handler = &natsHandler{
				url: *url,
			}
		}
	})
	app.Command("mem", "Use in-memory testing backend", func(cmd *cli.Cmd) {
		cmd.Action = func() {
			cr.handler = newMemHandler()
		}
	})

	app.After = func() {
		cr.ServeStatus(*probePort)
		if err := cr.Serve("tcp", *port); err != nil {
			log.Fatal(err)
		}
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal("App stopped with error")
	}
}

type CommandReceiver struct {
	handler  handler
	counters counters
}

func (r *CommandReceiver) Serve(connType string, port int) error {
	lis, err := net.Listen(connType, fmt.Sprintf(":%d", port))
	if err != nil {
		errors.Wrap(err, "failed to listen")
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	RegisterMessageSourceServer(grpcServer, &server{r.handler, r.counters})
	RegisterMessageSinkServer(grpcServer, &server{r.handler, r.counters})
	if err := grpcServer.Serve(lis); err != nil {
		return errors.Wrap(err, "failed to serve grpc")
	}
	return nil
}

func (r *CommandReceiver) ServeStatus(port int) {
	router := mux.NewRouter()

	probe := router.PathPrefix("/__/").Subrouter()
	probe.Methods(http.MethodGet).Handler(op.NewHandler(getOpStatus(r.handler)))

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: router,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Fatal(errors.Wrap(err, "failed to server status"))
		}
	}()
}
