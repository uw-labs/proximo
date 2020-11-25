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

	cli "github.com/jawher/mow.cli"
	"github.com/nats-io/stan.go"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/proto"

	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/backend/acl"
	"github.com/uw-labs/proximo/backend/kafka"
	"github.com/uw-labs/proximo/backend/mem"
	"github.com/uw-labs/proximo/backend/natsstreaming"
	proximo_proto "github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

const (
	consumeEndpoint = "consume"
	publishEndpoint = "publish"

	// max message sizes are not really something proximo wants to enforce; it is
	// up to the underlying broker what they are, so we just use some large hard
	// coded large values here to replace the 4MB default.
	maxGRPCMessageSize = 1024 * 1024 * 128
)

func main() {
	var (
		sourceFactory proximo.AsyncSourceFactory
		sinkFactory   proximo.AsyncSinkFactory
		enabled       map[string]bool
	)

	app := cli.App("proximo", "GRPC Proxy gateway for message queue systems")

	port := app.Int(cli.IntOpt{
		Name:   "port",
		Value:  6868,
		Desc:   "Port to listen on",
		EnvVar: "PROXIMO_PORT",
	})

	endpoints := app.String(cli.StringOpt{
		Name:   "endpoints",
		Value:  fmt.Sprintf("%s,%s", consumeEndpoint, publishEndpoint),
		Desc:   "The proximo endpoints to expose (consume, publish)",
		EnvVar: "PROXIMO_ENDPOINTS",
	})

	debug := app.Bool(cli.BoolOpt{
		Name:   "debug",
		Desc:   "Enable debug mode, which will produce log output, and may be more resource intensive",
		Value:  false,
		EnvVar: "PROXIMO_DEBUG",
	})

	configFile := app.String(cli.StringOpt{
		Name:   "acl-config",
		Desc:   "ACL Config file",
		EnvVar: "PROXIMO_ACL_CONFIG",
	})

	app.Before = func() {
		enabled = parseEndpoints(*endpoints)
	}

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
		kafkaConsumerSessionTimeout := cmd.Int(cli.IntOpt{
			Name:   "consumer-session-timeout",
			Desc:   "Duration in seconds after which consumer session should timeout.",
			EnvVar: "PROXIMO_KAFKA_CONSUMER_SESSION_TIMEOUT",
		})
		kafkaMaxMessageBytes := cmd.Int(cli.IntOpt{
			Name:   "max-message-bytes",
			Desc:   "Max message bytes to use in client config.  0 means client default",
			EnvVar: "PROXIMO_KAFKA_MAX_MESSAGE_BYTES",
			Value:  0,
		})

		cmd.Action = func() {
			brokers := strings.Split(*brokerString, ",")

			if enabled[consumeEndpoint] {
				sourceFactory = &kafka.AsyncSourceFactory{
					Brokers:        brokers,
					Version:        *kafkaVersion,
					SessionTimeout: time.Duration(*kafkaConsumerSessionTimeout) * time.Second,
					Debug:          *debug,
				}
			}
			if enabled[publishEndpoint] {
				sinkFactory = &kafka.AsyncSinkFactory{
					Brokers:         brokers,
					Version:         *kafkaVersion,
					Debug:           *debug,
					MaxMessageBytes: *kafkaMaxMessageBytes,
					KeyFunc: func(message substrate.Message) []byte {
						var msg proximo_proto.Message
						if err := proto.Unmarshal(message.Data(), &msg); err != nil {
							panic(err)
						}
						return msg.GetKey()
					},
				}
			}

			log.Printf("Using kafka at %s\n", brokers)
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
		pingIntervalSeconds := cmd.Int(cli.IntOpt{
			Name:   "ping-interval",
			Value:  3,
			Desc:   "interval in seconds for connection pings",
			EnvVar: "PROXIMO_NATS_PING_INTERVAL_SECONDS",
		})
		pingNumTimeouts := cmd.Int(cli.IntOpt{
			Name:   "num-ping-timeouts",
			Value:  5,
			Desc:   "number of pings to time out before connection considered broken",
			EnvVar: "PROXIMO_NATS_NUM_PING_TIMEOUTS",
		})
		cmd.Action = func() {
			if enabled[consumeEndpoint] {
				sourceFactory = natsstreaming.AsyncSourceFactory{
					URL:                    *url,
					ClusterID:              *cid,
					MaxInflight:            *maxInflight,
					ConnectionNumPings:     *pingNumTimeouts,
					ConnectionPingInterval: *pingIntervalSeconds,
				}
			}
			if enabled[publishEndpoint] {
				sinkFactory = natsstreaming.AsyncSinkFactory{
					URL:       *url,
					ClusterID: *cid,
				}
			}

			log.Printf("Using NATS streaming server at %s with cluster id %s and max inflight %v\n", *url, *cid, *maxInflight)
		}
	})

	app.Command("mem", "Use in-memory testing backend", func(cmd *cli.Cmd) {
		cmd.Action = func() {
			h := mem.NewBackend()

			if enabled[consumeEndpoint] {
				sourceFactory = h
			}
			if enabled[publishEndpoint] {
				sinkFactory = h
			}

			log.Printf("Using in memory testing backend")
		}
	})

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}

	if *debug {
		log.Println("Running in debug mode. This means producing log output and disabling message discarding.")
	}

	if *configFile != "" {
		cfg, err := acl.ConfigFromFile(*configFile)
		if err != nil {
			log.Fatal(err)
		}

		if sourceFactory != nil {
			sourceFactory = acl.AsyncSourceFactory{
				Config: cfg,
				Next:   sourceFactory,
			}
		}

		if sinkFactory != nil {
			sinkFactory = acl.AsyncSinkFactory{
				Config: cfg,
				Next:   sinkFactory,
			}
		}
	}

	if err := listenAndServe(sourceFactory, sinkFactory, *port, *debug); err != nil {
		log.Fatal(err)
	}

	log.Println("Server terminated cleanly")
}

func parseEndpoints(endpoints string) map[string]bool {
	enabled := make(map[string]bool, 2)

	for _, endpoint := range strings.Split(endpoints, ",") {
		switch endpoint {
		case consumeEndpoint, publishEndpoint:
			log.Printf("%s endpoint enabled\n", endpoint)
			enabled[endpoint] = true
		default:
			log.Fatalf("invalid expose-endpoint flag: %s", endpoint)
		}
	}

	return enabled
}

func listenAndServe(sourceFactory proximo.AsyncSourceFactory, sinkFactory proximo.AsyncSinkFactory, port int, debug bool) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}
	defer lis.Close()

	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 5 * time.Minute,
		}),
		grpc.MaxRecvMsgSize(maxGRPCMessageSize),
		grpc.MaxSendMsgSize(maxGRPCMessageSize),
	}
	grpcServer := grpc.NewServer(opts...)
	defer grpcServer.Stop()

	if sourceFactory != nil {
		proximo_proto.RegisterMessageSourceServer(grpcServer, &proximo.SourceServer{SourceFactory: sourceFactory, SkipDiscard: debug})
	}
	if sinkFactory != nil {
		proximo_proto.RegisterMessageSinkServer(grpcServer, &proximo.SinkServer{SinkFactory: sinkFactory})
	}

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
