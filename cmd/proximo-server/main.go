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

	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/backend/acl"
	"github.com/uw-labs/proximo/backend/kafka"
	"github.com/uw-labs/proximo/backend/mem"
	"github.com/uw-labs/proximo/proto"
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
	app := &cli.App{
		Name:  "proximo",
		Usage: "GRPC Proxy gateway for message queue systems",
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:    "port",
				Value:   6868,
				Usage:   "Port to listen on",
				EnvVars: []string{"PROXIMO_PORT"},
			},
			&cli.StringFlag{
				Name:    "endpoints",
				Value:   fmt.Sprintf("%s,%s", consumeEndpoint, publishEndpoint),
				Usage:   "The proximo endpoints to expose (consume, publish)",
				EnvVars: []string{"PROXIMO_ENDPOINTS"},
			},
			&cli.BoolFlag{
				Name:    "debug",
				Usage:   "Enable debug mode, which will produce log output, and may be more resource intensive",
				Value:   false,
				EnvVars: []string{"PROXIMO_DEBUG"},
			},
			&cli.StringFlag{
				Name:    "acl-config",
				Usage:   "ACL Config file",
				EnvVars: []string{"PROXIMO_ACL_CONFIG"},
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "kafka",
				Usage: "Use kafka backend",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "brokers",
						Value:   "localhost:9092",
						Usage:   `Broker addresses e.g., "server1:9092,server2:9092"`,
						EnvVars: []string{"PROXIMO_KAFKA_BROKERS"},
					},
					&cli.StringFlag{
						Name:    "version",
						Usage:   "Kafka Version e.g. 1.1.1, 0.10.2.0",
						EnvVars: []string{"PROXIMO_KAFKA_VERSION"},
					},
					&cli.IntFlag{
						Name:    "consumer-session-timeout",
						Usage:   "Duration in seconds after which consumer session should timeout.",
						EnvVars: []string{"PROXIMO_KAFKA_CONSUMER_SESSION_TIMEOUT"},
					},
					&cli.IntFlag{
						Name:    "max-message-bytes",
						Usage:   "Max message bytes to use in client config. 0 means client default",
						Value:   0,
						EnvVars: []string{"PROXIMO_KAFKA_MAX_MESSAGE_BYTES"},
					},
				},
				Action: func(c *cli.Context) error {
					return runWithKafka(c)
				},
			},
			{
				Name:  "mem",
				Usage: "Use in-memory testing backend",
				Action: func(c *cli.Context) error {
					return runWithMem(c)
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func runWithKafka(c *cli.Context) error {
	enabled := parseEndpoints(c.String("endpoints"))
	var (
		sourceFactory proximo.AsyncSourceFactory
		sinkFactory   proximo.AsyncSinkFactory
	)

	brokers := strings.Split(c.String("brokers"), ",")

	if enabled[consumeEndpoint] {
		sourceFactory = &kafka.AsyncSourceFactory{
			Brokers:        brokers,
			Version:        c.String("version"),
			SessionTimeout: time.Duration(c.Int("consumer-session-timeout")) * time.Second,
			Debug:          c.Bool("debug"),
		}
	}
	if enabled[publishEndpoint] {
		sinkFactory = &kafka.AsyncSinkFactory{
			Brokers:         brokers,
			Version:         c.String("version"),
			Debug:           c.Bool("debug"),
			MaxMessageBytes: c.Int("max-message-bytes"),
		}
	}

	log.Printf("Using kafka at %s\n", brokers)
	return setupAndServe(c, sourceFactory, sinkFactory)
}

func runWithMem(c *cli.Context) error {
	enabled := parseEndpoints(c.String("endpoints"))
	var (
		sourceFactory proximo.AsyncSourceFactory
		sinkFactory   proximo.AsyncSinkFactory
	)

	h := mem.NewBackend()

	if enabled[consumeEndpoint] {
		sourceFactory = h
	}
	if enabled[publishEndpoint] {
		sinkFactory = h
	}

	log.Printf("Using in memory testing backend")
	return setupAndServe(c, sourceFactory, sinkFactory)
}

func setupAndServe(c *cli.Context, sourceFactory proximo.AsyncSourceFactory, sinkFactory proximo.AsyncSinkFactory) error {
	port := c.Int("port")
	debug := c.Bool("debug")
	configFile := c.String("acl-config")

	if debug {
		log.Println("Running in debug mode. This means producing log output and disabling message discarding.")
	}

	if configFile != "" {
		cfg, err := acl.ConfigFromFile(configFile)
		if err != nil {
			return err
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

	return listenAndServe(sourceFactory, sinkFactory, port, debug)
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
		return fmt.Errorf("failed to listen: %w", err)
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
		proto.RegisterMessageSourceServer(grpcServer, &proximo.SourceServer{SourceFactory: sourceFactory, SkipDiscard: debug})
	}
	if sinkFactory != nil {
		proto.RegisterMessageSinkServer(grpcServer, &proximo.SinkServer{SinkFactory: sinkFactory})
	}

	errCh := make(chan error, 1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() { errCh <- grpcServer.Serve(lis) }()
	select {
	case err := <-errCh:
		return fmt.Errorf("failed to serve grpc: %w", err)
	case <-sigCh:
		return nil
	}
}
