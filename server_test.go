package proximo

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/uw-labs/proximo/backend/mock"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
	proximoc "github.com/uw-labs/substrate/proximo"
)

var (
	backend    *mock.Backend
	grpcServer *grpc.Server
)

func Setup() error {
	lis, err := net.Listen("tcp", ":6868")
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}

	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 5 * time.Minute,
		}),
	}
	grpcServer = grpc.NewServer(opts...)

	backend = mock.NewBackend()

	proto.RegisterMessageSourceServer(grpcServer, &SourceServer{SourceFactory: backend})
	proto.RegisterMessageSinkServer(grpcServer, &SinkServer{SinkFactory: backend})
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server stopped with error: %s", err)
		}
	}()

	// Wait for server to start
	cc, err := grpc.Dial("localhost:6868", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return errors.Wrap(err, "failed to open client connection")
	}

	return cc.Close()
}

func TestMain(m *testing.M) {
	if err := Setup(); err != nil {
		fmt.Fprintf(os.Stderr, "testing: failed to setup tests: %v", err)
		os.Exit(1)
	}

	exitCode := m.Run()

	grpcServer.Stop() // This will also close the listener
	os.Exit(exitCode)
}

func TestProduceServer_Publish(t *testing.T) {
	assert := require.New(t)
	toPublish := [][]byte{
		[]byte("publish-message-1"),
		[]byte("publish-message-2"),
		[]byte("publish-message-3"),
		[]byte("publish-message-4"),
		[]byte("publish-message-5"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	sink, err := proximoc.NewAsyncMessageSink(proximoc.AsyncMessageSinkConfig{
		Broker:   "localhost:6868",
		Topic:    "publish-test",
		Insecure: true,
	})
	assert.NoError(err)
	client := substrate.NewSynchronousMessageSink(sink)
	defer client.Close()

	for _, data := range toPublish {
		assert.NoError(client.PublishMessage(ctx, &substrateMsg{data: data}))
	}
	assert.NoError(client.Close())

	published := backend.GetTopic("publish-test")
	assert.Equal(len(toPublish), len(published))
	for i, msg := range toPublish {
		assert.Equal(msg, published[i].Data())
	}
}

func TestConsumeServer_Consume(t *testing.T) {
	assert := require.New(t)
	expected := []substrate.Message{
		&substrateMsg{data: []byte("consume-message-1")},
		&substrateMsg{data: []byte("consume-message-2")},
		&substrateMsg{data: []byte("consume-message-3")},
		&substrateMsg{data: []byte("consume-message-4")},
		&substrateMsg{data: []byte("consume-message-5")},
	}
	backend.SetTopic("consume-test", expected)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	source, err := proximoc.NewAsyncMessageSource(proximoc.AsyncMessageSourceConfig{
		Broker:        "localhost:6868",
		Topic:         "consume-test",
		ConsumerGroup: "test-consumer-group",
		Insecure:      true,
	})
	assert.NoError(err)
	client := substrate.NewSynchronousMessageSource(source)
	defer func() { assert.NoError(client.Close()) }()

	consumed := make([]substrate.Message, 0, len(expected))
	err = client.ConsumeMessages(ctx, func(_ context.Context, msg substrate.Message) error {
		consumed = append(consumed, msg)
		return nil
	})
	assert.NoError(err)
	assert.Equal(len(expected), len(consumed))

	for i, msg := range expected {
		assert.Equal(msg.Data(), consumed[i].Data())
	}
}

type substrateMsg struct {
	data []byte
}

func (msg *substrateMsg) Data() []byte {
	return msg.data
}
