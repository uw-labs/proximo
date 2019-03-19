package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/uw-labs/proximo/backend/mock"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/proximo/proximoc-go"
	"github.com/uw-labs/substrate"
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
	go func() { grpcServer.Serve(lis) }()

	// Wait for server to start
	cc, err := grpc.Dial("localhost:6868", grpc.WithInsecure(), grpc.WithBlock())
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

	client, err := proximoc.DialProducer(ctx, "localhost:6868", "publish-test")
	assert.NoError(err)
	defer func() { assert.NoError(client.Close()) }()

	for _, msg := range toPublish {
		assert.NoError(client.Produce(msg))
	}

	published := backend.GetTopic("publish-test")
	assert.Equal(len(toPublish), len(published))
	for i, msg := range toPublish {
		assert.Equal(msg, published[i].Data())
	}
}

func TestConsumeServer_Consume(t *testing.T) {
	assert := require.New(t)
	expected := []substrate.Message{
		&proximoMsg{
			msg: &proto.Message{
				Data: []byte("consume-message-1"),
			},
		},
		&proximoMsg{
			msg: &proto.Message{
				Data: []byte("consume-message-2"),
			},
		},
		&proximoMsg{
			msg: &proto.Message{
				Data: []byte("consume-message-3"),
			},
		},
	}
	backend.SetTopic("consume-test", expected)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	consumed := make([]*proximoc.Message, 0, 3)
	err := proximoc.ConsumeContext(ctx, "localhost:6868", "test-consumer", "consume-test", func(msg *proximoc.Message) error {
		consumed = append(consumed, msg)
		return nil
	})
	assert.NoError(err)
	assert.Equal(len(expected), len(consumed))

	for i, msg := range expected {
		assert.Equal(msg.Data(), consumed[i].Data)
	}
}
