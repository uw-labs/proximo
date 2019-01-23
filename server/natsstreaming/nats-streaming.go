package natsstreaming

import (
	"context"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/natsstreaming"

	"github.com/uw-labs/proximo/id"
	"github.com/uw-labs/proximo/proto"
)

// SourceInitialiser is an implementation of the `server.SourceInitialiserInterface`
// that initialises substrate source for NATS Streaming
type SourceInitialiser struct {
	URL         string
	ClusterID   string
	MaxInflight int
}

func (i SourceInitialiser) NewSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	return natsstreaming.NewAsyncMessageSource(natsstreaming.AsyncMessageSourceConfig{
		URL:         i.URL,
		ClusterID:   i.ClusterID,
		ClientID:    "proximo-nats-streaming-" + id.Generate(),
		Subject:     req.GetTopic(),
		QueueGroup:  req.GetConsumer(),
		MaxInFlight: i.MaxInflight,
	})
}

// SinkInitialiser is an implementation of the `server.SinkInitialiserInterface`
// that initialises substrate sink for NATS Streaming
type SinkInitialiser struct {
	URL       string
	ClusterID string
}

func (i SinkInitialiser) NewSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	return natsstreaming.NewAsyncMessageSink(natsstreaming.AsyncMessageSinkConfig{
		URL:       i.URL,
		ClusterID: i.ClusterID,
		ClientID:  "proximo-nats-streaming-" + id.Generate(),
		Subject:   req.GetTopic(),
	})
}
