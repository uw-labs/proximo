package natsstreaming

import (
	"context"
	"fmt"
	"time"

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
	AckWait     time.Duration
}

func (i SourceInitialiser) NewSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	return natsstreaming.NewAsyncMessageSource(natsstreaming.AsyncMessageSourceConfig{
		URL:         i.URL,
		ClusterID:   i.ClusterID,
		ClientID:    "proximo-nats-streaming-" + id.Generate(),
		Subject:     req.GetTopic(),
		QueueGroup:  req.GetConsumer(),
		MaxInFlight: i.MaxInflight,
		AckWait:     i.AckWait,
		Offset:      toNATSffset(req.InitialOffset),
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

func toNATSffset(offset proto.Offset) int64 {
	switch offset {
	case proto.OFFSET_DEFAULT:
		return natsstreaming.OffsetOldest
	case proto.OFFSET_OLDEST:
		return natsstreaming.OffsetOldest
	case proto.OFFSET_NEWEST:
		return natsstreaming.OffsetNewest
	default:
		panic(fmt.Sprintf("unknown offset: %s", offset))
	}
}
