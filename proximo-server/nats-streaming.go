package main

import (
	"context"

	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/natsstreaming"
)

type NATSStreamingAsyncSourceFactory struct {
	URL                    string
	ClusterID              string
	MaxInflight            int
	ConnectionNumPings     int
	ConnectionPingInterval int
}

func (f NATSStreamingAsyncSourceFactory) NewAsyncSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	var offset int64
	switch req.GetInitialOffset() {
	case proto.Offset_OFFSET_OLDEST, proto.Offset_OFFSET_DEFAULT:
		offset = natsstreaming.OffsetOldest
	case proto.Offset_OFFSET_NEWEST:
		offset = natsstreaming.OffsetNewest
	}
	return natsstreaming.NewAsyncMessageSource(natsstreaming.AsyncMessageSourceConfig{
		URL:                    f.URL,
		ClusterID:              f.ClusterID,
		Subject:                req.GetTopic(),
		QueueGroup:             req.GetConsumer(),
		Offset:                 offset,
		MaxInFlight:            f.MaxInflight,
		ConnectionNumPings:     f.ConnectionNumPings,
		ConnectionPingInterval: f.ConnectionPingInterval,
	})
}

type NATSStreamingAsyncMessageFactory struct {
	URL       string
	ClusterID string
}

func (f NATSStreamingAsyncMessageFactory) NewAsyncSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	return natsstreaming.NewAsyncMessageSink(natsstreaming.AsyncMessageSinkConfig{
		URL:       f.URL,
		ClusterID: f.ClusterID,
		Subject:   req.GetTopic(),
	})
}
