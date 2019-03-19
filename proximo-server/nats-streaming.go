package main

import (
	"context"

	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/natsstreaming"
)

type NATSStreamingAsyncSourceFactory struct {
	url                 string
	clusterID           string
	maxInflight         int
	pingIntervalSeconds int
	numPingTimeouts     int
}

func (f NATSStreamingAsyncSourceFactory) NewAsyncSource(ctx context.Context, conf consumerConfig) (substrate.AsyncMessageSource, error) {
	var offset int64
	switch conf.offset {
	case proto.Offset_OFFSET_OLDEST, proto.Offset_OFFSET_DEFAULT:
		offset = natsstreaming.OffsetOldest
	case proto.Offset_OFFSET_NEWEST:
		offset = natsstreaming.OffsetNewest
	}
	return natsstreaming.NewAsyncMessageSource(natsstreaming.AsyncMessageSourceConfig{
		URL:                    f.url,
		ClusterID:              f.clusterID,
		Subject:                conf.topic,
		QueueGroup:             conf.consumer,
		Offset:                 offset,
		MaxInFlight:            f.maxInflight,
		ConnectionNumPings:     f.numPingTimeouts,
		ConnectionPingInterval: f.pingIntervalSeconds,
	})
}

type NATSStreamingAsyncMessageFactory struct {
	url       string
	clusterID string
}

func (f NATSStreamingAsyncMessageFactory) NewAsyncSink(ctx context.Context, config SinkConfig) (substrate.AsyncMessageSink, error) {
	return natsstreaming.NewAsyncMessageSink(natsstreaming.AsyncMessageSinkConfig{
		URL:       f.url,
		ClusterID: f.clusterID,
		ClientID:  "proximo" + generateID(),
		Subject:   config.Topic,
	})
}
