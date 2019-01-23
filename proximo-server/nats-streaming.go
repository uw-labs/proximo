package main

import (
	"context"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/natsstreaming"
)

type natsStreamingSourceInitialiser struct {
	url         string
	clusterID   string
	maxInflight int
}

func (i natsStreamingSourceInitialiser) NewSource(ctx context.Context, req *StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	return natsstreaming.NewAsyncMessageSource(natsstreaming.AsyncMessageSourceConfig{
		URL:         i.url,
		ClusterID:   i.clusterID,
		ClientID:    "proximo-nats-streaming-" + generateID(),
		Subject:     req.GetTopic(),
		QueueGroup:  req.GetConsumer(),
		MaxInFlight: i.maxInflight,
	})
}

type natsStreamingSinkInitialiser struct {
	url       string
	clusterID string
}

func (i natsStreamingSinkInitialiser) NewSink(ctx context.Context, req *StartPublishRequest) (substrate.AsyncMessageSink, error) {
	return natsstreaming.NewAsyncMessageSink(natsstreaming.AsyncMessageSinkConfig{
		URL:       i.url,
		ClusterID: i.clusterID,
		ClientID:  "proximo-nats-streaming-" + generateID(),
		Subject:   req.GetTopic(),
	})
}
