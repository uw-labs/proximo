package main

import (
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/natsstreaming"
)

type natsStreamingSourceInitialiser struct {
	url         string
	clusterID   string
	maxInflight int
}

func (i natsStreamingSourceInitialiser) New(conf consumerConfig) (substrate.AsyncMessageSource, error) {
	return natsstreaming.NewAsyncMessageSource(natsstreaming.AsyncMessageSourceConfig{
		URL:         i.url,
		ClusterID:   i.clusterID,
		ClientID:    "proximo-nats-streaming-" + generateID(),
		Subject:     conf.topic,
		QueueGroup:  conf.consumer,
		MaxInFlight: i.maxInflight,
	})
}

type natsStreamingSinkInitialiser struct {
	url       string
	clusterID string
}

func (i natsStreamingSinkInitialiser) New(conf producerConfig) (substrate.AsyncMessageSink, error) {
	return natsstreaming.NewAsyncMessageSink(natsstreaming.AsyncMessageSinkConfig{
		URL:       i.url,
		ClusterID: i.clusterID,
		ClientID:  "proximo-nats-streaming-" + generateID(),
		Subject:   conf.topic,
	})
}
