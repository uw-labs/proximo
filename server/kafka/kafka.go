package kafka

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/kafka"

	"github.com/uw-labs/proximo/proto"
)

// SourceInitialiser is an implementation of the `server.SourceInitialiserInterface`
// that initialises substrate source for Kafka
type SourceInitialiser struct {
	Brokers []string
	Version *sarama.KafkaVersion
}

func (i SourceInitialiser) NewSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	return kafka.NewAsyncMessageSource(kafka.AsyncMessageSourceConfig{
		ConsumerGroup: req.GetConsumer(),
		Topic:         req.GetTopic(),
		Brokers:       i.Brokers,
		Version:       i.Version,
	})
}

// SinkInitialiser is an implementation of the `server.SinkInitialiserInterface`
// that initialises substrate sink for Kafka
type SinkInitialiser struct {
	Brokers []string
	Version *sarama.KafkaVersion
}

func (i SinkInitialiser) NewSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	return kafka.NewAsyncMessageSink(kafka.AsyncMessageSinkConfig{
		Topic:   req.GetTopic(),
		Brokers: i.Brokers,
		Version: i.Version,
	})
}
