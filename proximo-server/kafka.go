package main

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/kafka"
)

type kafkaSourceInitialiser struct {
	brokers []string
	version *sarama.KafkaVersion
}

func (i kafkaSourceInitialiser) NewSource(ctx context.Context, req *StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	return kafka.NewAsyncMessageSource(kafka.AsyncMessageSourceConfig{
		ConsumerGroup: req.GetConsumer(),
		Topic:         req.GetTopic(),
		Brokers:       i.brokers,
		Version:       i.version,
	})
}

type kafkaSinkInitialiser struct {
	brokers []string
	version *sarama.KafkaVersion
}

func (i kafkaSinkInitialiser) NewSink(ctx context.Context, req *StartPublishRequest) (substrate.AsyncMessageSink, error) {
	return kafka.NewAsyncMessageSink(kafka.AsyncMessageSinkConfig{
		Topic:   req.GetTopic(),
		Brokers: i.brokers,
		Version: i.version,
	})
}
