package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/kafka"

	"github.com/uw-labs/proximo/proto"
)

// SourceInitialiser is an implementation of the `server.SourceInitialiserInterface`
// that initialises substrate source for Kafka
type SourceInitialiser struct {
	Brokers                  []string
	Version                  *sarama.KafkaVersion
	MetadataRefreshFrequency time.Duration
	OffsetsRetention         time.Duration
}

func (i SourceInitialiser) NewSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	return kafka.NewAsyncMessageSource(kafka.AsyncMessageSourceConfig{
		ConsumerGroup:            req.GetConsumer(),
		Topic:                    req.GetTopic(),
		Offset:                   toKafkaOffset(req.GetInitialOffset()),
		Brokers:                  i.Brokers,
		Version:                  i.Version,
		MetadataRefreshFrequency: i.MetadataRefreshFrequency,
		OffsetsRetention:         i.OffsetsRetention,
	})
}

// SinkInitialiser is an implementation of the `server.SinkInitialiserInterface`
// that initialises substrate sink for Kafka
type SinkInitialiser struct {
	Brokers         []string
	Version         *sarama.KafkaVersion
	MaxMessageBytes int
	KeyFunc         func(substrate.Message) []byte
}

func (i SinkInitialiser) NewSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	return kafka.NewAsyncMessageSink(kafka.AsyncMessageSinkConfig{
		Topic:           req.GetTopic(),
		Brokers:         i.Brokers,
		Version:         i.Version,
		MaxMessageBytes: i.MaxMessageBytes,
		KeyFunc:         i.KeyFunc,
	})
}

func toKafkaOffset(offset proto.Offset) int64 {
	switch offset {
	case proto.OFFSET_DEFAULT:
		return kafka.OffsetOldest
	case proto.OFFSET_OLDEST:
		return kafka.OffsetOldest
	case proto.OFFSET_NEWEST:
		return kafka.OffsetNewest
	default:
		panic(fmt.Sprintf("unknown offset: %s", offset))
	}
}
