package kafka

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/kafka"
)

type AsyncSourceFactory struct {
	Brokers                  []string
	Version                  *sarama.KafkaVersion
	OffsetsRetention         time.Duration
	MetadataRefreshFrequency time.Duration
}

func (f AsyncSourceFactory) NewAsyncSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	var offset int64
	switch req.GetInitialOffset() {
	case proto.Offset_OFFSET_OLDEST, proto.Offset_OFFSET_DEFAULT:
		offset = kafka.OffsetOldest
	case proto.Offset_OFFSET_NEWEST:
		offset = kafka.OffsetNewest
	}
	return kafka.NewAsyncMessageSource(kafka.AsyncMessageSourceConfig{
		ConsumerGroup:            req.GetConsumer(),
		Topic:                    req.GetTopic(),
		Offset:                   offset,
		Brokers:                  f.Brokers,
		Version:                  f.Version,
		OffsetsRetention:         f.OffsetsRetention,
		MetadataRefreshFrequency: f.MetadataRefreshFrequency,
	})
}

type AsyncSinkFactory struct {
	Brokers         []string
	Version         *sarama.KafkaVersion
	MaxMessageBytes int
	KeyFunc         func(substrate.Message) []byte
}

func (f AsyncSinkFactory) NewAsyncSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	return kafka.NewAsyncMessageSink(kafka.AsyncMessageSinkConfig{
		Topic:           req.GetTopic(),
		Brokers:         f.Brokers,
		Version:         f.Version,
		MaxMessageBytes: f.MaxMessageBytes,
		KeyFunc:         f.KeyFunc,
	})
}
