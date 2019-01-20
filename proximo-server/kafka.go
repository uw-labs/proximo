package main

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/kafka"
)

type kafkaSourceInitialiser struct {
	brokers []string
	version *sarama.KafkaVersion
}

func (i kafkaSourceInitialiser) NewSource(req *StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	return kafka.NewAsyncMessageSource(kafka.AsyncMessageSourceConfig{
		ConsumerGroup: req.GetConsumer(),
		Topic:         req.GetTopic(),
		Brokers:       i.brokers,
		Version:       i.version,
	})
}

type kafkaProduceHandler struct {
	brokers []string
	version *sarama.KafkaVersion
}

func (h *kafkaProduceHandler) HandleProduce(ctx context.Context, cfg producerConfig, forClient chan<- *Confirmation, messages <-chan *Message) error {
	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Errors = true
	conf.Producer.Retry.Max = 3
	conf.Producer.Timeout = time.Duration(60) * time.Second

	sp, err := sarama.NewSyncProducer(h.brokers, conf)
	if err != nil {
		return err
	}
	defer sp.Close()

	for {
		select {
		case m := <-messages:
			pm := &sarama.ProducerMessage{
				Topic: cfg.topic,
				Value: sarama.ByteEncoder(m.GetData()),
				// Key = TODO:
			}

			_, _, err = sp.SendMessage(pm)
			if err != nil {
				return err
			}
			forClient <- &Confirmation{MsgID: m.GetId()}
		case <-ctx.Done():
			return nil
		}
	}
}
