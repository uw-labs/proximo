package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"google.golang.org/grpc/grpclog"

	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/kafka"
)

type kafkaConsumeHandler struct {
	brokers []string
	version *sarama.KafkaVersion
}

func (h *kafkaConsumeHandler) HandleConsume(ctx context.Context, conf consumerConfig, forClient chan<- *proto.Message, confirmRequest <-chan *proto.Confirmation) error {
	toConfirmIds := make(chan string)

	errors := make(chan error)

	var offset int64
	switch conf.offset {
	case proto.Offset_OFFSET_DEFAULT, proto.Offset_OFFSET_OLDEST:
		offset = sarama.OffsetOldest
	case proto.Offset_OFFSET_NEWEST:
		offset = sarama.OffsetNewest
	default:
		offset = sarama.OffsetOldest
	}
	// TODO: un hardcode some of this stuff
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = offset
	config.Metadata.RefreshFrequency = 30 * time.Second
	if h.version != nil {
		config.Version = *h.version
	}

	c, err := cluster.NewConsumer(h.brokers, conf.consumer, []string{conf.topic}, config)
	if err != nil {
		return err
	}

	defer func() {
		_ = c.Close()
	}()

	go func() {
		err := h.consume(ctx, c, forClient, toConfirmIds, conf.topic, conf.consumer)
		if err != nil {
			errors <- err
		}
	}()

	var toConfirm []string
	for {
		select {
		case tc := <-toConfirmIds:
			toConfirm = append(toConfirm, tc)
		case cr := <-confirmRequest:
			if len(toConfirm) < 1 {
				return errInvalidConfirm
			}
			if toConfirm[0] != cr.GetMsgID() {
				return errInvalidConfirm
			}
			err := h.confirm(ctx, c, cr.GetMsgID(), conf.topic)
			if err != nil {
				return err
			}
			toConfirm = toConfirm[1:]
		case <-ctx.Done():
			return nil
		case err := <-errors:
			return err
		}
	}
}

func (h *kafkaConsumeHandler) consume(ctx context.Context, c *cluster.Consumer, forClient chan<- *proto.Message, toConfirmID chan string, topic, consumer string) error {

	grpclog.Println("started consume loop")
	defer grpclog.Println("exited consume loop")

	for {
		select {
		case msg, ok := <-c.Messages():
			if !ok {
				return errors.New("kafka message channel was closed")
			}
			confirmID := fmt.Sprintf("%d-%d", msg.Offset, msg.Partition)
			select {
			case toConfirmID <- confirmID:
			case <-ctx.Done():
				grpclog.Println("context is done")
				return c.Close()
			}
			select {
			case forClient <- &proto.Message{Data: msg.Value, Id: confirmID}:
			case <-ctx.Done():
				grpclog.Println("context is done")
				return c.Close()
			}
		case err := <-c.Errors():
			grpclog.Printf("kafka error causing consume exit %v\n", err)
			return err
		case <-ctx.Done():
			grpclog.Println("context is done")
			return c.Close()
		}
	}
}

func (h *kafkaConsumeHandler) confirm(ctx context.Context, c *cluster.Consumer, id string, topic string) error {
	spl := strings.Split(id, "-")
	o, err := strconv.ParseInt(spl[0], 10, 64)
	if err != nil {
		return fmt.Errorf("error parsing message id '%s' : %s", id, err.Error())
	}
	p, err := strconv.ParseInt(spl[1], 10, 32)
	if err != nil {
		return fmt.Errorf("error parsing message id '%s' : %s", id, err.Error())
	}
	c.MarkPartitionOffset(topic, int32(p), o, "")
	return nil
}

type KafkaAsyncSinkFactory struct {
	brokers []string
	version *sarama.KafkaVersion
}

func (f KafkaAsyncSinkFactory) NewAsyncSink(ctx context.Context, config SinkConfig) (substrate.AsyncMessageSink, error) {
	return kafka.NewAsyncMessageSink(kafka.AsyncMessageSinkConfig{
		Topic:   config.Topic,
		Brokers: f.brokers,
		Version: f.version,
	})
}
