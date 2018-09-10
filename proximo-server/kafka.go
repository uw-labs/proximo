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
)

type kafkaHandler struct {
	brokers []string
}

func (h *kafkaHandler) HandleConsume(ctx context.Context, consumer, topic string, forClient chan<- *Message, confirmRequest <-chan *Confirmation) error {
	toConfirmIds := make(chan string)

	errors := make(chan error)

	// TODO: un hardcode some of this stuff
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Metadata.RefreshFrequency = 30 * time.Second

	c, err := cluster.NewConsumer(h.brokers, consumer, []string{topic}, config)
	if err != nil {
		return err
	}

	defer func() {
		_ = c.Close()
	}()

	go func() {
		err := h.consume(ctx, c, forClient, toConfirmIds, topic, consumer)
		if err != nil {
			errors <- err
		}
	}()

	for {
		select {
		case conf := <-confirmRequest:
			err := h.confirm(ctx, c, conf.GetMsgID(), toConfirmIds, topic)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		case err := <-errors:
			return err
		}
	}
}

func (h *kafkaHandler) consume(ctx context.Context, c *cluster.Consumer, forClient chan<- *Message, toConfirmID chan string, topic, consumer string) error {

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
			case forClient <- &Message{Data: msg.Value, Id: confirmID}:
			case <-ctx.Done():
				grpclog.Println("context is done")
				return c.Close()
			}
			select {
			case toConfirmID <- confirmID:
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

func (h *kafkaHandler) confirm(ctx context.Context, c *cluster.Consumer, id string, toConfirmID chan string, topic string) error {
	select {
	case cid := <-toConfirmID:
		if cid != id {
			return errInvalidConfirm
		}
		spl := strings.Split(cid, "-")
		o, err := strconv.ParseInt(spl[0], 10, 64)
		if err != nil {
			return fmt.Errorf("error parsing message id '%s' : %s", id, err.Error())
		}
		p, err := strconv.ParseInt(spl[1], 10, 32)
		if err != nil {
			return fmt.Errorf("error parsing message id '%s' : %s", id, err.Error())
		}
		c.MarkPartitionOffset(topic, int32(p), o, "")
	case <-ctx.Done():
	}
	return nil
}

func (h *kafkaHandler) HandleProduce(ctx context.Context, topic string, forClient chan<- *Confirmation, messages <-chan *Message) error {
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
				Topic: topic,
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
