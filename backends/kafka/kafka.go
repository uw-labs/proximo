package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/uw-labs/proximo"
	"google.golang.org/grpc/grpclog"
)

type KafkaHandler struct {
	Brokers []string
}

func (h *KafkaHandler) HandleConsume(ctx context.Context, consumer, topic string, forClient chan<- *proximo.Message, confirmRequest <-chan *proximo.Confirmation) error {
	toConfirmIds := make(chan string)

	errors := make(chan error)

	// TODO: un hardcode some of this stuff
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Metadata.RefreshFrequency = 30 * time.Second

	c, err := cluster.NewConsumer(h.Brokers, consumer, []string{topic}, config)
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

func (h *KafkaHandler) consume(ctx context.Context, c *cluster.Consumer, forClient chan<- *proximo.Message, toConfirmID chan string, topic, consumer string) error {

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
			case forClient <- &proximo.Message{Data: msg.Value, Id: confirmID}:
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

func (h *KafkaHandler) confirm(ctx context.Context, c *cluster.Consumer, id string, toConfirmID chan string, topic string) error {
	select {
	case cid := <-toConfirmID:
		if cid != id {
			return proximo.ErrInvalidConfirm
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

func (h *KafkaHandler) HandleProduce(ctx context.Context, topic string, forClient chan<- *proximo.Confirmation, messages <-chan *proximo.Message) error {
	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true

	sp, err := sarama.NewSyncProducer(h.Brokers, conf)
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
			forClient <- &proximo.Confirmation{MsgID: m.GetId()}
		case <-ctx.Done():
			return nil
		}
	}
}
