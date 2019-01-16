package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/uw-labs/proximo/pkg/backend"
	"github.com/uw-labs/proximo/pkg/proto"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"google.golang.org/grpc/grpclog"
)

type kafkaHandler struct {
	brokers []string
	version *sarama.KafkaVersion
}

// NewHandler returns an instance of `backend.Handler` that uses Kafka
func NewHandler(version *sarama.KafkaVersion, brokers []string) (backend.Handler, error) {
	return &kafkaHandler{
		brokers: brokers,
		version: version,
	}, nil
}

func (h *kafkaHandler) HandleConsume(ctx context.Context, conf backend.ConsumerConfig, forClient chan<- *proto.Message, confirmRequest <-chan *proto.Confirmation) error {
	toConfirmIds := make(chan string)

	errors := make(chan error)

	// TODO: un hardcode some of this stuff
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Metadata.RefreshFrequency = 30 * time.Second
	if h.version != nil {
		config.Version = *h.version
	}

	c, err := cluster.NewConsumer(h.brokers, conf.Consumer, []string{conf.Topic}, config)
	if err != nil {
		return err
	}

	defer func() {
		_ = c.Close()
	}()

	go func() {
		err := h.consume(ctx, c, forClient, toConfirmIds, conf.Topic, conf.Consumer)
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
				return backend.ErrInvalidConfirm
			}
			if toConfirm[0] != cr.GetMsgID() {
				return backend.ErrInvalidConfirm
			}
			err := h.confirm(ctx, c, cr.GetMsgID(), conf.Topic)
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

func (h *kafkaHandler) consume(ctx context.Context, c *cluster.Consumer, forClient chan<- *proto.Message, toConfirmID chan string, topic, consumer string) error {

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

func (h *kafkaHandler) confirm(ctx context.Context, c *cluster.Consumer, id string, topic string) error {
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

func (h *kafkaHandler) HandleProduce(ctx context.Context, cfg backend.ProducerConfig, forClient chan<- *proto.Confirmation, messages <-chan *proto.Message) error {
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
				Topic: cfg.Topic,
				Value: sarama.ByteEncoder(m.GetData()),
				// Key = TODO:
			}

			_, _, err = sp.SendMessage(pm)
			if err != nil {
				return err
			}
			forClient <- &proto.Confirmation{MsgID: m.GetId()}
		case <-ctx.Done():
			return nil
		}
	}
}
