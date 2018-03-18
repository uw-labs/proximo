package nsq

import (
	"context"
	"log"

	"github.com/nsqio/go-nsq"
	"github.com/uw-labs/proximo"
)

type NSQHandler struct {
	NsqdTcpAddress   []string
	LookupdHTTPAddrs []string
	Channel          string //must be diferent than ""
}

func (h *NSQHandler) HandleConsume(ctx context.Context, consumer, topic string, forClient chan<- *proximo.Message, confirmRequest <-chan *proximo.Confirmation) error {

	config := nsq.NewConfig()
	nsqconsumer, err := nsq.NewConsumer(topic, h.Channel, config)
	if err != nil {
		return err
	}
	go func() {
		if h.LookupdHTTPAddrs != nil {
			err := nsqconsumer.ConnectToNSQLookupds(h.LookupdHTTPAddrs)
			if err != nil {
				log.Fatalln(err)
			}
		} else {
			err := nsqconsumer.ConnectToNSQDs(h.NsqdTcpAddress)
			if err != nil {
				log.Fatalln(err)
			}
		}

	}()

	ch := make(chan *proximo.Message, 0)
	nsqconsumer.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		//consume the message
		ch <- &proximo.Message{
			Data: m.Body,
			Id:   proximo.GenerateID(),
		}
		return nil
	}))

	for {
		select {
		case <-confirmRequest:
			// drop
		case m := <-ch:
			forClient <- &proximo.Message{
				Data: m.Data,
				Id:   proximo.GenerateID(),
			}
		case <-ctx.Done():
			nsqconsumer.Stop()
			return nil
		}
	}
	return nil
}

func (h *NSQHandler) HandleProduce(ctx context.Context, topic string, forClient chan<- *proximo.Confirmation, messages <-chan *proximo.Message) error {
	producer, err := nsq.NewProducer(h.NsqdTcpAddress[0], nsq.NewConfig())
	if err != nil {
		return err
	}

	for {
		select {
		case msg := <-messages:
			err := producer.Publish(topic, msg.GetData())
			if err != nil {
				return err
			}
			select {
			case forClient <- &proximo.Confirmation{MsgID: msg.GetId()}:
			case <-ctx.Done():
				producer.Stop()
				return nil
			}
		case <-ctx.Done():
			producer.Stop()
			return nil
		}
	}
}
