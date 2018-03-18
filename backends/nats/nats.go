package nats

import (
	"context"

	"github.com/nats-io/nats"
	"github.com/uw-labs/proximo"
)

type NatsHandler struct {
	Url string
}

func (h *NatsHandler) HandleConsume(ctx context.Context, consumer, topic string, forClient chan<- *proximo.Message, confirmRequest <-chan *proximo.Confirmation) error {

	conn, err := nats.Connect(h.Url)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch := make(chan *nats.Msg, 64) //TODO: make 64 configurable at startup time
	sub, err := conn.ChanSubscribe(topic, ch)
	if err != nil {
		return err
	}

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
			return sub.Unsubscribe()
		}
	}
}

func (h *NatsHandler) HandleProduce(ctx context.Context, topic string, forClient chan<- *proximo.Confirmation, messages <-chan *proximo.Message) error {

	conn, err := nats.Connect(h.Url)
	if err != nil {
		return err
	}

	for {
		select {
		case msg := <-messages:
			err := conn.Publish(topic, msg.GetData())
			if err != nil {
				return err
			}
			select {
			case forClient <- &proximo.Confirmation{msg.GetId()}:
			case <-ctx.Done():
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}
