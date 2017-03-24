package main

import (
	"context"

	"github.com/nats-io/nats"
)

type natsHandler struct {
	url string
}

func (h *natsHandler) HandleConsume(ctx context.Context, consumer, topic string, forClient chan<- *Message, confirmRequest <-chan *Confirmation) error {

	conn, err := nats.Connect(h.url)
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
			forClient <- &Message{
				Data: m.Data,
				Id:   generateID(),
			}
		case <-ctx.Done():
			return sub.Unsubscribe()
		}
	}
}

func (h *natsHandler) HandleProduce(ctx context.Context, topic string, forClient chan<- *Confirmation, messages <-chan *Message) error {

	conn, err := nats.Connect(h.url)
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
			case forClient <- &Confirmation{msg.GetId()}:
			case <-ctx.Done():
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}
