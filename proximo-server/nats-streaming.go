package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/pkg/errors"
)

type natsStreamingHandler struct {
	url       string
	clusterID string
}

func (h *natsStreamingHandler) HandleConsume(ctx context.Context, consumer, topic string, forClient chan<- *Message, confirmRequest <-chan *Confirmation) error {
	conn, err := stan.Connect(h.clusterID, consumer+generateID(), stan.NatsURL(h.url))
	if err != nil {
		return err
	}
	defer conn.Close()

	ackQueue := make(chan *stan.Msg, 32) // TODO: configurable buffer

	ackErrors := make(chan error)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case msg := <-ackQueue:
				select {
				case cr := <-confirmRequest:
					seq, err := strconv.ParseUint(cr.MsgID, 10, 64)
					if err != nil {
						ackErrors <- fmt.Errorf("failed to parse message sequence '%v'", cr.MsgID)
						return
					}
					if seq != msg.Sequence {
						ackErrors <- fmt.Errorf("unexpected message sequence. was %v but wanted %v.", seq, msg.Sequence)
						return
					}
					if err := msg.Ack(); err != nil {
						ackErrors <- fmt.Errorf("failed to ack message with NATS: %v.", err.Error())
						return
					}
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	f := func(msg *stan.Msg) {
		select {
		case <-ctx.Done():
			return
		case forClient <- &Message{Data: msg.Data, Id: strconv.FormatUint(msg.Sequence, 10)}:
			ackQueue <- msg
		}
	}

	_, err = conn.QueueSubscribe(
		topic,
		consumer,
		f,
		stan.StartAt(pb.StartPosition_First),
		stan.DurableName(consumer),
		stan.SetManualAckMode(),
		stan.AckWait(60*time.Second),
	)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		wg.Wait()
		return conn.Close()
	case err := <-ackErrors:
		wg.Wait()
		conn.Close()
		return err
	}

}

func (h *natsStreamingHandler) HandleProduce(ctx context.Context, topic string, forClient chan<- *Confirmation, messages <-chan *Message) error {
	conn, err := stan.Connect(h.clusterID, generateID(), stan.NatsURL(h.url))
	if err != nil {
		return err
	}
	defer conn.Close()

	for {
		select {
		case <-ctx.Done():
			return conn.Close()
		case msg := <-messages:
			err := conn.Publish(topic, msg.GetData())
			if err != nil {
				return err
			}
			select {
			case forClient <- &Confirmation{MsgID: msg.GetId()}:
			case <-ctx.Done():
				return conn.Close()
			}
		}
	}
}

func (h *natsStreamingHandler) Status() (bool, error) {
	conn, err := stan.Connect(h.clusterID, generateID(), stan.NatsURL(h.url))
	if err != nil {
		return false, errors.Wrapf(err, "failed to connect to %s", h.url)
	}
	defer conn.Close()
	if conn.NatsConn().Status() != nats.CONNECTED {
		return false, errors.New("connection status different than CONNECTED")
	}
	return true, nil
}
