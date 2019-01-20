package main

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"github.com/uw-labs/substrate"
)

type amqpInitialiser struct {
	address string
}

func (i amqpInitialiser) New(conf consumerConfig) (substrate.AsyncMessageSource, error) {
	conn, err := amqp.Dial(i.address)
	if err != nil {
		return nil, err
	}

	return &amqpAsyncMessageSource{
		address:  i.address,
		topic:    conf.topic,
		consumer: conf.consumer,
		conn:     conn,
	}, nil
}

type amqpAsyncMessageSource struct {
	address  string
	topic    string
	consumer string

	conn *amqp.Connection
}

type amqpSubstrateMessage struct {
	id   uint64
	data []byte
}

func (msg *amqpSubstrateMessage) Data() []byte {
	return msg.data
}

func (msg *amqpSubstrateMessage) DiscardPayload() {
	msg.data = nil
}

func (h *amqpAsyncMessageSource) Close() error {
	return h.conn.Close()
}

func (h *amqpAsyncMessageSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	ch, err := h.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	// ensure exchange exists (but don't create it)
	err = ch.ExchangeDeclarePassive(
		h.topic, // name
		"topic", // kind of exchange
		true,    // durable
		false,   // autodelete
		false,   // internal
		false,   // noWait
		nil,     // args
	)
	if err != nil {
		return err
	}

	// ensure queue exists and create if needed
	q, err := ch.QueueDeclare(
		h.topic+":"+h.consumer, // name
		true,                   // durable
		false,                  // delete when usused
		false,                  // exclusive
		false,                  // no-wait
		nil,                    // arguments
	)
	if err != nil {
		return err
	}

	// bind queue to exchange if not already done.
	err = ch.QueueBind(q.Name, "", h.topic, false, nil)
	if err != nil {
		return err
	}

	fromAMQP, err := ch.Consume(
		q.Name,     // queue
		h.consumer, // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return err
	}

	eg, ctx := errgroup.WithContext(ctx)
	toAck := make(chan uint64)

	eg.Go(func() error {
		idsToAck := make([]uint64, 0)
		for {
			select {
			case <-ctx.Done():
				return nil
			case id := <-toAck:
				idsToAck = append(idsToAck, id)
			case msg := <-acks:
				amqpMsg, ok := msg.(*amqpSubstrateMessage)
				if !ok {
					return errors.Errorf("unexpected message type: %v", msg)
				}
				if len(idsToAck) == 0 {
					return substrate.InvalidAckError{Acked: amqpMsg}
				}
				if amqpMsg.id != idsToAck[0] {
					return substrate.InvalidAckError{Acked: amqpMsg, Expected: &amqpSubstrateMessage{id: idsToAck[0]}}
				}
				if err := ch.Ack(amqpMsg.id, false); err != nil {
					return errors.Wrap(err, "failed to acknowledge the message")
				}
				idsToAck = idsToAck[1:]
			}
		}
	})

	eg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case msg := <-fromAMQP:
				select {
				case <-ctx.Done():
					return nil
				case toAck <- msg.DeliveryTag:
				}
				select {
				case <-ctx.Done():
					return nil
				case messages <- &amqpSubstrateMessage{id: msg.DeliveryTag, data: msg.Body}:
				}
			}
		}
	})

	return eg.Wait()
}

func (h *amqpAsyncMessageSource) Status() (*substrate.Status, error) {
	panic("not implemented")
}
