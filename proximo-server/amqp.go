package main

import (
	"context"
	"strconv"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type amqpHandler struct {
	address string
}

func (h *amqpHandler) HandleConsume(ctx context.Context, consumer, topic string, forClient chan<- *Message, confirmRequest <-chan *Confirmation) error {
	conn, err := amqp.Dial(h.address)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	// ensure exchange exists (but don't create it)
	err = ch.ExchangeDeclarePassive(
		topic,   // name
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
		topic+":"+consumer, // name
		true,  // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}

	// bind queue to exchange if not already done.
	err = ch.QueueBind(q.Name, "", topic, false, nil)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(
		q.Name,   // queue
		consumer, // consumer
		false,    // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)
	if err != nil {
		return err
	}

	errs := make(chan error, 1)
	go func() {
		for cr := range confirmRequest {

			id, err := strconv.ParseUint(cr.GetMsgID(), 10, 64)
			if err != nil {
				errs <- err
				return
			}
			if err := ch.Ack(id, false); err != nil {
				errs <- err
				return
			}
		}
	}()

	for {
		select {
		case msg := <-msgs:
			message := &Message{Id: strconv.FormatUint(msg.DeliveryTag, 10), Data: msg.Body}
			forClient <- message //TODO: can block. fix.
		case <-ctx.Done():
			return ch.Close()
		case err := <-errs:
			return err
		}
	}
}

func (h *amqpHandler) HandleProduce(ctx context.Context, topic string, forClient chan<- *Confirmation, messages <-chan *Message) error {
	panic("not implemented")
}

func (h *amqpHandler) Status() (bool, error) {
	c, err := amqp.Dial(h.address)
	if err != nil {
		return false, errors.Wrapf(err, "failed to dial %s", h.address)
	}
	s := c.ConnectionState()
	if !s.HandshakeComplete {
		return false, errors.New("handshake not completed")
	}
	if err := c.Close(); err != nil {
		return false, errors.Wrapf(err, "failed to close connection to %s", h.address)
	}
	return true, nil
}
