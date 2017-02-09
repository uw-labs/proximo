package main

import (
	"context"
	"strconv"

	"github.com/streadway/amqp"
	"github.com/utilitywarehouse/proximo/go-proximo"
)

type amqpHandler struct {
	address string
}

func (mq *amqpHandler) handle(ctx context.Context, consumer, topic string, forClient chan *proximo.Message, confirmRequest chan *proximo.Confirmation) error {
	conn, err := amqp.Dial(mq.address)
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
			println("acking")

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
			message := &proximo.Message{Id: strconv.FormatUint(msg.DeliveryTag, 10), Data: msg.Body}
			forClient <- message //TODO: can block. fix.
		case <-ctx.Done():
			return ch.Close()
		case err := <-errs:
			return err
		}
	}
}
