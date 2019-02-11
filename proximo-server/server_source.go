package main

import (
	"context"
	"errors"
	"io"
	"strings"
)

var (
	errStartedTwice   = errors.New("consumption already started")
	errInvalidConfirm = errors.New("invalid confirmation")
	errNotConnected   = errors.New("not connected to a topic")
	errInvalidRequest = errors.New("invalid consumer request - this is possibly a bug in your client library")
)

type consumerConfig struct {
	consumer string
	topic    string
}

type consumeHandler interface {
	HandleConsume(ctx context.Context, conf consumerConfig, forClient chan<- *Message, confirmRequest <-chan *Confirmation) error
}

type consumeServer struct {
	handler consumeHandler
}

func (s *consumeServer) Consume(stream MessageSource_ConsumeServer) error {

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	startRequest := make(chan *StartConsumeRequest)
	confirmRequest := make(chan *Confirmation)
	errors := make(chan error, 3)

	go func() {
		started := false
		for {
			msg, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				if strings.HasSuffix(err.Error(), "context canceled") {
					return
				}
				errors <- err
				return
			}
			switch {
			case msg.GetStartRequest() != nil:
				if started {
					errors <- errStartedTwice
					return
				}
				startRequest <- msg.GetStartRequest()
				started = true
			case msg.GetConfirmation() != nil:
				if !started {
					errors <- errInvalidConfirm
					return
				}
				confirmRequest <- msg.GetConfirmation()
			default:
				errors <- errInvalidRequest
				return
			}
		}
	}()

	var topic string
	var consumer string

	select {
	case sr := <-startRequest:
		topic = sr.GetTopic()
		consumer = sr.GetConsumer()
	case <-ctx.Done():
		return nil //ctx.Err()
	}

	forClient := make(chan *Message)

	go func() {
		for {
			select {
			case m := <-forClient:
				err := stream.Send(m)
				if err != nil {
					if strings.HasSuffix(err.Error(), "context canceled") {
						return
					}
					errors <- err
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		conf := consumerConfig{
			consumer: consumer,
			topic:    topic,
		}
		err := s.handler.HandleConsume(ctx, conf, forClient, confirmRequest)
		if err != nil {
			errors <- err
		}
	}()

	select {
	case err := <-errors:
		return err
	case <-ctx.Done():
		return nil
	}

}
