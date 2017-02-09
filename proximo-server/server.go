package main

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/utilitywarehouse/proximo/go-proximo"
)

var (
	errStartedTwice   = errors.New("consumption already started")
	errInvalidConfirm = errors.New("invalid confirmation")
)

type consumeServer struct {
	handler func(ctx context.Context, consumer, topic string, forClient chan *proximo.Message, confirmRequest chan *proximo.Confirmation) error
}

func (s *consumeServer) Consume(stream proximo.MessageSource_ConsumeServer) error {

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	startRequest := make(chan *proximo.StartRequest)
	confirmRequest := make(chan *proximo.Confirmation)
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
		return ctx.Err()
	}

	forClient := make(chan *proximo.Message)
	defer close(forClient)

	go func() {
		for m := range forClient {
			err := stream.Send(m)
			if err != nil {
				errors <- err
				return
			}
		}
	}()

	return s.handler(ctx, consumer, topic, forClient, confirmRequest)
}
