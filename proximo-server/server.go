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
	errNotConnected   = errors.New("not connected to a topic")
	errInvalidRequest = errors.New("invalid consumer request - this is possibly a bug in your client library")
)

type Handler interface {
	HandleConsume(ctx context.Context, consumer, topic string, forClient chan<- *proximo.Message, confirmRequest <-chan *proximo.Confirmation) error
	HandleProduce(ctx context.Context, topic string, forClient chan<- *proximo.Confirmation, messages <-chan *proximo.Message) error
}

type server struct {
	handler Handler
}

func (s *server) Consume(stream proximo.MessageSource_ConsumeServer) error {

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	startRequest := make(chan *proximo.StartConsumeRequest)
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

	go func() {
		err := s.handler.HandleConsume(ctx, consumer, topic, forClient, confirmRequest)
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

func (s *server) Publish(stream proximo.MessageSink_PublishServer) error {

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	startRequest := make(chan *proximo.StartPublishRequest)
	messages := make(chan *proximo.Message)
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
			case msg.GetMsg() != nil:
				if !started {
					errors <- errNotConnected
					return
				}
				messages <- msg.GetMsg()
			}
		}
	}()

	var topic string

	select {
	case sr := <-startRequest:
		topic = sr.GetTopic()
	case <-ctx.Done():
		return nil //ctx.Err()
	}

	forClient := make(chan *proximo.Confirmation)
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

	go func() {
		err := s.handler.HandleProduce(ctx, topic, forClient, messages)
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
