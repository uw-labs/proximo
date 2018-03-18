package proximo

import (
	"context"
	"errors"
	"io"
	"strings"
)

var (
	ErrStartedTwice   = errors.New("consumption already started")
	ErrInvalidConfirm = errors.New("invalid confirmation")
	ErrNotConnected   = errors.New("not connected to a topic")
	ErrInvalidRequest = errors.New("invalid consumer request - this is possibly a bug in your client library")
)

type Handler interface {
	HandleConsume(ctx context.Context, consumer, topic string, forClient chan<- *Message, confirmRequest <-chan *Confirmation) error
	HandleProduce(ctx context.Context, topic string, forClient chan<- *Confirmation, messages <-chan *Message) error
}

type Server struct {
	Handler Handler
}

func (s *Server) Consume(stream MessageSource_ConsumeServer) error {

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
					errors <- ErrStartedTwice
					return
				}
				startRequest <- msg.GetStartRequest()
				started = true
			case msg.GetConfirmation() != nil:
				if !started {
					errors <- ErrInvalidConfirm
					return
				}
				confirmRequest <- msg.GetConfirmation()
			default:
				errors <- ErrInvalidRequest
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
		err := s.Handler.HandleConsume(ctx, consumer, topic, forClient, confirmRequest)
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

func (s *Server) Publish(stream MessageSink_PublishServer) error {

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	startRequest := make(chan *StartPublishRequest)
	messages := make(chan *Message)
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
					errors <- ErrStartedTwice
					return
				}
				startRequest <- msg.GetStartRequest()
				started = true
			case msg.GetMsg() != nil:
				if !started {
					errors <- ErrNotConnected
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

	forClient := make(chan *Confirmation)
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
		err := s.Handler.HandleProduce(ctx, topic, forClient, messages)
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
