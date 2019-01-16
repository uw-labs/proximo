package server

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/uw-labs/proximo/pkg/backend"
	"github.com/uw-labs/proximo/pkg/proto"
)

var (
	errStartedTwice   = errors.New("consumption already started")
	errInvalidConfirm = errors.New("invalid confirmation")
	errNotConnected   = errors.New("not connected to a topic")
	errInvalidRequest = errors.New("invalid consumer request - this is possibly a bug in your client library")
)

type server struct {
	handler backend.Handler
}

func New(handler backend.Handler) proto.MessageServer {
	return &server{handler: handler}
}

func (s *server) Consume(stream proto.MessageSource_ConsumeServer) error {

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	startRequest := make(chan *proto.StartConsumeRequest)
	confirmRequest := make(chan *proto.Confirmation)
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

	forClient := make(chan *proto.Message)

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
		conf := backend.ConsumerConfig{
			Consumer: consumer,
			Topic:    topic,
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

// messageSink_PublishServer is a subset of the auto-generated
// MessageSink_PublishServer interface and makes things easier in tests.
type messageSink_PublishServer interface {
	Send(*proto.Confirmation) error
	Recv() (*proto.PublisherRequest, error)
	Context() context.Context
}

func (s *server) Publish(stream proto.MessageSink_PublishServer) error {
	return s.publish(stream)
}

func (s *server) publish(stream messageSink_PublishServer) error {

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	startRequest := make(chan *proto.StartPublishRequest)
	messages := make(chan *proto.Message)
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

	forClient := make(chan *proto.Confirmation)

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
		defer close(forClient)
		conf := backend.ProducerConfig{
			Topic: topic,
		}
		err := s.handler.HandleProduce(ctx, conf, forClient, messages)
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
