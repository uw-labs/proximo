package main

import (
	"context"
	"errors"
	"io"
	"strings"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	errStartedTwice   = status.Error(codes.InvalidArgument, "consumption already started")
	errInvalidConfirm = status.Error(codes.InvalidArgument, "invalid confirmation")
	errNotConnected   = errors.New("not connected to a topic")
	errInvalidRequest = status.Error(codes.InvalidArgument, "invalid consumer request - this is possibly a bug in your client library")
)

type consumerConfig struct {
	consumer string
	topic    string
}

type producerConfig struct {
	topic string
}

type consumeHandler interface {
	HandleConsume(ctx context.Context, conf consumerConfig, forClient chan<- *Message, confirmRequest <-chan *Confirmation) error
}

type produceHandler interface {
	HandleProduce(ctx context.Context, conf producerConfig, forClient chan<- *Confirmation, messages <-chan *Message) error
}

type consumeServer struct {
	handler consumeHandler
}

func (s *consumeServer) Consume(stream MessageSource_ConsumeServer) error {
	sCtx := stream.Context()
	eg, ctx := errgroup.WithContext(sCtx)

	forClient := make(chan *Message)
	startRequest := make(chan *StartConsumeRequest)
	confirmRequest := make(chan *Confirmation)

	eg.Go(func() error {
		started := false
		for {
			msg, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				if strings.HasSuffix(err.Error(), "context canceled") {
					return nil
				}
				return err
			}
			switch {
			case msg.GetStartRequest() != nil:
				if started {
					return errStartedTwice
				}
				started = true
				select {
				case startRequest <- msg.GetStartRequest():
				case <-ctx.Done():
					return nil
				}
			case msg.GetConfirmation() != nil:
				if !started {
					return errInvalidConfirm
				}
				select {
				case confirmRequest <- msg.GetConfirmation():
				case <-ctx.Done():
					return nil
				}
			default:
				return errInvalidRequest
			}
		}
	})

	eg.Go(func() error {
		for {
			select {
			case m := <-forClient:
				err := stream.Send(m)
				if err != nil {
					if strings.HasSuffix(err.Error(), "context canceled") {
						return nil
					}
					return err
				}
			case <-ctx.Done():
				return nil
			}
		}
	})

	eg.Go(func() error {
		var conf consumerConfig

		select {
		case sr := <-startRequest:
			conf.topic = sr.GetTopic()
			conf.consumer = sr.GetConsumer()
		case <-ctx.Done():
			return nil //ctx.Err()
		}

		if err := s.handler.HandleConsume(ctx, conf, forClient, confirmRequest); err != nil {
			return err
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	if err := sCtx.Err(); err == context.Canceled {
		return status.Error(codes.Canceled, err.Error())
	}
	return sCtx.Err()
}

// messageSink_PublishServer is a subset of the auto-generated
// MessageSink_PublishServer interface and makes things easier in tests.
type messageSink_PublishServer interface {
	Send(*Confirmation) error
	Recv() (*PublisherRequest, error)
	Context() context.Context
}

type produceServer struct {
	handler produceHandler
}

func (s *produceServer) Publish(stream MessageSink_PublishServer) error {
	return s.publish(stream)
}

func (s *produceServer) publish(stream messageSink_PublishServer) error {

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

	forClient := make(chan *Confirmation)

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
		conf := producerConfig{
			topic: topic,
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
