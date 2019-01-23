package main

import (
	"context"
	"io"
	"strings"
)

type producerConfig struct {
	topic string
}

type produceHandler interface {
	HandleProduce(ctx context.Context, conf producerConfig, forClient chan<- *Confirmation, messages <-chan *Message) error
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
