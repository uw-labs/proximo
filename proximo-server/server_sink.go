package main

import (
	"context"
	"io"
	"strings"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type producerConfig struct {
	topic string
}

type produceHandler interface {
	HandleProduce(ctx context.Context, conf producerConfig, forClient chan<- *Confirmation, messages <-chan *Message) error
}

type produceServer struct {
	handler produceHandler
}

func (s *produceServer) Publish(stream MessageSink_PublishServer) error {
	sCtx := stream.Context()

	// This context with cancel is used when a goroutine
	// terminates cleanly to shut down the other ones as well
	ctx, cancel := context.WithCancel(sCtx)
	eg, ctx := errgroup.WithContext(ctx)

	startRequest := make(chan *StartPublishRequest)
	messages := make(chan *Message)

	eg.Go(func() error {
		defer cancel()

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
				select {
				case startRequest <- msg.GetStartRequest():
				case <-ctx.Done():
					return nil
				}
				started = true
			case msg.GetMsg() != nil:
				if !started {
					return errNotConnected
				}
				select {
				case messages <- msg.GetMsg():
				case <-ctx.Done():
					return nil
				}
			default:
				return errInvalidRequest
			}
		}
	})

	forClient := make(chan *Confirmation)

	eg.Go(func() error {
		defer cancel()

		for {
			select {
			case msg := <-forClient:
				if err := stream.Send(msg); err != nil {
					return err
				}
			case <-ctx.Done():
				return nil
			}
		}
	})

	eg.Go(func() error {
		defer cancel()

		var conf producerConfig
		select {
		case sr := <-startRequest:
			conf.topic = sr.GetTopic()
		case <-ctx.Done():
			return nil
		}

		return s.handler.HandleProduce(ctx, conf, forClient, messages)
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	if err := sCtx.Err(); err == context.Canceled {
		return status.Error(codes.Canceled, err.Error())
	}
	return sCtx.Err()
}
