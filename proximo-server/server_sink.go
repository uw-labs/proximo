package main

import (
	"context"
	"io"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/uw-labs/proximo/internal/proto"
	"github.com/uw-labs/proximo/pkg/rungroup"
)

type producerConfig struct {
	topic string
}

type produceHandler interface {
	HandleProduce(ctx context.Context, conf producerConfig, forClient chan<- *proto.Confirmation, messages <-chan *proto.Message) error
}

type produceServer struct {
	handler produceHandler
}

func (s *produceServer) Publish(stream proto.MessageSink_PublishServer) error {
	sCtx := stream.Context()

	eg, ctx := rungroup.New(sCtx)

	messages := make(chan *proto.Message)
	forClient := make(chan *proto.Confirmation)
	startRequest := make(chan *proto.StartPublishRequest)

	eg.GoAsync(func() error {
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

	eg.GoAsync(func() error {
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
