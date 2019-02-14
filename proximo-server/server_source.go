package main

import (
	"context"
	"io"
	"strings"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/uw-labs/proximo/proto"
)

var (
	errStartedTwice   = status.Error(codes.InvalidArgument, "consumption already started")
	errInvalidConfirm = status.Error(codes.InvalidArgument, "invalid confirmation")
	errNotConnected   = status.Error(codes.InvalidArgument, "not connected to a topic")
	errInvalidRequest = status.Error(codes.InvalidArgument, "invalid consumer request - this is possibly a bug in your client library")
)

type consumeHandler interface {
	HandleConsume(ctx context.Context, req *proto.StartConsumeRequest, forClient chan<- *proto.Message, confirmRequest <-chan *proto.Confirmation) error
}

type consumeServer struct {
	handler consumeHandler
}

func (s *consumeServer) Consume(stream proto.MessageSource_ConsumeServer) error {
	sCtx := stream.Context()

	// This context with cancel is used when a goroutine
	// terminates cleanly to shut down the other ones as well
	ctx, cancel := context.WithCancel(sCtx)
	eg, ctx := errgroup.WithContext(ctx)

	forClient := make(chan *proto.Message)
	confirmRequest := make(chan *proto.Confirmation)
	startRequest := make(chan *proto.StartConsumeRequest)

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
		defer cancel()

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
		defer cancel()

		var req *proto.StartConsumeRequest
		select {
		case req = <-startRequest:
		case <-ctx.Done():
			return nil
		}

		return s.handler.HandleConsume(ctx, req, forClient, confirmRequest)
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	if err := sCtx.Err(); err == context.Canceled {
		return status.Error(codes.Canceled, err.Error())
	}
	return sCtx.Err()
}
