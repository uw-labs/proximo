package main

import (
	"context"
	"io"
	"strings"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pkg/errors"
	"github.com/uw-labs/substrate"
)

// SinkInitialiser is an object that initialises `substrate.AsyncMessageSink`
// based on provided `PublisherRequest`.
type SinkInitialiser interface {
	NewSink(ctx context.Context, req *StartPublishRequest) (substrate.AsyncMessageSink, error)
}

// produceServer implements the MessageSinkServer interface
type produceServer struct {
	initialiser SinkInitialiser
}

// substrateMessage is a wrapper around proximo message that implements the substrate.Message interface
type substrateMessage struct {
	proximoMsg *Message
}

func (msg *substrateMessage) Data() []byte {
	return msg.proximoMsg.GetData()
}

// receiveProducerStream is a subset of the MessageSink_PublishServer interface
// that only exposes the receive function
type receiveProducerStream interface {
	Recv() (*PublisherRequest, error)
}

// sendProducerStream is a subset of the MessageSink_PublishServer interface
// that only exposes the send function
type sendProducerStream interface {
	Send(*Confirmation) error
}

func (s *produceServer) Publish(stream MessageSink_PublishServer) error {
	sCtx := stream.Context()
	eg, ctx := errgroup.WithContext(sCtx)

	startRequest := make(chan *StartPublishRequest)

	acks := make(chan substrate.Message)
	messages := make(chan substrate.Message)

	eg.Go(func() error {
		return s.receiveFromClient(ctx, stream, startRequest, messages)
	})

	eg.Go(func() error {
		return s.sendAcksToClient(ctx, stream, acks)
	})

	eg.Go(func() error {
		var req *StartPublishRequest

		select {
		case <-ctx.Done():
			return nil
		case req = <-startRequest:
		}

		sink, err := s.initialiser.NewSink(ctx, req)
		if err != nil {
			return err
		}
		defer sink.Close()

		return sink.PublishMessages(ctx, acks, messages)
	})

	if err := eg.Wait(); err != nil {
		return err
	}

	if err := sCtx.Err(); err == context.Canceled {
		return status.Error(codes.Canceled, err.Error())
	}
	return sCtx.Err()
}

func (s *produceServer) receiveFromClient(ctx context.Context, stream receiveProducerStream, startRequest chan<- *StartPublishRequest, toSubstrate chan<- substrate.Message) error {
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
			startRequest <- msg.GetStartRequest()
			started = true
		case msg.GetMsg() != nil:
			if !started {
				return errNotConnected
			}
			toSubstrate <- &substrateMessage{proximoMsg: msg.GetMsg()}
		default:
			return errInvalidRequest
		}
	}
}

func (s *produceServer) sendAcksToClient(ctx context.Context, stream sendProducerStream, fromSubstrate <-chan substrate.Message) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case ackMsg := <-fromSubstrate:
			sMsg, ok := ackMsg.(*substrateMessage)
			if !ok {
				return errors.Errorf("wrong message type from substrate - message: %s", sMsg)
			}
			if err := stream.Send(&Confirmation{MsgID: sMsg.proximoMsg.Id}); err != nil {
				return err
			}
		}
	}
}
