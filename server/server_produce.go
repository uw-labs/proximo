package server

import (
	"context"
	"io"
	"strings"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pkg/errors"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

// SinkInitialiser is an object that initialises `substrate.AsyncMessageSink`
// based on provided `PublisherRequest`.
type SinkInitialiser interface {
	NewSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error)
}

// ProduceServer implements the MessageSinkServer interface
type ProduceServer struct {
	Initialiser SinkInitialiser
}

// substrateMessage is a wrapper around proximo message that implements the substrate.Message interface
type substrateMessage struct {
	proximoMsg *proto.Message
}

func (msg *substrateMessage) Data() []byte {
	return msg.proximoMsg.GetData()
}

// receiveProducerStream is a subset of the MessageSink_PublishServer interface
// that only exposes the receive function
type receiveProducerStream interface {
	Recv() (*proto.PublisherRequest, error)
}

// sendProducerStream is a subset of the MessageSink_PublishServer interface
// that only exposes the send function
type sendProducerStream interface {
	Send(*proto.Confirmation) error
}

func (s *ProduceServer) Publish(stream proto.MessageSink_PublishServer) error {
	sCtx := stream.Context()
	eg, ctx := errgroup.WithContext(sCtx)

	startRequest := make(chan *proto.StartPublishRequest)

	acks := make(chan substrate.Message)
	messages := make(chan substrate.Message)

	eg.Go(func() error {
		return s.receiveFromClient(ctx, stream, startRequest, messages)
	})

	eg.Go(func() error {
		return s.sendAcksToClient(ctx, stream, acks)
	})

	eg.Go(func() error {
		var req *proto.StartPublishRequest

		select {
		case <-ctx.Done():
			return nil
		case req = <-startRequest:
		}

		sink, err := s.Initialiser.NewSink(ctx, req)
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

func (s *ProduceServer) receiveFromClient(ctx context.Context, stream receiveProducerStream, startRequest chan<- *proto.StartPublishRequest, toSubstrate chan<- substrate.Message) error {
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
			case <-ctx.Done():
				return nil
			case startRequest <- msg.GetStartRequest():
			}
		case msg.GetMsg() != nil:
			if !started {
				return errNotConnected
			}
			select {
			case <-ctx.Done():
				return nil
			case toSubstrate <- &substrateMessage{proximoMsg: msg.GetMsg()}:
			}
		default:
			return errInvalidRequest
		}
	}
}

func (s *ProduceServer) sendAcksToClient(ctx context.Context, stream sendProducerStream, fromSubstrate <-chan substrate.Message) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case ackMsg := <-fromSubstrate:
			sMsg, ok := ackMsg.(*substrateMessage)
			if !ok {
				return errors.Errorf("wrong message type from substrate - message: %v", ackMsg)
			}
			if err := stream.Send(&proto.Confirmation{MsgID: sMsg.proximoMsg.Id}); err != nil {
				return err
			}
		}
	}
}
