package main

import (
	"context"
	"io"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
	"github.com/uw-labs/sync/gogroup"
)

var (
	errStartedTwice   = status.Error(codes.InvalidArgument, "consumption already started")
	errInvalidConfirm = status.Error(codes.InvalidArgument, "invalid confirmation")
	errNotConnected   = status.Error(codes.InvalidArgument, "not connected to a topic")
	errInvalidRequest = status.Error(codes.InvalidArgument, "invalid consumer request - this is possibly a bug in your client library")
)

type SourceServer struct {
	SourceFactory AsyncSourceFactory
}

func (s *SourceServer) Consume(stream proto.MessageSource_ConsumeServer) error {
	sCtx := stream.Context()

	g, ctx := gogroup.New(sCtx)

	confirmations := make(chan string)
	startRequest := make(chan *proto.StartConsumeRequest)

	toAck := make(chan *ackMsg)
	acks := make(chan substrate.Message)
	messages := make(chan substrate.Message)

	g.Go(func() error {
		return s.receiveConfirmations(ctx, stream, startRequest, confirmations)
	})
	g.Go(func() error {
		return s.handleAcks(ctx, confirmations, acks, toAck)
	})
	g.Go(func() error {
		return s.sendMessages(ctx, stream, messages, toAck)
	})
	g.Go(func() error {
		var req *proto.StartConsumeRequest
		select {
		case req = <-startRequest:
		case <-ctx.Done():
			return nil
		}

		source, err := s.SourceFactory.NewAsyncSource(ctx, req)
		if err != nil {
			return err
		}
		defer source.Close()

		return source.ConsumeMessages(ctx, messages, acks)
	})

	if err := g.Wait(); err != nil {
		return err
	}

	if err := sCtx.Err(); err == context.Canceled {
		return status.Error(codes.Canceled, err.Error())
	}
	return sCtx.Err()
}

// receiveSourceStream is a subset of proto.MessageSource_ConsumeServer that only exposes the receive method
type receiveSourceStream interface {
	Recv() (*proto.ConsumerRequest, error)
}

// receiveConfirmations receives confirmations from the client
func (s *SourceServer) receiveConfirmations(ctx context.Context, stream receiveSourceStream, startRequest chan<- *proto.StartConsumeRequest, confirmations chan<- string) error {
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
			case confirmations <- msg.GetConfirmation().GetMsgID():
			case <-ctx.Done():
				return nil
			}
		default:
			return errInvalidRequest
		}
	}
}

// sendSourceStream is a subset of proto.MessageSource_ConsumeServer that only exposes the send method
type sendSourceStream interface {
	Send(*proto.Message) error
}

// sendMessages sends messages to the client
func (s *SourceServer) sendMessages(ctx context.Context, stream sendSourceStream, messages <-chan substrate.Message, toAck chan<- *ackMsg) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-messages:
			aMsg := &ackMsg{
				id:  generateID(),
				msg: msg,
			}
			pMsg := &proto.Message{
				Id: aMsg.id,
				// Read the data now so that we can safely discard the payload
				// once the message is passed to the ack handler.
				Data: msg.Data(),
			}
			select {
			case <-ctx.Done():
				return nil
			case toAck <- aMsg:
			}
			err := stream.Send(pMsg)
			if err != nil {
				if strings.HasSuffix(err.Error(), "context canceled") {
					return nil
				}
				return err
			}
		}
	}
}

func (s *SourceServer) handleAcks(ctx context.Context, confirmations <-chan string, acks chan<- substrate.Message, toAck <-chan *ackMsg) error {
	ackMap := make(map[string]substrate.Message)

	for {
		select {
		case <-ctx.Done():
			return nil
		case ackMsg := <-toAck:
			if dMsg, ok := ackMsg.msg.(substrate.DiscardableMessage); ok {
				dMsg.DiscardPayload() // Discard payload to save space
			}
			ackMap[ackMsg.id] = ackMsg.msg
		case msgID := <-confirmations:
			sMsg, ok := ackMap[msgID]
			if !ok {
				return status.Errorf(codes.InvalidArgument, "no message to confirm with id: %s", msgID)
			}

			// Substrate backend should always be ready to accept
			// an acknowledgement, so this shouldn't block
			select {
			case <-ctx.Done():
				return nil
			case acks <- sMsg:
			}

			delete(ackMap, msgID)
		}
	}
}

// ackMsg is a mapping from substrate message to proximo message id
type ackMsg struct {
	id  string
	msg substrate.Message
}
