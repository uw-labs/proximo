package server

import (
	"context"
	"io"
	"strings"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gofrs/uuid"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

var (
	errStartedTwice   = status.Error(codes.InvalidArgument, "consumption already started")
	errInvalidConfirm = status.Error(codes.InvalidArgument, "invalid confirmation")
	errNotConnected   = status.Error(codes.InvalidArgument, "not connected to a topic")
	errInvalidRequest = status.Error(codes.InvalidArgument, "invalid consumer request - this is possibly a bug in your client library")
)

// SourceInitialiser is an object that initialises `substrate.AsyncMessageSource`
// based on provided `StartConsumeRequest`.
type SourceInitialiser interface {
	NewSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error)
}

// ConsumeServer implements the MessageSourceServer interface
type ConsumeServer struct {
	Initialiser SourceInitialiser
}

// ackMessage is a mapping from substrate message to proximo message id
type ackMessage struct {
	id           string
	substrateMsg substrate.Message
}

// receiveConsumerStream is a subset of the MessageSource_ConsumeServer interface
// that only exposes the receive function
type receiveConsumerStream interface {
	Recv() (*proto.ConsumerRequest, error)
}

// sendConsumerStream is a subset of the MessageSource_ConsumeServer interface
// that only exposes the send function
type sendConsumerStream interface {
	Send(*proto.Message) error
}

func (s *ConsumeServer) Consume(stream proto.MessageSource_ConsumeServer) error {
	sCtx := stream.Context()
	eg, ctx := errgroup.WithContext(sCtx)

	startRequest := make(chan *proto.StartConsumeRequest)
	confirmations := make(chan *proto.Confirmation)

	toAck := make(chan *ackMessage)
	acks := make(chan substrate.Message)
	messages := make(chan substrate.Message)

	eg.Go(func() error {
		return s.receiveFromClient(ctx, stream, startRequest, confirmations)
	})
	eg.Go(func() error {
		return s.sendMessagesToClient(ctx, stream, messages, toAck)
	})
	eg.Go(func() error {
		return s.passAcksToSubstrate(ctx, confirmations, acks, toAck)
	})
	eg.Go(func() error {
		var req *proto.StartConsumeRequest

		select {
		case req = <-startRequest:
		case <-ctx.Done():
			return nil
		}

		source, err := s.Initialiser.NewSource(ctx, req)
		if err != nil {
			return err
		}
		defer source.Close()

		return source.ConsumeMessages(ctx, messages, acks)
	})

	if err := eg.Wait(); err != nil {
		if ackErr, ok := err.(substrate.InvalidAckError); ok {
			return status.Error(codes.InvalidArgument, ackErr.Error())
		}
		return status.Error(codes.Unavailable, err.Error())
	}

	if err := sCtx.Err(); err == context.Canceled {
		return status.Error(codes.Canceled, err.Error())
	}
	return sCtx.Err()
}

func (s *ConsumeServer) receiveFromClient(ctx context.Context, stream receiveConsumerStream, startRequest chan<- *proto.StartConsumeRequest, confirmRequest chan<- *proto.Confirmation) error {
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
			case <-ctx.Done():
				return nil
			case confirmRequest <- msg.GetConfirmation():
			}
		default:
			return errInvalidRequest
		}
	}
}

func (s *ConsumeServer) sendMessagesToClient(ctx context.Context, stream sendConsumerStream, fromSubstrate <-chan substrate.Message, toAck chan<- *ackMessage) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case sMsg := <-fromSubstrate:
			ackMsg := &ackMessage{
				id:           uuid.Must(uuid.NewV4()).String(),
				substrateMsg: sMsg,
			}
			pMsg := &proto.Message{
				Id: ackMsg.id,
				// Read the data now so that we can safely discard the payload
				// once the message is passed to the ack handler.
				Data: sMsg.Data(),
			}
			select {
			case <-ctx.Done():
				return nil
			case toAck <- ackMsg:
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

func (s *ConsumeServer) passAcksToSubstrate(ctx context.Context, fromClient <-chan *proto.Confirmation, toSubstrate chan<- substrate.Message, toAck <-chan *ackMessage) error {
	ackMap := make(map[string]substrate.Message)

	for {
		select {
		case <-ctx.Done():
			return nil
		case ackMsg := <-toAck:
			if dMsg, ok := ackMsg.substrateMsg.(substrate.DiscardableMessage); ok {
				dMsg.DiscardPayload() // Discard payload to save space
			}
			ackMap[ackMsg.id] = ackMsg.substrateMsg
		case ack := <-fromClient:
			sMsg, ok := ackMap[ack.MsgID]
			if !ok {
				return status.Errorf(codes.InvalidArgument, "no message to confirm with id `%s`", ack.MsgID)
			}
			delete(ackMap, ack.MsgID)

			select {
			case <-ctx.Done():
				return nil
			case toSubstrate <- sMsg:
			}
		}
	}
}
