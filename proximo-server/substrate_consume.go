package main

import (
	"context"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/gofrs/uuid"
	"github.com/uw-labs/substrate"
)

type SourceInitialiser interface {
	New(conf consumerConfig) (substrate.AsyncMessageSource, error)
}

type ackMessage struct {
	id           string
	substrateMsg substrate.Message
}

type substrateConsumeHandler struct {
	Initialiser SourceInitialiser
}

func (h substrateConsumeHandler) HandleConsume(ctx context.Context, conf consumerConfig, forClient chan<- *Message, confirmRequest <-chan *Confirmation) error {
	source, err := h.Initialiser.New(conf)
	if err != nil {
		return err
	}
	defer source.Close()

	eg, gCtx := errgroup.WithContext(ctx)

	toAck := make(chan *ackMessage)
	acks := make(chan substrate.Message)
	messages := make(chan substrate.Message)

	eg.Go(func() error {
		return h.passMessagesToClient(gCtx, messages, forClient, toAck)
	})
	eg.Go(func() error {
		return h.passAcksToSubstrate(gCtx, confirmRequest, acks, toAck)
	})
	eg.Go(func() error {
		return source.ConsumeMessages(gCtx, messages, acks)
	})

	if err = eg.Wait(); err != nil {
		if ackErr, ok := err.(substrate.InvalidAckError); ok {
			return status.Error(codes.InvalidArgument, ackErr.Error())
		}
		return status.Error(codes.Unavailable, err.Error())
	}

	if err = ctx.Err(); err == context.Canceled {
		return status.Error(codes.Canceled, err.Error())
	}
	return ctx.Err()
}

func (h substrateConsumeHandler) passMessagesToClient(ctx context.Context, fromSubstrate <-chan substrate.Message, toClient chan<- *Message, toAck chan<- *ackMessage) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case sMsg := <-fromSubstrate:
			ackMsg := &ackMessage{
				id:           uuid.Must(uuid.NewV4()).String(),
				substrateMsg: sMsg,
			}
			pMsg := &Message{
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
			select {
			case <-ctx.Done():
				return nil
			case toClient <- pMsg:
			}
		}
	}
}

func (h substrateConsumeHandler) passAcksToSubstrate(ctx context.Context, fromClient <-chan *Confirmation, toSubstrate chan<- substrate.Message, toAck <-chan *ackMessage) error {
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
