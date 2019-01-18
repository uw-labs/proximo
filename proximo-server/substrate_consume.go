package main

import (
	"context"

	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/uw-labs/substrate"
)

// TODO: wrap errors with appropriate gRPC error codes

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
		return err
	}

	return ctx.Err()
}

func (h substrateConsumeHandler) passMessagesToClient(ctx context.Context, messages <-chan substrate.Message, forClient chan<- *Message, toAck chan<- *ackMessage) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case sMsg := <-messages:
			ackMsg := &ackMessage{
				id:           uuid.Must(uuid.NewV4()).String(),
				substrateMsg: sMsg,
			}
			select {
			case <-ctx.Done():
				return nil
			case toAck <- ackMsg:
			}
			pMsg := &Message{
				Id:   ackMsg.id,
				Data: sMsg.Data(),
			}
			select {
			case <-ctx.Done():
				return nil
			case forClient <- pMsg:
			}
		}
	}
}

func (h substrateConsumeHandler) passAcksToSubstrate(ctx context.Context, confirmRequest <-chan *Confirmation, acks chan<- substrate.Message, toAck <-chan *ackMessage) error {
	ackMap := make(map[string]substrate.Message)

	for {
		select {
		case <-ctx.Done():
			return nil
		case ackMsg := <-toAck:
			if dMsg, ok := ackMsg.substrateMsg.(substrate.DiscardableMessage); ok {
				// Discard payload to save space
				dMsg.DiscardPayload()
			}
			ackMap[ackMsg.id] = ackMsg.substrateMsg
		case conf := <-confirmRequest:
			sMsg, ok := ackMap[conf.MsgID]
			if !ok {
				return errors.Errorf("invalid confirmation for message with id `%s`", conf.MsgID)
			}
			delete(ackMap, conf.MsgID)

			select {
			case <-ctx.Done():
				return nil
			case acks <- sMsg:
			}
		}
	}
}
