package main

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/pkg/errors"
	"github.com/uw-labs/substrate"
)

type SinkInitialiser interface {
	New(conf producerConfig) (substrate.AsyncMessageSink, error)
}

type substrateMessage struct {
	proximoMsg *Message
}

func (msg *substrateMessage) Data() []byte {
	return msg.proximoMsg.GetData()
}

type substrateProduceHandler struct {
	Initialiser SinkInitialiser
}

func (h substrateProduceHandler) HandleProduce(ctx context.Context, conf producerConfig, forClient chan<- *Confirmation, messages <-chan *Message) error {
	sink, err := h.Initialiser.New(conf)
	if err != nil {
		return err
	}
	defer sink.Close()

	eg, gCtx := errgroup.WithContext(ctx)

	acks := make(chan substrate.Message)
	substrateMessages := make(chan substrate.Message)

	eg.Go(func() error {
		return h.passMessagesToSubstrate(gCtx, messages, substrateMessages)
	})
	eg.Go(func() error {
		return h.passAcksToClient(gCtx, forClient, acks)
	})
	eg.Go(func() error {
		return sink.PublishMessages(gCtx, acks, substrateMessages)
	})

	if err = eg.Wait(); err != nil {
		return err
	}

	return ctx.Err()
}

func (h substrateProduceHandler) passMessagesToSubstrate(ctx context.Context, messages <-chan *Message, toSubstrate chan<- substrate.Message) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case pMsg := <-messages:
			sMsg := &substrateMessage{
				proximoMsg: pMsg,
			}
			select {
			case <-ctx.Done():
				return nil
			case toSubstrate <- sMsg:
			}
		}
	}
}

func (h substrateProduceHandler) passAcksToClient(ctx context.Context, forClient chan<- *Confirmation, fromSubstrate <-chan substrate.Message) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case ackMsg := <-fromSubstrate:
			sMsg, ok := ackMsg.(*substrateMessage)
			if !ok {
				return errors.Errorf("wrong message type from substrate - message: %s", sMsg)
			}
			select {
			case <-ctx.Done():
				return nil
			case forClient <- &Confirmation{MsgID: sMsg.proximoMsg.Id}:
			}
		}
	}
}
