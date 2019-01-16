package backend

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/uw-labs/proximo/pkg/proto"
)

type CallbackHandler struct {
	Delegate Handler

	BeforeConsume    func(msg *proto.Message)
	BeforeProduce    func(msg *proto.Message)
	BeforeConsumeAck func(ack *proto.Confirmation)
	BeforeProduceAck func(ack *proto.Confirmation)

	OnConsumeError func(err error)
	OnProduceError func(err error)
}

func (h *CallbackHandler) HandleConsume(ctx context.Context, conf ConsumerConfig, forClient chan<- *proto.Message, confirmRequest <-chan *proto.Confirmation) error {
	eg, ctx := errgroup.WithContext(ctx)
	delegateForClient, delegateConfirmRequest := forClient, confirmRequest

	if h.BeforeConsume != nil {
		fromDelegate := make(chan *proto.Message)
		delegateForClient = fromDelegate

		eg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case msg := <-fromDelegate:
					h.BeforeConsume(msg)
					forClient <- msg
				}
			}
		})
	}

	if h.BeforeConsumeAck != nil {
		toDelegate := make(chan *proto.Confirmation)
		delegateConfirmRequest = toDelegate

		eg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case ack := <-confirmRequest:
					h.BeforeConsumeAck(ack)
					toDelegate <- ack
				}
			}
		})
	}

	eg.Go(func() error {
		return h.Delegate.HandleConsume(ctx, conf, delegateForClient, delegateConfirmRequest)
	})

	err := eg.Wait()
	if err != nil && h.OnConsumeError != nil {
		h.OnConsumeError(err)
	}
	return err
}

func (h *CallbackHandler) HandleProduce(ctx context.Context, conf ProducerConfig, forClient chan<- *proto.Confirmation, messages <-chan *proto.Message) error {
	eg, ctx := errgroup.WithContext(ctx)
	delegateForClient, delegateMessages := forClient, messages

	if h.BeforeProduce != nil {
		toDelegate := make(chan *proto.Message)
		delegateMessages = toDelegate

		eg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case msg := <-messages:
					h.BeforeProduce(msg)
					toDelegate <- msg
				}
			}
		})
	}

	if h.BeforeProduceAck != nil {
		fromDelegate := make(chan *proto.Confirmation)
		delegateForClient = fromDelegate

		eg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case ack := <-fromDelegate:
					h.BeforeProduceAck(ack)
					forClient <- ack
				}
			}
		})
	}

	eg.Go(func() error {
		return h.Delegate.HandleProduce(ctx, conf, delegateForClient, delegateMessages)
	})

	err := eg.Wait()
	if err != nil && h.OnProduceError != nil {
		h.OnProduceError(err)
	}
	return err
}
