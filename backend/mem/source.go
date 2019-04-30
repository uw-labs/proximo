package mem

import (
	"context"

	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

type memSource struct {
	backend *memBackend
	config  *proto.StartConsumeRequest
}

func (s memSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	newSub := &sub{
		topic:    s.config.GetTopic(),
		consumer: s.config.GetConsumer(),
		offset:   s.config.GetInitialOffset(),
		msgs:     messages,
		ctx:      ctx,
	}

	select {
	case <-ctx.Done():
		return nil
	case s.backend.subs <- newSub:
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-acks:
			// drop
		}
	}
}

func (s memSource) Close() error {
	return nil
}

func (s memSource) Status() (*substrate.Status, error) {
	panic("not implemented")
}
