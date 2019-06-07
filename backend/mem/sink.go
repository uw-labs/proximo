package mem

import (
	"context"

	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

type memSink struct {
	backend *memBackend
	config  *proto.StartPublishRequest
}

func (s memSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
	toAck := make(chan substrate.Message)
	go s.sendConfirmations(ctx, acks, toAck)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-messages:
			select {
			case s.backend.incomingMessages <- &produceReq{topic: s.config.GetTopic(), message: msg}:
				select {
				case acks <- msg:
				case <-ctx.Done():
					return nil
				}
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (s memSink) sendConfirmations(ctx context.Context, acks chan<- substrate.Message, toAck <-chan substrate.Message) {
	toConfirm := make([]substrate.Message, 0)
	for {
		if len(toConfirm) == 0 {
			select {
			case <-ctx.Done():
				return
			case msg := <-toAck:
				toConfirm = append(toConfirm, msg)
			}
		} else {
			select {
			case <-ctx.Done():
				return
			case ids := <-toAck:
				toConfirm = append(toConfirm, ids)
			case acks <- toConfirm[0]:
				toConfirm = toConfirm[1:]
			}
		}
	}
}

func (s memSink) Close() error {
	return nil
}

func (s memSink) Status() (*substrate.Status, error) {
	return &substrate.Status{Working: true}, nil
}
