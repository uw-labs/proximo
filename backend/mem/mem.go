package mem

import (
	"context"

	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

func NewBackend() AsyncSinkSourceFactory {
	mh := &memBackend{
		incomingMessages: make(chan *produceReq, 1024),
		subs:             make(chan *sub, 1024),
		last100:          make(map[string][]substrate.Message),
	}
	go mh.loop()
	return mh
}

type memBackend struct {
	incomingMessages chan *produceReq
	subs             chan *sub

	last100 map[string][]substrate.Message
}

func (h *memBackend) NewAsyncSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	return memSource{
		backend: h,
		config:  req,
	}, nil
}

func (h *memBackend) NewAsyncSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	return memSink{
		backend: h,
		config:  req,
	}, nil
}

type memSource struct {
	backend *memBackend
	config  *proto.StartConsumeRequest
}

func (s memSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.backend.subs <- &sub{
		topic:    s.config.GetTopic(),
		consumer: s.config.GetConsumer(),
		offset:   s.config.GetInitialOffset(),
		msgs:     messages,
		ctx:      ctx,
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
	panic("not implemented")
}

func (h memBackend) loop() {
	subs := make(map[string]map[string][]*sub)

	for {
		select {
		case s := <-h.subs:
			all := subs[s.topic]
			if all == nil {
				all = make(map[string][]*sub)
				subs[s.topic] = all
			}
			forThisConsumer := all[s.consumer]
			forThisConsumer = append(forThisConsumer, s)
			all[s.consumer] = forThisConsumer

			if s.offset != proto.Offset_OFFSET_NEWEST {
				h.sendLast100(s)
			}

		case inm := <-h.incomingMessages:
			consumers := subs[inm.topic]
			for _, consumer := range consumers {
				var remaining []*sub
				sentOne := false
				for _, sub := range consumer {
					if !sentOne {
						select {
						case <-sub.ctx.Done():
							// drop expired consumers
						case sub.msgs <- inm.message:
							remaining = append(remaining, sub)
							sentOne = true
						}
					} else {
						remaining = append(remaining, sub)
					}
				}
			}

			h.last100[inm.topic] = append(h.last100[inm.topic], inm.message)
			for len(h.last100[inm.topic]) > 100 {
				h.last100[inm.topic] = h.last100[inm.topic][1:]
			}
		}
	}
}

func (h *memBackend) sendLast100(s *sub) {
	for _, m := range h.last100[s.topic] {
		select {
		case s.msgs <- m:
		case <-s.ctx.Done():
			return
		}
	}
}

type produceReq struct {
	topic   string
	message substrate.Message
}

type sub struct {
	topic    string
	consumer string
	offset   proto.Offset
	msgs     chan<- substrate.Message
	ctx      context.Context
}
