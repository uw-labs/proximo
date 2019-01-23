package mem

import (
	"context"

	"github.com/uw-labs/substrate"

	"github.com/uw-labs/proximo/proto"
)

// NewBackend returns and starts a new instance of the in memory backend.
func NewBackend() *Backend {
	mh := &Backend{
		incomingMessages: make(chan *produceReq, 1024),
		subs:             make(chan *sub, 1024),
		last100:          make(map[string][]substrate.Message),
	}
	go mh.loop()
	return mh
}

// Backend is an in memory implementation of a message queue. It implements
// both `server.SourceInitialiser` and `server.SinkInitialiser` interfaces.
// The exposed functions return substrate clients that use given instance
// of this object.
type Backend struct {
	incomingMessages chan *produceReq
	subs             chan *sub

	last100 map[string][]substrate.Message
}

func (h *Backend) NewSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	return memSource{
		backend: h,
		req:     req,
	}, nil
}

func (h *Backend) NewSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	return memSink{
		backend: h,
		req:     req,
	}, nil
}

type memSource struct {
	backend *Backend
	req     *proto.StartConsumeRequest
}

func (s memSource) ConsumeMessages(ctx context.Context, messages chan<- substrate.Message, acks <-chan substrate.Message) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.backend.subs <- &sub{
		topic:    s.req.GetTopic(),
		consumer: s.req.GetConsumer(),
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
	backend *Backend
	req     *proto.StartPublishRequest
}

func (s memSink) PublishMessages(ctx context.Context, acks chan<- substrate.Message, messages <-chan substrate.Message) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-messages:
			select {
			case s.backend.incomingMessages <- &produceReq{topic: s.req.Topic, message: msg}:
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

func (s memSink) Close() error {
	return nil
}

func (s memSink) Status() (*substrate.Status, error) {
	panic("not implemented")
}

func (h Backend) loop() {
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

			for _, m := range h.last100[s.topic] {
				s.msgs <- m
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

type produceReq struct {
	topic   string
	message substrate.Message
}

type sub struct {
	topic    string
	consumer string
	msgs     chan<- substrate.Message
	ctx      context.Context
}
