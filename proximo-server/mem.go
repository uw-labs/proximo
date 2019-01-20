package main

import (
	"context"

	"github.com/uw-labs/substrate"
)

func newMemBackend() *memBackend {
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

func (h *memBackend) NewSource(req *StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	return memSource{
		backend: h,
		req:     req,
	}, nil
}

type memSource struct {
	backend *memBackend
	req     *StartConsumeRequest
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

func (h *memBackend) HandleProduce(ctx context.Context, conf producerConfig, forClient chan<- *Confirmation, messages <-chan *Message) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-messages:
			select {
			case h.incomingMessages <- &produceReq{conf.topic, msg}:
				select {
				case forClient <- &Confirmation{MsgID: msg.GetId()}:
				case <-ctx.Done():
					return nil
				}
			case <-ctx.Done():
				return nil
			}
		}

	}
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
						case sub.msgs <- &substrateMessage{inm.message.GetData()}:
							remaining = append(remaining, sub)
							sentOne = true
						}
					} else {
						remaining = append(remaining, sub)
					}
				}
			}

			h.last100[inm.topic] = append(h.last100[inm.topic], &substrateMessage{inm.message.GetData()})
			for len(h.last100[inm.topic]) > 100 {
				h.last100[inm.topic] = h.last100[inm.topic][1:]
			}
		}
	}
}

type produceReq struct {
	topic   string
	message *Message
}

type sub struct {
	topic    string
	consumer string
	msgs     chan<- substrate.Message
	ctx      context.Context
}

type substrateMessage struct {
	data []byte
}

func (m *substrateMessage) Data() []byte {
	return m.data
}
