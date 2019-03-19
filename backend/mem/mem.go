package mem

import (
	"context"

	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

func NewBackend() proximo.AsyncSinkSourceFactory {
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
