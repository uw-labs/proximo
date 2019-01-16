package mem

import (
	"context"

	"github.com/uw-labs/proximo/internal/id"
	"github.com/uw-labs/proximo/pkg/backend"
	"github.com/uw-labs/proximo/pkg/proto"
)

// NewHandler returns an instance of `backend.Handler` that stores messages in memory
func NewHandler() (backend.Handler, error) {
	mh := &memHandler{
		incomingMessages: make(chan *produceReq, 1024),
		subs:             make(chan *sub, 1024),
		last100:          make(map[string][]*proto.Message),
	}
	go mh.loop()

	return mh, nil
}

type memHandler struct {
	incomingMessages chan *produceReq
	subs             chan *sub

	last100 map[string][]*proto.Message
}

func (h *memHandler) HandleConsume(ctx context.Context, conf backend.ConsumerConfig, forClient chan<- *proto.Message, confirmRequest <-chan *proto.Confirmation) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	h.subs <- &sub{conf.Topic, conf.Consumer, forClient, ctx}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-confirmRequest:
			// drop
		}
	}
}

func (h *memHandler) HandleProduce(ctx context.Context, conf backend.ProducerConfig, forClient chan<- *proto.Confirmation, messages <-chan *proto.Message) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-messages:
			select {
			case h.incomingMessages <- &produceReq{conf.Topic, msg}:
				select {
				case forClient <- &proto.Confirmation{MsgID: msg.GetId()}:
				case <-ctx.Done():
					return nil
				}
			case <-ctx.Done():
				return nil
			}
		}

	}
}

func (h memHandler) loop() {
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
						case sub.msgs <- &proto.Message{Data: inm.message.GetData(), Id: id.Generate()}:
							remaining = append(remaining, sub)
							sentOne = true
						}
					} else {
						remaining = append(remaining, sub)
					}
				}
			}

			h.last100[inm.topic] = append(h.last100[inm.topic], &proto.Message{Data: inm.message.GetData(), Id: id.Generate()})
			for len(h.last100[inm.topic]) > 100 {
				h.last100[inm.topic] = h.last100[inm.topic][1:]
			}
		}
	}
}

type produceReq struct {
	topic   string
	message *proto.Message
}

type sub struct {
	topic    string
	consumer string
	msgs     chan<- *proto.Message
	ctx      context.Context
}
