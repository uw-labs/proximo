package mem

import (
	"context"

	"github.com/uw-labs/proximo"
)

func NewMemHandler() *MemHandler {
	mh := &MemHandler{
		incomingMessages: make(chan *produceReq, 1024),
		subs:             make(chan *sub, 1024),
		last100:          make(map[string][]*proximo.Message),
	}
	go mh.loop()
	return mh
}

type MemHandler struct {
	incomingMessages chan *produceReq
	subs             chan *sub

	last100 map[string][]*proximo.Message
}

func (h *MemHandler) HandleConsume(ctx context.Context, consumer, topic string, forClient chan<- *proximo.Message, confirmRequest <-chan *proximo.Confirmation) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	h.subs <- &sub{topic, consumer, forClient, ctx}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-confirmRequest:
			// drop
		}
	}
}

func (h *MemHandler) HandleProduce(ctx context.Context, topic string, forClient chan<- *proximo.Confirmation, messages <-chan *proximo.Message) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-messages:
			select {
			case h.incomingMessages <- &produceReq{topic, msg}:
				select {
				case forClient <- &proximo.Confirmation{MsgID: msg.GetId()}:
				case <-ctx.Done():
					return nil
				}
			case <-ctx.Done():
				return nil
			}
		}

	}
}

func (h MemHandler) loop() {
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
						case sub.msgs <- &proximo.Message{inm.message.GetData(), proximo.GenerateID()}:
							remaining = append(remaining, sub)
							sentOne = true
						}
					} else {
						remaining = append(remaining, sub)
					}
				}
			}

			h.last100[inm.topic] = append(h.last100[inm.topic], &proximo.Message{inm.message.GetData(), proximo.GenerateID()})
			for len(h.last100[inm.topic]) > 100 {
				h.last100[inm.topic] = h.last100[inm.topic][1:]
			}
		}
	}
}

type produceReq struct {
	topic   string
	message *proximo.Message
}

type sub struct {
	topic    string
	consumer string
	msgs     chan<- *proximo.Message
	ctx      context.Context
}
