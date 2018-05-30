package main

import (
	"context"
)

func newMemHandler() *memHandler {
	mh := &memHandler{
		incomingMessages: make(chan *produceReq, 1024),
		subs:             make(chan *sub, 1024),
		last100:          make(map[string][]*Message),
	}
	go mh.loop()
	return mh
}

type memHandler struct {
	incomingMessages chan *produceReq
	subs             chan *sub

	last100 map[string][]*Message
}

func (h *memHandler) HandleConsume(ctx context.Context, consumer, topic string, forClient chan<- *Message, confirmRequest <-chan *Confirmation) error {
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

func (h *memHandler) HandleProduce(ctx context.Context, topic string, forClient chan<- *Confirmation, messages <-chan *Message) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-messages:
			select {
			case h.incomingMessages <- &produceReq{topic, msg}:
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

func (h *memHandler) Status() (bool, error) {
	return true, nil
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
						case sub.msgs <- &Message{inm.message.GetData(), generateID()}:
							remaining = append(remaining, sub)
							sentOne = true
						}
					} else {
						remaining = append(remaining, sub)
					}
				}
			}

			h.last100[inm.topic] = append(h.last100[inm.topic], &Message{inm.message.GetData(), generateID()})
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
	msgs     chan<- *Message
	ctx      context.Context
}
