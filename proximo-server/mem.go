package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"

	"github.com/utilitywarehouse/proximo/go-proximo"
)

func newMemHandler() *memHandler {
	mh := &memHandler{make(chan *produceReq, 1024), make(chan *sub, 1024)}
	go mh.loop()
	return mh
}

type memHandler struct {
	incomingMessages chan *produceReq
	subs             chan *sub
}

func (h *memHandler) HandleConsume(ctx context.Context, consumer, topic string, forClient chan<- *proximo.Message, confirmRequest <-chan *proximo.Confirmation) error {
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

func (h *memHandler) HandleProduce(ctx context.Context, topic string, forClient chan<- *proximo.Confirmation, messages <-chan *proximo.Message) error {
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
						case sub.msgs <- &proximo.Message{inm.message.GetData(), makeID()}:
							remaining = append(remaining, sub)
							sentOne = true
						}
					} else {
						remaining = append(remaining, sub)
					}
				}
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

func makeID() string {
	random := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	_, err := rand.Read(random)
	if err != nil {
		panic(err)
	}
	return base64.URLEncoding.EncodeToString(random)
}
