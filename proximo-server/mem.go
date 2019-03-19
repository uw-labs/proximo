package main

import (
	"context"

	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

func newMemHandler() *memHandler {
	mh := &memHandler{
		incomingMessages: make(chan *produceReq, 1024),
		subs:             make(chan *sub, 1024),
		last100:          make(map[string][]*proto.Message),
	}
	go mh.loop()
	return mh
}

type memHandler struct {
	incomingMessages chan *produceReq
	subs             chan *sub

	last100 map[string][]*proto.Message
}

func (h *memHandler) NewAsyncSink(ctx context.Context, config SinkConfig) (substrate.AsyncMessageSink, error) {
	return memSink{
		backend: h,
		config:  config,
	}, nil
}

func (h *memHandler) HandleConsume(ctx context.Context, conf consumerConfig, forClient chan<- *proto.Message, confirmRequest <-chan *proto.Confirmation) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	h.subs <- &sub{conf.topic, conf.consumer, forClient, ctx}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-confirmRequest:
			// drop
		}
	}
}

type memSink struct {
	backend *memHandler
	config  SinkConfig
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
			case s.backend.incomingMessages <- &produceReq{topic: s.config.Topic, message: msg}:
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
						case sub.msgs <- &proto.Message{Data: inm.message.Data(), Id: generateID()}:
							remaining = append(remaining, sub)
							sentOne = true
						}
					} else {
						remaining = append(remaining, sub)
					}
				}
			}

			h.last100[inm.topic] = append(h.last100[inm.topic], &proto.Message{Data: inm.message.Data(), Id: generateID()})
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
	msgs     chan<- *proto.Message
	ctx      context.Context
}
