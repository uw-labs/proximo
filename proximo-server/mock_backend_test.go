package main

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/uw-labs/proximo/proto"
)

// MockBackend is a simple backend implementation that allows one consumer or publisher at a time and
// allows user to set the messages to be consumed or check the messages that were produced.
type MockBackend struct {
	mutex    sync.Mutex
	messages map[string][]*proto.Message
}

// NewMockBackend returns a new instance of the mock backend.
func NewMockBackend() *MockBackend {
	return &MockBackend{
		messages: make(map[string][]*proto.Message),
	}
}

// GetTopic returns all messages published to a given topic.
func (b *MockBackend) GetTopic(topic string) []*proto.Message {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.messages[topic]
}

// SetTopic sets messages to be consumed for a given topic.
func (b *MockBackend) SetTopic(topic string, messages []*proto.Message) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.messages[topic] = messages
}

func (b *MockBackend) HandleConsume(ctx context.Context, req *proto.StartConsumeRequest, forClient chan<- *proto.Message, confirmRequest <-chan *proto.Confirmation) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	messages, ok := b.messages[req.GetTopic()]
	if !ok || len(messages) == 0 {
		return nil
	}

	msgIndex := 0
	toAckIdx := 0

	processConfirm := func(confirm *proto.Confirmation) error {
		if toAckIdx == msgIndex {
			return status.Error(codes.InvalidArgument, "no acknowledgement expected")
		}
		if messages[toAckIdx].Id != confirm.MsgID {
			return status.Error(codes.InvalidArgument, "wrong acknowledgement")
		}
		toAckIdx++
		return nil
	}

	for {
		if msgIndex < len(messages) {
			select {
			case <-ctx.Done():
				return nil
			case forClient <- messages[msgIndex]:
				msgIndex++
			case confirm := <-confirmRequest:
				if err := processConfirm(confirm); err != nil {
					return err
				}
				if toAckIdx == len(messages) {
					return nil
				}
			}
		} else {
			select {
			case <-ctx.Done():
				return nil
			case confirm := <-confirmRequest:
				if err := processConfirm(confirm); err != nil {
					return err
				}
				if toAckIdx == len(messages) {
					return nil
				}
			}
		}
	}
}

func (b *MockBackend) HandleProduce(ctx context.Context, conf producerConfig, forClient chan<- *proto.Confirmation, messages <-chan *proto.Message) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	msgs, ok := b.messages[conf.topic]
	if !ok {
		msgs = make([]*proto.Message, 0)
	}
	defer func() {
		b.messages[conf.topic] = msgs
	}()

	toConfirm := make([]*proto.Confirmation, 0)
	for {
		if len(toConfirm) == 0 {
			select {
			case <-ctx.Done():
				return nil
			case msg := <-messages:
				msgs = append(msgs, msg)
				toConfirm = append(toConfirm, &proto.Confirmation{MsgID: msg.Id})
			}
		} else {
			select {
			case <-ctx.Done():
				return nil
			case msg := <-messages:
				msgs = append(msgs, msg)
				toConfirm = append(toConfirm, &proto.Confirmation{MsgID: msg.Id})
			case forClient <- toConfirm[0]:
				toConfirm = toConfirm[1:]
			}
		}
	}
}
