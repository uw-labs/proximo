package main

import (
	"context"
	"testing"
	"time"
)

// TestProduceCloseWithAckPending tried to recreate a scenario where a backend
// is trying to send an ack while we are shutting down.  This resulted in a
// panic due to a write to a closed channel.  This test is to ensure the bug
// cannot be re-introduced later.
func TestProduceCloseWithAckPending(t *testing.T) {
	handler := newMockProduceHandler()

	svr := &produceServer{handler}

	tscs := newTestMessageSourceProduceServer()

	// start publish server
	pubErr := make(chan error, 1)
	go func() {
		pubErr <- svr.publish(tscs)
	}()

	// start request, then send message.
	tscs.toSend <- &PublisherRequest{StartRequest: &StartPublishRequest{Topic: "topic"}}
	tscs.toSend <- &PublisherRequest{Msg: &Message{Id: "message1", Data: []byte("message payload")}}

	// without acking message, exit
	tscs.cancel()
	err := <-pubErr
	if err != nil {
		t.Error(err)
	}

	// now ack message
	handler.releaseOne <- struct{}{}

	// ugly, but due to the nature of this bug, we don't have anything to
	// sync on properly.
	time.Sleep(10 * time.Millisecond)
}

func newTestMessageSourceProduceServer() *testMessageSourceProduceServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &testMessageSourceProduceServer{
		ctx:       ctx,
		cancel:    cancel,
		toSend:    make(chan *PublisherRequest),
		toSendErr: make(chan error, 1),
	}
}

type testMessageSourceProduceServer struct {
	ctx       context.Context
	cancel    func()
	toSend    chan *PublisherRequest
	toSendErr chan error
}

func (ms *testMessageSourceProduceServer) Send(*Confirmation) error {
	return nil
}

func (ms *testMessageSourceProduceServer) Recv() (*PublisherRequest, error) {
	select {
	case tr := <-ms.toSend:
		return tr, nil
	case err := <-ms.toSendErr:
		return nil, err
	}
}

func (ms *testMessageSourceProduceServer) Context() context.Context {
	return ms.ctx
}

func newMockProduceHandler() *mockProduceHandler {
	return &mockProduceHandler{
		make(chan struct{}),
	}
}

type mockProduceHandler struct {
	// write to this chan to release a confirmation for a message that's been sent.
	releaseOne chan struct{}
}

func (mh *mockProduceHandler) HandleProduce(ctx context.Context, conf producerConfig, forClient chan<- *Confirmation, messages <-chan *Message) error {

	go mh.loop(forClient, messages)

	<-ctx.Done()
	return nil
}

func (mh *mockProduceHandler) loop(forClient chan<- *Confirmation, messages <-chan *Message) {
	var toConfirm []*Message
	for {
		var rel chan struct{}
		if len(toConfirm) > 0 {
			rel = mh.releaseOne
		}
		select {
		case m := <-messages:
			toConfirm = append(toConfirm, m)
		case <-rel:
			forClient <- &Confirmation{toConfirm[0].Id}
			toConfirm = toConfirm[1:]
		}
	}
}
