package mem

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

// TestMemSink_PublishMessagesBuffersConfirmations verifies that
// sendConfirmations buffers messages when the acks channel is
// back-pressured, allowing PublishMessages to keep accepting new
// messages from the caller.
//
// In the broken code messages were sent directly to acks, so
// PublishMessages blocked on every message until the consumer
// drained acks.  With the fix messages route through toAck and
// sendConfirmations buffers them in toConfirm.
func TestMemSink_PublishMessagesBuffersConfirmations(t *testing.T) {
	assert := require.New(t)

	b := NewBackend().(*memBackend)
	sink, err := b.NewAsyncSink(context.Background(), &proto.StartPublishRequest{Topic: "test-topic"})
	assert.NoError(err)

	acks := make(chan substrate.Message)     // unbuffered - back-pressure
	msgs := make(chan substrate.Message, 10) // buffered
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for i := 0; i < 3; i++ {
			msgs <- &testMsg{data: []byte("hello")}
		}
	}()

	done := make(chan struct{})
	go func() {
		sink.PublishMessages(ctx, acks, msgs)
		close(done)
	}()

	// Give PublishMessages time to process the three messages.
	// With the buggy code it blocks on the first ack, so only one
	// message reaches the backend.  With the fix,
	// sendConfirmations buffers all three in toConfirm while
	// waiting for acks to drain, so all three reach the backend.
	time.Sleep(300 * time.Millisecond)

	cancel()
	<-done

	assert.Equal(3, b.messageCount("test-topic"), "expected all three messages to be buffered in the backend")
}

type testMsg struct {
	data []byte
}

func (m *testMsg) Data() []byte {
	return m.data
}
