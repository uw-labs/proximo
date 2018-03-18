package gcm

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/uw-labs/proximo"
)

type GCMHandler struct {
	GOOGLE_CLOUD_PROJECT string
	//	GOOGLE_APPLICATION_CREDENTIALS string

	client *pubsub.Client
	once   sync.Once
}

func (g *GCMHandler) Connect() error {
	ctx := context.Background()
	// [START auth]
	if g.GOOGLE_CLOUD_PROJECT == "" {
		fmt.Fprintf(os.Stderr, "GOOGLE_CLOUD_PROJECT must be set.\n")
		os.Exit(1)
	}
	var err error
	g.client, err = pubsub.NewClient(ctx, g.GOOGLE_CLOUD_PROJECT)
	return err
}

func NewGCMHandler(project string) *GCMHandler {
	return &GCMHandler{
		GOOGLE_CLOUD_PROJECT: project,
	}
}

func (h *GCMHandler) HandleConsume(ctx context.Context, consumer, topic string, forClient chan<- *proximo.Message, confirmRequest <-chan *proximo.Confirmation) error {

	if err := h.Connect(); err != nil {
		return err
	}
	h.once.Do(func() {
		if _, err := h.client.CreateTopic(context.Background(), topic); err != nil {
			log.Fatalln(err)
		}
	})
	sub := h.client.Subscription(topic)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := make(chan *proximo.Message, 0)
	go func() {
		err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
			fmt.Printf("Got message: %q\n", string(msg.Data))
			ch <- &proximo.Message{Data: msg.Data, Id: proximo.GenerateID()}
			msg.Ack()
		})
		if err != nil {
			return
		}
	}()

	for {
		select {
		case <-confirmRequest:
			// drop
		case m := <-ch:
			forClient <- &proximo.Message{
				Data: m.Data,
				Id:   proximo.GenerateID(),
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (h *GCMHandler) HandleProduce(ctx context.Context, topic string, forClient chan<- *proximo.Confirmation, messages <-chan *proximo.Message) error {

	if err := h.Connect(); err != nil {
		return err
	}
	h.once.Do(func() {
		if _, err := h.client.CreateTopic(context.Background(), topic); err != nil {
			log.Fatalln(err)
		}
	})
	defer h.client.Close()

	for {
		select {
		case msg := <-messages:
			t := h.client.Topic(topic)
			result := t.Publish(ctx, &pubsub.Message{
				Data: msg.Data,
			})
			// Block until the result is returned and a server-generated
			_, err := result.Get(ctx)
			if err != nil {
				return err
			}
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
