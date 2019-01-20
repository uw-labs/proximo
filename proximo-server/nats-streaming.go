package main

import (
	"context"

	"github.com/uw-labs/substrate"
	"github.com/uw-labs/substrate/natsstreaming"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats-streaming"
)

type natsStreamingSourceInitialiser struct {
	url         string
	clusterID   string
	maxInflight int
}

func (i natsStreamingSourceInitialiser) NewSource(ctx context.Context, req *StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	return natsstreaming.NewAsyncMessageSource(natsstreaming.AsyncMessageSourceConfig{
		URL:         i.url,
		ClusterID:   i.clusterID,
		ClientID:    "proximo-nats-streaming-" + generateID(),
		Subject:     req.GetTopic(),
		QueueGroup:  req.GetConsumer(),
		MaxInFlight: i.maxInflight,
	})
}

type natsStreamingProduceHandler struct {
	clusterID   string
	maxInflight int
	nc          *nats.Conn
}

func newNatsStreamingProduceHandler(url, clusterID string, maxInflight int) (*natsStreamingProduceHandler, error) {
	nc, err := nats.Connect(url, nats.Name("proximo-nats-streaming-"+generateID()))
	if err != nil {
		return nil, err
	}
	return &natsStreamingProduceHandler{nc: nc, clusterID: clusterID, maxInflight: maxInflight}, nil
}

func (h *natsStreamingProduceHandler) Close() error {
	h.nc.Close()
	return nil
}

func (h *natsStreamingProduceHandler) HandleProduce(ctx context.Context, conf producerConfig, forClient chan<- *Confirmation, messages <-chan *Message) error {

	conn, err := stan.Connect(h.clusterID, generateID(), stan.NatsConn(h.nc))
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return conn.Close()
		case msg := <-messages:
			err := conn.Publish(conf.topic, msg.GetData())
			if err != nil {
				conn.Close()
				return err
			}
			select {
			case forClient <- &Confirmation{MsgID: msg.GetId()}:
			case <-ctx.Done():
				return conn.Close()
			}
		}
	}

}
