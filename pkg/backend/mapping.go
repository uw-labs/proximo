package backend

import (
	"context"
	"errors"

	"github.com/uw-labs/proximo/pkg/proto"
)

var ErrNoDelegate = errors.New("no delegate specified")

type mappingHandler struct {
	delegate Handler
	topicMap map[string]string
}

// NewMappingHandler returns a handler than maps topics according to the provided map
func NewMappingHandler(topicMap map[string]string, delegate Handler) (Handler, error) {
	if delegate == nil {
		return nil, ErrNoDelegate
	}

	return &mappingHandler{
		delegate: delegate,
		topicMap: topicMap,
	}, nil
}

func (h *mappingHandler) HandleConsume(ctx context.Context, conf ConsumerConfig, forClient chan<- *proto.Message, confirmRequest <-chan *proto.Confirmation) error {
	if topic, ok := h.topicMap[conf.Topic]; ok {
		conf.Topic = topic
	}
	return h.delegate.HandleConsume(ctx, conf, forClient, confirmRequest)
}

func (h *mappingHandler) HandleProduce(ctx context.Context, conf ProducerConfig, forClient chan<- *proto.Confirmation, messages <-chan *proto.Message) error {
	if topic, ok := h.topicMap[conf.Topic]; ok {
		conf.Topic = topic
	}
	return h.delegate.HandleProduce(ctx, conf, forClient, messages)
}
