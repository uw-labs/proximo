package backend

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/uw-labs/proximo/pkg/proto"
)

type filterHandler struct {
	delegate      Handler
	consumeTopics map[string]bool
	produceTopics map[string]bool
}

// NewFilterHandler returns a handler that only allows access to topics specified in the provided maps
func NewFilterHandler(consumeTopics map[string]bool, produceTopics map[string]bool, delegate Handler) (Handler, error) {
	if delegate == nil {
		return nil, ErrNoDelegate
	}

	return &filterHandler{
		delegate:      delegate,
		consumeTopics: consumeTopics,
	}, nil
}

func (h *filterHandler) HandleConsume(ctx context.Context, conf ConsumerConfig, forClient chan<- *proto.Message, confirmRequest <-chan *proto.Confirmation) error {
	if h.consumeTopics != nil {
		if allowed := h.consumeTopics[conf.Topic]; !allowed {
			return status.Errorf(codes.PermissionDenied, "not allowed to consume from topic with name `%s`", conf.Topic)
		}
	}

	return h.delegate.HandleConsume(ctx, conf, forClient, confirmRequest)
}

func (h *filterHandler) HandleProduce(ctx context.Context, conf ProducerConfig, forClient chan<- *proto.Confirmation, messages <-chan *proto.Message) error {
	if h.produceTopics != nil {
		if allowed := h.produceTopics[conf.Topic]; !allowed {
			return status.Errorf(codes.PermissionDenied, "not allowed to produce to topic with name `%s`", conf.Topic)
		}
	}

	return h.delegate.HandleProduce(ctx, conf, forClient, messages)
}
