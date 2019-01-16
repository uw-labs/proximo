package backend

import (
	"context"
	"errors"

	"github.com/uw-labs/proximo/pkg/proto"
)

var ErrInvalidConfirm = errors.New("invalid confirmation")

type ConsumerConfig struct {
	Consumer string
	Topic    string
}

type ProducerConfig struct {
	Topic string
}

// Handler represents a backend implementation for the proximo server
type Handler interface {
	HandleConsume(ctx context.Context, conf ConsumerConfig, forClient chan<- *proto.Message, confirmRequest <-chan *proto.Confirmation) error
	HandleProduce(ctx context.Context, conf ProducerConfig, forClient chan<- *proto.Confirmation, messages <-chan *proto.Message) error
}
