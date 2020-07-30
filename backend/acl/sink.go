package acl

import (
	"context"

	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

type AsyncSinkFactory struct {
	cfg  Config
	sink proximo.AsyncSinkFactory
}

func ProximoACLSinkFactory(cfg Config, factory proximo.AsyncSinkFactory) (proximo.AsyncSinkFactory, error) {
	return &AsyncSinkFactory{
		cfg:  cfg,
		sink: factory,
	}, nil
}

func (s *AsyncSinkFactory) NewAsyncSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	scope, err := s.cfg.GetClientScope(ctx)
	if err != nil {
		return nil, err
	}

	if !containsRegex(scope.Publish, req.Topic) {
		return nil, ErrUnauthorized
	}

	return s.sink.NewAsyncSink(ctx, req)
}
