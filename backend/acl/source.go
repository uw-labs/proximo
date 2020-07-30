package acl

import (
	"context"

	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

type AsyncSourceFactory struct {
	cfg    Config
	source proximo.AsyncSourceFactory
}

func ProximoACLSourceFactory(cfg Config, factory proximo.AsyncSourceFactory) (proximo.AsyncSourceFactory, error) {
	return &AsyncSourceFactory{
		cfg:    cfg,
		source: factory,
	}, nil
}

func (s *AsyncSourceFactory) NewAsyncSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	scope, err := s.cfg.GetClientScope(ctx)
	if err != nil {
		return nil, err
	}

	if !containsRegex(scope.Publish, req.Topic) {
		return nil, ErrUnauthorized
	}

	return s.source.NewAsyncSource(ctx, req)
}
