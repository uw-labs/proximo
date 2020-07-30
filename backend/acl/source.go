package acl

import (
	"context"

	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

type ACLFactorySource struct {
	cfg    ACLConfig
	source proximo.AsyncSourceFactory
}

func ProximoACLSourceFactory(cfg ACLConfig, factory proximo.AsyncSourceFactory) (proximo.AsyncSourceFactory, error) {
	return &ACLFactorySource{
		cfg:    cfg,
		source: factory,
	}, nil
}

func (s *ACLFactorySource) NewAsyncSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	scope, err := s.cfg.GetClientScope(ctx)
	if err != nil {
		return nil, err
	}

	if !containsRegex(scope.Publish, req.Topic) {
		return nil, ErrUnauthorized
	}

	return s.source.NewAsyncSource(ctx, req)
}
