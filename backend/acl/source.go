package acl

import (
	"context"

	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

type AsyncSourceFactory struct {
	Config Config
	Next   proximo.AsyncSourceFactory
}

func (s AsyncSourceFactory) NewAsyncSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	scope, err := s.Config.GetClientScope(ctx)
	if err != nil {
		return nil, err
	}

	if !containsRegex(scope.Publish, req.Topic) {
		return nil, ErrUnauthorized
	}

	return s.Next.NewAsyncSource(ctx, req)
}
