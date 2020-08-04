package acl

import (
	"context"

	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

type AsyncSinkFactory struct {
	Config Config
	Next   proximo.AsyncSinkFactory
}

func (s AsyncSinkFactory) NewAsyncSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	scope, err := s.Config.GetClientScope(ctx)
	if err != nil {
		return nil, err
	}

	if !containsRegex(scope.Publish, req.Topic) {
		return nil, ErrUnauthorized
	}

	return s.Next.NewAsyncSink(ctx, req)
}
