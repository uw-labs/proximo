package acl

import (
	"context"

	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	if !containsRegex(scope.Consume, req.Topic) {
		return nil, status.Errorf(codes.PermissionDenied, "no consume permissions for topic %v", req.Topic)
	}

	return s.Next.NewAsyncSource(ctx, req)
}
