package main

import (
	"context"

	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

type AsyncSinkFactory interface {
	NewAsyncSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error)
}

type AsyncSourceFactory interface {
	NewAsyncSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error)
}

type AsyncSinkSourceFactory interface {
	AsyncSinkFactory
	AsyncSourceFactory
}
