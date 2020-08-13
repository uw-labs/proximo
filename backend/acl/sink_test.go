package acl

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uw-labs/proximo/proto"
	"github.com/uw-labs/substrate"
)

func Test_FactoryConsumeOnlyClientCannotPublish(t *testing.T) {
	assert := require.New(t)

	c, err := ConfigFromString(fmt.Sprintf(`
default:
  roles: []
roles:
- id: "read-dogs"
  consume: ["dogs.*"]
clients:
- id: "dog-consumer"
  secret: "%s"
  roles: ["read-dogs"]`, passwordHash))

	s := AsyncSinkFactory{
		Config: c,
		Next:   backend{},
	}

	ctx := createCtx("dog-consumer", password)

	_, err = s.NewAsyncSink(ctx, &proto.StartPublishRequest{
		Topic: "dogs",
	})

	assert.Equal(err, ErrUnauthorized)
}

func Test_FactoryPublishOnlyClientCanPublish(t *testing.T) {
	assert := require.New(t)

	c, err := ConfigFromString(fmt.Sprintf(`
default:
  roles: []
roles:
- id: "write-dogs"
  publish: ["dogs.*"]
clients:
- id: "dog-publisher"
  secret: "%s"
  roles: ["write-dogs"]`, passwordHash))

	s := AsyncSinkFactory{
		Config: c,
		Next:   backend{},
	}

	ctx := createCtx("dog-publisher", password)

	_, err = s.NewAsyncSink(ctx, &proto.StartPublishRequest{
		Topic: "dogs",
	})

	assert.NoError(err)
}

type backend struct {
}

func (s backend) NewAsyncSource(ctx context.Context, req *proto.StartConsumeRequest) (substrate.AsyncMessageSource, error) {
	return nil, nil
}

func (s backend) NewAsyncSink(ctx context.Context, req *proto.StartPublishRequest) (substrate.AsyncMessageSink, error) {
	return nil, nil
}
