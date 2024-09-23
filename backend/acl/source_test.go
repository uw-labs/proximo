package acl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uw-labs/proximo/proto"
)

func Test_FactoryPublishOnlyClientCannotConsume(t *testing.T) {

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

	require.NoError(t, err)

	s := AsyncSourceFactory{
		Config: c,
		Next:   backend{},
	}

	ctx := createCtx("dog-publisher", password)

	_, err = s.NewAsyncSource(ctx, &proto.StartConsumeRequest{
		Topic: "dogs",
	})

	assertPermissionDenied(t, err, "no consume permissions for topic dogs")
}

func Test_FactoryConsumeOnlyClientCanConsume(t *testing.T) {
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

	require.NoError(t, err)

	s := AsyncSourceFactory{
		Config: c,
		Next:   backend{},
	}

	ctx := createCtx("dog-consumer", password)

	_, err = s.NewAsyncSource(ctx, &proto.StartConsumeRequest{
		Topic: "dogs",
	})

	assert.NoError(err)
}
