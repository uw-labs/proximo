package acl

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

var (
	password     = "password"
	passwordHash = "$2y$10$2AzC3Z8L18cP.crFi.ZDsuFdbwrYu16Lnh8y7U1wMO3QPanYuwJIm"
)

func Test_ConsumeOnlyClientCannotPublish(t *testing.T) {
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
	assert.NoError(err)

	ctx := createCtx("dog-consumer", password)
	s, err := c.GetClientScope(ctx)
	assert.NoError(err)

	assert.True(containsRegex(s.Consume, "dogs"), "should be able to consume")
	assert.True(containsRegex(s.Consume, "dogs-puppies"), "should be able to consume")

	assert.False(containsRegex(s.Publish, "dogs"), "shouldn't be able to publish")
	assert.False(containsRegex(s.Publish, "dogs-puppies"), "shouldn't be able to publish")
}

func Test_PublishOnlyClientCAnnotConsume(t *testing.T) {
	assert := require.New(t)

	c, err := ConfigFromString(fmt.Sprintf(`
default:
  roles: []
roles:
- id: "write-dogs"
  publish: ["dogs"]
clients:
- id: "dog-consumer"
  secret: "%s"
  roles: ["write-dogs"]`, passwordHash))
	assert.NoError(err)

	ctx := createCtx("dog-consumer", password)
	s, err := c.GetClientScope(ctx)
	assert.NoError(err)

	assert.True(containsRegex(s.Publish, "dogs"), "should be able to publish")
	assert.True(containsRegex(s.Publish, "dogs-puppies"), "should be able to publish")

	assert.False(containsRegex(s.Consume, "dogs"), "shouldn't be able to consume")
	assert.False(containsRegex(s.Consume, "dogs-puppies"), "shouldn't be able to consume")
}

func Test_UnauthorizedClientNotAllowed(t *testing.T) {
	assert := require.New(t)

	c, err := ConfigFromString(fmt.Sprintf(`
default:
  roles: []
roles:
- id: "write-dogs"
  publish: ["dogs"]
clients:
- id: "dog-consumer"
  secret: "%s"
  roles: ["write-dogs"]`, passwordHash))
	assert.NoError(err)

	ctx := createCtx("dog-consumer", "123")
	_, err = c.GetClientScope(ctx)

	assert.Equal(err, ErrUnauthorized)
}

func Test_ClientWithNoAuthHasDefaultAccess(t *testing.T) {
	assert := require.New(t)

	c, err := ConfigFromString(fmt.Sprintf(`
default:
  roles: ["read-cats"]
roles:
- id: "read-dogs"
  publish: ["dogs"]
- id: "read-cats"
  consume: ["cats"]
clients:
- id: "dog-consumer"
  secret: "%s"
  roles: ["read-dogs"]`, passwordHash))
	assert.NoError(err)

	s, err := c.GetClientScope(context.Background())
	assert.NoError(err)

	assert.True(containsRegex(s.Consume, "cats"), "should be able to consume cats")

	assert.False(containsRegex(s.Consume, "dogs"), "shouldn't be able to consume dogs")
	assert.False(containsRegex(s.Publish, "cats"), "shouldn't be able to publish cats")
	assert.False(containsRegex(s.Publish, "dogs"), "shouldn't be able to publish dogs")
}

func Test_ClientWithMultipleRoles(t *testing.T) {
	assert := require.New(t)

	c, err := ConfigFromString(fmt.Sprintf(`
default:
  roles: ["rw-monkeys"]
roles:
- id: "read-dogs"
  consume: ["dogs.*"]
- id: "write-cats"
  publish: ["cats.*"]
- id: "rw-monkeys"
  consume: ["monkeys"]
  publish: ["monkeys"]
clients:
- id: "dog-consumer"
  secret: "%s"
  roles: ["read-dogs", "write-cats"]`, passwordHash))
	assert.NoError(err)

	ctx := createCtx("dog-consumer", password)
	s, err := c.GetClientScope(ctx)
	assert.NoError(err)

	assert.True(containsRegex(s.Consume, "dogs"), "should be able to consume dogs")
	assert.False(containsRegex(s.Publish, "dogs"), "shouldn't be able to publish dogs")

	assert.False(containsRegex(s.Consume, "cats"), "shouldn't be able to consume cats")
	assert.True(containsRegex(s.Publish, "cats"), "should be able to publish cats")

	assert.True(containsRegex(s.Consume, "monkeys"), "should be able to consume monkeys")
	assert.True(containsRegex(s.Publish, "monkeys"), "should be able to publish monkeys")
}

func createCtx(user, pass string) context.Context {
	ctx := context.Background()
	md := metadata.Pairs("Authorization", fmt.Sprintf("Bearer %s", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", user, pass)))))
	reqCtx := metadata.NewIncomingContext(ctx, md)
	return reqCtx
}
