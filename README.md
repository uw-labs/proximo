# Proximo [![CircleCI](https://circleci.com/gh/uw-labs/proximo.svg?style=svg)](https://circleci.com/gh/uw-labs/proximo)
Proximo is a proxy for multiple different publish-subscribe queuing systems.

It is based on a GRPC interface definition, making it easy to create new client libraries.
It already supports a number of popular queueing systems, and adding new ones is intended to be simple.

## Goals
* Expose multiple consumer (fan out) semantics where needed

* Minimise overhead over direct use of a given queuing system

* Allow configuration of the underlying queue system via runtime configuration of Proximo

* Allow replacement of a queueing system with no change to the Proximo client applications

* Enabling easy creation of client libraries for new languages (anything that has GRPC support)

## Non goals
* Exposing specific details of the underlying queue system via the client API

## Server

This is the Proximo server implementation, written in Go

[proximo server](proximo-server/README.md)

## Proximo client libraries

* **go** - [substrate](https://github.com/uw-labs/substrate) - we recommend to use substrate to access
proximo from go

## API definition (protobuf)

[protobuf definitions](proto/)

## Access Control

Access Control is supported using an optional config file, using the `PROXIMO_ACL_CONFIG`.

In this example, all clients can access the topics that start with `products` but only a client called
`product-writer` has permission to to write to these topics.

```yaml
default:
  roles: ["read-products"]
roles:
- id: "read-products"
  consume: ["products.*"]
- id: "write-products"
  publish: ["products.*"]
clients:
- id: "product-writer"
  secret: "$2y$10$2AzC3Z8L18cP.crFi.ZDsuFdbwrYu16Lnh8y7U1wMO3QPanYuwJIm" # pass is bcrypted hash of "password"
  roles: ["write-products"]
```

Add the token to the context, example:

```golang
sink, _ := proximo.NewAsyncMessageSink(proximo.AsyncMessageSinkConfig{
  Broker:   "localhost:6868",
  Topic:    "products",
  Insecure: true,
})

token := base64.StdEncoding.EncodeToString(fmt.Sprintf("%s:%s", "product-writer", "password"))
md := metadata.Pairs("Authorization", fmt.Sprintf("Bearer %s", token))
reqCtx := metadata.NewOutgoingContext(ctx, md)

sink.PublishMessage(reqCtx, &Message{Data: []byte("hello world")})
```
