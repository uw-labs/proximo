FROM golang:1.9-alpine AS build
RUN apk update && apk add make git gcc musl-dev
WORKDIR /go/src/app
ADD . /go/src/app/
RUN go get ./... \
  && CGO_ENABLED=0 go build -ldflags '-s -extldflags "-static"' -o /proximo-server . \
  && apk del go git musl-dev \
  && rm -rf $GOPATH

FROM alpine:3.6
RUN apk add --no-cache ca-certificates && mkdir /app
COPY --from=build /proximo-server /proximo-server
CMD [ "/proximo-server" ]
