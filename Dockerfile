FROM golang:1.12-alpine AS build
RUN apk update && apk add make git gcc musl-dev
WORKDIR /proximo
ADD . /proximo/

RUN go get -v ./...
RUN CGO_ENABLED=0 go build -ldflags '-s -extldflags "-static"' -o /proximo-server ./cmd/proximo-server
RUN CGO_ENABLED=0 go build -ldflags '-s -extldflags "-static"' -o /proximo-client ./cmd/proximo-client

FROM alpine:3.9
RUN apk add --no-cache ca-certificates
COPY --from=build /proximo-server /bin/proximo-server
COPY --from=build /proximo-client /bin/proximo-client

ENTRYPOINT [ "proximo-server" ]
CMD ["--help"]
