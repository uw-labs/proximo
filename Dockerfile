FROM golang:1.26-alpine AS build
RUN apk add --no-cache make git gcc musl-dev
WORKDIR /proximo

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags '-s -extldflags "-static"' -o /proximo-server ./cmd/proximo-server
RUN CGO_ENABLED=0 go build -ldflags '-s -extldflags "-static"' -o /proximo-client ./cmd/proximo-client

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /proximo-server /bin/proximo-server
COPY --from=build /proximo-client /bin/proximo-client

ENTRYPOINT [ "proximo-server" ]
CMD ["--help"]
