install-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1
	go install github.com/bufbuild/buf/cmd/buf@v1.69.0

protos: install-tools
	rm -rf gen && buf generate --config buf.yaml --template buf.gen.yaml
	go mod tidy
