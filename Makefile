install-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.0
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
	go install github.com/bufbuild/buf/cmd/buf@v1.3.1

protos: install-tools
	rm -rf gen && buf generate --config buf.yaml --template buf.gen.yaml
	go mod tidy
