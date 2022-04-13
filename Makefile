install-tools:
	go install \
		google.golang.org/protobuf/cmd/protoc-gen-go \
		google.golang.org/grpc/cmd/protoc-gen-go-grpc \
		github.com/bufbuild/buf/cmd/buf

protos: install-tools
	rm -rf gen && buf generate --config buf.yaml --template buf.gen.yaml
	go mod tidy
