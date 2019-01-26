#!/usr/bin/env bash

python -m grpc_tools.protoc -I=./../proto --python_out=./proximo --grpc_python_out=./proximo ../proto/*.proto
