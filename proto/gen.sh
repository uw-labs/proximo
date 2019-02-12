#!/bin/sh
protoc -I. --go_out=plugins=grpc,import_path=proto:. ./*.proto

