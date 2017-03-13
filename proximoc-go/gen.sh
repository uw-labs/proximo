#!/bin/sh
protoc -I../proto --go_out=plugins=grpc,import_path=proximoc:. ../proto/*.proto

