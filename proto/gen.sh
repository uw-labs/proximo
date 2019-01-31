#!/bin/sh
protoc -I../proto --gogoslick_out=plugins=grpc,import_path=proto:. *.proto

