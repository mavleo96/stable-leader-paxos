#!/bin/bash

rm -rf pb
mkdir -p pb

protoc -I=proto --go_out=paths=source_relative:pb --go-grpc_out=paths=source_relative:pb proto/paxos.proto

go mod tidy
