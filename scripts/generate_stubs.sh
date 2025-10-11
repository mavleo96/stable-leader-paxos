#!/bin/bash

rm -rf pb
mkdir -p pb/paxos

protoc -I=proto --go_out=paths=source_relative:pb/paxos --go-grpc_out=paths=source_relative:pb/paxos proto/paxos.proto

go mod tidy
