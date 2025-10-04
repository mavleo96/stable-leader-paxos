#!/bin/bash

rm -rf pb
mkdir -p pb/bank pb/paxos

protoc -I=proto --go_out=paths=source_relative:pb/bank --go-grpc_out=paths=source_relative:pb/bank proto/bank.proto
protoc -I=proto --go_out=paths=source_relative:pb/paxos --go-grpc_out=paths=source_relative:pb/paxos proto/paxos.proto

go mod tidy
