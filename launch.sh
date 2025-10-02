#!/bin/bash

CONFIG_FILE="./config.yaml"

mkdir -p "./logs"
for NODE_ID in $(yq e '.nodes[].id' $CONFIG_FILE); do
    go run cmd/server/main.go --id "$NODE_ID" --config "$CONFIG_FILE" > "./logs/$NODE_ID" 2>&1 &
done
