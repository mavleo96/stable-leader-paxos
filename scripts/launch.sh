#!/bin/bash

CONFIG_FILE="./configs/config.yaml"

rm -rf "./logs" "./data"
mkdir -p "./logs" "./data"
for NODE_ID in $(yq e '.nodes[].id' $CONFIG_FILE); do
    go run cmd/server/main.go --id "$NODE_ID" --config "$CONFIG_FILE" > "./logs/$NODE_ID" 2>&1 &
done
