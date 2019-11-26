#!/bin/bash
set -ex
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
protoc --gofast_out=plugins=grpc:"$SCRIPT_DIR" --proto_path="$SCRIPT_DIR" "$SCRIPT_DIR"/walle/*.proto
protoc --gofast_out=plugins=grpc:"$SCRIPT_DIR" --proto_path="$SCRIPT_DIR" "$SCRIPT_DIR"/walleapi/*.proto
