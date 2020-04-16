#!/bin/bash
set -ex
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
for filename in "$SCRIPT_DIR"/**/*.proto; do
	protoc \
		--gogofaster_out=paths=source_relative,plugins=grpc:"$SCRIPT_DIR" \
		--proto_path="$GOPATH/pkg/mod/github.com/gogo/protobuf@v1.3.1/gogoproto" \
		--proto_path="$GOPATH/pkg/mod/github.com/gogo/protobuf@v1.3.1/protobuf/google/protobuf" \
		--proto_path="$SCRIPT_DIR" \
		"$filename"
done
