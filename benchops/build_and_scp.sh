#!/bin/bash
set -ex
set -o pipefail

SCRIPTPATH=`realpath $0`
SOURCEDIR=$(dirname $(dirname "$SCRIPTPATH"))

docker run -i -t --rm \
	-v $SOURCEDIR:/root/src:cached \
	-v $HOME/.tt_cache:/root/.cache:delegated \
	-w /root/src \
	tt-walle:latest \
	/root/go1.14/bin/go install ./walle/walle ./walle/wctl

gcloud compute scp $HOME/.tt_cache/goroot/bin/wctl wctl-0:
for i in {0..2}; do
	gcloud compute scp $HOME/.tt_cache/goroot/bin/walle wnode-$i:
done