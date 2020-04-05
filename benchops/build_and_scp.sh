#!/bin/bash
set -ex
set -o pipefail

SCRIPTPATH=`realpath $0`
SOURCEDIR=$(dirname $(dirname "$SCRIPTPATH"))

N=$1

docker run -i -t --rm \
	-v $SOURCEDIR:/root/src:cached \
	-v $HOME/.tt_cache:/root/.cache:delegated \
	-w /root/src \
	tt-walle:latest \
	/root/go1.14/bin/go install ./walle/walle ./walle/wctl

gcloud compute scp $HOME/.tt_cache/goroot/bin/wctl wctl-0:
if [[ -z $N ]]; then
	exit 0
fi
for i in $(seq 0 $N); do
	gcloud compute ssh wnode-$i -- "sudo systemctl stop walle"
	gcloud compute scp $HOME/.tt_cache/goroot/bin/walle wnode-$i:
	gcloud compute ssh wnode-$i -- "sudo systemctl start walle"
done