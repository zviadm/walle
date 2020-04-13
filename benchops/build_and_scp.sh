#!/bin/bash
set -ex
set -o pipefail

SCRIPTPATH=`realpath $0`
SOURCEDIR=$(dirname $(dirname "$SCRIPTPATH"))

N=$1
COPYONLY=$2

docker run -i -t --rm \
	-v $SOURCEDIR:/root/src:cached \
	-v $HOME/.tt_cache:/root/.cache:delegated \
	-w /root/src \
	tt-walle:latest \
	/root/go1.14/bin/go install ./walle/walle ./walle/wctl

if [[ -z $N ]]; then
	gcloud compute scp $HOME/.tt_cache/goroot/bin/wctl wctl-0:
	exit 0
fi
for i in $(seq 0 $N); do
	gcloud compute ssh wnode-$i -- "sudo systemctl stop walle" || true
	gcloud compute scp $HOME/.tt_cache/goroot/bin/walle wnode-$i:
	(test -z $COPYONLY) && gcloud compute ssh wnode-$i -- "sudo systemctl start walle"
done
exit 0