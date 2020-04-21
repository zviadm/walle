#!/bin/bash
set -ex
set -o pipefail

SCRIPTPATH=`realpath $0`
SOURCEDIR=$(dirname $(dirname "$SCRIPTPATH"))
ZONES=(us-central1-a us-central1-b us-central1-c)

N=$1
COPYONLY=$2

docker run -i -t --rm \
	-v $SOURCEDIR:/root/src:cached \
	-v $HOME/.tt_cache:/root/.cache:delegated \
	-w /root/src \
	tt-walle:latest \
	/root/go1.14/bin/go install ./walle/walle ./walle/wctl

if [[ -z $N || -n "$COPYONLY" ]]; then
	gcloud compute scp $HOME/.tt_cache/goroot/bin/wctl wctl-0:
fi
if [[ -z $N ]]; then
	exit 0
fi
for i in $(seq 0 $N); do
	ZONE=${ZONES[$i]}
	gcloud compute ssh --zone $ZONE wnode-$i -- "sudo systemctl stop walle" || true
	gcloud compute scp --zone $ZONE $HOME/.tt_cache/goroot/bin/walle wnode-$i:
	(test -z $COPYONLY) && gcloud compute ssh --zone $ZONE wnode-$i -- "sudo systemctl start walle"
	(test -z $COPYONLY) && sleep "30s"
done
exit 0