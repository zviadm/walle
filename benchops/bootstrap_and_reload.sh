#!/bin/bash
set -ex
set -o pipefail

SCRIPTPATH=`realpath $0`
SCRIPTDIR=$(dirname "$SCRIPTPATH")
ZONES=(us-central1-a us-central1-b us-central1-c)

$SCRIPTDIR/build_and_scp.sh 2 copyonly

ZONE=${ZONES[0]}
gcloud compute ssh --zone $ZONE wnode-0 -- "
test -f /mnt/disks/w0/walle/root.pb ||
./walle -walle.storage_dir=/mnt/disks/w0/walle -walle.host=\`(hostname -f)\` -walle.port=5005 -walle.bootstrap_uri=/cluster/bench"
gcloud compute scp --zone $ZONE wnode-0:/mnt/disks/w0/walle/root.pb /tmp/root.pb
for i in {1..2}; do
	ZONE=${ZONES[$i]}
	gcloud compute scp --zone $ZONE /tmp/root.pb wnode-$i:
done
gcloud compute scp /tmp/root.pb wctl-0:

for i in {0..2}; do
	ZONE=${ZONES[$i]}
	gcloud compute scp --zone $ZONE $SCRIPTDIR/walle.service wnode-$i:
	gcloud compute ssh --zone $ZONE wnode-$i -- "
test -f /mnt/disks/w0/walle/root.pb || (mkdir -p /mnt/disks/w0/walle && mv ./root.pb /mnt/disks/w0/walle)
sudo mv ./walle.service /etc/systemd/system
sudo systemctl daemon-reload
sudo systemctl restart walle"
done