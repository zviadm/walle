#!/bin/bash
set -ex
set -o pipefail

SCRIPTPATH=`realpath $0`
SCRIPTDIR=$(dirname "$SCRIPTPATH")
source "$SCRIPTDIR/datadog.key"

IMAGEFAMILY="ubuntu-1804-lts"
ZONES=(us-central1-a us-central1-b us-central1-c)

# gcloud compute instances create wctl-0 --zone us-central1-a \
# 	--image-project="ubuntu-os-cloud" --image-family=$IMAGEFAMILY --machine-type="n1-highcpu-2"
# gcloud compute ssh wctl-0 -- "
# env DD_AGENT_MAJOR_VERSION=7 DD_API_KEY=$DD_API_KEY bash -c \"\$(curl -L https://raw.githubusercontent.com/DataDog/datadog-agent/master/cmd/agent/install_script.sh)\""
for i in {0..2}; do
	ZONE=${ZONES[$i]}
	gcloud compute instances create wnode-$i --zone $ZONE \
		--image-project="ubuntu-os-cloud" --image-family=$IMAGEFAMILY --machine-type="n1-highcpu-4" \
		--local-ssd interface="nvme"
done
for i in {0..2}; do
	ZONE=${ZONES[$i]}
	gcloud compute ssh --zone $ZONE wnode-$i -- "
sudo mkfs.ext4 -F /dev/nvme0n1
sudo mkdir -p /mnt/disks/w0
sudo mount -o discard,defaults,nobarrier /dev/nvme0n1 /mnt/disks/w0
sudo chmod a+w /mnt/disks/w0 &&
(echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled) &&
env DD_AGENT_MAJOR_VERSION=7 DD_API_KEY=$DD_API_KEY bash -c \"\$(curl -L https://raw.githubusercontent.com/DataDog/datadog-agent/master/cmd/agent/install_script.sh)\""
done