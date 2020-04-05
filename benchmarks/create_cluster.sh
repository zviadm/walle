#!/bin/bash
set -ex
set -o pipefail

IMAGEFAMILY="ubuntu-1804-lts"
MACHINETYPE="n1-highcpu-2"

gcloud compute instances create wctl-0 \
	--image-project="ubuntu-os-cloud" --image-family=$IMAGEFAMILY --machine-type=$MACHINETYPE
for i in {0..2}; do
	gcloud compute instances create wnode-$i \
		--image-project="ubuntu-os-cloud" --image-family=$IMAGEFAMILY --machine-type=$MACHINETYPE \
		--local-ssd interface="nvme"
done
sleep "2s"
for i in {0..2}; do
	gcloud compute ssh wnode-$i -- "
sudo mkfs.ext4 -F /dev/nvme0n1
sudo mkdir -p /mnt/disks/w0
sudo mount /dev/nvme0n1 /mnt/disks/w0
sudo chmod ug+w /mnt/disks/w0
"
done
