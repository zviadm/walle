#!/bin/bash
set -ex
set -o pipefail

IMAGEFAMILY="ubuntu-1804-lts"
gcloud compute instances create wctl-0 \
	--image-project="ubuntu-os-cloud" --image-family=$IMAGEFAMILY --machine-type="n1-highcpu-2"
for i in {0..2}; do
	gcloud compute instances create wnode-$i \
		--image-project="ubuntu-os-cloud" --image-family=$IMAGEFAMILY --machine-type="n1-highcpu-4" \
		--local-ssd interface="nvme"
done

gcloud compute ssh wctl-0 -- "sudo apt-get install -y --no-install-recommends libsnappy-dev"
for i in {0..2}; do
	gcloud compute ssh wnode-$i -- "
sudo apt-get install -y --no-install-recommends libsnappy-dev
sudo mkfs.ext4 -F /dev/nvme0n1
sudo mkdir -p /mnt/disks/w0
sudo mount -o discard,defaults,nobarrier /dev/nvme0n1 /mnt/disks/w0
sudo chmod a+w /mnt/disks/w0
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
"
done