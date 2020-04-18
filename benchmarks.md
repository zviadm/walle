# Benchmarks & Performance

Key aspect of WALLE experiment is to demonstrate feasibility of this system from performance perspective.
While benchmarks don't always tell the full story of what might happen in real production scenarios, they can
still show case what is actually possible, and find an upper bound of sorts on performance.

## Setup

Benchmarks were setup with 1 dedicated benchmarking client node and 3 WALLE nodes. Nodes were setup in
GCP, each node in different availability zone of the same region. Janky scripts that were used to setup
up benchmarks are located in [./benchops](./benchops) folder.

Hardware:
* WCTL benchmarking node:
	- n1-highcpu-2
* WALLE nodes:
	- n1-highcpu-4
	- Local SSD, mounted with `nobarrier` flag

## Scenarios

Steady state, with occasional node restarts:
* Push 10k QPS, 10MB/s with 1 stream.
* Push 10k QPS, 10MB/s with 10 stream.
* Push 10k QPS, 10MB/s with 30 stream.
* Push 8k QPS, 50MB/s with 1 stream. (high BW)

Full node replacement:
* Push 10k QPS, 10MB/s with 30 streams, while backfilling 100GB of data on one node.

Steady state, with active data trimming (TODO(zviadm)):
* Data trimming isn't implemented thus no benchmark yet.

## Results

### Steady state

* For 10k QPS, 10MB/s scenarios:
	* Latencies stayed stable, tail latencies highest with 30 streams: `p.95 ~20ms`, `p.999 ~50ms`
	* Run with `1 stream` only pushed ~9k QPS, which is limited mostly by client, since entries need to
	written in order.
	* Node restarts and gap backfilling due to restarts didn't have any noticeable consequences.

* For 8k QPS, 50MB/s scenario:
	* Latencies were very stable: `.p999 <100ms`.
	* Demonstrates that even a single stream can push very high bandwidth even if QPS
	has limitations for single stream.

### Node replacement

In this experiment, after nodes were filled up to ~100GB of data, one of the nodes was
fully wiped and replaced with a clean node.
* p999 latencies went from ~50ms to ~100ms. Still very stable.
* Full backfill finished in <45 minutes. Backfilled at a rate of 65-70 MB/s.

# Performance considerations

...

## Hardware

List of hardware priorities for WALLE nodes.

### SSD storage & `fsync` latency
`fsync` latency is fundamental requirement to achieve low latency writes. Low latency `fsync`-s can be achieved
by using battery backed disk devices or battery backed raid controllers. GCP style Local SSD that flushes
every 2 seconds, mounted with `nobarrier` option, is also going to be fine from durability perspective
for almost all use cases.

### CPU capacity
WALLE servers are generally going to be CPU bound, especially for higher QPS, lower bandwidth use cases. Ratio of
~4-8 hyperthreaded cores (i.e. vCPUs) per each SSD disk would provide good balance between CPU and Disk.

### Disk i/o capacity
Disk i/o capacity is unlikely to be bottleneck with reasonable SSD disks. WALLE reads and writes from disk are well
optimized. It is more likely that either CPU or disk space capacity would run out first, before disk i/o would become
a problem on SSDs.

### Memory
WALLE has fairly small memory requirements. Ratio of 1GB per hyperthreaded core should be enough in all use
cases. Most likely, even less memory per core could work too.