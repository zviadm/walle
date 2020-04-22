# Benchmarks & Performance

Key aspect of WALLE experiment is to demonstrate feasibility of this system from performance perspective.
While benchmarks don't always tell the full story of what might happen in real production scenarios, they can
still show case what is actually possible, and find an upper bound of sorts on performance.

## Setup

Benchmarks were setup with 1 dedicated benchmarking client node and 3 WALLE nodes. Nodes were setup in
GCP. Each node was setup in different availability zone of the same region. Janky scripts that were used to
setup up benchmarks are located in [./benchops](./benchops) folder.

Hardware:
* WCTL benchmarking node:
	- n1-highcpu-4
* WALLE nodes:
	- n1-highcpu-4
	- Local SSD, mounted with `nobarrier` flag

## Scenarios

Steady state, with occasional node restarts:
* Push 10k QPS, 10MB/s with 1 stream.
* Push 10k QPS, 10MB/s with 10 streams.
* Push 10k QPS, 10MB/s with 30 streams.
* Push 10k QPS, 50MB/s with 1 stream. (high BW)

Full node replacement:
* Push 10k QPS, 10MB/s with 30 streams, while backfilling 100GB of data on one node.

Steady state, with active data trimming:
* Push 10k QPS, 10MB/s with 30 streams.
* Trimming periodically to keep maximum 100 million entries total (~70GB compressed size)

## Results

### Steady state

* For 10k QPS, 10MB/s scenarios:
	* Latencies stayed stable, tail latencies highest with 30 streams: `p.95 ~20ms`, `p.999 ~50ms`
	* Node restarts and gap backfilling due to restarts didn't have any noticeable consequences.

* For 10k QPS, 50MB/s scenario:
	* Latencies still stable: `.p999 ~200-300ms`.
	* Demonstrates that even a single stream can push very high bandwidth if slightly larger
	entries are used.
	* With such high BW scenarios, tail latencies can be bottlenecked by client itself, not
	by server performance.

### Node replacement

In this experiment, after nodes were filled up to ~100GB of data, one of the nodes was
fully wiped and replaced with a clean node.
* p999 latencies went from ~50ms to ~100ms. Still very stable.
* Full backfill finished in <45 minutes. Backfilled at a rate of 65-70 MB/s.

### Steady state, with trimming

This experiment was run for 24hr time period to make sure performance would be stable
even with large datasets that continue to be periodically trimmed.
Periodic trimming of old data is pretty cheap and doesn't have noticeable effects on
either throughput or tail latencies.

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