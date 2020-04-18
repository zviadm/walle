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

Full node replacement:
* Push 10k QPS, 10MB/s with 30 streams, while backfilling 100GB of data on one node.

Steady state, with active data trimming (TODO(zviadm)):
* Data trimming isn't implemented thus no benchmark yet.

## Results

### Steady state

All scenarios had more or less same latency results:
* p95 ~20ms
* p999 ~50ms

Run with `1 stream` only was able to push only ~9k QPS. Other runs did all 10k QPS.
Node restarts and gap backfilling due to restarts didn't have any noticeable consequences.

### Node replacement

In this experiment, after nodes were filled up to ~100GB of data, one of the nodes was
fully wiped and replaced with a clean node.
* p999 latencies went from ~50ms to ~100ms. Still very stable.
* Full backfill finished in <45 minutes. Backfilled at a rate of 65-70 MB/s.

# Performance considerations

...