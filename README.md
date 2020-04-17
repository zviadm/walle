# WALLE - Write-ahead Log & Leader Election

WALLE is an experimental system to provide different kind of reusable piece for building other persistent and replicated
systems like databases. At high level, WALLE provides integrated leader election and durable write ahead logging. Primary
goal of WALLE experiment is to show that it is possible to have a distributed write ahead log, that meets all criteria
for using it as a write ahead log for variety of persistent systems.

Key aspects that WALLE strives for:
- Durability and consistency. Equivalent to database systems (i.e. FSYNC).
- Low tail latencies (even in `0.99` and `0.999` percentiles). `p.75` latency of a single
write ~"network round trip + fsync". `p.999-s` <100ms even under high throughput.
- High throughput. Using pipelining, instead of batching, to achieve very high throughput while still
maintaining very low tail latencies.

Similar projects:
- https://wecode.wepay.com/posts/waltz-a-distributed-write-ahead-log
- https://logdevice.io/
- TODO(zviadm): Few others to mention (from LinkedIn, Uber, ...)

## Overview

Traditionally there have been two primary ways to build new types of persistent systems using existing core pieces:
1. Build a new system on top of an existing distributed key value store.
2. Build a new storage engine for a distribute database that supports custom storage engines.

For first approach, there are natural limitations on performance, since not everything maps performantly to a
key/value store. And, if due to performance, new system needs to maintain its own in-memory state,
that suddenly requires all the machinery for consistency and routing to be built for the new system too.

Second approach was more popular in non distributed databases like MySQL, but is less common with the new distributed
databases. There are quite a lot of practical complications with this approach. Point-to-point replication tends
to be too tightly coupled with the storage engine itself, especially in distributed databases. Also, storage engine is
generally built as part of the database binary, making it further more complicated to extend the existing system
for new use cases.

With WALLE, primary idea is to get rid of both point-to-point replication and local write ahead log. This way WAL is
a completely separate system, potentially running on completely different set of nodes, with well defined abstractions
on an RPC boundary.

TODO(zviadm): Insert graphic of replication with WALLE vs Point-to-point replication.

## Setup

Terms:
- Deployment
- Cluster
- Node
- Stream

### Deployment

There would generally be only one production deployment of WALLE for all use cases. Even in multi
region setup, there would still be only one main deployment. Separate deployments may exist to separate production
and testing clusters for safety reasons, and to provide safer grounds for testing upgrades and other operational experiments.

### Cluster

Each deployment consists of separate WALLE clusters. Clusters can be region local, or cross region
depending on the use case. Restriction is that each node can only belong to one cluster.

### Node

Node is a single WALLE process. Each node only belongs to one cluster. Each disk on a host is expected to be
handled by separate process, thus each disk would belong to separate node.

## Stream

Each cluster consists of separate streams, and nodes can serve multiple different streams from the same cluster. Stream
is replicated, ordered, exclusively written set of entries. Streams can be independently configured for replication level
and region placement. Each stream is fully independent from each other.

In a nut shell, each stream is expected to be a separate durable write ahead log, that has only one exclusive writer
at a time. Thus it is expected that each partition of a system that is using WALLE, would have separate streams for
each of those partitions.

### Writer

Exclusive writer of a stream is simply referred to as a "writer". Concept of exclusive "writer" is what ties in leader
election and write ahead logging together. System that is writing data to WALLE doesn't need to use another piece
of infrastructure to do leader election. WALLE provides APIs that can be used to elect and claim a "writer"
for a particular stream. APIs also come with protocols on how failover happens if current "writer" is no longer responsive.

Exclusive writer of a stream also comes with the built-in "read lease" or "master lease" concept. This allows
exclusive "writer" to serve reads of the "current state", without needing to perform network calls and other
synchronization operations. More on this in the detailed [protocol](./protocol.md) description.

### Topology

TODO(zviadm):...

# More

* [Protocol](./protocol.md) documentation
* [Architecture](./architecture.md) overview
* [Benchmarks](./benchmarks.md) and notes on performance
