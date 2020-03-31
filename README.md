# WALLE - Write-ahead Log, Lossless Edition

WALLE is an experimental system to provide different kind of reusable piece for building other peristent and replicated
systems like databases. At high, level WALLE provides integrated leader election and durable write ahead logging. Primary
goal of WALLE experiment is to show that it is possible to have a distributed write ahead log, that meet all the criterias
for using it as a write ahead log for most of the varieties of persistent systems.

Key aspects that WALLE strives for:
- Durability and consistency. Equivalent to database systems.
- Low tail latencies (even in 0.99 and 0.999 percentiles). Latency of a single write ~ "1 network round trip + Fsync".
- High throughput. Using pipelining instead of batching to achieve very high throughput while still maintaining very
low tail latencies.

Similar projects:
- https://wecode.wepay.com/posts/waltz-a-distributed-write-ahead-log
- TODO(zviad)...

## Overview

Traditionally there have been two primary ways to build new type of persistent systems using existing core piecies:
1. Build a new system on top of an existing distributed key value store.
2. Build a new storage engine for a distribute database that supports custom storage engines.

For first approach there are natural limitations on performance, since not everything maps performantly to a
key/value store. And more importantly, if due to performance, new system needs to maintain its own in-memory state,
that suddenly requires all the machinery for consistency and routing to be built for new system too.

Second approach was more popular in non distributed databases like MySQL, but is less common with the new distributed
databases. Thre are quite a lot of practical complications with this approach. Point-to-point replication tends
to be too tightly coupled with the storage engine itself, especially in distributed databases. Also storage engine is
generally built as part of the database binary, making it further more complicated to extend the existing system
for new use cases.

With WALLE, primary idea is to get rid of both point-to-point replication and local write ahead log. This way WAL is
a completely separate system, running on completely different set of nodes, with well defined abstractions on an RPC
boundary.

TODO(zviad): Insert graphic of replication with WALLE vs Point-to-point replication.

## Architecture

### Streams

### Topology

###

##


##
