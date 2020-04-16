# Architecture

This section describes key high level ideas behind internal architecture of WALLE. These key ideas play
main role in satisfying three main requirements for the system:
* Durability.
* Low tail latencies.
* High throughout.

## Pipelining instead of batching (Throughput & Latency)

Batching is a standard method for improving throughput of a system. However, batching generally leads to
increased tail latencies and can be problematic for low latency use cases.

Let's say we have a client (i.e. "writer") that is creating ordered entries and it wants to write
these entries to WALLE stream in correct order.

Batching approach would entail client "batching" all ready ordered entries together and sending
it in a single request. Once request comes back as success, next batch of ready entries can be sent out
and so on. This approach can achieve pretty good throughput, however, it can have very bad tail latencies.
Some items might have to wait for full duration of previous batch execution, thus causing significant
stalls for some of the items. This approach can also have very bad tail latency degradations if some sort
of a momentary stall happens on the server, causing client to accumulate a large batch of entries.

To avoid this issue in WALLE, instead of batching, it uses "pipelining". Writer sends all entries that
are ready to be written right away, in parallel. Entries get sorted in a queue on the server, and still
get committed in correct order. This approach is significantly more complex than batching, especially
when taking into account failures/retries/threading/etc.... However, if done correctly, it can achieve
both better throughput and much lower tail latencies compared to simpler batching approach.

## Group Commit (Durability & Throughput)

In addition to request pipeline-ing, WALLE server also performs similar technique to database
[group commit](https://mariadb.com/kb/en/group-commit-for-the-binary-log/). WALLE performs `fsyncs`-s only
from a single thread, and all writes that are ready at the same time get batched behind a single `fsync` call.
This significantly improves throughput even when `fsync`-s are slow. However if `fsync`-s are slow, latencies
will naturally be slow too (it is unavoidable to have at least some of the calls take ~2x`fsync` duration).
Thus, for best use of WALLE, it is important to have hardware setup that allows `fsync`-s to happen in <10ms
times.

## Gap backfilling (Durability, Throughput & Latency)

Important part of WALLE design is the ability for new servers to quickly join an active quorum without
having to backfill all entries first. More details about protocol itself in [protocol](./protocol) documentation.

To handle backfilling of entries without degrading performance, WALLE servers maintain two separate tables for
each stream. First table contains entries only from live requests. Second table contains entries only from backfilling
background thread. This way, both tables only do appending writes. Appends are significantly faster and more
performant, compared to doing out of order inserts to backfill entries in an already existing table. Cursor abstraction
hides away the fact that there are two separate tables and provides merged view of both to read entries
in increasing EntryId order.

## WiredTiger storage engine (Durability)

WALLE stores all its persistent state in a local [WiredTiger](http://source.wiredtiger.com/develop/index.html) database.
WiredTiger's BTree engine is a good fit for WALLE's use-case, since all writes are appends in different tables. Because of
predictable write behavior, writes shouldn't cause any page loads or page evictions, and can always be fully cached.
