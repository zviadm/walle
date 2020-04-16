# Architecture

This section describes key high level ideas behind internal architecture of WALLE. These key ideas play
main role in satisfying three main requirements for the system:
* Durability.
* Low tail latencies.
* High throughout.

## Pipelining instead of batching




## Gap backfilling

Important part of WALLE design is the ability for new servers to quickly join an active quorum without
having to backfill all entries first. More details about protocol itself in [protocol](./protocol.md) documentation.

To handle backfilling of entries without degrading performance, WALLE servers maintain two separate tables for
each stream. First table contains entries only from live requests. Second table contains entries only from backfilling
background thread. This way, both tables only do appending writes. Appends are siginificantly faster and more
performant compared to doing out of order inserts to backfill entries in an already existing table. Cursor abstraction
hides away the fact that there are two separate tables and provides merged view of both to read entries
in increasing EntryId order.
