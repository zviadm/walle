# Protocol - Why not use basic Paxos or Raft?

Since WALLE provides exclusive "writer" concept, it must implement some sort of consensus algorithm internally
to support this. First question of course would be: why not use standard well known algorithm like Paxos or Raft?
The answer to that is due to two main concerns/requirements: "read leases" and "gap handling".

## Read leases

An issue with most standard consensus algorithms, in their base form, is that they tradeoff everything else
for absolute correctness. Including trading off performance for data reads. In practice, this tradeoff rarely
tends to be acceptable. In traditional Paxos or Raft, even when they have a concept of a "master", to read
"current state", first a write needs to be performed to synchronize. Famous "Paxos Made Live"
[[1]](https://www.cs.utexas.edu/users/lorenzo/corsi/cs380d/papers/paper2-1.pdf) paper talks
about this concept in "master leases" section.

There aren't well known standardized versions of "master leases" extension for any of the popular consensus
protocols. Thus a lot of open source systems that implement either Paxos or Raft, tend to have their own flavors
to extend the core protocol to support the leasing concept. Of course such an extension is a major modification
of the original protocol and it can no longer really be classified as a "standard algorithm".

WALLE uses custom consensus protocol that supports "read lease" concept by design not as a tacked on extension to
an existing protocol. Because of that, overall protocol is simpler to implement compared to Paxos or Raft. And
from performance perspective, it allows writes to be confirmed with only 1 network round trip.

Concept of "read lease", does add additional constraints for reads of the current state to be correct. Leases only
work if "time delta" of few seconds can be measured consistently between separate nodes. So, for example, if one host
measures "time passed" to be 'X' seconds (where 'X' is generally pretty small <10), another node in that time period,
must measure time passed to be also within +/-25% of 'X' seconds. In practice, this requirement is easily satisfied, since cpu
clocks don't drift apart that widely in short time periods. However, it is hypothetically possible for this requirement to
be violated, if nodes are run in specilized virtual environments where time delta doesn't come directly from the
monotonic clock of the host system. In any case, all systems that use any kind of time based lease concept, have this
same requirement too, and this is a better tradeoff of correctness and performance for system like WALLE. (Note that
this additional requirement is only for reads, writes would still remain correct and exclusive even with fully messed up clocks).

## Gap handling

Lets say particular stream is being served by 3 nodes: `n1`, `n2`, `n3`. As with most other consensus protocols, only majority
of the nodes need to be up for stream to continue to be available. Lets say one of the nodes fails and it is replaced:
* Node: `n3` goes down. This is fine since `n1` and `n2` can form a majority so stream stays up.
* Since node `n3` isn't coming back, it is replaced with another node `n4`, so now members are `n1`, `n2`, `n4`.

Now question is: when can `n4` start accepting new writes? In a lot of existing protocols, `n4` only starts accepting
new writes once it fully catches up to `n1` & `n2` nodes. However if size of the log is large (100s of GBs), catching
up can take a long time. This creates potential dangers of both data loss and availability issues if either `n1` or
`n2` node fails or flakes in that time period. Another issue is that if there is an extened time period when only 2 nodes
are accepting live writes, it can affect tail latencies, since now any type of glitch or a stall on either `n1` or
`n2` affects live writes.

In WALLE `n4` node immediatelly joins and starts accepting new writes from the last committed entry. It creates a gap
for historical entries that it is missing and starts backfilling them in the background. This way, live writes are still
accepted by all three nodes. If another node, say `n2` were to go down, system still stays up and stable, and there is
still no data loss because `n1` node still has all the entries that is getting replicated in the background.


# API

- ClaimWriter
- WriterStatus
- PutEntry

## ClaimWriter

## WriterStatus

## PutEntry





[1]