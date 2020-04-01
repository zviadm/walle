# Protocol - Why not use basic Paxos or Raft?

Since WALLE provides exclusive "writer" concept, it must implement some sort of consensus algorithm internally
to support this. First question of course would be: why not use standard well known algorithm like Paxos or Raft?
The big issue with most standard consensus algorithms, in their base form, is that they tradeoff everything else
for absolute correctness, including for data reads. However, in practice, this tradeoff is rarely acceptable.
In traditional Paxos or Raft, even when they have a concept of a "master", to read "current state", first a write
needs to be performed to synchronize. Famous "Paxos Made Live" [1] paper talks about this concept in "master leases"
section.

This concept of "read lease" or "master lease" exists in different forms in most of the open source systems that
use either Paxos or Raft. Each one of those implementations, generally has their own flavor of how they have handled
the extension of the protocol. However this addition of "master lease" concept is so fundamental, that once it is
implemented, it is no longer really a "standard algorithm".

WALLE uses custom consensus protocol that supports "read lease" concept by design. Because of that, overall protocol
is quite a lot simpler to implement compared to Paxos or Raft. And from performance perspective, it allows writes to
be confirmed with only 1 network round trip. Concept of "read lease", does add additional constraints for reads
of the current state to be correct. Leases only work if "time delta" of few seconds can be measured consistently
between separate nodes. So, for example, if one host measures "time passed" to be 'X' seconds (where 'X' is generally pretty
small <10), another node in that time period, must measure time passed to be also within +/-25% of 'X' seconds.
In practice, this requirement is easily satisfied, since cpu clocks don't drift apart that widely in short time periods.
However, it is hypothetically possible for this requirement to be violated, if nodes are run in specilized virtual environments,
where time delta doesn't come directly from the monotonic clock of the host system. In any case, all systems that use
any kind of time based lease concept, have same requirement too, and I believe this is a better tradeoff of correctness
and performance, compared to correctness at absolutely all costs. (Note that the additional requirement is only for reads,
writes would still remain correct and exclusive even with fully messed up clocks).

## API

- ClaimWriter
- WriterStatus
- PutEntry

### ClaimWriter

### WriterStatus

### PutEntry





[1] https://www.cs.utexas.edu/users/lorenzo/corsi/cs380d/papers/paper2-1.pdf