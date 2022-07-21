# LogLog's design

## Log

Logically the core and only responsibility of LogLog is maintaining
an ordered append-only binary *Log*.

LogLog allows appending new *entries* (bunch of bytes) to this binary log.
Unlike most other similiar solutions it does not interpret
the content of each entry. It will only add a very short prefix and suffix
to each entry for the purpose of local and cluster-wise consistency and
crash resistance and append new data to the existing log.

The content of the log (including prefix and suffix of each entry) is
directly accessible to the clients. Clients request binary data from log in chunks
by log position and limit on the amount of data they would like to receive.
LogLog does not maintain any index of entries stored in a log.

In normal circumstances LogLog server will not return parts of the
log that are not yet "commited". Client can request to block
and wait for new data if desired, to avoid any extra polling latency.

## Supported operations

* Append entry to the log
* Read log directly in pages by offset and size limit
* Upload the entry data to arbitrary node (to help data replication)

## Segments

Internally log data is stored in segment files. LogLog
will create a new segment file after current one crosses
certain limit.

Each segment file starts with a short preffix, mostly containing
the Log offset position of the data it contains.

## Supported client-side functionality

It might seem like the core functionality supported by LogLog
is very minimal and might be limiting, but it is actually possible to build out
everything on top of it, client side.

### Adding multiple log events at the time

If clients wish to be able to add multiple sub-entries to the
log in one "append" operations, they need to serialize and deserialize
entries to include multiple sub-entries (by e.g. prefixing them with
their length).

### Data integrity

LogLog does not perform any data consistency checks, neither
on read, nor write side. Applications that need consistency
guarantees need to append their own checksums before appending
entries to the log, and check them when reading entries.
If inconsistency was detected, data can be re-requested from
a different node. When appending entries clients upload
entry content multiple nodes directly (more on it later),
there should always be enough independent copies to restore from.

### Higher level data structures

Although LogLog maintains only log, it is expected that
it's users will build state machines following entries
in the log and maintaining any higher level data structures
as they see fit. There's no need for a built in key-value
stores or any other functionality.


## Rationale for extreme functional minimalism

The main rationale is performance. Since ordering a log
is ultimately a sequential operation, the log is always going
to be a bottleneck of any system, and scaling it horizontally
will require sacrifices in terms of consistency.

By offloading absolutely every unncessary logic to the client
side, a LogLog cluster can (in theory at least) handle
higher workloads before becoming a bottleneck.

The extreme transparency of the internal log storage, and 1:1
mapping between how data is persisted and how it is exposed and
accessible by the clients, enables minimal overhead read and write operations.


## HA Cluster

### Independent node uploads

Since all the writes will ultimately have to be handled by
the Cluster leader node (due to consistency), LogLog will allow
(and even require) each writting client to upload the data to all the
cluster nodes before commiting the new entry to the log.
This is to take as much load from the leader node as possible,
and also to avoid possibility of one corrupted copy of the entry.

### Raft

Otherwise LogLog works is a standard Raft way: there are elections,
leader, folloers, log entries contain Term, and consistency and HA is
maintained this way.

## Modes of applicability

### Independent Cluster

LogLog can be deployed as an independent cluster, and accessed remotely
using Client libraries.

### Built-in

Since every distributed database is built on top of distributed log
in a Raft cluster, it should be possible to use LogLog nodes locally:
either in a separate process, or as a library. This should allow re-using
all the functionality while avoiding extra network overheads.


