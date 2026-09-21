# Indexed routes and operation deadlines

The service implementation made ownership explicit, but publication and actor
stop still scanned every subscription. Operation expiry scanned every reserved
slot even when no deadline was due, and a scope report inspected unrelated
operations. A bounded table limits memory; it does not make these scans suitable
for frequent native control work.

## Decision

The Rust core keeps an authoritative route table and generation-keyed indexes
for source/port, source actor, route owner, and target actor. Each route has one
membership in each index. Insert and removal update all memberships under the
existing World state mutex, and empty sets are removed. No mutable map accessor
allows callers to bypass those updates.

Publication and startup-output flushing select only the named source/port's
routes. Incoming-producer drain checks select only the consumer's routes.
Actor stop unions the routes it owns, produces, or receives, then removes each
once. Admission order remains ascending subscription ID. Duplicate checks use
the relevant source index and retain the existing typed/untyped rules.

Supervisor notification claim and discard use the actor's existing child list.
Pending notifications keep their child generation from retiring until claimed
or discarded, so this list remains authoritative. Selection still uses the
earliest failure sequence. A claimed notification retains the supervisor's
control slot until its lease finishes.

The operation table has an ordered, removable deadline entry for every pending
operation: `(deadline, slot, generation)`. Completion, cancellation, and admission
rollback remove that entry immediately. Retained terminal results consume their
normal result slot but no deadline entry. Expiry visits only due entries and
returns them in the previous slot/generation order so diagnostics remain stable.
There is no lazy heap of canceled entries that can grow with total historical
submissions. Scope reports union owner/target operation indexes and count an
operation with both endpoints in the scope once.

These are native metadata changes. They add no Python callbacks, worker tasks,
pending-sender queues, or payload copies. Index memory is bounded by the existing
subscription and operation limits. Identity includes the World and actor or
operation generation; reuse cannot make an old membership address a replacement.

## Evidence

`crates/runtime-core/examples/index_scale.rs` measures publication/claim to one
subscriber, stopping an owner with one route, and operation expiry with no due
deadlines, as unrelated table occupancy increases. See
[benchmark method](../validation/benchmark-method.md) for measurements and limits.

Independent deterministic models exercise 20,000 operation transitions and
9,000 route mutations. They check admission outcomes, slot reuse, counts,
publication order, consumer dependencies, quota reuse, expiry, and cleanup
against simple test-owned state. Focused regressions cover concurrent publication
and cancellation, third-party route owners, and queued-delivery fencing.
Internal table tests check index cardinality and empty-index cleanup.

## Remaining work

Publication now reserves a bounded ingress ticket. The first routing batch
captures the selected route snapshot; later bounded attempts revalidate route
generation and lifecycle before admission. Source FIFO and round-robin source
selection preserve progress across batches. The first snapshot copy and route
index lookup remain proportional to selected fan-out under the World mutex;
this is not a fixed-latency guarantee. Actor allocation, queued event expiry,
and drain-root discovery are addressed by the subsequent
[event maintenance indexes](0014-indexed-event-maintenance.md). Other readiness
and activity-notification scans remain.
Full inspection intentionally visits the state it reports. This decision removes
specific unrelated-resource scans; it does not establish high-scale throughput,
bounded lock latency, or production capacity.
