# Bounded publication tickets

Status: accepted for the native routing milestone.

## Decision

`publish` and typed port publication reserve a bounded native routing ingress
ticket. The ticket acknowledges accepted routing work; it is not a receipt for
handler execution or every destination mailbox. It exposes `Staged`, `Queued`,
`Routing(report)`, and `Terminal(report)` states. Terminal reports retain
bounded metadata only and no native event payload. Dropping an external ticket
clone does not cancel accepted work; global and per-source quota remains held
until routing finishes and all ticket clones are dropped.

The first routing batch captures the subscription snapshot. Subscribers added
after that point are excluded. Each later batch revalidates route generation
and owner/target lifecycle before mailbox admission, so unsubscribe or stop
fences prevent a new invocation claim even when a route was in the snapshot.
Destinations are independent: healthy targets can be admitted while saturated
or fenced targets are rejected.

Routing is FIFO for one source across its ports and uses round-robin turns
between ready sources. The default batch attempts at most 32 destinations per
turn. Fan-out and aggregate snapshot-entry limits are separate from ticket
capacity. A publication that cannot capture its bounded snapshot terminates
with `SnapshotFull` or `FanoutLimit` without silently truncating destinations.

The report's `matched` count is zero when no snapshot was captured, including
fan-out-limit, snapshot-full, cancellation-before-first-batch, and equivalent
early terminal outcomes. `staged` records that the publication spent time in a
startup staging queue; it is a history flag, not a current queue count.
Successful routing has outcome `Routed`, describing completion of the routing
pass and per-destination admission accounting. It does not mean handlers ran
or effects became durable.

Source stop cancels unrouted publications. Source drain rejects new ordinary
publication ingress while allowing accepted routing and permitted completions
until the shared deadline. Each draining target records a publication sequence
cutoff. An ordinary publication accepted before that cutoff may still enter its
mailbox; one accepted afterward is rejected for that target. The cutoff uses
admission order, including when a virtual clock has not advanced. Authorized
completion publications may enter a quiescing target after its cutoff. All
other generation, route, lifecycle, deadline, and capacity checks still apply.

Target stop and unsubscribe fence unclaimed route deliveries. Deadline expiry
releases snapshot storage and the unrouted publication's payload reference.
Terminal ticket metadata continues to consume quota while external handles
remain. Cancellation or expiry after snapshot capture counts the unattempted
tail as rejected; admissions already reported are historical even if those
deliveries are subsequently discarded. Shutdown traversal still owns lifecycle
cancellation and native control progress.

Cancel-close, including failure policy `StopWorld`, terminalizes router
readiness with `Error::ActorStopped`, wakes its idle observer, and retains no
new observer on later polls. Observer wake/drop runs outside the state and
signal locks. Drain does not terminalize readiness because owned completions
may still arrive. The native routing service exits on terminal readiness.

The first snapshot copy traverses the selected fan-out and performs route-index
lookups under the World mutex. Configured fan-out and aggregate snapshot-entry
limits cap this work. Batch size bounds destination attempts, not every
operation in that critical section, and does not imply a fixed latency
guarantee. Shared payload storage is charged once while pending routing and
admitted deliveries retain it. Mailbox and ticket metadata are separately
bounded by entry counts.

The defaults are 256 pending-plus-retained tickets globally, 32 per source,
4,096 destinations per publication, 16,384 simultaneous snapshot entries, and
32 destination attempts per routing turn. Snapshot inspection separates pending
work from retained terminal tickets; their sum consumes the ticket quota.
Terminal handles retain no World reference and remain readable after disposal.
Per-source quotas use the complete actor identity, including its generation.
World totals include terminal tickets from retired or reused sources; scoped
reports follow sources in the current attached subtree. Ancestor-wide ticket
quotas and historical subtree result accounting are outside this slice.

## Compatibility and migration

This is a breaking change to the pre-alpha return type: existing publication
method names now return a ticket instead of immediate destination counts.
Plain Rust core callers drive bounded `World::route_batch()` turns themselves.
`NativeRuntime` drives a dedicated readiness-based router automatically;
`TestWorld` includes it in explicit bounded pumps. Neither Python emission nor
ticket inspection pumps work. On a live runtime, native workers may finish
routing before the caller first inspects the returned ticket.

Python exposes immutable `PublicationTicket`, `PublicationStatus`,
`PublicationOutcome`, and `DeliveryReport` values. Use `ticket.id`,
`ticket.source`, `ticket.status()`, and `ticket.report()`. The last returns
`None` until terminal. Python terminal statuses name their outcome (`Routed`,
`Cancelled`, `Expired`, `FanoutLimit`, or `SnapshotFull`); Rust represents them
as `PublicationStatus::Terminal(report)`. A routed report records destination
admission, including individual rejection; it does not acknowledge handler
completion. See the [runnable Python example](../../examples/publications.py).

This decision supersedes the initial synchronous-publication wording in
[ADR 0001](0001-native-boundary.md) and the initial slice contract. Components
and ports use the same ticket semantics; startup-staged output becomes queued
routing work when the parent activation fence flushes it.

## Evidence and limits

Core publication tests cover first-batch snapshots, bounded turns, FIFO and
source fairness, quota rollback, partial fan-out, unsubscribe and stop
fences, frozen-clock drain cutoffs, permitted completions, deadlines, staging,
generation reuse, reentrant observers, and retained ticket metadata.
The contract remains at-most-once invocation attempts for admitted deliveries;
it does not add retries, durable effects, all-or-nothing fan-out, or arbitrary
payload retention in reports.
