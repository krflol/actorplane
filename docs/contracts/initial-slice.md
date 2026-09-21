# Initial executable slice

Historical scope of the first milestone. Drain, requests, and diagnostics were
added afterward; see [the current API](api.md) and
[the lifecycle decision](../architecture/0002-drain-operations-diagnostics.md).

The design in [[actorplane]] is the long-term contract. This implementation starts
with Gates 1 and 2, plus the lifecycle and authoring pieces necessary to exercise
them. Features are reported as implemented only after their tests pass.

## Execution and ownership

Rust owns identities, lifecycle fences, immutable payloads, bounded mailboxes,
native routing, counters, timers and metrics. Tokio runs native tasks. Python
instances and callbacks stay in the foreground Python driver. Every endpoint is
an owner scope; component scopes can be children of an actor. Cancellation
cascades. Callbacks execute outside native registry locks.

## Payloads and limits

The first codecs are native Pulse(i64), CountSnapshot(count: u64, total: i64),
and named/versioned records containing signed 64-bit integer sequences. Python
records support int and tuple[int, ...] fields, copied into native storage.
Unsupported types fail registration. No arbitrary Python payloads or pickle.
Limits cover owners, subscriptions, queue entries/bytes, native retained payload
bytes, record elements, and native tasks/timers. Admission rejects immediately;
there are no tasks waiting to send and no lossless-delivery claim.

## Outcomes and stopping

Send acknowledges mailbox admission. Publish returns a bounded native routing
ticket; plain core callers drive `World::route_batch()` and read its terminal report for
per-destination admission results. The first routing batch captures the route
snapshot, with FIFO per source and round-robin turns between sources. Handlers
are always queued.
Claim and stop serialize at the native lifecycle fence. Cancel discards queued
deliveries; in-flight leases remain visible until completion. Generation reuse
cannot deliver old work to replacement owners. Native cleanup and Python cleanup
are separately reported. Initial cancellation is immediate; drain is deferred.

## Acceptance

- Run a native timer/source -> counter -> native sink without PyO3 dependencies.
- Demonstrate bounded native-to-Python delivery while Python is stalled.
- Deliberately hold the GIL in a test-only extension function, capture progress
  timestamps during that interval and cancel natively before releasing the GIL.
- Exercise queue and byte budgets, isolated fan-out rejection, stale/cross-World
  references, startup rollback, subscription cancellation, self-send, callback
  failure, repeated teardown, driver thread affinity and non-reentrancy.

The publication ticket decision supersedes the earlier synchronous-publication
wording; see [bounded publication tickets](../architecture/0017-publication-tickets.md).
Unsupported release gates remain explicit in the project status.
