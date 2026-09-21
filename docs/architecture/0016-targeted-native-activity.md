# Targeted native activity notifications

## Context

Native executor observers need a bounded way to learn that work or lifecycle
state changed for an actor they own. A World-wide broadcast wakes every
observer for every mutation, while a readiness query alone cannot wake an
observer that is already waiting on an actor version.

## Decision

The core keeps one activity record for each actor that has registered an
observer. The record is keyed by the actor slot and fenced by the complete
`ActorRef` generation. Its monotonic version changes while the World state
mutation is being committed. A poll compares its observed version and either
returns the new version or stores one waker for that actor. Re-polling replaces
the previous waker, so each actor has one observer slot rather than an
unbounded waiter list.

Mutations mark records while the State mutex is held. Marks are coalesced in a
bounded dirty-slot set. Wakers are removed from that set and invoked only
after the State and activity locks have been released. A generation replacement
retires the old observer and wakes it; the replacement starts with a fresh
version. If a mark would exceed the per-slot version maximum, that slot reports
`LimitExceeded` instead of wrapping and producing a false unchanged version.

Activity dependencies follow the native ownership graph. A mutation marks its
actor and ancestors. Subtree lifecycle changes mark the affected subtree.
Draining producer changes can mark quiescing consumers reached through indexed
routes and their ancestors. Route mutations mark the route owner, source, and
target. Operation reservation, completion, expiry, and cancellation mark both
endpoints; taking a retained terminal result marks its owner. Service and component mutations use these
same ownership and route marks, so the observer only needs to poll its own
actor version.

The notification is a wake hint. The observer must recheck lifecycle,
claimability, deadlines, route validity, and operation status under the World
lock before invoking application code. A mark does not claim a delivery,
failure, startup callback, or cleanup callback.

## Consequences

The design preserves actor-local polling while keeping generation reuse safe.
It avoids invoking arbitrary wakers under runtime locks and bounds observer
memory by registered actor slots. Each mark advances the version, but several
marks can share one wake invocation. Callers must treat a changed version as
“poll again,” not as an event count.

The activity record does not replace the Python phase readiness index. Python
Start, Failure, Delivery, and Cleanup use their ordered hints and condition
variable. Native activity remains a versioned observer mechanism, while the
native scheduler performs authoritative claims and lifecycle checks.

## Limits

This mechanism does not cancel native timers, deferred replies, or CPU jobs.
Those paths retain their bounded periodic cancellation/deadline checks, and
control maintenance remains responsible for expiry and shutdown progress.
Publication tickets and routing batches are specified separately in
[publication tickets](0017-publication-tickets.md); this activity mechanism
does not replace their scheduler or introduce lock partitioning. World state
still uses the existing mutex. Native callbacks and user handlers
remain cooperative, and a wake cannot preempt a running callback.

Relevant evidence includes core `activity.rs` unit tests and the
`tests/activity.rs`, `tests/activity_targeted.rs`, `tests/activity_deadlines.rs`,
and `tests/activity_failure.rs` integration tests, plus native
`tests/sdk_activity.rs`. These exercise generation replacement, coalesced
wakes, lifecycle and route dependencies, operation endpoints, reentrant wakers,
poll/admission races, and isolation from unrelated work. The
`activity_scale` example and [measurement notes](../validation/benchmark-method.md)
record the local registry cost at increasing unrelated observer counts.
