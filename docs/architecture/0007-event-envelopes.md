# Event envelopes and bounded context

Event payloads are immutable native data, but delivery also needs bounded
metadata that explains where the event came from, where it is going, and which
logical operation it belongs to. The runtime therefore carries an immutable
envelope alongside each admitted payload. The envelope is native state and
never contains Python objects, callbacks, exception objects, or arbitrary
dictionaries.

An envelope contains a World-local event ID, schema kind/ID/version, source
when one was supplied, destination, owner, optional dispatcher and destination
port, the World monotonic enqueue timestamp, an optional monotonic deadline,
optional correlation and causation IDs, and optional bounded trace context.
Lifecycle contexts have no envelope. A claimed delivery exposes its envelope;
operation outcomes expose the retained completion envelope.

`MessageOptions` is the input boundary. The PyO3 bridge accepts a five-element
tuple containing source, timeout in milliseconds, correlation ID, causation ID,
and trace. Each element may be `None`. Source handles are strict
`(world:u64, slot:u32, generation:u64)` tuples with nonzero World and
generation. IDs are positive `u64` values. Timeouts are bounded to 24 hours.
An explicit timeout is converted from the current monotonic instant and takes
the earlier of it and any inherited deadline.

Trace context contains a nonzero 16-byte trace ID, nonzero 8-byte span ID, a
strict boolean sampling flag, and at most eight unique baggage pairs. Baggage
keys are 1–64 ASCII bytes using `[A-Za-z0-9_.-]`; values are at most 256 UTF-8
bytes; total key/value bytes are at most 1,024. Trace metadata is charged as
25 bytes plus key/value bytes, in addition to shared payload storage and
per-delivery mailbox accounting. Invalid metadata is rejected before payload
admission, and native storage is copied before Python-owned buffers can mutate.

Context propagation is explicit. Sends, timers, requests, and typed-port
emissions inherit deadline, correlation, causation, and trace. A child event
uses the parent event ID as its default correlation when the parent has none,
and uses the parent event ID as causation. A request without an explicit
correlation uses its own request event ID. A completion reply reverses the
source/destination relationship, retains request correlation and trace, and
sets causation to the request event ID. A claimed handler may finish after its
deadline; it is not forcibly killed.

Admission rejects zero or already-expired deadlines. Native maintenance
expires queued and startup-staged deliveries and releases their retained
storage; claims recheck deadlines under the World fence. Expiry increments the
bounded `expired` metric and records bounded event/operation identity in
diagnostics without payload or trace baggage. Timer payloads that expire before
admission are not counted as admitted-delivery expiry. Reply timeout and late
completion use the same single-winner terminal rule as other operations.

Fan-out assigns distinct event IDs to admitted deliveries while preserving
explicit correlation and trace. Shared payload and trace bytes are charged
once to the physical retained budget. Each destination mailbox charges those
bytes against its own limit; fixed envelope fields are bounded by entry counts. Event
IDs are World-local and may contain gaps after failed reservations.

Pending requests retain their shared input and envelope until a terminal
outcome. Replies reserve result storage while that input is still retained,
so capacity planning must include both. The native operation retains enough
context to propagate a reply after the foreground handler has returned.

Expiry of queued and staged work increments `expired`, `cancelled`, and
`discarded`. Rejected ingress and held timers are separate from those counts.
Native maintenance currently scans the bounded actor registry; indexed expiry
and owner-indexed resource cleanup remain future work.

The implementation does not provide OpenTelemetry export, retries, durable
effects, or a general distributed tracing protocol. Those features must retain
the same ownership, deadline, and bounded-metadata rules if added later.
