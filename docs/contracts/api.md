# Current runtime contract

The project uses the MIT License and has not been published. This document describes
the implemented native runtime and Python authoring APIs, including general schemas,
components, interfaces, typed ports, bounded event-envelope metadata, and an initial
source-level Rust component SDK, virtual-time TestWorld, and bounded native
World service leases.

## Python surface

`World`, `Actor`, `ActorRef`, `Context`, `Event`, `event`, `handles`, `Pulse`,
`CountSnapshot`, `IntRange`, `FloatPolicy`, `Length`, `FailurePolicy`, `Failure`,
and `TraceFrame` are exported from `actorplane`. A World starts two native workers.
`Component`, `component`, `Interface`, `Output`, `Port`, `BoundComponent`,
`LinkToken`, and the `native` factory module provide component composition.
The first `step()` or `run()` binds its foreground driver thread. Later dispatch,
Python lifecycle cleanup and `close()` must occur on that thread. `spawn()`
registers a class; it does not construct an instance until the driver runs.

`world.step()` claims at most one pending failure notification per supervisor,
starts pending actors, claims at most one queued business event per actor,
and runs eligible cleanup. `world.run_for(seconds)` pumps until its time bound or
all Python actors stop. These durations are cooperative: a blocking Python
handler can prevent the driver from returning. Recursive dispatch and blocking
close from a handler raise `AffinityError`. Native processing remains independent.

`ActorRef` carries a World ID, slot and generation and weakly references its World.
`ref.send(event)` and `world.send(ref, event)` return mailbox admission IDs.
Wrong-World, stale, stopped, not-ready and full-queue cases reject. Ordinary
external threads can send, but must not drive callbacks or mutate actor state.
Messages without a declared input handler reject with `SchemaMismatch` before
queue admission, including direct sends and requests to Python actors.
There is no API to retrieve a live Python actor instance.

`configure(ctx)` and `on_start(ctx)` run once per actor. Failed startup fences the
actor, cancels owned native work, and releases Python registration. Root startup
failure re-raises the original error; child failure queues a parent notification.
Native cleanup is asynchronous and observable. `on_stop(ctx)`
runs after fencing, once, on the driver, with children before parents. Cleanup failures are surfaced after
other pending cleanup completes. A root handler failure stops the actor and
raises `HandlerFailed` with the original exception as its cause by default.
Child failures stop the child and notify its parent through queued control work.

`ctx.native_counter(interval, window, target=None)` creates fresh native source,
counter and sink scopes. Durations are seconds; the bridge rounds positive
sub-millisecond periods up to milliseconds and accepts at most 24 hours. The
returned native handle exposes `owner`, `source`, `counter`, `sink` tuples and
`stats()`. A target subscribes to `CountSnapshot` summaries. The `.counter` input
accepts `Pulse` data. The source emits `Pulse(1)`. Windows sum values and count
admitted pulses; missed timer ticks are skipped. No Python object or callback is
used per pulse. The source, accumulator and sink all use core queue admission.

`ctx.after(seconds, event, target=ctx.actor)` creates a bounded owned one-shot
timer. Its copied payload is charged while the timer waits. Delays registered
during startup begin after activation. Stop cancels the timer; its final send
checks owner eligibility atomically with target admission. `ctx.subscribe`
installs a native route between endpoint handles. Cancelling either endpoint
retires the route and prevents new invocation claims through it.

Failure supervision is explicit and bounded. `FailurePolicy.STOP_ACTOR` is the
default and stops the failed actor and its owned child subtree. `STOP_WORLD`
fences every actor in the World. `CONTINUE` consumes the failed delivery,
records the bounded native failure, and keeps the actor eligible for later
messages; `step()` and `run()` continue dispatching. Policies can be set on `World`, `spawn(...,
failure_policy=...)`, or `Context.spawn(...)`. Child actors use `parent=`
ownership and inherit the World policy unless overridden. Startup and cleanup
failures cannot resume the failed actor. Automatic restarts are not implemented.

Failure notifications are queued for a parent and delivered by the next
foreground pump. A pump gives control notifications progress alongside at most
one business delivery; failure callbacks never run inline inside the failed
handler. Only one notification is pending for a child. Repeated failures keep
the first record and increment its bounded `coalesced` count. A pending
notification prevents failed-owner slot reuse until it is claimed or the
parent is stopped. A custom `on_failure` hook may return `FailurePolicy`
actions or `None`; a stopped child cannot be resurrected. The default hook
escalates to `STOP_WORLD`, while `CONTINUE` applies only to a still-live actor.

Structured failure records include lifecycle phase, handler, qualified
exception type, actor/event/schema/operation identities, action, and bounded
traceback metadata. Handler and exception type strings are limited to 128 UTF-8
bytes, file names to 256, function names to 128, and traceback capture walks at
most 256 frames while retaining the final eight. Exception text, arguments,
locals, source lines, traceback objects, and frame objects are not captured.
Diagnostic ring capacity zero still supports supervisor notifications. Native
panic guards cover ordinary runtime boundaries; process aborts, panic
destructors, and OS stderr from the native panic hook remain outside the
structured-report privacy guarantee.

`world.stop(ref)` requests immediate actor cancellation; `request_stop()` fences
the entire native World without invoking nested cleanup. `close(timeout=1.0)` fences
the World and waits for tracked native tasks under one shared deadline. The
report separates `native_done`, `python_done`, `in_flight`, and deliveries
discarded by that close operation. Prior stop discards remain in cumulative
metrics. Native async tasks may be aborted at the deadline; incomplete work is
reported. This API does not kill executing Python or arbitrary native code.

`world.stop_report(ref=None)` is a read-only shutdown snapshot for one actor
subtree or the entire World. It reports `queued`, `delivery_in_flight`,
`control_in_flight`, `native_tasks`, `python_pending`,
`pending_notifications`, unique pending `outstanding_operations`, terminal
`retained_operations`, cumulative `errors`, latest bounded `last_error`,
`in_flight`, `native_done`, `python_done`, and sticky `timed_out` state. The
`in_flight` value is delivery plus control plus native tasks. A read-only report
has `discarded == 0`; the initial report from `stop()` is captured before
cleanup, so call `stop_report()` again to refresh it. `native_done` can be true
while Python registrations or terminal results remain. Child error totals and
latest samples roll into parents, and World totals survive root generation
reuse. Reports retain only the latest error sample; the diagnostic ring is the
separate bounded history.

Close waits for native work only within its shared deadline. It never kills an
executing Python callback or a non-cooperative native task. Deadline timeout
is sticky in subsequent reports, while Python cleanup may complete later.

Drain mode is available as `stop(ref, mode="drain", deadline=seconds)`,
`request_stop(mode="drain", deadline=seconds)`, and
`close(timeout=seconds, mode="drain")`. `stop()` and `request_stop()` are nonblocking
lifecycle requests; the foreground driver must continue with `step()` or `run_for()`
to execute already-admitted Python deliveries. `close()` drives the drain and
waits cooperatively under its deadline. New admissions are rejected after
the drain fence. A deadline is shared by the operation and is not extended by
individual owners. Callbacks already executing are allowed to return, and
`on_stop` runs after queued Python work drains or the deadline expires.

Request/reply operations are bounded by `max_operations` (default 256). Call
`world.request(owner, target, event, timeout=seconds)` or
`ctx.request(target, event, timeout=seconds)` to obtain an `OperationHandle`.
The request handler receives opaque `ctx.operation`; `ctx.reply(event)` has
one winning reply and returns `False` for late or duplicate replies. `poll()`
is nonblocking and returns `None` while pending or an `OperationOutcome` with
state `Completed`, `TimedOut`, `Cancelled`, `OwnerStopped`, `TargetStopped`, or `Failed`.
`result()` and `take()` consume terminal results; `result()` raises
`OperationError` while pending, `TimeoutError` for timeout, and
`OperationError` for other terminal failures. `cancel()` is single-winner.
Operation state is owner-scoped: once the owner is fully retired, its handle
may become stale. Terminal results occupy their bounded slot until consumed
or owner retirement.

## Event envelopes and context

Every admitted delivery carries an immutable native envelope. It exposes an
World-local event ID, schema kind/ID/version, destination and owner handles, optional
source, optional typed dispatcher and destination-port handles, a monotonic
`enqueued_at_ns` value from the World epoch, optional deadline, correlation and
causation IDs, and optional trace context. A lifecycle context has no envelope;
it becomes available after a delivery is claimed. Python claim metadata and
operation outcomes expose the same envelope fields. Python handlers read the
frozen `ctx.envelope` snapshot; completed `OperationOutcome` objects expose
`.envelope`. The owner is the destination. External ingress has no source;
context-originated work identifies the actor that sent it. Port handles are
`(world, slot, generation, index)` tuples. Timestamps use the World's native
monotonic epoch and cannot be compared directly with Python's `time.monotonic()`.

Python `MessageOptions(timeout=None, correlation_id=None, causation_id=None,
trace=None)` accepts a relative timeout in seconds, positive `u64` correlation
and causation IDs, and an optional `TraceContext`. It is accepted through the
`options=` keyword on sends, requests, replies, timers, and port operations.
Actor source handles are supplied by the context; the internal bridge converts
timeouts to milliseconds. Trace IDs are 16 bytes, span IDs are 8 bytes, both
must be nonzero, `sampled` is a strict boolean, and baggage has at most eight
unique entries. Baggage keys are 1–64 ASCII bytes using alphanumeric,
underscore, period, or hyphen characters; values are at most 256 UTF-8 bytes and
the combined baggage is at most 1,024 bytes. Trace metadata is charged at
`25 + key bytes + value bytes`, added to payload bytes for retained-budget and
per-mailbox accounting. Fixed envelope fields are bounded by entry limits.

Child sends, timers, requests, and typed-port emissions inherit the parent
deadline, correlation, and trace context. Causation identifies the triggering
event. An explicit timeout uses
the earlier deadline and cannot extend an inherited one. A child correlation
defaults to its parent event ID and its causation ID is the parent event ID.

Replies retain the request context after the handler returns: the reply source
is the target, destination is the requester, causation is the request event ID,
and the request correlation and trace are preserved. A request without an
explicit or inherited correlation uses its own request event ID. Its original
input and trace remain charged until a terminal outcome; replying requires
room for the result while that input is still retained.

Admission rejects a zero or past deadline before queueing. Native maintenance
expires queued and startup-staged messages, releases their retained bytes, and
increments `expired`, `cancelled`, and `discarded`; timer payloads that expire before
admission are not counted as admitted delivery expiry. Claims recheck deadlines,
and an already claimed callback is allowed to finish. A reply deadline remains
the single terminal winner, so a late completion cannot replace timeout.

Fan-out gives each admitted delivery a distinct World-local event ID while
preserving explicit correlation and trace context. Shared immutable payload
storage is charged once; each mailbox still accounts for its own delivery.
Deadline-expiry diagnostics retain only bounded event/operation identities and
never payloads or trace baggage. There is no OpenTelemetry exporter or retry
protocol in this implementation. See the [envelope example](../../examples/envelopes.py).

## Components and typed ports

`component(factory, **config)` is an immutable class declaration. Every actor
receives fresh owned component scopes and state. Python factories derive from
`Component`; native factories are `native.PulseSource(interval=...)`,
`native.WindowCounter(window=...)`, and `native.SnapshotSink`, configured through
`component(...)`. Durations are seconds. Python constructor configuration accepts
bounded immutable primitives and tuples. Components share the World actor limit;
each native reference component also uses one task permit.

Input ports use `@handles(Event)` method names. `Output(Event)` declares a named
output. Component attributes resolve to immutable `BoundComponent` proxies with
an `owner` reference and typed ports; they do not expose live Python instances.
`port.send(event)` admits to an input; `port.emit(event)` publishes an output
and returns a `PublicationTicket`. Its `id` and `source` properties identify
accepted native routing work. `status()` returns a `PublicationStatus`, and
`report()` returns `None` until routing terminates, then an immutable
`DeliveryReport`. These methods do not pump work. A live `World` runs routing
on native workers, which may finish before the caller first inspects the ticket.
`TestWorld` requires explicit bounded pumping; plain Rust core callers drive
`World::route_batch()` themselves.

The report exposes `matched`, `admitted`, `rejected`, the historical `staged`
flag, and an `outcome`: `Routed`, `Cancelled`, `Expired`, `FanoutLimit`, or
`SnapshotFull`. Python terminal statuses use these same names; intermediate
statuses are `Staged`, `Queued`, and `Routing`. A `Routed` report records
completed routing, including any destination rejections. It does not prove
handler execution. The first batch captures the subscription snapshot, and each
destination attempt revalidates the route and target. Global/per-source ticket
capacity includes pending work and retained terminal handles. Dropping a ticket
does not cancel work; its quota slot is released only after routing terminates
and every handle is dropped. Terminal tickets retain no payload or World and
remain readable after disposal. See the [publication example](../../examples/publications.py)
and [routing contract](../architecture/0017-publication-tickets.md).

Output emission uses the foreground driver. `ctx.link(output, input)` and
`ctx.subscribe(output, actor_ref)` return an owner-scoped `LinkToken`;
`cancel()` is idempotent. Losing a token does not cancel its route. Cancellation
of the route owner or either endpoint fences unclaimed deliveries. Cross-World
references and incompatible port schemas reject.

`Interface(name, version, inputs=..., outputs=...)` describes named port contracts;
`implements = (contract, ...)` declares implementations. Interface semantics are
FIFO admission, immediate overload rejection, and cancellation of unclaimed work
when its owner ends. Runtime errors include schema/port mismatch, not-ready,
stopped/stale/cross-World references, and queue or budget exhaustion. Rust checks
registered layouts and rejects incompatible reuse of a name/version identity.
`depends_on=("component_name",)` orders startup and `requires=(contract,)` checks
interfaces against those dependencies, including their actual native registrations.

Composition is fixed during startup. Constructor arguments, dependencies, cycles,
and interfaces are validated before activation. Output events emitted during
startup are charged to native payload budgets and per-owner mailbox entry/byte
limits. Root activation releases staged outputs through bounded queue admission;
the subscription snapshot is taken by the first native routing batch after that
release. Publication tickets preserve source FIFO across ports and use
round-robin bounded turns between sources.
Individual target rejections appear in diagnostics and counters. Failed startup
discards staged work and tears down children before parents. Already claimed
callbacks may emit typed completion effects during drain. An ordinary publication
accepted before a target's drain cutoff may enter its quiescing mailbox; one
accepted afterward is rejected for that target. The cutoff uses publication
sequence order, even when virtual time has not advanced. Authorized completion
publications remain eligible after the cutoff, subject to the other admission
checks. Source drain rejects new ordinary publication ingress.
Native consumers wait for linked upstream work and their own queue to drain before
finishing. Cyclic event routes or an active external upstream may require the
shared drain deadline. Native counter arithmetic overflow stops the component
and records a structured failure.

Limits are 64 components per class, 64 ports and 16 interfaces per endpoint,
1,024 World interface identities, and a Python composition depth of 32 with
1,024 nodes. Interface identities persist for the World's lifetime. Inspection
includes native component contracts, typed link ownership, and staged queue bytes.
See [component example and design](../architecture/0006-components-ports.md).

## Event codecs

`@event(name="example.Data", version=1)` accepts a plain annotated class or a
frozen dataclass. Supported fields are signed 64-bit `int`, `float`, `bool`,
`str`, `bytes`, enum symbols, `T | None`, `tuple[T, ...]`, and nested `@event`
records. `Annotated[int, IntRange(min, max)]` supplies explicit signed/unsigned
64-bit bounds, including `uint64`. `FloatPolicy(allow_non_finite=True)` opts into
infinities and canonical NaNs; the default requires finite floats. Signed zero
is preserved. Floats require Python `float`; booleans are not accepted as ints.

`Length(n)` bounds UTF-8 bytes, byte buffers, or sequence elements. The default
is 4,096 bytes per string/buffer and 4,096 items per sequence. Fields and values
are also subject to 256 fields per record, depth 16, 1,024 schema nodes, 4,096
total value nodes, and the World event byte cap. Empty records are supported.
The structured payload's schema ID charges four bytes in addition to its
encoded body. Unsupported declarations fail during decoration/registration;
value and decoding errors raise `CodecError` with bounded field context.

Lists and bytearray/memoryview inputs are copied before admission; nested
sequences decode as tuples and buffers as bytes. Records and enum values use
their exact compiled classes. Aliases and undeclared enum combinations are
rejected. Names and layouts have collision-checked native registration;
handler schema batches commit atomically. See
[the canonical format](../architecture/0005-native-schemas.md) for all encodings.

Schema IDs are bounded World-local registration numbers. Each registered root
name/version pair belongs to one Python event class within the World; nested
uses of an identity must have identical layouts. Handler schemas are checked
when the actor class is registered; runtime native routing uses numeric IDs and
Rust enums. No JSON, pickle, arbitrary Python payloads or dynamic predicates are
used in the native path. Native schema registrations live for the World's lifetime,
up to `max_schemas`; stopping an actor or failing its startup does not reclaim
schema identities. Python codec references are released when close completes.
A pending drain or executing callback can register a bounded
completion schema while new work remains fenced. Python plans retain exact
classes within the World; there is no process-global event-class registry.

## Limits and inspection

Default bridge limits:

| Resource | Limit |
| --- | ---: |
| Actor/component owner scopes | 128 |
| Entries per endpoint mailbox | 64 |
| Payload bytes per mailbox | 65,536 |
| Shared native payload bytes | 4,194,304 |
| Subscriptions | 512 |
| Bytes per event | 32,768 |
| Native tasks, including timers | 256 |
| Python/native root schema identities | 128 |
| Pending and retained terminal operations | 256 |
| Diagnostic history records | 1,024 |
| Pending plus retained terminal publications | 256 |
| Publications per source (pending plus retained) | 32 |
| Destinations per publication snapshot | 4,096 |
| Simultaneous routing snapshot entries | 16,384 |
| Destination attempts per routing batch | 32 |

Pass `max_actors`, `mailbox_capacity`, `mailbox_bytes`,
`native_payload_budget`, `max_subscriptions`, `max_event_bytes`, `max_tasks`,
`max_operations`, `diagnostic_capacity`, and `max_schemas` to `World`.
Publication settings are `max_publications` (1..65,536),
`max_publications_per_source` (1..`max_publications`),
`max_publication_fanout` (1..65,536), `max_routing_snapshot_entries`
(1..1,048,576), and `routing_batch_size` (1..4,096).
Live Worlds have one fixed native control task and one routing task outside the
business `max_tasks` permits, shown as `control_tasks` and `routing_tasks` in
inspection. TestWorld performs maintenance during explicit pumps and has no
background control task; its routing task consumes the explicit poll budget.
Each pipeline consumes four native scopes and three
task slots in addition to its Python parent. There is one in-flight invocation
per endpoint. There are no pending-sender tasks or unbounded callback batches.
Native payload storage is charged once across shared fan-out, including held
timer payloads and claimed deliveries. Queue/registry metadata is separately
bounded by registration and entry counts. These bounds do not cover arbitrary
Python application allocations or total process RSS.

`world.inspect()` returns native identities, generations, ownership, lifecycle,
queued entries/bytes, in-flight deliveries, native tasks, payload retention,
subscriptions, cumulative delivery counters, and Python registration counts.
`world.diagnostics(after_sequence=0, limit=100)` reads the bounded native
diagnostic ring. Entries carry sequence, elapsed time, code, and optional actor
and operation identities; the response reports the next sequence, dropped
count, and any requested gap. Pass `diagnostic_capacity` to configure the ring;
the requested limit must not exceed that capacity.
Page using the last returned entry's `sequence` as `after_sequence`;
`next_sequence` describes the producer's current position and is not a page
cursor. Setting capacity to zero disables retained history and counts drops.
Stopped slots remain as bounded tombstones until reuse. `submitted` counts send
or publication attempts. `admitted` counts destination mailbox admissions;
`rejected` includes both ingress failures and destination rejections, including
an unrouted snapshot tail cancelled or expired after capture. Per-ticket reports
provide `matched`, `admitted`, and `rejected` destination accounting. One accepted
publication can therefore admit multiple deliveries. `publication_submitted`,
`publication_admitted`, and `publication_rejected` count publication ingress
attempts, acceptances, and rejections. `publication_completed`,
`publication_cancelled`, `publication_expired`, and `publication_failed` count
terminal routing outcomes; the last covers fan-out and snapshot-capacity failures.
`started`, `completed`, `failed`, and `cancelled` describe delivery leases.
Counter stats distinguish generated inputs, processed inputs, generated summaries,
and completed sink work.

Inspection separates `publications_pending` from `publications_retained`
(terminal handles); their sum consumes ticket capacity. `routing_snapshot_entries`
reports captured destination entries. Stop reports likewise expose
`outstanding_publications` and `retained_publications`. Pending work prevents
native drain completion; retained terminal metadata does not. Cancel-close,
including failure policy `StopWorld`, wakes and retires the router observer.
Drain keeps it available for accepted work and authorized completions.
Actor-scoped counts follow the currently attached ownership subtree. For retained
tickets from retired or reused descendant generations, use World-wide inspection
or `world.stop_report()` totals; terminal handles can outlive that subtree.

## Rust component SDK

`actorplane_native::sdk::{NativeBehavior, NativeContext, NativeSpec, NativeLimits}`
provides serialized native state and hooks. `prepare_native` validates a schema
configuration and runs factory/startup on the caller thread, then returns a
`Starting` owner for linking and activation. Worker hooks consume bounded
delivery batches and effects; a period enables ticks that skip missed intervals.

Context methods emit typed output, send, reply, schedule timers, or defer a
request reply to a `Send + 'static` future with owned native input. Jobs and timers
reserve both component and global task permits. Deferred results retain the
original request deadline and metadata. Control claims fence native hooks
against stop; noncooperative work remains visible in incomplete shutdown reports.
`defer_cpu_reply` accepts a synchronous Rust computation with the same owned
input and `JobContext`, running it on the separately bounded CPU pool.
See [the SDK architecture](../architecture/0008-native-sdk.md) for defaults,
drain/failure behavior, caller-thread startup, and application memory obligations.

## Virtual-time tests

`actorplane.testing.TestWorld` uses the same authoring path and real native SDK
with an explicitly driven scheduler. `run_until_idle()` leaves time frozen;
`advance(seconds)` jumps by milliseconds rounded upward and then pumps ready
work. Missed periodic ticks are skipped. Pass `dispatch_python=False` to expire
work and drive native tasks while Python dispatch remains stalled.

`native_responder(EventType)` creates a bounded native deferred-reply component.
After requesting and pumping, `pending_native()` exposes pending identities;
`complete(operation, event)` queues one result, observed after another pump.
Normal schemas, operation outcomes, generation fencing, and payload budgets
apply. `now()` reports virtual seconds. Drain shutdown shares this clock.

The creating thread owns all controls. `max_steps` bounds cooperative work at
native poll/Python turn boundaries; `TestStepLimit` reports exhaustion. Blocking
application hooks need an external wall-clock watchdog. The Rust
`actorplane-test::TestWorld` facade drives the same native runtime without Python.
See [the complete TestWorld contract](../architecture/0009-testworld.md) for
ordering, timing, budget, and shutdown limitations.

## Framed TCP

`from actorplane.tcp import TcpFrame, listen, native_echo` exposes real native
TCP on a live `World`. `listen(world, owner, target, ...)` creates a listener
child under `owner`; accepted connections are children of that listener. The
returned immutable `TcpListener` exposes its actual `owner`, bound `address`,
`stats()`, `connections()`, and `close(mode="cancel", deadline=1.0)`.
Closing the listener leaves an independently owned target actor alive.

The wire format is a four-byte unsigned big-endian length followed by that many
bytes. Empty frames are valid. Each connection makes one `TcpFrame(data: bytes)`
request and waits for one `TcpFrame` reply before writing and reading again.
A Python handler uses `@handles(TcpFrame)` and `ctx.reply(TcpFrame(...))`;
`native_echo(world, parent=None)` supplies the same contract entirely in Rust.
Python handlers require foreground World pumping; native echo does not.

Defaults are numeric `host="127.0.0.1"`, `port=0`, `max_connections=16`,
`max_frame_bytes=16384`, `read_buffer_bytes=1048576`,
`write_buffer_bytes=1048576`, and read/request/write timeouts of 5/1/5 seconds.
Connection limits are 1..1024, frame limits 1..65536, and each buffer limit
1..64 MiB. Timeouts must be positive and at most 24 hours; Python rounds them
up to milliseconds. `max_frame_bytes + 8` must fit the World's event byte cap.
Listener and connection tasks also consume the ordinary actor/task limits.

Read storage and canonicalization scratch reserve both listener and World byte
budgets. The held reply remains charged to the World and additionally reserves
the listener write budget. Admission or budget failure closes the connection;
there is no pending-sender queue. Cancellation drops incomplete frames, fences
operations, and closes sockets. Drain stops accepting/reading and allows an
already admitted request/reply to finish under the shared shutdown deadline.
A completed write means local socket-write completion, not peer processing.
Partial writes may reach the peer before cancellation and are never retried.

Both helpers reject `TestWorld`. Rust callers enable `native-io`; Python wheels
include it. See [the transport contract](../architecture/0010-framed-tcp.md) and
[connection workflow](../../examples/connection_workflow.py) for accounting,
target ownership, statistics, and executable evidence.

## Native CPU operations

`World(cpu_workers=2, max_cpu_jobs=32)` bounds blocking worker threads and total
admitted CPU jobs, including those waiting for a worker. Valid limits are 1..64
and 1..4096. CPU work also consumes the component's job limit, the World's native
task limit, and native input byte reservations. Queued work checks cancellation
without waiting for a busy worker. Already running closures retain their
reservations until they return and must cooperate with cancellation.

`from actorplane.cpu import native_sum_squares, stats` exposes the supplied
native computation. `target = native_sum_squares(world, parent=owner)` creates
an owned native component. `world.request(owner, target, Pulse(n))` replies with
`CountSnapshot(n, n*(n+1)*(2*n+1)//6)` for integers 0..1,000,000. The native
implementation iterates with bounded cancellation checkpoints; invalid input
records a bounded component failure. Real CPU execution rejects `TestWorld`.

`stats(world)` and `world.inspect()["cpu"]` expose configured limits, queued and
running work, and submitted/completed/cancelled/panicked/rejected counts. The
counter handle survives close without retaining the World or worker threads.
It continues to observe a noncooperative job until that job actually returns.
Request outcomes remain authoritative: a finished computation does not imply
successful or durable delivery. See [the complete CPU contract](../architecture/0011-bounded-cpu.md)
and [the executable example](../../examples/cpu.py).

## World services

`actorplane.services.register(world, name, target, contract)` publishes an
active root native component under a name and exact `Interface`. It returns a
`ServiceRef`; `service.acquire(owner)` creates a lease scope under a starting or
active actor, checking the original service generation. The module-level
`acquire(world, owner, name, contract, expected_target=None)` can look up the
current registration. Names are 1..128 UTF-8 bytes. Python actors and native
children cannot be registered as services.

`lease.request(port, event, timeout=1.0, options=None)` returns an
`OperationHandle`. It requires an active holder, the exact declared Python input
class, and native schema validation. The lease scope owns the operation and is
the envelope source; handler correlation, trace, causation, and deadline context
are propagated. `lease.link(output, target_port)` creates a typed output route
owned by that scope and is available during configuration. Only ports in the
published interface are exposed by these methods.

`lease.release()` stops its scope and returns whether a live lease was released.
It remains idempotent after scope retirement or World close. Dropping the token
does not release anything. Holder stop releases its scopes without stopping the
service; service stop unregisters it and releases all leases without stopping
holders. The scope generation fences old requests, links, and late completions.
Retirement invalidates operation handles and retained results; consume results
while the lease is alive. Drain accepts admitted completions until its shared
deadline and rejects new work. Native drain completion may retire the scope
before a later Python poll.

World limits default to `max_services=64`, `max_service_leases=1024`, and
`max_leases_per_actor=16`, with inclusive maxima 4096, 65536, and 1024. Zero
disables capacity. Each lease consumes one actor slot and no worker task;
requests and links also use the ordinary bounded budgets. Service/lease
snapshots are available in `world.inspect()`. Canceled requests can retain bytes
in the shared service queue until skipped, expired, or stopped. Running native
work remains accounted to its service until it returns.

See [service ownership and limits](../architecture/0012-world-services.md) and
the [Python example](../../examples/services.py). This API supports named native
endpoints, requests, and links; it does not automatically connect TCP adapters or
provide generic resource pools.

## Native activity

Native Rust executors separately use `World::activity(actor)` and
`poll_activity(actor, observed, cx)`. Snapshot the version before inspecting
work; then poll with that version to avoid losing a concurrent transition.
Each actor has one observer slot, so a pending poll replaces its stored Waker.
Own work, owned descendants, effective lifecycle, route and operation changes,
and relevant draining producers can advance the version. Unrelated actors do
not advance it. Versions are opaque and wakes coalesce; lifecycle and claims
must be rechecked after waking. Stopping actors return ready; stale handles and
version exhaustion return errors. See
[targeted native activity](../architecture/0016-targeted-native-activity.md).

## Python readiness

The Python driver reads native Start/Failure/Delivery/Cleanup hints in immutable
registration order. Each phase captures a cutoff, so callback-created
registrations wait for a later phase. An earlier handler can enqueue work for a
later actor in the same business pump; each actor receives at most one business
claim per pump. Failure control precedes business, and Python cleanup visits
descendants before parents. Actual claims still revalidate deadlines, routes,
operations, generations, and lifecycle. Idle actors require no per-actor bridge
calls. The detached driver wait wakes on native readiness, with a one-second
maximum shortened by run/drain deadlines; `TestWorld` retains explicit virtual
advance. See [Python readiness](../architecture/0015-python-readiness.md).

## Deliberate limitations

This slice provides immediate cancellation, bounded drain mode, volatile delivery
and reject-on-full queues. It has no retries, durable effects,
TLS/DNS/reconnecting transports, business-mailbox coalescing, native trace exporters,
or asyncio driver. Callbacks are synchronous; coroutine handlers are
rejected.

Native routing and lifecycle use a bounded registry mutex. Source/port and
ownership indexes select routes for publication and cleanup; operation deadlines
and scoped shutdown counts use bounded indexes, and supervisor notification
cleanup uses the ownership tree. Queued/staged event expiry selects due actors
from an exact removable deadline index; activation selects staged sources, and
drain maintenance selects active roots. Nested drains merge under the earliest
deadline, including an earlier child deadline when a parent starts draining.
Allocation uses an exact live-actor count. The first publication snapshot copy
and route-index lookup are proportional to selected fan-out under the World
mutex; destination attempts per routing turn are bounded by the configured batch size. Ticket
metadata and retained terminal handles continue to consume global/source quota
until their clones are dropped. Sustained contention measurements remain before
high-scale claims. See
[indexed routes and deadlines](../architecture/0013-indexed-routing-deadlines.md)
and [event maintenance](../architecture/0014-indexed-event-maintenance.md).
Native cancellation and control maintenance use
nominal 1 ms polls; SDK periodic hooks use native timer deadlines. Actual
resolution depends on the OS. No real-time or throughput guarantee
is made. GIL-enabled CPython 3.11 is the current compatibility target;
free-threading, subinterpreters, active-World fork and interpreter reinitialization
are unsupported. Explicitly close Worlds before interpreter teardown.
