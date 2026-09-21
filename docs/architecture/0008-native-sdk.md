# Source-level native SDK

The source-level SDK in `crates/runtime-native/src/sdk.rs` lets a Rust
component implement `NativeBehavior` and run it inside a bounded actor scope.
The behavior receives a single mutable `NativeContext`; its `on_start`,
`on_event`, `on_tick`, `on_failure`, `on_quiesce`, `on_drain`, and `on_stop` hooks are
serialized. One behavior value is exclusively owned by its native task, so a
hook never runs concurrently with another hook on the same component.

`NativeRuntime::prepare_native` validates and canonicalizes the supplied
`Schema`/`Value` configuration, allocates the owner and declared ports, calls
the factory, and invokes `on_start` synchronously on the caller thread. A
successful result is still `Starting`; the caller must activate its owner.
The worker then handles steady-state events and lifecycle hooks. A factory or
startup failure rolls back the native scope while preserving the original
bounded `NativeError`/failure code.

The context exposes the owner, current envelope and operation, lifecycle
state, named ports, and bounded effects:

- `emit` and `send` admit typed port publications and actor messages;
- `reply` completes the current request once;
- `after` creates an owned one-shot timer;
- `defer_reply` retains the supplied immutable native input and runs an
  asynchronous closure with `HeldPayload` and `JobContext`;
- `defer_cpu_reply` uses a separately bounded blocking pool with the same input
  and request context, preserving task ownership until running work returns.

Effects are charged before admission and limited by
`effects_per_callback`. `defer_reply` also consumes both the component-local
job permit and the runtime-wide task permit. Timers retain both permits too;
each component executor consumes one global task permit. Its deadline is the original
request deadline; cancellation checks the deadline, owner lifecycle, and
operation status. A non-cooperative job cannot be made cooperative by close,
so shutdown reports whether native work completed by the shared deadline.

If the current request has already become terminal or its result slot has been
retired, `defer_reply` succeeds as a no-op and drops its input and factory without
invoking the factory or submitting a job. It rechecks this state if preparation
fails during a concurrent cancellation. This prevents normal requester retirement
from failing a shared responder. A forced handler/cancellation interleaving checks
that no factory runs and a subsequent fresh request still completes.

`NativeLimits` defaults to 32 events per turn, 64 effects per callback, eight
jobs, 32 KiB per deferred input, and 16 KiB configuration bytes. Events,
effects, and configuration are bounded to 1–1024, 1–1024, and 1–65536
respectively; jobs are 0–1024, input is at most 1 MiB, and a zero input budget
is valid only when jobs are disabled. `max_jobs = 0` disables deferred jobs and one-shot timers.
The configuration is schema-encoded and decoded before the factory receives
it, producing a fresh canonical value rather than sharing caller-owned data.
Periods are optional and must be positive and no longer than 24 hours.

`DrainPolicy::Producer` rejects descriptors containing input ports. Producers
quiesce and retire production. Consumer components receive queued events and may emit
their final completion from `on_drain`. The core enforces port direction and
schema identity before admission. Native output staging remains bounded while
an owner is starting, and owner stop cancels its timers, jobs, queues, and
retained payloads.

The worker waits on World activity, so an idle component does not poll at a
component-specific interval. The subsequent [targeted activity
change](0016-targeted-native-activity.md) limits notifications to affected actor
dependencies. The runtime control service and deferred-job
watchers retain their bounded one-millisecond maintenance polls. Periodic
callbacks skip missed ticks and preserve cadence instead of replaying every
missed occurrence.

Startup, periodic, and drain hooks acquire native control claims before the
stop fence. A claimed callback may finish, and remains visible in shutdown
accounting. `on_stop` runs during retirement and is not an effect phase: sends, publications,
timers, and deferred replies are rejected there. Native panic guards convert
ordinary hook and job panics into bounded failure records and stop the
component. Ordinary component-destructor panics are also contained, including
startup rollback. A panicking behavior skips its `on_stop` hook. Supervisors
receive reserved, coalescing failure notifications; `on_failure` returns an
action and defaults to `StopWorld`. Native task completion and retained payload cleanup are observable
through the runtime shutdown report; a timeout reports incomplete work rather
than claiming it was killed.

The SDK currently covers source-level native behavior, typed core ports,
bounded timers/jobs, and core lifecycle integration. The [framed TCP transport](0010-framed-tcp.md)
adds an owned native I/O path and uses the SDK for its echo handler.
The [CPU executor](0011-bounded-cpu.md) adds worker/admission caps, queued
cancellation, and a concrete native sum-of-squares operation. World service
leases remain future work.
[TestWorld](0009-testworld.md) now drives this SDK with a shared virtual clock
and controlled deferred replies. Private component state, captured closures, and other arbitrary Rust
allocations require bounds enforced by the author. SDK limits cover admitted
runtime resources and are not an allocator or process RSS guarantee. Blocking
hooks, future polls, and destructors cannot be preempted. The native panic hook may still write to the operating system's
stderr; structured failure records do not promise to capture or suppress that
text.

See the [current API contract](../contracts/api.md), the [native component
ports architecture](0006-components-ports.md), and the finite
[`native_sdk` example](../../crates/runtime-native/examples/native_sdk.rs).
