# Local validation evidence

Date: September 21, 2026. Host: Windows x64, CPython 3.11.8 (GIL enabled),
Rust/Cargo 1.97.0. Dependencies are pinned in Cargo.lock and uv.lock.

The current slice is validated with native unit/integration tests and Python
integration tests against the built extension. The native dependency graph has
one package for core, 16 for native execution, and 17 for the Rust test harness
with the workspace's I/O feature selection;
none reaches PyO3 or the
Python adapter. No hosted CI results are claimed.

Final local checks: **253 core tests, 96 native tests, one Rust TestWorld facade
test, and 215 Python tests passed** (565 total). Workspace Clippy with warnings denied, formatting,
dependency isolation and the clean release-wheel installation also passed.

The suite includes 200 threaded completion/cancel races, 100 actor/operation
generation-reuse cases, and 100 four-way completion/cancel/target-stop/timeout
races. Repeated native tests exercise 40 single-World cycles and 20 paired-World
cycles. These are bounded regression tests, not sustained soak or formal model
checking. Request and drain deadlines are checked during deliberate GIL holds.
Service coverage adds 64 concurrent request/release races, lease and service
generation reuse, global/per-holder/actor capacity rollback, interface-port
restriction, configuration-time linking, cross-holder service revocation,
holder/service drain, and service-owned work retention after holder stop.
Python and Rust examples share one native CPU service across two independent
holders, stop one holder, complete through the other, and close with no retained
payload bytes. The full Rust run is recorded locally in
`target/services-rust-validation.log`.
The subsequent index migration passed the full native suite recorded in
`target/indexed-rust-validation.log`. Independent seeded models exercise 20,000
operation transitions and 9,000 route mutations, checking results, counts,
admission order, dependencies, and generation reuse against simple model state.
An additional 64 publication/unsubscribe races verify queued fencing and byte
release. Internal tests check exact index memberships and bounded deadline
storage through completion/rollback/reuse. Supervisor regressions check that
claim and stop affect only the intended parent's notifications. These remain
bounded regression/model exercises, not sustained stress or exhaustive proofs.
See [[benchmark-method]] for local scan-scaling measurements.
Publication-ticket coverage also exercises ingress quota rollback, first-batch
snapshot capture, source FIFO, round-robin bounded routing turns, fan-out and
aggregate snapshot limits, staged-output flush, deadline expiry, and stop,
drain, unsubscribe, and generation fences. Plain core tests explicitly pump
World.route_batch(); NativeRuntime routes automatically and TestWorld uses its
explicit bounded pump. These checks establish the ticket contract and cleanup
accounting, while sustained contention and hosted routing evidence remain open.
The final Rust publication migration run is recorded in
`target/publications-rust-validation.log`. Its 15 publication integration tests
and two internal tests include aggregate snapshot competition, exact metadata
reconstruction over 100 retirement cycles, result-capacity rollback, and
publication ID exhaustion. Three additional router shutdown regressions cover
idle observer wake/drop reentry, terminal polling without observer retention,
drain completions, and `StopWorld` failure cleanup. Frozen-clock cutoff tests
distinguish pre-drain ordinary ingress from post-cutoff ingress and authorized
completions. Two native router tests cover automatic progress and virtual pump
bounds, including 65 destinations and a zero-subscriber publication. A separate
virtual counter regression holds routing while drain runs, verifies both counter
and sink remain retained, then explicitly routes source input and the final
summary before checking complete cleanup. This covers the premature-retirement
race found by the paired-World drain/cancel stress test. Six Python
ticket tests cover staged/queued status, terminal quota retention and release,
fan-out failure, disposal, process fences, and inspection/shutdown counts.

The release wheel smoke includes an explicitly pumped queued-to-routed ticket.
`examples/publications.py` separately demonstrates two destination admissions
and checks actual handler execution. The `routing_batches` example and
[[benchmark-method]] document first-snapshot cost and bounded destination
attempts without claiming bounded lock latency or production throughput.
The event-maintenance migration is recorded in
`target/maintenance-rust-validation.log`. Ten focused regressions cover FIFO and
equal deadlines, failed admission, request timeout fencing, generation reuse,
staged expiry/stop/self-links, and earliest-deadline merging for nested drains.
Four internal tests check exact metadata, including reconstruction from live
queues/staging, service lease accounting, and repeated reuse. A separate
four-seed model exercises 12,000 queue transitions and compares every actor's
queue entries/bytes, delivery order, global expiry counts, actor capacity, and
retained bytes; coverage assertions require each generated action and meaningful
admission/claim/expiry/full-queue outcomes. These checks add no timing thresholds.
The subsequent Python readiness migration is recorded in
`target/readiness-rust-validation.log` and `target/readiness-python-validation.log`.
Eight core integration tests cover readiness ordering, generation reuse, held
leases, coalescing, ancestor activation, cleanup, and wakeups. Four internal
tests include exact membership/notification-count reconstruction, expiry,
service scopes, 100 retirement cycles, and allocation-order overflow rollback.
Thirteen Python cases cover callback mutations, same-pump and deferred delivery,
cleanup cutoffs, fixed idle query work, detached cross-thread delivery/stop,
and run waits. Existing component-drain tests caught an extra wait after the
last registration was cleaned up; the driver now rechecks before waiting.
Python coverage includes child teardown, structured failures and supervisor
fairness, diagnostic retention, shutdown reports, and close-discard accounting.
An additional 100 claim-versus-parent-stop races exercise supervisor leases.
Native task factory/poll panics and a deliberately noncooperative poll test
containment and truthful incomplete shutdown. Python subprocess tests cover
explicit close before interpreter exit, cleanup after exceptions, and repeated
close. These do not establish support for subinterpreters or interpreter restart.

Schema coverage includes native/Python encoding, nested records, exact node and
depth limits, `uint64` bounds, canonical float policies, native validation of
every truncation of a nested event, registration rollback, mutable snapshots,
and structured timer delivery during a held GIL. This bounded mutation corpus
is regression evidence; coverage-guided fuzzing remains to be added.

Component coverage includes typed links, incompatible native interfaces, startup
staging bounds and rollback, stale ports, partial fan-out, separate Python state,
driver affinity, and a native component pipeline progressing during a held GIL.
Python and native snapshot sinks share an interface/drain test. A native drain
test consumes 65 queued inputs across multiple batches and emits the final
partial-window summary before retiring its consumer. These are bounded
regressions; long-running component soak coverage remains.

The source-level Rust SDK runs the built-in components and a custom configured
deferred-reply example. SDK coverage checks independent state and replies,
startup rollback, effect/input/job bounds, dual local/global timer budgets,
supervisor continuation, cancellation/timeout input release, and actor/operation
generation reuse. A noncooperative job retains truthful incomplete shutdown
accounting. Native hook claims fence startup, ticks, and drain against stop;
blocking ticks remain visible until released. Ordinary component-destructor
panics are contained during startup rollback and normal retirement.

Activity regressions verify idle executor turns remain stable, overdue ticks
wait for a held control claim, and waker disposal/wake can re-enter World APIs
without holding World locks. A panicking wake does not skip other observers.
Native executor activity now targets affected actors, ownership ancestors,
route/operation endpoints, and quiescing consumers whose producers changed.
Fourteen new integration tests cover isolation, task completion during drain,
external operation owner failure/stop, maintenance/claim/late-result expiry,
control claims, and 32 poll/admission races. Four registry unit tests cover
coalescing, scoped version overflow, and replacement observers; three operation
metadata tests cover indexed endpoint selection. Sixteen idle native components
retain their versions and executor turn counts through 16 unrelated deliveries.
Logs are `target/activity-rust-validation.log`,
`target/activity-python-validation.log`, `target/activity-checks-validation.log`,
and `target/activity-wheel-validation.log`; the Rust log includes the full run
and final focused checks. See [[benchmark-method]] for local registry timings.
Python dispatch uses native phase indexes and a condition-variable wait.
Maintenance indexes deadlines and drain roots, but processes affected queues
and draining trees under the World mutex. This evidence does not establish loaded throughput
or bounded lock latency. Control, timer, and deferred cancellation paths retain
their periodic checks.

Envelope coverage includes bounded metadata validation, generation-fenced
sources, historical source provenance, physical trace accounting, distinct
fan-out IDs, exact port identities, schema versions, request context retention,
and context propagation without deadline extension. Queued and staged expiry
are exercised natively; Python tests hold the GIL while queued and timer-held
payloads expire. A threaded port-send regression distinguishes external ingress
from the currently executing handler. The release wheel also checks request
envelopes and trace baggage after installation into a clean environment.

## Representative executions

CPU coverage checks CPU/component/global-task admission limits, queued cancellation
and timeout while workers are busy, request metadata, normal and oversized output,
panic containment, and cancellation immediately before deferred submission.
A job-held output reservation is checked through result admission, including
failed growth rollback and conservative overlap with the core result charge.
A two-worker test blocks both CPU workers while a native timer still fires and a
queued CPU request cancels. Running cancellation is followed by reuse of the
same operation slot before the old computation returns; its old result cannot
affect the replacement. Admitted replies also complete during scoped drain.

A noncooperative computation keeps the close report incomplete and retains its
input/task reservation until released. Unpolled and worker-reserved jobs exercise
panicking capture destructors while verifying that accounting remains live
during destruction. Python checks exact results at 0, 1, 7, and 1,000,000, input
range errors, configuration limits, and post-close CPU inspection. A held-GIL
probe submits eight computations after the measured hold begins, verifies exact
results, stops the native target inside that interval, and records zero adapter
bridge entries. This is native progress evidence, not a CPU throughput benchmark.

The CPU examples reported:

```text
sum_squares(1000000)=333333833333500000; CPU completed=1; tasks=0; retained bytes=0
sum_squares(7)=140; native CPU completion; retained bytes=0
```

TCP coverage checks fragmented/coalesced and empty frames, oversized/truncated
input, connection/task caps, read/request deadlines, startup rollback, shared
World byte reservations across listeners, partial-read cancellation, admitted
reply drain, and generation-fenced late results after connection-slot reuse.
A deterministic one-byte-capacity writer checks timeout after a partial prefix;
this is not a measurement of operating-system TCP send-buffer pressure.

A full-suite run exposed requester cancellation between a native handler claim
and deferred-job submission. The fixed SDK treats an already terminal/retired
request as a benign no-op. A forced interleaving checks no job/factory submission
and successful subsequent requests. The TCP cancellation/reuse regression also
passed 50 consecutive standalone runs after this fix.

Python TCP tests share the same event/reply contract with native echo. With the
Python driver stalled and its mailbox full, another connection is rejected while
an independent native echo continues. A dedicated probe checks eight socket
round trips and native listener shutdown inside the recorded GIL-hold interval,
zero adapter bridge entries during that interval, and zero buffers afterward.
World close checks the wider target scope and zero retained payload bytes.

The native TCP example and Python connection workflow reported:

```text
tcp loopback: frames_written=3 bytes_written=16 native_done=true in_flight=0
native roundtrip=1; pending request cancelled by scoped timer; sockets and bytes released
```

Virtual-time coverage runs real timers, deferred replies, and native SDK
components without sleeping. It checks frozen idle time, exact 10 ms deadlines,
shared diagnostic timestamps, overflow rejection, canceled/retired completions,
native task poll budgets, bounded registry admission, payload release, and drain
timeout reports. Twenty fresh Python TestWorlds produce identical native
source/counter summary and enqueue-timestamp traces. Controls reject wrong-thread
and reentrant driving before time or pending-work mutation. This is bounded
repeatability evidence for the pinned scheduler, not production replay.

The Rust `virtual_pipeline` example advances ten 10 ms steps, checks ten source
inputs and ten processed inputs, then drains the real sink and checks zero tasks
and retained bytes. `examples/testing.py` exercises the public Python facade,
with the output:

```text
virtual timer at 10 ms; controlled reply completed; retained bytes=0
```

A native example run over roughly 500 ms reported:

```text
generated_inputs=32 counter_completed=32 generated_summaries=4 sink_completed=4
world_submitted=36 world_admitted=36 world_completed=36 world_rejected=0
active_tasks=0 retained_payload_bytes=0
```

One deliberately held-GIL probe reported:

```json
{
  "progress_in_hold": 4,
  "native_stopped_in_hold": true,
  "hold_start_ns": 81900,
  "hold_end_ns": 180415000,
  "first_progress_ns": 52791100,
  "last_progress_ns": 113815200,
  "bridge_entries_during_hold": 0,
  "python_queued": 2,
  "rejected": 2
}
```

Timestamps use the runtime's monotonic epoch. The Rust observer sampled sink
progress during the hold and requested owner cancellation before the attached
test call returned. Bridge-entry instrumentation counts this adapter's entries;
it is not a global interpreter-attachment profiler. Dependency isolation and
the absence of Python values/callbacks in native crates support that boundary.
The separate callback resumption test checks native progress across a finite
GIL hold on a running Python-authored World, then resumes its foreground driver.

These are semantic demonstrations, not throughput benchmarks. Timer granularity,
build mode and scheduler load affect event counts. No speedup or latency target
is inferred from this run.

## Reproduction

Follow [[README]] for compilation and tests. `scripts/smoke_wheel.py` installs the
release wheel into a clean environment under `target/wheel-smoke`, checks native
summary delivery, nested schema payloads, Python/native component links and completion
drain, request/reply, structured diagnostics, supervision, virtual timer delivery,
controlled native TestWorld replies and timeout, framed TCP echo and cleanup,
native CPU computation, post-close counters, independent service leases,
queued-to-routed publication tickets, shutdown accounting and cleanup, confirms that
the MIT license is included, and verifies that test-only GIL-hold functions
are absent. The tested wheel is in `dist/` and is ignored by Git.

The publication milestone's captured logs are
`target/publications-rust-validation.log`,
`target/publications-python-validation.log`,
`target/publications-clippy.log`,
`target/publications-no-default.log`,
`target/publications-wheel-build.log`, and
`target/publications-wheel-smoke.log`. These are local, ignored build artifacts;
the checked-in tests, examples, and reproduction commands provide the repeatable
checks.

See [[benchmark-method]] for a separate native queue measurement and its limits.
