# Native benchmark method

`cargo run --locked --release -p actorplane-native --example measure` measures a
bounded native core `Pulse` workload with 10,000 sends and one claimant on a
separate OS thread. It does not measure Tokio component scheduling. The mailbox
has 128 entries and a 64 KiB byte limit; each Pulse contains one signed 64-bit
integer. Full-mailbox retries are counted.

Each latency sample starts immediately before the successful send attempt and
ends after claim, before handler completion. This includes admission lock wait
and queue residence; it excludes earlier rejected attempts. Throughput covers
all attempts through all 10,000 completed leases. The separate stop measurement
times stopping the now-empty target, not shutdown under load.

The example uses monotonic `Instant` timestamps, no Python dependency or callback, a 2 MiB retained-payload budget, and a 30 second watchdog. Its output is a small local scheduling measurement, not a production capacity claim. Results vary with OS scheduling, CPU load, build mode, timer resolution, and contention. It does not compare against direct Python calls because that would be a different workload and guarantee.

Record the exact command, Rust toolchain, host/OS, configuration, event count, rejected retry count, completed count, and all reported latency fields with each run. The sample cap is fixed at 10,000 events and no unbounded history is retained.

Example local Windows x64 result (Rust 1.97.0, release profile, September 20 2026):

```text
events=10000 completed=10000 rejected_retries=6851 throughput_eps=1408450.7
latency_admission_attempt_to_claim_p50_ns=92900 p95_ns=128100
p99_ns=172300 max_ns=240500 stop_ns=1400
mailbox_capacity=128 mailbox_bytes=65536 native_payload_budget=2097152
retained_bytes=0
```

This single local run is illustrative and should not be treated as a stable capacity or latency target.

## Unrelated route and operation occupancy

Run `cargo run --locked --release -p actorplane-core --example index_scale`.
This Python-free core example selects 0, 128, 1,024, or 8,192 unrelated routes.
For each size it creates an unrelated source with that many subscribers, then
measures 2,000 publications and completed claims through a separate source with
one subscriber. Every publication must admit exactly one delivery and cleanup
must return retained payload bytes to zero. It separately measures 101 stops of
a fresh owner with one route, with actor allocation and subscription setup
outside the timing interval. The stop columns are sample median and p95.

A separate operation table holds the same number of pending operations with
deadlines one hour in the future. Its column measures the average time across
2,000 expiry calls with no due deadline. World maintenance, actor scanning,
Tokio scheduling, and Python costs are excluded from this expiry measurement.

Configuration uses `max_actors = unrelated_routes + 8`,
`max_subscriptions = unrelated_routes + 8`, and the remaining Rust core defaults.
The operation-table capacity is `max(1, unrelated_routes)`. Payloads are signed
64-bit `Pulse` values. Each measured target is drained immediately; there is no
overload, rejection, or slow consumer in this workload. One OS thread executes
the benchmark, with no native runtime worker pool.

Local observations on Windows x64, Intel Core i7-12700H, Rust 1.97.0 release
profile, September 20, 2026, before and after indexed routing, operation
deadlines, and supervisor cleanup:

| Unrelated entries | Publish + claim before / after (ns/call) | Owner stop before / after (median ns) | Owner stop before / after (p95 ns) | Idle expiry before / after (ns/call) |
|---:|---:|---:|---:|---:|
| 0 | 502 / 637 | 300 / 1,200 | 400 / 1,500 | 9 / 9 |
| 128 | 857 / 633 | 1,000 / 900 | 1,500 / 1,100 | 104 / 7 |
| 1,024 | 3,665 / 496 | 5,900 / 700 | 8,700 / 800 | 569 / 7 |
| 8,192 | 28,638 / 568 | 57,700 / 1,400 | 79,600 / 4,700 | 5,125 / 7 |

The logs are retained locally as `target/index-scale-before.log` and
`target/index-scale-after.log`. These are single runs, affected by clock
resolution, CPU frequency, scheduler noise, allocation, cache state, and host
load. They are evidence for removing the measured unrelated-table scans, not a
claim that every workload improved: the zero-background stop sample costs more
with indexes. These historical samples used synchronous publication. The current
example explicitly completes routing batches before claiming, so new samples
must not be directly compared with this table as the same operation. This
experiment does not measure contention, sustained throughput, payload codecs,
or tail latency under load. Unit/model tests verify
index contents and cleanup without timing thresholds.

## Idle World maintenance and actor allocation

Run `cargo run --locked --release -p actorplane-core --example maintenance_scale`.
This Python-free core example allocates 0, 128, 1,024, or 8,192 active native root
actors with `max_actors = actors + 1` and otherwise default configuration. It
uses a manual clock so no measured call crosses a deadline. The first two
columns average 2,000 `World.maintain` calls: first with empty queues, then with
one Pulse per actor whose deadline is one hour ahead. There are no operations,
drains, registered executor observers, or worker threads in this workload.

Allocation averages 1,000 individually timed allocations into the spare slot.
Each allocated actor remains Starting and is stopped outside the timing
interval. Finally the example expires every queued Pulse, verifies the exact
expiry count and zero retained payload bytes, and checks native close completion.

Local Windows x64 measurements on Intel Core i7-12700H, Rust 1.97.0 release,
September 20, 2026, before and after queued-event, staged-source, and drain-root
indexes plus the live-actor counter:

| Actors | Empty maintenance before / after (ns/call) | Future-queue maintenance before / after (ns/call) | Allocation before / after (ns/call) |
|---:|---:|---:|---:|
| 0 | 32 / 42 | 26 / 37 | 56 / 50 |
| 128 | 1,512 / 37 | 1,557 / 50 | 137 / 55 |
| 1,024 | 15,757 / 37 | 18,913 / 54 | 1,315 / 82 |
| 8,192 | 142,281 / 37 | 198,301 / 94 | 7,630 / 694 |

Local logs are `target/maintenance-scale-before.log` and
`target/maintenance-scale-after.log`. These single runs show the effect of
removing the measured scans; they do not establish constant end-to-end latency
or production capacity. The empty World incurs index-check overhead, and the
allocation samples still vary with occupancy and host conditions. Clock
resolution, caches, CPU frequency, scheduling, and allocation affect results.
Due-event processing, populated drain trees, staging flush, activity notifications,
contention, and sustained load are outside these timed intervals.

## Idle Python driver readiness

Build with `uv run --no-sync maturin develop --features extension-module,test-support`,
then run `uv run --no-sync python scripts/measure_python_readiness.py`.
The script creates 0, 128, 1,024, or 8,192 empty Python actors, performs their
startup outside the timed interval, and measures 1,000 calls to `World.step()`.
Each must return zero processed callbacks. It uses `max_actors = max(1, actors)`
and otherwise default World settings, including the real native runtime.
There are no messages, failure notifications, schemas beyond the initial
types, or application callbacks in the measured idle steps. Cleanup is outside
the timed interval.

Local Windows x64, Intel Core i7-12700H, Rust 1.97.0 **debug extension**, GIL-enabled
CPython 3.11.8, September 20, 2026:

| Registered actors | Before (ns/step) | After (ns/step) |
|---:|---:|---:|
| 0 | 1,353 | 6,372 |
| 128 | 512,943 | 7,284 |
| 1,024 | 5,719,970 | 6,460 |
| 8,192 | 67,559,337 | 6,810 |

The local logs are `target/python-readiness-before.log` and
`target/python-readiness-after.log`. These single-run debug measurements show
the cost of the removed registration scans. They include bridge and driver
overhead and are affected by host scheduling, clock resolution, caches, and CPU
frequency. Empty-World cost increases because the driver now queries native
phase indexes. Release throughput, active callback costs, notification latency,
native executor wakeups, and contention remain outside this measurement.
Regression tests separately check that idle Worlds with 0, 1, and 1,024 actors
make a bounded number of readiness queries and no actor state/claim calls.

## Unrelated native activity observers

Run `cargo run --locked --release -p actorplane-core --example activity_scale`.
The Python-free core example creates one measured native root and 0, 128,
1,024, or 8,192 unrelated native roots, using `max_actors = observers + 1`
and otherwise default configuration. After activation, every actor registers
an activity version. One OS thread performs 2,000 Pulse send/claim/finish
cycles on the measured actor. Setup, inspection, and close are outside timing.
No Wakers or native worker pool are present: this measures registry version
maintenance rather than scheduler wake execution. The example checks exact
completion counts, zero retained payload bytes, native close completion,
and unchanged unrelated activity versions.

Local Windows x64, Intel Core i7-12700H, Rust 1.97.0 release, September 20, 2026:

| Unrelated observers | Before (ns/cycle) | After (ns/cycle) | Unrelated versions changed before / after |
|---:|---:|---:|---:|
| 0 | 399 | 697 | 0 / 0 |
| 128 | 1,259 | 631 | 128 / 0 |
| 1,024 | 9,575 | 579 | 1,024 / 0 |
| 8,192 | 81,925 | 624 | 8,192 / 0 |

The baseline was linked against the verified release core from the preceding
Python-readiness milestone before targeted marks were installed. Logs are
`target/activity-scale-before.log` and `target/activity-scale-after.log`.
The zero-change assertion was added after the baseline capture; the measured
loop is identical. These single runs show removal of unrelated registry work,
with additional marking overhead at zero background observers. Clock precision,
cache state, frequency, allocation, and host scheduling affect the samples.
They do not establish loaded throughput, notification latency, lock contention,
or bounded fan-out cost. Native SDK tests separately check that 16 idle
components do not execute extra turns while an unrelated component handles work.

## Publication ingress and bounded routing turns

Run `cargo run --locked --release -p actorplane-core --example routing_batches`.
This Python-free, single-threaded core example publishes one Pulse to 0, 32,
128, 1,024, or 4,096 subscribers with `routing_batch_size = 32`. It separately
times ingress, the first batch (including snapshot capture), the slowest batch,
and all routing turns. Actor and route setup, claims, and close are outside
these timing intervals. Every target starts empty and accepts its delivery.

The example verifies no destination is admitted during ingress, at most 32
destination attempts occur per batch, the exact batch count is
`max(1, ceil(subscribers / 32))`, all destinations are admitted, and cleanup
returns snapshot entries and retained payload bytes to zero. Those assertions
check the contract; there are no timing pass/fail thresholds.

Local Windows x64, Intel Core i7-12700H, Rust 1.97.0 release, September 21, 2026:

| Subscribers | Ingress (ns) | First batch (ns) | Slowest batch (ns) | All routing (ns) | Batches |
|---:|---:|---:|---:|---:|---:|
| 0 | 23,000 | 15,000 | 15,000 | 15,300 | 1 |
| 32 | 2,800 | 39,900 | 39,900 | 40,000 | 1 |
| 128 | 1,600 | 16,700 | 22,700 | 71,100 | 4 |
| 1,024 | 3,900 | 56,400 | 56,400 | 559,000 | 32 |
| 4,096 | 8,600 | 314,000 | 314,000 | 2,868,000 | 128 |

The local log is `target/routing-batches-scale.log`. Each row is one cold
sample; allocation, caches, clock precision, CPU frequency, and host activity
affect the timings. No previous implementation was measured for this workload.
This is neither a throughput comparison nor a tail-latency guarantee. The first
batch still copies the selected subscription snapshot under the World mutex;
its cost grows with fan-out and index lookups, capped by the configured fan-out
and aggregate snapshot-entry limits. The batch limit bounds destination
attempts, not every operation performed within a critical section. Loaded
contention, multiple publishers, saturated consumers, and scheduler latency
remain outside this measurement.
