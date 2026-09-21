# ADR 0001: A small native execution boundary

Status: accepted for the initial implementation.

This decision records the initial scope. Subsequent decisions add
[drain and operations](0002-drain-operations-diagnostics.md),
[failure supervision](0003-failure-supervision.md),
[shutdown reports](0004-shutdown-reports.md),
[general native schemas](0005-native-schemas.md), and
[bounded publication tickets](0017-publication-tickets.md). See the
[current API contract](../contracts/api.md) for implemented behavior.

The native runtime must make progress without the Python dispatcher. The Cargo
workspace therefore has three crates: `actorplane-core`, `actorplane-native`
(including its Tokio execution adapter), and `actorplane-python`. Only the last
depends on PyO3. A dependency-graph check enforces that direction.

Every endpoint has a World ID, slot and generation. Source, counter and sink use
separate native owner scopes below their parent. Core admission and invocation
claims are fenced by lifecycle state. A lease holds its native payload budget
until completion. No registry mutex is held while executing Python.

The first payload family is intentionally narrow: native pulses and count
snapshots, plus registered integer records and integer sequences. The facade
compiles a codec at registration. Sequences are copied into Rust storage before
admission and materialized only after an invocation claim. Names and versions
are validated within a World. Schema IDs are World-local registration IDs.

Subscriptions, native tasks, and publication tickets have finite limits.
Native fan-out uses nonblocking mailbox admission and reports matched, admitted,
and rejected destinations through a retained routing ticket. The ticket captures
its route snapshot at the first native routing batch, processes bounded batches,
and does not promise handler execution or durable effects. See
[the publication ticket decision](0017-publication-tickets.md) for snapshot,
FIFO, fairness, and quota semantics.

The foreground Python driver provides a single callback lane and rejects nested
dispatch. Native tasks never call Python. Short detached waits let the driver
idle without holding the interpreter. Explicit close fences the World, stops
native tasks and releases Python-owned references on the driver.

Immediate cancellation is the only initial stop mode. Drain, generalized
operations and their terminal-result state machines are not advertised. Native
components are trusted, cooperative code; this release contains no arbitrary
blocking-job executor.

The test-only `test-support` Cargo feature exposes a finite GIL-hold probe. A
native observer records sink progress and requests native cancellation before
the GIL is released. Instrumented bridge entry counts and the dependency
firewall accompany the timing evidence. This hook is absent from release wheels.
