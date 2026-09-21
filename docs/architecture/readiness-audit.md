# Readiness audit

This audit compares `actorplane.md` with executable evidence in the workspace. “Proved” means the current implementation and tests exercise the behavior. “Partial” means a bounded slice exists with explicit limits. “Missing” means it remains future work. The project is not release-ready and the original specification remains unchanged.

## Invariants

| Invariant | Status | Current evidence and limitation |
|---|---|---|
| I-01 Rust core without Python | Proved for core/native crates | Cargo dependency firewall, native build without default features, Rust tests, formatting, Clippy and native examples passed in the hosted Linux job. |
| I-02 Native path independent of Python | Validated for current paths | Native pipelines, framed TCP, and CPU operations progress and stop during measured GIL holds. Other integrations remain outside this evidence. |
| I-03 Rust-owned routing/payloads | Validated for current payload types | Typed native buffers, general schema validation, queued delivery, and bounded ownership are implemented. Native envelopes carry identity, deadlines, correlation, causation, and bounded trace context. |
| I-04 Defined ownership | Validated for current resources | Actor/component trees, typed links, task leases, requests, staged/held payloads, failure/control leases, diagnostics, TCP scopes, and World service leases have explicit ownership. Shared service work remains service-owned after holder stop; broader adapters remain. |
| I-05 Bounded buffering/concurrency | Partial | Mailboxes, bytes, payloads, operations, failure records/control slots, diagnostics, tasks, subscriptions, and event sizes are bounded. Publications add global/per-source pending-plus-retained ticket quotas, fan-out/snapshot caps, and bounded destination attempts. TCP adds connection caps and listener/World buffers. CPU work adds explicit worker/admission caps and retains reservations through running work and capture destruction. The SDK bounds configuration, callback effects, deferred inputs, and local jobs/timers; authors must bound private buffers and captures. |
| I-06 World/generation fencing | Proved for current handles | Actor and operation generation tests cover stale references and replacement slots. |
| I-07 Queued delivery | Proved | Claims and leases are queued; self/nested work does not invoke inline. |
| I-08 Python affinity/non-reentrancy | Partial | Foreground driver and adapter checks exist; full supported-version matrix remains. |
| I-09 No lock across Python | Partial | Current adapter separates native state and Python callbacks; systematic lock-order stress remains. |
| I-10 Retired work fencing | Validated for current paths | Actor/component, typed-port, task, request, SDK hook/deferred completion, and TCP connection cancellation/generation fencing are tested. Claimed external writes may be partial; cancellation closes the connection without rollback. |
| I-11 Admission versus processing/effects | Proved for current metrics | Submission, admission, claim, completion, rejection, cancellation, and operation outcomes are distinct. |
| I-12 Native observability without Python | Partial | Snapshots, metrics, bounded structured failure history, shutdown work accounting, drain/operation reasons, and native progress exist. Bounded trace propagation exists; native exporters remain absent. |

## Gates

| Gate | Status | Evidence and remaining work |
|---|---|---|
| 1. A real Rust World | Proved for the initial slice | Native source/accumulator/timer/sink, framed TCP, bounded delivery, cleanup, and race tests exist. Sustained stress remains. |
| 2. One explicit Python boundary | Validated for initial platform matrix | Typed codec, foreground driver, bounded native-to-Python delivery, held-GIL coverage, and clean wheels passed with CPython 3.11.16 on hosted Windows x64, Linux x64 and macOS ARM64. Broader Python/platform support remains outside this evidence. |
| 3. Ownership under failure/overload | Partial, advancing | Drain, requests, component scopes, task leases, generation fencing, structured failures, supervisor notifications, diagnostics, detailed shutdown reports, TCP cleanup, World service leases, indexed route cleanup, and process-exit regressions exist. Further control scheduling, sustained stress, and hosted shutdown evidence remain. |
| 4. Useful authoring library | Partial | General schemas/codecs, immutable component declarations, interfaces, typed ports, native links, startup staging, native envelopes, an initial Rust execution SDK, `TestWorld`, framed TCP, bounded CPU operations, and World services exist. Broader integrations and authoring tutorials remain. |
| 5. Real application proof | Local initial proof | A bounded TCP loopback and connection workflow combine native echo, Python policy, requests, a scoped timer, cancellation, and zero-byte cleanup. Sustained application evidence remains. |
| 6. Release readiness | Partial evidence only | MIT is selected and 0.1.0 is published on PyPI as a Windows CPython 3.11 wheel and verified source archive. The initial hosted platform matrix passed. Broader stress coverage, benchmark baselines, tutorials and final compatibility documentation remain. Fuzz execution is disabled. |

## Feature ledger

| Area | Status | Current boundary |
|---|---|---|
| Initial payloads/codecs | Proved for the initial slice | Pulse, count snapshots, and bounded integer records are implemented. |
| General schemas | Validated for current type system | Explicit integer ranges, float policies, bool/string/bytes/enum/optional/sequence/nested records, canonical codecs, collision checking, and atomic registration. Native envelopes provide registered schema identity. |
| Event envelopes | Validated for current scope | Native delivery identities, deadlines, bounded trace baggage, propagation, queued/staged/timer expiry, and retained reply context. Native exporters and distributed tracing remain absent. |
| Drain mode | Proved for current core | Quiescing, FIFO drain, shared deadlines, cancellation escalation, child/task retention, and timeout reports are tested. |
| Request/result operations | Proved for current core | Bounded reservation, terminal winner, timeout/cancel, target/owner fencing, retained results, and late-result handling exist. |
| Child scopes | Validated for current scope | Parent cascades, fresh Python/native components, generation-safe replacement, and bounded ownership depth exist. |
| Failure policy | Validated for current scope | Bounded structured records, lifecycle phases, queued/coalesced parent notifications, default stop, explicit continuation, and native task factory/poll panic containment. Automatic restart is absent. |
| Shutdown reports | Validated for current scope | Separate delivery/control/task/operation/Python cleanup counts; bounded latest error plus cumulative totals survive retirement. Native deadline and cooperative Python overrun evidence exist. |
| Diagnostics | Partial | Bounded ring with sequence/gap reads, timestamps, actor/operation handles, structured failure metadata, rejection, timeout, and cancellation codes. This is not tracing or a native diagnostic exporter. |
| Components/interfaces/ports | Partial | Native-validated contracts, immutable declarations, per-instance state, typed links, startup output staging, and shared Python/native sink contract tests exist. Built-ins execute through the source-level Rust SDK. Service interfaces restrict exposed request and output ports. |
| Native execution SDK | Initial implementation validated | Schema configuration, exclusive state, serialized hooks, bounded effects and async/CPU deferred requests, local/global job/timer budgets, stop-fenced control claims, supervisor handling, and bounded panic records. Startup is synchronous on the caller; workers run steady-state hooks. The TCP echo and shared CPU service use this SDK. Broader integrations remain. |
| World services | Initial implementation validated | Native root registration, exact published interfaces, bounded actor-owned child leases, typed requests/output links, holder isolation, service-wide revocation, drain, stale-generation fencing, stable inspection, and Python/Rust examples. Acquisitions use indexed lookups; generic resource pools and automatic I/O adapter integration remain outside this slice. |
| CPU/blocking work | Initial implementation validated | Separate worker and admission caps, queued cancellation, guarded lifetime through unpolled/aborted tasks and destructor panics, noncooperative shutdown accounting, retained inspection, exact computation results, and measured held-GIL progress. Running closures require cooperative cancellation; sustained-load measurements remain. |
| Publication/routing scale | Partial | Source/port/owner/target indexes now feed bounded publication tickets. The first routing batch captures a bounded snapshot; source FIFO and round-robin batches revalidate route generations and lifecycle before each admission. Snapshot copying and index lookup remain proportional to selected fan-out under the World mutex; sustained contention, stress, and hosted release evidence remain. |
| Expiry/control indexing | Partial | Pending operations and queued/staged events have exact removable deadline indexes. Allocation uses a live count; activation selects staged sources; drain maintenance selects roots. Scoped reports union operation indexes, and supervisor claim/discard follows the child tree. Due queues and draining trees are still traversed. |
| Native activity | Initial implementation validated | Per-actor versions and coalesced wakers target ownership, lifecycle, routing, draining, operation, and service dependencies. Registered storage is bounded by actor slots, generation changes retire observers, and wakes run outside locks. Isolation, poll/admission races, timeout/failure endpoints, reentry, and idle SDK turns are tested. Single-observer semantics and periodic control/timer/deferred checks remain. |
| Python readiness | Initial implementation validated | Native phase indexes, monotonic registration cursors, one metadata hint per query, dynamic same-pump delivery ordering, cleanup cutoffs, claim revalidation, and detached condition-variable waits replace idle registration scans. Real waits respect run/drain deadlines; virtual controls preserve their explicit quantum. Callbacks remain cooperative. |
| Native I/O | Initial framed TCP validated | Owned listeners and connections, sequential typed request/reply, bounded framing and scratch, timeouts, stop/drain, and held-GIL evidence. No TLS, DNS, reconnect, multiplexing, or virtual socket simulation. |
| `TestWorld`/virtual time | Initial implementation validated | Python and Rust facades drive the real native SDK and core with one shared clock, controlled bounded replies, stable subscription/ingress ordering, and bounded cooperative pumps. Full traces repeat across 20 fresh Worlds. Blocking hooks require an external watchdog; arbitrary I/O and multithreaded replay are outside this contract. |
| Stress/model/fuzz | Partial | Systematic race and budget tests plus independent models for 41,000 operation/route/queue mutations exist. Queue models assert action and outcome coverage and check every actor's queue against test-owned state. A bounded native publication stress run completed 237 lifecycle rounds over 60 seconds; see [stress evidence](../validation/native-stress.md). Combined resource/lock stress remains. Experimental fuzz sources are inactive; fuzz execution is disabled at the user's request. |
| Benchmarks | Local samples | See [benchmark method](../validation/benchmark-method.md) for native queue and before/after index-scaling measurements. Controlled production baselines remain. |
| License/name | Published | MIT selected; actorplane is registered on PyPI and the source repository is public. |

Asyncio integration, free-threaded Python, coroutine handlers, remote actors, durability, hot reload, binary plugins, and similar features remain explicitly deferred by the architecture.

Hosted CI is configured in the public [GitHub repository](https://github.com/krflol/actorplane).
The [first run](https://github.com/krflol/actorplane/actions/runs/35598889835)
was blocked by GitHub account billing/spending limits before any of its four
jobs started. It supplies no platform validation evidence.

After the repository became public, the billing gate stopped blocking these
runners. [Run 35600713028](https://github.com/krflol/actorplane/actions/runs/35600713028)
passed all four jobs after correcting one scheduling-dependent test. See
[hosted platform evidence](../validation/hosted-platforms.md).
