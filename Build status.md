# Build status

Updated September 21, 2026. The original contract is [[actorplane]]. The workspace contains a working Rust core/native slice and a PyO3 boundary, but it is not release-ready and has not been published.

## Implemented

- Python-free Rust core with World/actor generations, bounded mailboxes and retained payload budgets.
- Native source/accumulator/timer/sink reference path and tracked native task lifetimes.
- Queued delivery claims, child ownership scopes, stop fencing, drain mode, shared deadlines, and timeout reports.
- Bounded request/result operations with reservation-before-admission, terminal outcomes, timeout/cancel fencing, and retained completion results.
- Bounded structured failure records, reserved/coalescing supervisor notifications, native task panic containment, and diagnostic history with sequence/gap-aware reads.
- Detailed read-only shutdown reports, operation and callback accounting, retained failure evidence, cooperative deadline reporting, and process-exit regressions.
- PyO3 foreground driver, typed initial events, lifecycle hooks, callback affinity checks, overload handling, and held-GIL native progress evidence.
- General native schemas with strict integer ranges, floats, booleans, strings, bytes, enums, optionals, sequences, and nested records; canonical Python/Rust codecs and atomic registration.
- Fresh Python/native component instances, immutable declarations, native-validated interfaces and typed ports, owner-scoped links, bounded startup output staging, and drain completions across the Python/native boundary.
- Native event envelopes with source/schema/port identities, unique delivery IDs, deadlines, correlation, causation, bounded trace baggage, Python context propagation, and native queued/staged/timer expiry.
- Source-level Rust component SDK with validated configuration, serialized hooks, bounded effects and deferred jobs, native supervisor callbacks, and stop-fenced hook claims. Built-in components use this SDK; idle executors await World activity.
- Shared core/native virtual clock, Python and Python-free Rust `TestWorld` facades, explicit bounded pumps, controlled native request completions, stable subscription/admission ordering, and real native pipeline tests without wall-clock waits.
- Bounded framed TCP with owned listener/connection tasks, shared World scratch accounting, one request/reply per connection, read/write deadlines, cancel/drain cleanup, Python/native targets, and held-GIL socket progress evidence.
- Separately bounded CPU workers and admitted jobs, SDK CPU replies, queued cancellation, guarded capture destruction, post-close counters, truthful noncooperative shutdown, and a concrete native sum-of-squares component with held-GIL progress evidence.
- Bounded World service registration, exact interfaces, actor-owned lease scopes, typed requests and output links, isolated holder stop, service-wide lease revocation, drain/generation fencing, native inspection, and Python/Rust shared CPU service examples.
- Indexed route selection and ownership cleanup, exact removable operation deadlines, indexed scope operation counts, and supervisor child traversal. Independent models exercise 29,000 bounded mutations; local before/after measurements cover unrelated route and deadline occupancy.
- Exact queued/staged event deadline indexes, staged-source and drain-root registries, and live actor accounting. Nested drains preserve the earliest deadline. A separate queue model exercises 12,000 transitions; local measurements cover idle maintenance and allocation at increasing actor occupancy.
- Native Python readiness indexes with stable registration cursors, bounded metadata queries, failure priority, descendant-first cleanup, and detached condition-variable waits. Callback-created work and reused slots retain existing pump ordering; idle actors require no state/claim bridge calls.
- Targeted native activity versions and coalesced wakes across ownership, lifecycle, routing, draining, operation, and service dependencies. Unrelated observers retain their versions and idle executor turns; generation replacement, timeout paths, and reentrant wakers have regression coverage.
- Bounded publication tickets with first-batch route snapshots, per-source FIFO, round-robin bounded routing turns, fan-out/snapshot limits, target revalidation, staged-output integration, and retained terminal reports. Plain core callers pump `World.route_batch()`; NativeRuntime routes automatically and TestWorld requires explicit pumping.
- MIT license decision and private GitHub repository at [krflol/actorplane](https://github.com/krflol/actorplane). Project-name availability remains unverified.
- Native publication stress executable with bounded ticket retention, active/slow consumers, drain/stop races, generation reuse, accounting checks, and an independent watchdog. A local 60-second run completed 237 lifecycle rounds with no duplicate claims or ticket-report errors; see [[docs/validation/native-stress|native stress evidence]].

## Gate assessment

| Design gate | Current state |
|---|---|
| 1. A real Rust World | Working initial native vertical slice with bounded lifecycle and race evidence. |
| 2. One explicit Python boundary | Working initial boundary and clean Windows wheel; hosted platform matrix remains. |
| 3. Ownership under failure/overload | Partial: drain, requests, component scopes, structured failures, supervisor notifications, diagnostics, detailed shutdown reports, TCP scopes, World service leases, and indexed route cleanup are implemented. Further control scheduling, sustained stress, and hosted shutdown evidence remain. |
| 4. Useful authoring library | Partial: general schemas, components, interfaces, typed ports, native links, bounded startup staging, event envelopes, an initial Rust execution SDK, `TestWorld`, framed TCP, bounded CPU operations, and World services are implemented. Broader integrations and authoring tutorials remain. |
| 5. Real application proof | Local initial proof: native TCP echo plus a Python policy workflow with active requests, an owned timer, cancellation, and cleanup. Sustained application/stress evidence remains. |
| 6. Release readiness | Not complete: hosted CI/platform matrix, stress/fuzz coverage, benchmark baselines, tutorials, compatibility review, and name verification remain. |

## Validation references

The current API is documented in [[docs/contracts/api|API contract]]. Local Windows evidence is in [[docs/validation/windows-x64|Windows validation]], and measurement guidance is in [[docs/validation/benchmark-method|benchmark method]]. Current validation covers focused lifecycle, overload, request, drain, generation, diagnostic, and race behavior; it does not establish release-wide support or performance claims.

Local validation passed **565 tests: 253 core, 96 native, one Rust TestWorld facade,
and 215 Python tests**, plus workspace Clippy, formatting, Python dependency
isolation, a native build without default features, and a freshly installed MIT
release-wheel smoke test. The [first hosted run](https://github.com/krflol/actorplane/actions/runs/35598889835) was blocked before all four jobs started by GitHub account billing/spending limits. Hosted platform validation remains pending. Implementation used Luna delegates
with integration and adversarial review in the parent task.

## Next vertical slice

Fuzz execution is disabled at the user's request; experimental sources remain outside CI. The next executable checks are the hosted platform build/test matrix and native stress workflow.

Advance contention measurement and hosted platform/release evidence. Preserve the established schema, envelope, ownership, failure, shutdown, and publication-ticket guarantees in each extension. See [[docs/architecture/0017-publication-tickets|Publication tickets]], [[docs/architecture/0016-targeted-native-activity|Targeted native activity]], [[docs/architecture/0015-python-readiness|Python readiness]], [[docs/architecture/0014-indexed-event-maintenance|Indexed event maintenance]], [[docs/architecture/0013-indexed-routing-deadlines|Indexed routing and deadlines]], and [[docs/architecture/0012-world-services|World services]] for current execution, accounting, and lifecycle limits.
