# Native publication stress

On September 21, 2026, the Windows x64 release executable completed a local
60-second campaign with seed `0x1234`. The captured log is
`target/stress-soak.log` (an ignored local artifact).

| Worker | Lifecycle rounds | Publications accepted | Deliveries claimed | Duplicate claims | Ticket errors |
|---|---:|---:|---:|---:|---:|
| 1 | 118 | 1,312,680 | 522,678 | 0 | 0 |
| 2 | 119 | 1,356,934 | 563,050 | 0 | 0 |

Elapsed time was 60.460 seconds. Each worker repeatedly creates a World with two
publishers, one active consumer and one consumer whose mailbox is deliberately
left full. It requests drain, stops a publisher while it is still submitting,
joins workers, reuses an actor slot, and checks cleanup. The oracle checks
per-source ordering, duplicate deliveries, ticket reports, metric conservation,
bounded resource snapshots, consumer progress and zero retained resources after
cleanup. A separate watchdog terminates hangs.

Limits are eight actors and subscriptions, eight entries/256 bytes per mailbox,
512 retained payload bytes, 256 publications globally and 128 per source,
four fan-out/snapshot entries, one destination per routing batch, and 128
externally retained ticket handles. Two-second CI runs use shorter rounds.

```sh
cargo run --locked --release -p actorplane-native --example stress_runtime -- --ci --seed=0x1234
cargo run --locked --release -p actorplane-native --example stress_runtime -- --soak --seed=0x1234
```

This is bounded semantic stress evidence, not a throughput benchmark or a
production reliability guarantee. The seed controls generated work and jitter;
thread scheduling is not deterministic. Concurrent route replacement, operation
lifecycle churn and combined GIL/TCP/CPU shutdown stress remain separate gaps.
The slow consumer here is native. Fuzz execution is disabled at the user's
request and is not part of hosted CI.
