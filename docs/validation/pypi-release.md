# PyPI release preparation

The 0.1.0 pre-alpha artifacts were rebuilt on Windows x64 with CPython 3.11.8
and Rust 1.97.0 on September 21, 2026. Upload is pending the CI follow-up.

Maturin 1.8.3 left `workspace.default-members` pointing at an omitted test crate
in the source archive. The packaging tool is now pinned to 1.14.1, which includes
the [upstream source-distribution fix](https://github.com/PyO3/maturin/pull/2983).
The build creates an sdist, extracts it into a temporary directory, and compiles
the Windows wheel from that extracted source. Hosted CI now performs the same
check and retains both distributions. The dedicated PyPI description declares
the initial support scope and MIT license without vault-relative links.

```sh
uv sync --frozen --no-install-project
uv run --no-sync maturin build --release --locked --sdist --out dist
uv run --no-sync python scripts/smoke_wheel.py
```

The extracted-source build and clean wheel installation passed. The smoke test
checks native progress, schemas, components, requests, drain, supervision,
virtual time, TCP, CPU, services, publication tickets, license inclusion and
absence of test-only hooks. No fuzzing was run for this release preparation.
Local logs are `target/pypi-build.log` and `target/pypi-wheel-smoke.log`.

The [hosted CI retry](https://github.com/krflol/actorplane/actions/runs/35598889835/attempts/2)
again failed before any job started. GitHub reported account payments/spending
limits, so this run supplies no platform validation evidence. Account billing
must be resolved before hosted runners can execute; no code change clears that
gate. The workflow also accepts manual dispatch after resolution.

Current artifact SHA-256 digests:

```text
8fc4b6cf2a97e0c97070e3381e14858378a3eca8dbeb5cab13934d8665fba380  actorplane-0.1.0-cp311-cp311-win_amd64.whl
d6c178d06296c0b746cbfc07f0746b600ba2616618e74c653436e605ac2a2342  actorplane-0.1.0.tar.gz
```
