# Hosted platform validation

[Runtime contracts run 35629308482](https://github.com/krflol/actorplane/actions/runs/35629308482)
passed all four jobs on September 21, 2026, at commit
`7e17fb538828d3a1a1a84d7197ff2bfc2691ac66`.

| Job | Evidence |
|---|---|
| Linux native | 350 Rust tests; formatting; Clippy; dependency firewall; no-default-features build; native, virtual-time, TCP, CPU, service, indexing and routing examples; bounded publication stress. |
| Windows x64 | 227 Python tests with CPython 3.11.16; examples; release wheel built from the source archive; clean installation smoke checks. |
| Linux x64 | 227 Python tests with CPython 3.11.16; examples; `manylinux_2_34_x86_64` release wheel built from the source archive; clean installation smoke checks. |
| macOS ARM64 | 227 Python tests with CPython 3.11.16; examples; `macosx_11_0_arm64` release wheel built from the source archive; clean installation smoke checks. |

This includes the 12 [SQLite/pandas tutorial](../tutorials/sales-report.md) tests
and a separate run of the sales importer. Each platform's example stored five
orders, recognized one replay, generated four report groups totaling 24,199
cents, and reported completed native/Python shutdown. The previous
[run 35600713028](https://github.com/krflol/actorplane/actions/runs/35600713028)
established the initial all-platform pass with 215 Python tests.

The repository's initial private runs could not start because of GitHub account
billing/spending limits. Making the repository public allowed hosted runners to
execute. The first executing run exposed a macOS failure in the envelope fan-out
test: two immediate `World.step()` calls did not guarantee that the asynchronous
router had run. That test now uses `TestWorld.run_until_idle()` while retaining
all unique-ID, port-provenance, causation, correlation and trace assertions.
No production runtime change was needed for that failure.

The workflow retains platform wheel and source artifacts. The PyPI 0.1.0 release
still consists of the previously published Windows wheel and source archive;
Linux/macOS CI artifacts were not uploaded to PyPI during this validation.

This establishes the tested initial platform matrix. It does not establish
support for other Python versions, architectures or OS versions, nor production
load/latency guarantees. Fuzzing remains disabled and outside hosted CI.
