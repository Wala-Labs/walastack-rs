# Contributing to walastack-rs

Thanks for your interest in contributing to WalaStack.

## Status

WalaStack is in early foundational development. The architecture, crate
boundaries, and APIs are still being finalized. Major design decisions are
captured in the [architecture spec](https://walastack.com/docs/architecture)
and (eventually) in [RFCs](https://walastack.com/docs/rfcs).

## Development setup

Prerequisites:

- Rust 1.85 or later (pinned via `rust-toolchain.toml`)
- `cargo-nextest` for tests: `cargo install cargo-nextest`
- `cargo-deny` for advisory/license checks: `cargo install cargo-deny`

Standard loop:

```bash
cargo check --workspace
cargo nextest run --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

Or use the workspace task runner:

```bash
cargo xtask ci
```

## Pull requests

- Branch off `main`, keep PRs focused, prefer small.
- Use [Conventional Commits](https://www.conventionalcommits.org/) for commit
  messages (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`).
- All commits should be signed (`git commit -S`).
- CI must be green before merge.

## RFC process

Formal RFC process is deferred until the foundational architecture stabilizes.
For now, propose substantial design changes via a GitHub issue.

## Code of conduct

Participation is governed by the [Contributor Covenant](CODE_OF_CONDUCT.md).
