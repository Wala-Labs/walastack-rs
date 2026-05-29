# walastack-rs

> Rust implementation of [WalaStack](https://walastack.com) — trusted infrastructure
> for resilient and intelligent systems.

`walastack-rs` is the primary Rust Cargo workspace for the WalaStack ecosystem,
developed and stewarded by [Wala Labs](https://walalabs.tech).

It is the foundation for a Rust-first application platform focused on resilience, extensibility, and provider-agnostic infrastructure. The ecosystem is designed to support web applications, runtime services, observability, AI integrations, offline-first systems, and sovereign deployment scenarios.

---

## Status

**Pre-release, in active foundational development.**
APIs are unstable and will change. Not yet ready for production use.
The first tagged release will be `0.1.0-alpha.1`.

---

## Workspace layout

```text
walastack-rs/
├── crates/
│   ├── walastack             # Umbrella crate + prelude
│   ├── walastack-cli         # walastack CLI binary
│   ├── walastack-runtime     # Tokio runtime integration
│   ├── walastack-http        # HTTP types & body abstractions
│   ├── walastack-router      # Route matching
│   ├── walastack-service     # Service / middleware abstractions (Tower)
│   ├── walastack-app         # User-facing application framework primitive
│   ├── walastack-macros      # Procedural macros
│   └── walastack-test        # Testing utilities
├── examples/
│   └── hello-world           # Minimal example app
└── xtask/                    # Workspace task runner
```

Additional crates (`walastack-ai`, `walastack-sync`, `walastack-edge`,
`walastack-deploy`, `walastack-auth`, `walastack-db`, …) are planned for
later phases — see the [architecture overview](https://walastack.com/docs/architecture).

---

## Quick start (when Phase 1 lands)

```bash
cargo install walastack-cli
walastack new my-app
cd my-app
walastack dev
```

---

## Philosophy

WalaStack is not just another Rust web framework. It is an integrated
infrastructure ecosystem designed for environments where trust, resilience,
low latency, and sovereign deployment matter — public-sector platforms,
humanitarian operations, low-connectivity contexts, and AI-native applications
that need graceful degradation.

The Rust workspace serves as the core open-source foundation of the WalaStack ecosystem.

---

## License

Dual-licensed under either:

- [MIT License](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Vulnerability reports: see [SECURITY.md](SECURITY.md).
