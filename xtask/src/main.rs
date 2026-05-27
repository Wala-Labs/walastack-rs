//! Workspace task runner for walastack-rs.
//!
//! Invoked as `cargo xtask <subcommand>`. Phase 1 stub.
//!
//! Planned subcommands:
//! - `cargo xtask fmt`     — run rustfmt + taplo across the workspace
//! - `cargo xtask lint`    — run clippy with workspace lints
//! - `cargo xtask test`    — run cargo-nextest across the workspace
//! - `cargo xtask ci`      — run the full CI pipeline locally
//! - `cargo xtask release` — pre-flight checks before a release

#![allow(clippy::print_stdout)]

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if let Some(cmd @ ("fmt" | "lint" | "test" | "ci" | "release")) =
        args.get(1).map(String::as_str)
    {
        println!("xtask: subcommand '{cmd}' is a Phase 1 stub");
    } else {
        println!("xtask — walastack-rs workspace task runner");
        println!();
        println!("Usage: cargo xtask <fmt|lint|test|ci|release>");
    }
}
