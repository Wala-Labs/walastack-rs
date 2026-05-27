//! # walastack
//!
//! Umbrella crate for the WalaStack ecosystem.
//!
//! This crate re-exports the primary user-facing framework API and provides
//! a [`prelude`] module of common imports.
//!
//! See the [WalaStack architecture spec](https://walastack.com/docs/architecture)
//! for the broader context of this crate.

/// Common imports for WalaStack applications.
///
/// ```rust
/// use walastack::prelude::*;
/// ```
pub mod prelude {}

/// Internal smoke tests for the umbrella crate.
#[cfg(test)]
mod tests {
    /// Smoke test: confirms the umbrella crate compiles and links. Will grow
    /// as `prelude` accumulates real re-exports during Phase 1 implementation.
    #[test]
    const fn smoke() {}
}
