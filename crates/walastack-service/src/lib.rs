//! # walastack-service
//!
//! Service traits and combinators for asynchronous request / response
//! operations.
//!
//! Phase 1 re-exports the relevant Tower traits for use by middleware
//! that lands in later phases. The bulk of this crate's functionality
//! (middleware composition, retry / timeout policies, AI-fallback
//! integration, sync / offline middleware) arrives in subsequent phases.

pub use tower::{Service, ServiceExt};
