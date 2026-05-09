//! gRPC transport layer for Atomic VCS.
//!
//! Connects to an `atomic-server` instance and provides push, pull, and
//! real-time watch (auto-sync) operations.
//!
//! # URL scheme
//!
//! Use `grpc://host:port` for plaintext (dev) or `grpcs://host:port` for TLS.
//! The `atomic push` / `atomic pull` commands detect these schemes and route
//! to this crate instead of the legacy HTTP transport.
//!
//! # Example
//!
//! ```ignore
//! use atomic_transport::AtomicClient;
//!
//! let client = AtomicClient::connect("grpc://localhost:7777", Some("jwt-token")).await?;
//! client.push(&repo, "dev").await?;
//! ```

pub mod proto {
    tonic::include_proto!("atomic.v1");
}

mod auth;
mod client;
mod pull;
mod push;
mod watch;

pub use auth::{TokenClient, TokenRequest};
pub use client::AtomicClient;
pub use pull::pull_changes;
pub use push::push_changes;
pub use watch::watch_and_sync;

/// Returns true if the given URL should use the gRPC transport.
/// Schemes `grpc://` and `grpcs://` are ours; everything else uses the
/// legacy atomic-api HTTP transport.
pub fn is_grpc_url(url: &str) -> bool {
    url.starts_with("grpc://") || url.starts_with("grpcs://")
}

/// Convert a `grpc://` / `grpcs://` URL to the `http://` / `https://`
/// scheme that Tonic expects.
pub fn to_tonic_endpoint(url: &str) -> String {
    if let Some(rest) = url.strip_prefix("grpcs://") {
        format!("https://{}", rest)
    } else if let Some(rest) = url.strip_prefix("grpc://") {
        format!("http://{}", rest)
    } else {
        url.to_string()
    }
}
