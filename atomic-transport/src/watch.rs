//! Real-time watch (auto-sync) via the Watch gRPC stream.
//!
//! The server pushes a `ChangeNotification` whenever a new change lands on the
//! watched view. This module opens that stream and, for each notification,
//! fetches missing changes and saves them locally.
//!
//! Run `watch_and_sync` in a background task for the duration of an editor
//! session. Cancel the returned future to stop watching.

use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use atomic_core::change::Change;
use atomic_core::types::Hash;
use atomic_repository::Repository;

use crate::client::AtomicClient;
use crate::proto::{PullRequest, WatchRequest};

/// Watch `view` on the server and auto-apply incoming changes to `repo`.
///
/// Runs indefinitely until the server closes the stream or the task is
/// cancelled. Returns `Ok(())` on clean stream close.
pub async fn watch_and_sync(
    client: &mut AtomicClient,
    repo: &Repository,
    owner: &str,
    repo_name: &str,
    view: &str,
) -> Result<()> {
    info!("watching {}/{} view={}", owner, repo_name, view);

    let req = WatchRequest {
        owner: owner.to_string(),
        repo: repo_name.to_string(),
        view: view.to_string(),
        token: String::new(),
    };

    let mut stream = client
        .inner
        .watch(req)
        .await
        .context("watch RPC")?
        .into_inner();

    while let Some(notification) = stream.message().await.context("receive ChangeNotification")? {
        if notification.hash.len() != 32 {
            warn!(
                "received malformed ChangeNotification (hash len {})",
                notification.hash.len()
            );
            continue;
        }

        debug!("notification: new change on view {}", notification.view);

        // Pull only what we're missing by sending our current have-set
        let local_haves: Vec<Vec<u8>> = repo
            .iter_changes()
            .filter_map(|r: Result<Hash, _>| r.ok())
            .map(|h| h.as_bytes().to_vec())
            .collect();

        let pull_req = PullRequest {
            owner: owner.to_string(),
            repo: repo_name.to_string(),
            view: view.to_string(),
            have_hashes: local_haves,
            token: String::new(),
        };

        let mut pull_stream = match client.inner.pull(pull_req).await {
            Ok(r) => r.into_inner(),
            Err(e) => {
                warn!("pull after notification failed: {}", e);
                continue;
            }
        };

        let mut count = 0usize;
        while let Some(data) = pull_stream.message().await.unwrap_or(None) {
            if data.v3_bytes.is_empty() {
                continue;
            }
            let mut cursor = std::io::Cursor::new(&data.v3_bytes[..]);
            match Change::deserialize(&mut cursor) {
                Ok((change, _)) => {
                    if let Err(e) = repo.save_change(&change) {
                        warn!("failed to save auto-synced change: {}", e);
                    } else {
                        count += 1;
                    }
                }
                Err(e) => warn!("deserialize error during auto-sync: {}", e),
            }
        }

        if count > 0 {
            info!("auto-synced {} change(s) from {}/{}", count, owner, repo_name);
        }
    }

    info!("watch stream closed for {}/{}", owner, repo_name);
    Ok(())
}
