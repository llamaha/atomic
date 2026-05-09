//! Pull changes from atomic-server via gRPC.
//!
//! Protocol:
//!   1. Collect local hashes (SyncHave).
//!   2. Send PullRequest with have_hashes.
//!   3. Server streams ChangeData for everything we're missing.
//!   4. Deserialize each change and save to the local change store.

use anyhow::{Context, Result};
use tracing::{debug, info, warn};

use atomic_core::change::Change;
use atomic_core::types::Hash;
use atomic_repository::Repository;

use crate::client::AtomicClient;
use crate::proto::PullRequest;

/// Stats returned after a successful pull.
#[derive(Debug, Default)]
pub struct PullStats {
    pub downloaded: usize,
    pub already_present: usize,
}

/// Pull all changes the server has that we don't.
pub async fn pull_changes(
    client: &mut AtomicClient,
    repo: &Repository,
    owner: &str,
    repo_name: &str,
    view: &str,
) -> Result<PullStats> {
    // ── 1. Collect local hashes ───────────────────────────────────────────
    let local_hashes: Vec<Vec<u8>> = repo
        .iter_changes()
        .filter_map(|r: Result<Hash, _>| r.ok())
        .map(|h| h.as_bytes().to_vec())
        .collect();

    let local_count = local_hashes.len();
    debug!("local repo has {} changes, pulling from {}/{}", local_count, owner, repo_name);

    // ── 2. Send PullRequest ───────────────────────────────────────────────
    let req = PullRequest {
        owner: owner.to_string(),
        repo: repo_name.to_string(),
        view: view.to_string(),
        have_hashes: local_hashes,
        token: String::new(),
    };

    let mut stream = client
        .inner
        .pull(req)
        .await
        .context("pull RPC")?
        .into_inner();

    // ── 3. Receive and save changes ───────────────────────────────────────
    let mut downloaded = 0usize;

    while let Some(data) = stream.message().await.context("receive ChangeData")? {
        if data.v3_bytes.is_empty() {
            continue;
        }

        let mut cursor = std::io::Cursor::new(&data.v3_bytes[..]);
        let (change, _hash) = match Change::deserialize(&mut cursor) {
            Ok(pair) => pair,
            Err(e) => {
                warn!("failed to deserialize incoming change: {}", e);
                continue;
            }
        };

        repo.save_change(&change)
            .context("save pulled change")?;

        downloaded += 1;
        debug!("received and saved change #{}", downloaded);
    }

    info!("pull complete: {} new changes", downloaded);

    Ok(PullStats {
        downloaded,
        already_present: local_count,
    })
}
