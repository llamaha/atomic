//! Push local changes to atomic-server via gRPC.
//!
//! Protocol:
//!   1. Ask server which hashes it has via ListHashes.
//!   2. Compute diff: local - server = to upload.
//!   3. Open bidirectional Push stream.
//!   4. Send PushHeader, then SyncHave, then stream ChangeData for each missing change.
//!   5. Receive PushResult.

use std::collections::HashSet;

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};

use atomic_core::change::Change;
use atomic_core::types::{Base32, Hash};
use atomic_repository::Repository;

use crate::client::AtomicClient;
use crate::proto::{ChangeData, HashListRequest, PushHeader, PushMessage, SyncHave};
use crate::proto::push_message::Payload;

/// Stats returned after a successful push.
#[derive(Debug, Default)]
pub struct PushStats {
    pub uploaded: usize,
    pub already_present: usize,
    pub rejected: usize,
}

/// Push all changes from `repo` that the server is missing.
pub async fn push_changes(
    client: &mut AtomicClient,
    repo: &Repository,
    owner: &str,
    repo_name: &str,
    view: &str,
) -> Result<PushStats> {
    // ── 1. Collect local hashes ───────────────────────────────────────────
    let local_hashes: Vec<Hash> = repo
        .iter_changes()
        .filter_map(|r: Result<Hash, _>| r.ok())
        .collect();

    debug!("local repo has {} changes", local_hashes.len());

    // ── 2. Ask server which hashes it has ─────────────────────────────────
    let server_hashes: HashSet<[u8; 32]> = {
        let req = HashListRequest {
            owner: owner.to_string(),
            repo: repo_name.to_string(),
            view: view.to_string(),
            token: String::new(),
        };
        let mut stream = client.inner.list_hashes(req).await?.into_inner();
        let mut set = HashSet::new();
        while let Some(entry) = stream.message().await? {
            if entry.hash.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&entry.hash);
                set.insert(arr);
            }
        }
        set
    };

    debug!("server has {} changes", server_hashes.len());

    // ── 3. Compute what to upload ─────────────────────────────────────────
    let to_upload: Vec<Hash> = local_hashes
        .iter()
        .filter(|h| !server_hashes.contains(h.as_bytes()))
        .copied()
        .collect();

    if to_upload.is_empty() {
        info!("nothing to push — server is up to date");
        return Ok(PushStats {
            already_present: local_hashes.len(),
            ..Default::default()
        });
    }

    info!("pushing {} changes", to_upload.len());

    // ── 4. Open Push stream ───────────────────────────────────────────────
    let (tx, rx) = mpsc::channel::<PushMessage>(64);

    // Send header
    tx.send(PushMessage {
        payload: Some(Payload::Header(PushHeader {
            owner: owner.to_string(),
            repo: repo_name.to_string(),
            view: view.to_string(),
            token: String::new(),
        })),
    })
    .await
    .context("send push header")?;

    // Send SyncHave with all local hashes
    let have_bytes: Vec<Vec<u8>> = local_hashes
        .iter()
        .map(|h| h.as_bytes().to_vec())
        .collect();
    tx.send(PushMessage {
        payload: Some(Payload::Have(SyncHave { hashes: have_bytes })),
    })
    .await
    .context("send SyncHave")?;

    // Stream the missing changes
    let mut uploaded = 0usize;
    for hash in &to_upload {
        let change = repo
            .load_change(hash)
            .with_context(|| format!("load change {}", hash.to_base32()))?;

        let mut buf = Vec::new();
        change
            .serialize(&mut buf)
            .context("serialize change")?;

        tx.send(PushMessage {
            payload: Some(Payload::Change(ChangeData {
                v3_bytes: buf.into(),
            })),
        })
        .await
        .context("send ChangeData")?;

        uploaded += 1;
        debug!("sent {}", hash.to_base32());
    }

    drop(tx); // signal end of stream

    // ── 5. Await PushResult ───────────────────────────────────────────────
    let result = client
        .inner
        .push(ReceiverStream::new(rx))
        .await
        .context("push RPC")?
        .into_inner();

    for e in &result.errors {
        tracing::warn!("server push error: {}", e);
    }

    Ok(PushStats {
        uploaded,
        already_present: local_hashes.len() - to_upload.len(),
        rejected: result.rejected as usize,
    })
}
