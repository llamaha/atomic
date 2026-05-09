//! gRPC pull path — dispatched when the remote URL starts with `grpc://` or `grpcs://`.

use atomic_repository::Repository;
use atomic_transport::{AtomicClient, TokenClient, TokenRequest, pull_changes};

use crate::error::{CliError, CliResult};
use crate::output::{create_spinner, finish_error, finish_success, print_success};

/// Resolve a JWT token for the given gRPC remote (same logic as push).
async fn resolve_token(grpc_url: &str) -> CliResult<Option<String>> {
    if let Ok(token) = std::env::var("ATOMIC_GRPC_TOKEN") {
        if !token.is_empty() {
            return Ok(Some(token));
        }
    }

    let username = dialoguer::Input::<String>::new()
        .with_prompt("Username")
        .interact_text()
        .map_err(|e| CliError::Internal(anyhow::anyhow!("prompt error: {}", e)))?;

    let password = dialoguer::Password::new()
        .with_prompt("Password")
        .interact()
        .map_err(|e| CliError::Internal(anyhow::anyhow!("prompt error: {}", e)))?;

    let token_client = TokenClient::new(grpc_url);
    let token = token_client
        .get_token(TokenRequest { username, password })
        .await
        .map_err(|e| CliError::Internal(anyhow::anyhow!("authentication failed: {}", e)))?;

    Ok(Some(token))
}

/// Parse `owner/repo` out of a gRPC remote URL (`grpc://host:port/owner/repo`).
fn parse_owner_repo(grpc_url: &str) -> (String, String) {
    let path = grpc_url
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(grpc_url);

    let after_host = path.splitn(2, '/').nth(1).unwrap_or("");
    let mut parts = after_host.splitn(2, '/');
    let owner = parts.next().filter(|s| !s.is_empty()).unwrap_or("default");
    let repo  = parts.next().filter(|s| !s.is_empty()).unwrap_or("default");
    (owner.to_string(), repo.to_string())
}

/// Run a gRPC pull.
pub async fn run_grpc_pull(
    repo: &Repository,
    grpc_url: &str,
    view: &str,
    dry_run: bool,
) -> CliResult<()> {
    let (owner, repo_name) = parse_owner_repo(grpc_url);

    if dry_run {
        println!("Dry run: would pull from {}/{} view={}", owner, repo_name, view);
        println!("(gRPC dry run does not compute exact delta without connecting)");
        return Ok(());
    }

    // Acquire token
    let spinner = create_spinner("Authenticating...");
    let token = match resolve_token(grpc_url).await {
        Ok(t) => { finish_success(&spinner, "Authenticated"); t }
        Err(e) => { finish_error(&spinner, "Auth failed"); return Err(e); }
    };

    // Connect
    let spinner = create_spinner("Connecting...");
    let mut client = match AtomicClient::connect(grpc_url, token.as_deref()).await {
        Ok(c) => { finish_success(&spinner, "Connected"); c }
        Err(e) => {
            finish_error(&spinner, "Connection failed");
            return Err(CliError::Internal(anyhow::anyhow!("gRPC connect: {}", e)));
        }
    };

    // Pull
    let spinner = create_spinner("Pulling changes...");
    let stats = match pull_changes(&mut client, repo, &owner, &repo_name, view).await {
        Ok(s) => { finish_success(&spinner, "Done"); s }
        Err(e) => {
            finish_error(&spinner, "Pull failed");
            return Err(CliError::Internal(anyhow::anyhow!("gRPC pull: {}", e)));
        }
    };

    if stats.downloaded == 0 {
        print_success("Already up to date");
    } else {
        print_success(&format!(
            "Pull complete: {} new change(s) downloaded",
            stats.downloaded
        ));
    }

    Ok(())
}
