//! REST-based token acquisition from atomic-server.
//!
//! gRPC calls require a JWT bearer token, obtained via `POST /api/auth/token`
//! on the server's REST API (same host, same port as gRPC).

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Request body for `POST /api/auth/token`.
#[derive(Debug, Serialize)]
pub struct TokenRequest {
    pub username: String,
    pub password: String,
}

/// Response body from `POST /api/auth/token`.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    token: String,
}

/// Thin client for token acquisition against atomic-server's REST API.
pub struct TokenClient {
    base_url: String,
    http: reqwest::Client,
}

impl TokenClient {
    /// Create a new token client.
    ///
    /// `grpc_url` is the `grpc://` or `grpcs://` URL of the server. The REST
    /// API lives on the same host/port.
    pub fn new(grpc_url: &str) -> Self {
        let base_url = crate::to_tonic_endpoint(grpc_url);
        Self {
            base_url,
            http: reqwest::Client::new(),
        }
    }

    /// Obtain a JWT token by username + password.
    pub async fn get_token(&self, req: TokenRequest) -> Result<String> {
        let url = format!("{}/api/auth/token", self.base_url);
        let resp = self
            .http
            .post(&url)
            .json(&req)
            .send()
            .await
            .context("connect to atomic-server REST API")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("auth failed ({}): {}", status, body);
        }

        let token_resp: TokenResponse = resp.json().await.context("parse token response")?;
        Ok(token_resp.token)
    }
}
