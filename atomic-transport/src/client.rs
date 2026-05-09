//! Tonic channel wrapper with JWT auth metadata injection.

use anyhow::{Context, Result};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

use crate::proto::atomic_sync_client::AtomicSyncClient;
use crate::to_tonic_endpoint;

/// A connected client to an atomic-server instance.
///
/// Holds the Tonic channel and attaches the bearer token to every request
/// via an interceptor.
#[derive(Clone)]
pub struct AtomicClient {
    pub(crate) inner: AtomicSyncClient<
        tonic::service::interceptor::InterceptedService<Channel, AuthInterceptor>,
    >,
}

/// Tonic interceptor that injects `Authorization: Bearer <token>` into every
/// outgoing gRPC request.
#[derive(Clone)]
pub(crate) struct AuthInterceptor {
    token: Option<MetadataValue<tonic::metadata::Ascii>>,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(
        &mut self,
        mut req: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(ref token) = self.token {
            req.metadata_mut()
                .insert("authorization", token.clone());
        }
        Ok(req)
    }
}

impl AtomicClient {
    /// Connect to `grpc://host:port` or `grpcs://host:port`.
    ///
    /// `token` is an optional JWT obtained via [`crate::auth::TokenClient`].
    pub async fn connect(grpc_url: &str, token: Option<&str>) -> Result<Self> {
        let endpoint_url = to_tonic_endpoint(grpc_url);

        let channel = Channel::from_shared(endpoint_url.clone())
            .context("invalid gRPC endpoint URL")?
            .connect()
            .await
            .with_context(|| format!("connect to {}", endpoint_url))?;

        let auth = AuthInterceptor {
            token: token
                .map(|t| format!("Bearer {}", t).parse())
                .transpose()
                .context("invalid JWT token (non-ASCII characters)")?,
        };

        let inner = AtomicSyncClient::with_interceptor(channel, auth);
        Ok(Self { inner })
    }
}
