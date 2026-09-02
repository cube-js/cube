use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::actor::{connect_ws, Actor, ActorRequest};
use crate::error::TransportError;
use crate::result::QueryResult;

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub url: url::Url,
    pub username: Option<String>,
    pub password: Option<String>,
    pub ping_interval: Duration,
    pub no_heartbeat_timeout: Duration,
    pub max_connect_retries: u32,
    pub connect_timeout: Duration,
}

impl ClientConfig {
    pub fn new(url: url::Url) -> Self {
        // Peel credentials out of the URL (e.g. `wss://user:pass@host/path`) so
        // we can put them in the Authorization header and keep the request-URI
        // clean — most WS proxies reject userinfo in the request-line.
        let (url, username, password) = extract_userinfo(url);

        Self {
            url,
            username,
            password,
            ping_interval: Duration::from_secs(5),
            no_heartbeat_timeout: Duration::from_secs(30),
            max_connect_retries: 20,
            connect_timeout: Duration::from_secs(10),
        }
    }

    pub fn with_credentials(mut self, user: impl Into<String>, pass: impl Into<String>) -> Self {
        self.username = Some(user.into());
        self.password = Some(pass.into());
        self
    }
}

fn extract_userinfo(mut url: url::Url) -> (url::Url, Option<String>, Option<String>) {
    if url.username().is_empty() && url.password().is_none() {
        return (url, None, None);
    }

    let user = percent_decode(url.username());
    let pass = url.password().map(percent_decode);

    let _ = url.set_username("");
    let _ = url.set_password(None);

    (url, Some(user), pass)
}

fn percent_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_userinfo_from_url() {
        let url = url::Url::parse(
            "wss://production-6321-9-4:fe84ac54de19e0509b27ba7cab63fc55f7156ddd3707eb5d1c53b6bbb3c2819c@cube-store-aws-eu-west-1.cubecloudapp.dev/staging/3/ws",
        )
        .unwrap();
        let cfg = ClientConfig::new(url);
        assert_eq!(cfg.username.as_deref(), Some("production-6321-9-4"));
        assert_eq!(
            cfg.password.as_deref(),
            Some("fe84ac54de19e0509b27ba7cab63fc55f7156ddd3707eb5d1c53b6bbb3c2819c"),
        );
        // Userinfo stripped, path preserved, scheme intact.
        assert_eq!(cfg.url.username(), "");
        assert!(cfg.url.password().is_none());
        assert_eq!(cfg.url.scheme(), "wss");
        assert_eq!(cfg.url.path(), "/staging/3/ws");
        assert_eq!(
            cfg.url.host_str(),
            Some("cube-store-aws-eu-west-1.cubecloudapp.dev"),
        );
    }

    #[test]
    fn no_userinfo_leaves_url_untouched() {
        let url = url::Url::parse("ws://127.0.0.1:3030/").unwrap();
        let cfg = ClientConfig::new(url.clone());
        assert!(cfg.username.is_none());
        assert!(cfg.password.is_none());
        assert_eq!(cfg.url, url);
    }

    #[test]
    fn percent_encoded_password_is_decoded() {
        let url = url::Url::parse("wss://user:p%40ss@host/path").unwrap();
        let cfg = ClientConfig::new(url);
        assert_eq!(cfg.password.as_deref(), Some("p@ss"));
    }
}

#[derive(Clone)]
pub struct Client {
    tx: mpsc::UnboundedSender<ActorRequest>,
    server_version: Option<String>,
}

impl Client {
    pub async fn connect(cfg: ClientConfig) -> Result<Self, TransportError> {
        // Process id is auto-generated per Client and stays stable across reconnects.
        // Matches the JS driver's `getProcessUid()` semantics.
        let process_id = Uuid::new_v4().to_string();
        let (ws, version) = connect_ws(&cfg, &process_id).await?;
        let connection_id = Uuid::new_v4().to_string();
        let (tx, rx) = mpsc::unbounded_channel();
        let actor = Actor::new(cfg, connection_id, process_id, rx, ws);
        tokio::spawn(actor.run());
        Ok(Self {
            tx,
            server_version: version,
        })
    }

    /// Server version captured from the `X-CubeStore-Version` upgrade response header.
    pub fn server_version(&self) -> Option<&str> {
        self.server_version.as_deref()
    }

    /// Execute a SQL statement against cubestore and return the (Legacy-format) result.
    pub async fn query(&self, sql: impl Into<String>) -> Result<QueryResult, TransportError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorRequest::Query {
                sql: sql.into(),
                reply: reply_tx,
            })
            .map_err(|_| TransportError::Closed)?;
        reply_rx.await.map_err(|_| TransportError::Closed)?
    }

    /// Initiate a graceful shutdown of the background actor.
    pub fn close(&self) {
        let _ = self.tx.send(ActorRequest::Close);
    }
}
