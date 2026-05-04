//! Minimal daemon health endpoint.
//!
//! # Why
//! Supervisors need a stable, local signal that the daemon is ready without
//! scraping logs or exposing ingestion data.

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::{RagloomError, RagloomErrorKind};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum HealthStatus {
    Starting,
    Ready,
    NotReady,
    ShuttingDown,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthFailureReason {
    StartupFailed,
    RuntimeFailed,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct HealthSnapshot {
    status: HealthStatus,
    reason: Option<HealthFailureReason>,
}

/// Shared daemon readiness state.
#[derive(Debug, Clone)]
pub struct HealthState {
    inner: std::sync::Arc<std::sync::RwLock<HealthSnapshot>>,
}

impl Default for HealthState {
    fn default() -> Self {
        Self::starting()
    }
}

impl HealthState {
    pub fn starting() -> Self {
        Self {
            inner: std::sync::Arc::new(std::sync::RwLock::new(HealthSnapshot {
                status: HealthStatus::Starting,
                reason: None,
            })),
        }
    }

    pub fn mark_ready(&self) {
        self.set(HealthStatus::Ready, None);
    }

    pub fn mark_startup_failed(&self) {
        self.set(
            HealthStatus::NotReady,
            Some(HealthFailureReason::StartupFailed),
        );
    }

    pub fn mark_runtime_failed(&self) {
        self.set(
            HealthStatus::NotReady,
            Some(HealthFailureReason::RuntimeFailed),
        );
    }

    pub fn mark_shutting_down(&self) {
        self.set(HealthStatus::ShuttingDown, None);
    }

    pub fn is_shutting_down(&self) -> bool {
        self.snapshot().status == HealthStatus::ShuttingDown
    }

    pub fn status(&self) -> HealthStatus {
        self.snapshot().status
    }

    pub fn reason(&self) -> Option<HealthFailureReason> {
        self.snapshot().reason
    }

    fn set(&self, status: HealthStatus, reason: Option<HealthFailureReason>) {
        match self.inner.write() {
            Ok(mut snapshot) => {
                *snapshot = HealthSnapshot { status, reason };
            }
            Err(poisoned) => {
                let mut snapshot = poisoned.into_inner();
                *snapshot = HealthSnapshot { status, reason };
            }
        }
    }

    fn snapshot(&self) -> HealthSnapshot {
        match self.inner.read() {
            Ok(snapshot) => *snapshot,
            Err(poisoned) => *poisoned.into_inner(),
        }
    }
}

#[derive(Debug, Serialize)]
struct HealthResponse<'a> {
    status: &'a str,
    ready: bool,
    version: &'a str,
    build: BuildInfo<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<HealthFailureReason>,
}

#[derive(Debug, Serialize)]
struct BuildInfo<'a> {
    package: &'a str,
    version: &'a str,
}

impl HealthResponse<'_> {
    fn from_state(state: &HealthState) -> Self {
        let snapshot = state.snapshot();
        let (status, ready) = match snapshot.status {
            HealthStatus::Starting => ("starting", false),
            HealthStatus::Ready => ("ready", true),
            HealthStatus::NotReady => ("not_ready", false),
            HealthStatus::ShuttingDown => ("shutting_down", false),
        };

        Self {
            status,
            ready,
            version: env!("CARGO_PKG_VERSION"),
            build: BuildInfo {
                package: env!("CARGO_PKG_NAME"),
                version: env!("CARGO_PKG_VERSION"),
            },
            reason: snapshot.reason,
        }
    }
}

#[derive(Debug)]
pub struct HealthServer {
    shutdown: tokio::sync::watch::Sender<bool>,
    join: tokio::task::JoinHandle<()>,
}

impl HealthServer {
    pub async fn bind(addr: &str, state: HealthState) -> Result<Self, RagloomError> {
        let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            RagloomError::new(RagloomErrorKind::Io, e)
                .with_context(format!("failed to bind health endpoint: {addr}"))
        })?;
        Ok(Self::from_listener(listener, state))
    }

    fn from_listener(listener: tokio::net::TcpListener, state: HealthState) -> Self {
        let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(false);

        let join = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((stream, _)) => {
                                let state = state.clone();
                                tokio::spawn(async move {
                                    if let Err(err) = handle_connection(stream, state).await {
                                        tracing::debug!(
                                            event.name = "ragloom.health.connection_failed",
                                            error = %err,
                                            "ragloom.health.connection_failed"
                                        );
                                    }
                                });
                            }
                            Err(err) => {
                                tracing::warn!(
                                    event.name = "ragloom.health.accept_failed",
                                    error = %err,
                                    "ragloom.health.accept_failed"
                                );
                            }
                        }
                    }
                }
            }
        });

        Self { shutdown, join }
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.join.await;
    }
}

async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    state: HealthState,
) -> Result<(), std::io::Error> {
    let mut buf = [0_u8; 1024];
    let mut request = Vec::new();

    loop {
        let read = stream.read(&mut buf).await?;
        if read == 0 {
            return Ok(());
        }
        request.extend_from_slice(&buf[..read]);
        if request.windows(4).any(|w| w == b"\r\n\r\n") || request.len() >= 8192 {
            break;
        }
    }

    let request = String::from_utf8_lossy(&request);
    let request_line = request.lines().next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("");

    let (status_code, reason, body) = match (method, target) {
        ("GET", "/health") => {
            let response = HealthResponse::from_state(&state);
            let status = if response.ready { 200 } else { 503 };
            let reason = if response.ready {
                "OK"
            } else {
                "Service Unavailable"
            };
            (
                status,
                reason,
                serde_json::to_string(&response).expect("health response serializes"),
            )
        }
        ("GET", _) => (404, "Not Found", r#"{"error":"not_found"}"#.to_string()),
        _ => (
            405,
            "Method Not Allowed",
            r#"{"error":"method_not_allowed"}"#.to_string(),
        ),
    };

    let response = format!(
        "HTTP/1.1 {status_code} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn spawn_server(state: HealthState) -> (std::net::SocketAddr, HealthServer) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        (addr, HealthServer::from_listener(listener, state))
    }

    async fn request(addr: std::net::SocketAddr, raw: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
        stream.write_all(raw.as_bytes()).await.expect("write");
        let mut response = String::new();
        stream.read_to_string(&mut response).await.expect("read");
        response
    }

    fn response_body(response: &str) -> serde_json::Value {
        let (_, body) = response.split_once("\r\n\r\n").expect("body");
        serde_json::from_str(body).expect("json")
    }

    fn content_length_matches_body(response: &str) -> bool {
        let (headers, body) = response.split_once("\r\n\r\n").expect("body");
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("content-length") {
                    value.trim().parse::<usize>().ok()
                } else {
                    None
                }
            })
            .expect("content-length");
        content_length == body.len()
    }

    #[tokio::test]
    async fn health_response_reports_ready_status_and_build_info() {
        let state = HealthState::starting();
        state.mark_ready();
        let (addr, server) = spawn_server(state).await;

        let response = request(addr, "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n").await;

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("Content-Type: application/json"));
        assert!(content_length_matches_body(&response));
        let body = response_body(&response);
        assert_eq!(body["ready"], true);
        assert_eq!(body["status"], "ready");
        assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(body["build"]["package"], env!("CARGO_PKG_NAME"));
        assert_eq!(body["build"]["version"], env!("CARGO_PKG_VERSION"));
        assert!(body.get("reason").is_none());

        server.shutdown().await;
    }

    #[tokio::test]
    async fn health_response_returns_503_with_failure_reason_when_not_ready() {
        let state = HealthState::starting();
        state.mark_runtime_failed();
        let (addr, server) = spawn_server(state).await;

        let response = request(addr, "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n").await;

        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
        let body = response_body(&response);
        assert_eq!(body["ready"], false);
        assert_eq!(body["status"], "not_ready");
        assert_eq!(body["reason"], "runtime_failed");

        server.shutdown().await;
    }

    #[tokio::test]
    async fn health_endpoint_rejects_unknown_routes_and_methods() {
        let state = HealthState::starting();
        state.mark_ready();
        let (addr, server) = spawn_server(state).await;

        let missing = request(addr, "GET /missing HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
        assert!(missing.starts_with("HTTP/1.1 404 Not Found"));

        let method = request(addr, "POST /health HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
        assert!(method.starts_with("HTTP/1.1 405 Method Not Allowed"));

        server.shutdown().await;
    }

    #[tokio::test]
    async fn health_server_stops_on_shutdown() {
        let state = HealthState::starting();
        let (addr, server) = spawn_server(state).await;

        server.shutdown().await;

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            tokio::net::TcpStream::connect(addr),
        )
        .await;
        assert!(
            result.is_err() || result.expect("connect result").is_err(),
            "server should stop accepting connections"
        );
    }
}
