//! Qdrant sink implementation.
//!
//! # Why
//! Qdrant supports idempotent upserts keyed by point id. Ragloom leverages this
//! to achieve near exactly-once effects even with at-least-once execution.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{RagloomError, RagloomErrorKind};
use crate::sink::{DocumentIdentity, Sink, VectorPoint};

/// Qdrant HTTP client configuration.
///
/// # Why
/// Configuration is explicit so operators can tune batching and timeouts without
/// code changes.
#[derive(Debug, Clone)]
pub struct QdrantConfig {
    pub base_url: String,
    pub collection: String,
    pub timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct QdrantSink {
    config: QdrantConfig,
    client: reqwest::Client,
}

impl QdrantSink {
    pub fn new(config: QdrantConfig) -> Result<Self, RagloomError> {
        let mut builder = reqwest::Client::builder().timeout(config.timeout);
        if should_bypass_proxy(&config.base_url) {
            builder = builder.no_proxy();
        }

        let client = builder.build().map_err(|e| {
            RagloomError::new(RagloomErrorKind::Sink, e)
                .with_context("failed to build Qdrant HTTP client")
        })?;

        Ok(Self { config, client })
    }

    fn collection_url(&self) -> String {
        format!(
            "{}/collections/{}",
            self.config.base_url.trim_end_matches('/'),
            self.config.collection
        )
    }

    fn upsert_url(&self) -> String {
        format!(
            "{}/collections/{}/points?wait=true",
            self.config.base_url.trim_end_matches('/'),
            self.config.collection
        )
    }

    async fn check_collection_exists(&self, collection_url: &str) -> Result<bool, RagloomError> {
        let response = self.client.get(collection_url).send().await.map_err(|e| {
            RagloomError::new(RagloomErrorKind::Sink, e).with_context(format!(
                "qdrant bootstrap existence-check request failed (url={collection_url})"
            ))
        })?;

        if response.status().is_success() {
            return Ok(true);
        }

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }

        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "<failed to read body>".to_string());
        Err(
            RagloomError::from_kind(RagloomErrorKind::Sink).with_context(format!(
                "qdrant bootstrap existence-check returned unexpected status (url={collection_url}, status={status}, body={body})"
            )),
        )
    }

    pub async fn ensure_collection_exists(&self, vector_size: usize) -> Result<(), RagloomError> {
        let collection_url = self.collection_url();
        if self.check_collection_exists(&collection_url).await? {
            return Ok(());
        }

        let response = self
            .client
            .put(&collection_url)
            .json(&CreateCollectionRequest {
                vectors: CreateCollectionVectors {
                    size: vector_size,
                    distance: "Cosine",
                },
            })
            .send()
            .await
            .map_err(|e| {
                RagloomError::new(RagloomErrorKind::Sink, e).with_context(format!(
                    "qdrant bootstrap create request failed (url={collection_url})"
                ))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<failed to read body>".to_string());
            let create_error =
                RagloomError::from_kind(RagloomErrorKind::Sink).with_context(format!(
                    "qdrant bootstrap create returned non-success status (url={collection_url}, status={status}, body={body})"
                ));

            if let Ok(true) = self.check_collection_exists(&collection_url).await {
                return Ok(());
            }

            return Err(create_error);
        }

        let decoded: QdrantResponse = response.json().await.map_err(|e| {
            RagloomError::new(RagloomErrorKind::Sink, e).with_context(format!(
                "failed to decode qdrant bootstrap create response (url={collection_url})"
            ))
        })?;

        if decoded.status != "ok" {
            return Err(
                RagloomError::from_kind(RagloomErrorKind::Sink).with_context(format!(
                    "qdrant bootstrap create returned non-ok status in body (url={collection_url}, status={})",
                    decoded.status
                )),
            );
        }

        Ok(())
    }

    fn delete_url(&self) -> String {
        format!(
            "{}/collections/{}/points/delete?wait=true",
            self.config.base_url.trim_end_matches('/'),
            self.config.collection
        )
    }
}

fn should_bypass_proxy(base_url: &str) -> bool {
    reqwest::Url::parse(base_url)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .is_some_and(|host| matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1"))
}

#[async_trait::async_trait]
impl Sink for QdrantSink {
    async fn upsert_points(&self, points: Vec<VectorPoint>) -> Result<(), RagloomError> {
        let request = UpsertRequest {
            points: points
                .into_iter()
                .map(|p| QdrantPoint {
                    id: p.id.into_string(),
                    vector: p.vector,
                    payload: p.payload,
                })
                .collect(),
        };

        let response = self
            .client
            .put(self.upsert_url())
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                RagloomError::new(RagloomErrorKind::Sink, e).with_context(format!(
                    "qdrant upsert request failed (url={})",
                    self.upsert_url()
                ))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<failed to read body>".to_string());
            return Err(
                RagloomError::from_kind(RagloomErrorKind::Sink).with_context(format!(
                    "qdrant upsert returned non-success status (url={}, status={}, body={})",
                    self.upsert_url(),
                    status,
                    body
                )),
            );
        }

        let decoded: QdrantResponse = response.json().await.map_err(|e| {
            RagloomError::new(RagloomErrorKind::Sink, e).with_context(format!(
                "failed to decode qdrant response (url={})",
                self.upsert_url()
            ))
        })?;

        if decoded.status != "ok" {
            return Err(
                RagloomError::from_kind(RagloomErrorKind::Sink).with_context(format!(
                    "qdrant returned non-ok status in body (url={}, status={})",
                    self.upsert_url(),
                    decoded.status
                )),
            );
        }

        Ok(())
    }

    async fn delete_document_points(&self, identity: DocumentIdentity) -> Result<(), RagloomError> {
        let delete_url = self.delete_url();
        let request = DeletePointsRequest {
            filter: QdrantFilter {
                must: vec![QdrantMatchCondition {
                    key: "doc_id",
                    r#match: QdrantMatchValue {
                        value: identity.doc_id,
                    },
                }],
            },
        };

        let response = self
            .client
            .post(&delete_url)
            .json(&request)
            .send()
            .await
            .map_err(|e| {
                RagloomError::new(RagloomErrorKind::Sink, e)
                    .with_context(format!("qdrant delete request failed (url={delete_url})"))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<failed to read body>".to_string());
            return Err(
                RagloomError::from_kind(RagloomErrorKind::Sink).with_context(format!(
                    "qdrant delete returned non-success status (url={delete_url}, canonical_path={}, status={}, body={})",
                    identity.canonical_path,
                    status,
                    body
                )),
            );
        }

        let decoded: QdrantResponse = response.json().await.map_err(|e| {
            RagloomError::new(RagloomErrorKind::Sink, e).with_context(format!(
                "failed to decode qdrant delete response (url={delete_url})"
            ))
        })?;

        if decoded.status != "ok" {
            return Err(
                RagloomError::from_kind(RagloomErrorKind::Sink).with_context(format!(
                    "qdrant delete returned non-ok status in body (url={delete_url}, status={})",
                    decoded.status
                )),
            );
        }

        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct UpsertRequest {
    points: Vec<QdrantPoint>,
}

#[derive(Debug, Serialize)]
struct DeletePointsRequest {
    filter: QdrantFilter,
}

#[derive(Debug, Serialize)]
struct QdrantFilter {
    must: Vec<QdrantMatchCondition>,
}

#[derive(Debug, Serialize)]
struct QdrantMatchCondition {
    key: &'static str,
    r#match: QdrantMatchValue,
}

#[derive(Debug, Serialize)]
struct QdrantMatchValue {
    value: String,
}

#[derive(Debug, Serialize)]
struct CreateCollectionRequest<'a> {
    vectors: CreateCollectionVectors<'a>,
}

#[derive(Debug, Serialize)]
struct CreateCollectionVectors<'a> {
    size: usize,
    distance: &'a str,
}

#[derive(Debug, Serialize)]
struct QdrantPoint {
    id: String,
    vector: Vec<f32>,
    payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct QdrantResponse {
    status: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::sink::{PointId, VectorPoint};
    use crate::test_support::{TestHttpResponse, spawn_scripted_http_server};

    fn spawn_test_server(status: u16, body: &'static str) -> String {
        spawn_scripted_http_server(vec![TestHttpResponse::json(status, body)]).base_url()
    }

    fn test_point() -> VectorPoint {
        VectorPoint {
            id: PointId::parse("deadbeef").expect("valid id"),
            vector: vec![1.0, 2.0, 3.0],
            payload: serde_json::json!({"k":"v"}),
        }
    }

    #[test]
    fn bypasses_proxy_for_loopback_qdrant_urls() {
        assert!(should_bypass_proxy("http://127.0.0.1:6333"));
        assert!(should_bypass_proxy("http://localhost:6333"));
        assert!(!should_bypass_proxy("https://qdrant.example.com"));
    }

    #[cfg_attr(miri, ignore = "Miri does not support TCP socket tests")]
    #[tokio::test]
    async fn non_success_status_is_reported_as_error() {
        let base_url = spawn_test_server(500, r#"{"status":"error"}"#);

        let sink = QdrantSink::new(QdrantConfig {
            base_url,
            collection: "docs".to_string(),
            timeout: Duration::from_secs(5),
        })
        .expect("sink");

        let err = sink
            .upsert_points(vec![test_point()])
            .await
            .expect_err("should fail");

        assert_eq!(err.kind, RagloomErrorKind::Sink);
        assert!(err.to_string().contains("non-success"));
    }

    #[cfg_attr(miri, ignore = "Miri does not support TCP socket tests")]
    #[tokio::test]
    async fn ok_body_is_accepted() {
        let base_url = spawn_test_server(200, r#"{"status":"ok"}"#);

        let sink = QdrantSink::new(QdrantConfig {
            base_url,
            collection: "docs".to_string(),
            timeout: Duration::from_secs(5),
        })
        .expect("sink");

        sink.upsert_points(vec![test_point()]).await.expect("ok");
    }

    #[cfg_attr(miri, ignore = "Miri does not support TCP socket tests")]
    #[tokio::test]
    async fn delete_document_points_sends_doc_id_filter() {
        let server =
            spawn_scripted_http_server(vec![TestHttpResponse::json(200, r#"{"status":"ok"}"#)]);
        let base_url = server.base_url();

        let sink = QdrantSink::new(QdrantConfig {
            base_url,
            collection: "docs".to_string(),
            timeout: Duration::from_secs(5),
        })
        .expect("sink");

        sink.delete_document_points(DocumentIdentity {
            canonical_path: "file:///x/a.txt".to_string(),
            doc_id: "doc123".to_string(),
        })
        .await
        .expect("delete");

        let requests = server.join();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with("POST /collections/docs/points/delete?wait=true HTTP/1.1"));
        assert!(requests[0].contains(r#""key":"doc_id""#));
        assert!(requests[0].contains(r#""match":{"value":"doc123"}"#));
    }

    #[cfg_attr(miri, ignore = "Miri does not support TCP socket tests")]
    #[tokio::test]
    async fn non_ok_body_is_reported_as_error() {
        let base_url = spawn_test_server(200, r#"{"status":"error"}"#);

        let sink = QdrantSink::new(QdrantConfig {
            base_url,
            collection: "docs".to_string(),
            timeout: Duration::from_secs(5),
        })
        .expect("sink");

        let err = sink
            .upsert_points(vec![test_point()])
            .await
            .expect_err("should fail");

        assert_eq!(err.kind, RagloomErrorKind::Sink);
        assert!(err.to_string().contains("non-ok"));
    }

    #[cfg_attr(miri, ignore = "Miri does not support TCP socket tests")]
    #[tokio::test]
    async fn ensure_collection_exists_noops_when_collection_already_exists() {
        let server =
            spawn_scripted_http_server(vec![TestHttpResponse::json(200, r#"{"status":"ok"}"#)]);
        let base_url = server.base_url();

        let sink = QdrantSink::new(QdrantConfig {
            base_url,
            collection: "docs".to_string(),
            timeout: Duration::from_secs(5),
        })
        .expect("sink");

        sink.ensure_collection_exists(384).await.expect("ok");

        let requests = server.join();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with("GET /collections/docs HTTP/1.1"));
    }

    #[cfg_attr(miri, ignore = "Miri does not support TCP socket tests")]
    #[tokio::test]
    async fn ensure_collection_exists_creates_missing_collection() {
        let server = spawn_scripted_http_server(vec![
            TestHttpResponse::json(404, r#"{"status":"error"}"#),
            TestHttpResponse::json(200, r#"{"status":"ok"}"#),
        ]);
        let base_url = server.base_url();

        let sink = QdrantSink::new(QdrantConfig {
            base_url,
            collection: "docs".to_string(),
            timeout: Duration::from_secs(5),
        })
        .expect("sink");

        sink.ensure_collection_exists(384).await.expect("created");

        let requests = server.join();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].starts_with("GET /collections/docs HTTP/1.1"));
        assert!(requests[1].starts_with("PUT /collections/docs HTTP/1.1"));
        assert!(requests[1].contains(r#""size":384"#));
        assert!(requests[1].contains(r#""distance":"Cosine""#));
    }

    #[cfg_attr(miri, ignore = "Miri does not support TCP socket tests")]
    #[tokio::test]
    async fn ensure_collection_exists_tolerates_create_race_when_collection_now_exists() {
        let server = spawn_scripted_http_server(vec![
            TestHttpResponse::json(404, r#"{"status":"error"}"#),
            TestHttpResponse::json(409, r#"{"status":"error","result":{"code":"conflict"}}"#),
            TestHttpResponse::json(200, r#"{"status":"ok"}"#),
        ]);
        let base_url = server.base_url();

        let sink = QdrantSink::new(QdrantConfig {
            base_url,
            collection: "docs".to_string(),
            timeout: Duration::from_secs(5),
        })
        .expect("sink");

        sink.ensure_collection_exists(384).await.expect("ok");

        let requests = server.join();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].starts_with("GET /collections/docs HTTP/1.1"));
        assert!(requests[1].starts_with("PUT /collections/docs HTTP/1.1"));
        assert!(requests[2].starts_with("GET /collections/docs HTTP/1.1"));
    }

    #[cfg_attr(miri, ignore = "Miri does not support TCP socket tests")]
    #[tokio::test]
    async fn ensure_collection_exists_surfaces_create_failures_with_bootstrap_context() {
        let server = spawn_scripted_http_server(vec![
            TestHttpResponse::json(404, r#"{"status":"error"}"#),
            TestHttpResponse::json(500, r#"{"status":"error"}"#),
        ]);
        let base_url = server.base_url();

        let sink = QdrantSink::new(QdrantConfig {
            base_url,
            collection: "docs".to_string(),
            timeout: Duration::from_secs(5),
        })
        .expect("sink");

        let err = sink
            .ensure_collection_exists(384)
            .await
            .expect_err("should fail");

        assert_eq!(err.kind, RagloomErrorKind::Sink);
        assert!(err.to_string().contains("bootstrap"));
        assert!(err.to_string().contains("create"));
    }
}
