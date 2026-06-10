use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use ragloom::doc::{DocumentLoader, LoadedDocument};
use ragloom::embed::EmbeddingProvider;
use ragloom::error::{RagloomError, RagloomErrorKind};
use ragloom::ids::FileFingerprint;
use ragloom::pipeline::runtime::{
    AckingExecutor, LiveRetryPolicy, PipelineExecutor, RetryPolicy, Runtime, WorkExecutor,
    run_worker, run_worker_with_live_retry_failed_work_and_metrics,
};
use ragloom::sink::{DocumentIdentity, PointId, Sink, VectorPoint};
use ragloom::source::DirectoryScannerSource;
use ragloom::startup::{
    CompactStateConfig, ReplayFailedConfig, compact_state_command, replay_failed_command,
};
use ragloom::state::failed::{
    FailedWorkFailureKind, FailedWorkJournal, FailedWorkRecord, FailedWorkTerminalReason,
    FileFailedWorkStore, pending_failed_work,
};
use ragloom::state::wal::{FileWal, WalRecord, known_live_document_paths, unacked_work_items};
use ragloom::transform::chunker::recursive::RecursiveConfig;
use ragloom::transform::chunker::{RecursiveChunker, SizeMetric};

#[tokio::test]
async fn initial_modify_delete_and_restart_preserve_final_document_state() {
    let env = RecoveryEnv::new();
    let doc = env.write_doc("alpha.txt", "first version");
    let sink = RecordingSink::default();
    let executor = env.executor(sink.clone());

    let initial_work = env.scan_with_restart_boundary();
    assert_eq!(
        initial_work.len(),
        1,
        "initial ingest should plan exactly one file-version work item"
    );
    run_once(&env.wal_path, executor.clone(), initial_work).await;

    env.write_doc("alpha.txt", "second version with changed content");
    let modified_work = env.scan_with_restart_boundary();
    assert_eq!(
        modified_work.len(),
        1,
        "file modification should plan exactly one new file-version work item"
    );
    run_once(&env.wal_path, executor.clone(), modified_work).await;

    fs::remove_file(&doc).expect("delete source document");
    let delete_work = env.scan_with_restart_boundary();
    assert_eq!(
        delete_work,
        vec![WalRecord::DeleteDocument {
            canonical_path: doc.to_string_lossy().to_string(),
        }],
        "offline delete should be durably planned after restart"
    );
    run_once(&env.wal_path, executor, delete_work).await;

    let records = read_wal(&env.wal_path);
    assert!(
        unacked_work_items(&records).is_empty(),
        "final WAL should have no unacked work after ingest, modify, delete, and restart"
    );
    assert!(
        known_live_document_paths(&records).is_empty(),
        "deleted document should not remain in live document projection"
    );

    let ops = sink.operations().await;
    assert_eq!(
        operation_kinds(&ops),
        vec!["upsert", "upsert", "delete"],
        "sink operations should preserve ingest -> modify -> delete order"
    );
    assert!(
        sink.active_point_ids().await.is_empty(),
        "delete acknowledgement should leave no active points for the document"
    );
}

#[tokio::test]
async fn sink_success_before_ack_replays_without_incorrect_duplicate_point_identity() {
    let env = RecoveryEnv::new();
    env.write_doc("alpha.txt", "same durable document");
    let work = env.scan_with_restart_boundary();
    let sink = RecordingSink::default();
    let executor = env.executor(sink.clone());

    executor
        .execute(work[0].clone())
        .await
        .expect("sink write succeeds before ack is persisted");
    assert_eq!(
        unacked_work_items(&read_wal(&env.wal_path)),
        work,
        "work should remain replayable when the sink write wins but WAL ack is missing"
    );

    run_once(&env.wal_path, executor, work).await;

    let upsert_batches = sink.upsert_point_id_batches().await;
    assert_eq!(
        upsert_batches.len(),
        2,
        "crash replay should attempt the upsert twice"
    );
    assert_eq!(
        upsert_batches[0], upsert_batches[1],
        "replayed upsert must use the same deterministic point identity"
    );
    assert_eq!(
        sink.active_point_ids().await.len(),
        upsert_batches[0].len(),
        "idempotent sink view should contain one active point per deterministic chunk despite replay"
    );
    assert!(
        unacked_work_items(&read_wal(&env.wal_path)).is_empty(),
        "replayed work should be acknowledged after the later successful run"
    );
}

#[tokio::test]
async fn retry_exhaustion_replay_failed_and_later_success_acknowledge_work() {
    let env = RecoveryEnv::new();
    env.write_doc("flaky.txt", "transient failure then operator replay");
    let work = env.scan_with_restart_boundary();
    let failed_path = env.failed_path();
    let failed_journal = FailedWorkJournal::new(
        FileFailedWorkStore::open(&failed_path).expect("open failed-work store"),
    );

    let failing_executor = ScriptedExecutor::new(vec![
        Err(RagloomErrorKind::Sink),
        Err(RagloomErrorKind::Sink),
    ]);
    run_with_retry(
        work.clone(),
        failing_executor,
        Some(failed_journal),
        RetryPolicy {
            max_attempts: 2,
            max_queued_retries: 1,
            initial_backoff: std::time::Duration::ZERO,
            max_backoff: std::time::Duration::ZERO,
        },
    )
    .await;

    let failed_records = read_failed_work(&failed_path);
    assert_eq!(
        pending_failed_work(&failed_records).len(),
        1,
        "retry exhaustion should persist one pending failed-work item"
    );
    assert_eq!(
        failed_records,
        vec![FailedWorkRecord::Exhausted {
            id: 1,
            work: work[0].clone(),
            failure_kind: FailedWorkFailureKind::Sink,
            terminal_reason: FailedWorkTerminalReason::RetryExhausted,
            attempts: 2,
        }],
        "failed-work journal should describe the exhausted durable work"
    );

    let replayed = replay_failed_command(&ReplayFailedConfig {
        state_path: env.wal_path.to_string_lossy().to_string(),
    })
    .await
    .expect("replay failed work");
    assert_eq!(replayed, 1, "replay-failed should requeue the pending item");
    assert!(
        pending_failed_work(&read_failed_work(&failed_path)).is_empty(),
        "requeued failed work should no longer be pending"
    );

    let sink = RecordingSink::default();
    run_once(
        &env.wal_path,
        env.executor(sink.clone()),
        unacked_work_items(&read_wal(&env.wal_path)),
    )
    .await;

    assert!(
        unacked_work_items(&read_wal(&env.wal_path)).is_empty(),
        "later successful run should acknowledge replayed failed work"
    );
    assert_eq!(
        sink.active_point_ids().await.len(),
        sink.upsert_point_id_batches().await[0].len(),
        "later successful run should produce the expected sink state"
    );
}

#[tokio::test]
async fn delete_synchronization_survives_restart_before_and_after_ack() {
    let env = RecoveryEnv::new();
    let doc = env.write_doc("delete-me.txt", "delete sync");
    let sink = RecordingSink::default();
    let executor = env.executor(sink.clone());

    let ingest_work = env.scan_with_restart_boundary();
    run_once(&env.wal_path, executor.clone(), ingest_work).await;
    fs::remove_file(&doc).expect("delete source document");

    let pending_delete = env.scan_with_restart_boundary();
    assert_eq!(
        pending_delete,
        vec![WalRecord::DeleteDocument {
            canonical_path: doc.to_string_lossy().to_string(),
        }],
        "restart before delete ack should preserve pending delete work"
    );

    let records_before_ack = read_wal(&env.wal_path);
    assert_eq!(
        unacked_work_items(&records_before_ack),
        pending_delete,
        "delete must remain unacked before sink acknowledgement"
    );
    assert!(
        known_live_document_paths(&records_before_ack).is_empty(),
        "pending delete should remove the document from live projection"
    );

    run_once(&env.wal_path, executor, pending_delete).await;

    let records_after_ack = read_wal(&env.wal_path);
    assert!(
        unacked_work_items(&records_after_ack).is_empty(),
        "restart after delete ack should not replay delete work"
    );
    assert!(
        known_live_document_paths(&records_after_ack).is_empty(),
        "delete ack should keep the document absent from live projection"
    );
    assert_eq!(
        operation_kinds(&sink.operations().await),
        vec!["upsert", "delete"],
        "delete sink operation should be observable exactly once in this run"
    );
}

#[tokio::test]
async fn compacted_state_restart_preserves_pending_work_and_final_sink_operations() {
    let env = RecoveryEnv::new();
    let doc_a = "active.txt";
    let doc_b = "pending.txt";
    let doc_c = "delete.txt";

    let uncompacted_pending = seed_compaction_contract_state(&env, doc_a, doc_b, doc_c).await;
    let compacted_wal_path = env
        .wal_path
        .parent()
        .expect("wal parent")
        .join("wal-compacted.ndjson");
    fs::copy(&env.wal_path, &compacted_wal_path).expect("copy wal before compaction");
    let compacted_pending = unacked_work_items(&read_wal(&compacted_wal_path));
    assert_eq!(
        compacted_pending, uncompacted_pending,
        "test setup should seed equivalent pending work"
    );

    compact_state_command(&CompactStateConfig {
        state_path: compacted_wal_path.to_string_lossy().to_string(),
    })
    .await
    .expect("compact state");

    let uncompacted_records = read_wal(&env.wal_path);
    let compacted_records = read_wal(&compacted_wal_path);
    assert_eq!(
        unacked_work_items(&compacted_records),
        unacked_work_items(&uncompacted_records),
        "compaction should preserve restart replay work"
    );
    assert_eq!(
        known_live_document_paths(&compacted_records),
        known_live_document_paths(&uncompacted_records),
        "compaction should preserve restart source seed state"
    );

    let uncompacted_sink = RecordingSink::default();
    let compacted_sink = RecordingSink::default();
    run_once(
        &env.wal_path,
        env.executor(uncompacted_sink.clone()),
        unacked_work_items(&uncompacted_records),
    )
    .await;
    run_once(
        &compacted_wal_path,
        env.executor(compacted_sink.clone()),
        unacked_work_items(&compacted_records),
    )
    .await;

    assert_eq!(
        uncompacted_sink.operations().await,
        compacted_sink.operations().await,
        "compacted restart should produce the same final sink operations as uncompacted restart"
    );
}

#[tokio::test]
async fn filesystem_and_s3_identities_share_durable_recovery_contracts() {
    let filesystem = FileFingerprint {
        canonical_path: "/docs/a.md".to_string(),
        size_bytes: 64,
        mtime_unix_secs: 1_710_000_000,
        etag: None,
    };
    let s3 = FileFingerprint {
        canonical_path: "s3://docs-bucket/kb/a.md".to_string(),
        size_bytes: 64,
        mtime_unix_secs: 1_710_000_000,
        etag: Some("\"etag-a\"".to_string()),
    };

    for fingerprint in [filesystem, s3] {
        assert_identity_recovery_contract(fingerprint.clone(), "source-specific metadata").await;
    }
}

async fn assert_identity_recovery_contract(fingerprint: FileFingerprint, text: &str) {
    let wal_file = tempfile::NamedTempFile::new().expect("temp wal");
    let wal_path = wal_file.path().to_path_buf();
    let sink = RecordingSink::default();
    let executor = PipelineExecutor::with_chunker(
        Arc::new(DeterministicEmbedding),
        Arc::new(sink.clone()),
        Arc::new(StaticLoader::with_text(&fingerprint.canonical_path, text)),
        Arc::new(one_chunker()),
    );
    let work = WalRecord::WorkItemV2 {
        fingerprint: fingerprint.clone(),
    };

    {
        let mut wal = FileWal::open(&wal_path).expect("open wal");
        wal.append(work.clone()).expect("append work");
    }

    executor
        .execute(work.clone())
        .await
        .expect("first sink write before ack");
    run_once(&wal_path, executor, vec![work.clone()]).await;

    let upsert_batches = sink.upsert_point_id_batches().await;
    assert_eq!(
        upsert_batches.len(),
        2,
        "identity {} should be replayed once before acknowledgement",
        fingerprint.canonical_path
    );
    assert_eq!(
        upsert_batches[0], upsert_batches[1],
        "identity {} should replay with the same point id",
        fingerprint.canonical_path
    );
    assert!(
        unacked_work_items(&read_wal(&wal_path)).is_empty(),
        "identity {} should acknowledge after replay",
        fingerprint.canonical_path
    );
}

async fn seed_compaction_contract_state(
    env: &RecoveryEnv,
    active_name: &str,
    pending_name: &str,
    delete_name: &str,
) -> Vec<WalRecord> {
    let sink = RecordingSink::default();
    let executor = env.executor(sink);

    env.write_doc(active_name, "already acknowledged");
    let active_work = env.scan_with_restart_boundary();
    run_once(&env.wal_path, executor.clone(), active_work).await;

    env.write_doc(pending_name, "pending after restart");
    let pending_work = env.scan_with_restart_boundary();

    let delete_path = env.write_doc(delete_name, "delete after restart");
    let delete_ingest = env.scan_with_restart_boundary();
    run_once(&env.wal_path, executor, delete_ingest).await;
    fs::remove_file(delete_path).expect("delete seeded document");
    let pending_delete = env.scan_with_restart_boundary();

    pending_work
        .into_iter()
        .chain(pending_delete.into_iter())
        .collect()
}

async fn run_once(wal_path: &Path, executor: PipelineExecutor, work_items: Vec<WalRecord>) {
    let wal = Arc::new(tokio::sync::Mutex::new(
        FileWal::open(wal_path).expect("reopen wal for worker"),
    ));
    let executor = AckingExecutor {
        inner: executor,
        wal,
    };
    let (tx, rx) = tokio::sync::mpsc::channel(work_items.len().max(1));
    let worker = tokio::spawn(async move {
        run_worker(rx, executor).await;
    });
    for item in work_items {
        tx.send(item).await.expect("send work item");
    }
    drop(tx);
    worker.await.expect("worker join");
}

async fn run_with_retry(
    work_items: Vec<WalRecord>,
    executor: impl WorkExecutor,
    failed_work: Option<FailedWorkJournal>,
    policy: RetryPolicy,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(work_items.len().max(1));
    let worker = tokio::spawn(async move {
        run_worker_with_live_retry_failed_work_and_metrics(
            rx,
            executor,
            LiveRetryPolicy::new(policy).expect("retry policy"),
            failed_work,
            None,
            None,
        )
        .await;
    });
    for item in work_items {
        tx.send(item).await.expect("send retry work item");
    }
    drop(tx);
    worker.await.expect("retry worker join");
}

fn read_wal(path: &Path) -> Vec<WalRecord> {
    FileWal::open(path)
        .expect("reopen wal")
        .read_all()
        .expect("read wal")
}

fn read_failed_work(path: &Path) -> Vec<FailedWorkRecord> {
    FileFailedWorkStore::open(path)
        .expect("reopen failed-work")
        .read_all()
        .expect("read failed-work")
}

fn operation_kinds(ops: &[SinkOperation]) -> Vec<&'static str> {
    ops.iter()
        .map(|op| match op {
            SinkOperation::Upsert { .. } => "upsert",
            SinkOperation::Delete { .. } => "delete",
        })
        .collect()
}

fn one_chunker() -> RecursiveChunker {
    RecursiveChunker::new(RecursiveConfig {
        metric: SizeMetric::Chars,
        max_size: 2048,
        min_size: 0,
        overlap: 0,
    })
    .expect("one chunk config")
}

struct RecoveryEnv {
    _tmp: tempfile::TempDir,
    docs_dir: PathBuf,
    wal_path: PathBuf,
}

impl RecoveryEnv {
    fn new() -> Self {
        let tmp = tempfile::tempdir().expect("temp dir");
        let docs_dir = tmp.path().join("docs");
        fs::create_dir(&docs_dir).expect("create docs dir");
        let wal_path = tmp.path().join("state").join("wal.ndjson");
        Self {
            _tmp: tmp,
            docs_dir,
            wal_path,
        }
    }

    fn failed_path(&self) -> PathBuf {
        self.wal_path
            .parent()
            .expect("wal parent")
            .join("failed.ndjson")
    }

    fn write_doc(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.docs_dir.join(name);
        let mut file = fs::File::create(&path).expect("create document");
        write!(file, "{contents}").expect("write document");
        path
    }

    fn scan_with_restart_boundary(&self) -> Vec<WalRecord> {
        let records = read_wal(&self.wal_path);
        let seeded_paths = known_live_document_paths(&records);
        let source =
            DirectoryScannerSource::with_previously_observed_paths(&self.docs_dir, seeded_paths)
                .expect("create scanner");
        let wal = Arc::new(tokio::sync::Mutex::new(
            FileWal::open(&self.wal_path).expect("open wal for scan"),
        ));
        let mut runtime = Runtime::with_shared_wal(source, Arc::clone(&wal));

        let before = {
            let guard = wal.try_lock().expect("wal before scan");
            guard.read_all().expect("read wal before scan").len()
        };
        runtime.tick().expect("scan tick");
        let after = {
            let guard = wal.try_lock().expect("wal after scan");
            guard.read_all().expect("read wal after scan")
        };
        after[before..].to_vec()
    }

    fn executor(&self, sink: RecordingSink) -> PipelineExecutor {
        PipelineExecutor::with_chunker(
            Arc::new(DeterministicEmbedding),
            Arc::new(sink),
            Arc::new(FsFixtureLoader),
            Arc::new(one_chunker()),
        )
    }
}

#[derive(Debug, Clone)]
struct FsFixtureLoader;

#[async_trait]
impl DocumentLoader for FsFixtureLoader {
    async fn load(&self, path: &str) -> Result<LoadedDocument, RagloomError> {
        let text = fs::read_to_string(path).map_err(|e| {
            RagloomError::new(RagloomErrorKind::Io, e).with_context("failed to read fixture text")
        })?;
        Ok(LoadedDocument { text })
    }
}

#[derive(Debug, Clone)]
struct StaticLoader {
    documents: Arc<HashMap<String, String>>,
}

impl StaticLoader {
    fn with_text(path: &str, text: &str) -> Self {
        Self {
            documents: Arc::new(HashMap::from([(path.to_string(), text.to_string())])),
        }
    }
}

#[async_trait]
impl DocumentLoader for StaticLoader {
    async fn load(&self, path: &str) -> Result<LoadedDocument, RagloomError> {
        let text = self.documents.get(path).cloned().ok_or_else(|| {
            RagloomError::from_kind(RagloomErrorKind::Io).with_context("missing static document")
        })?;
        Ok(LoadedDocument { text })
    }
}

#[derive(Debug, Clone)]
struct DeterministicEmbedding;

#[async_trait]
impl EmbeddingProvider for DeterministicEmbedding {
    async fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>, RagloomError> {
        Ok(inputs
            .iter()
            .map(|input| {
                vec![
                    input.len() as f32,
                    input.bytes().fold(0u32, |sum, b| sum + b as u32) as f32,
                ]
            })
            .collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SinkOperation {
    Upsert {
        point_ids: Vec<PointId>,
        canonical_paths: Vec<String>,
    },
    Delete {
        canonical_path: String,
        doc_id: String,
    },
}

#[derive(Debug, Default, Clone)]
struct RecordingSink {
    inner: Arc<tokio::sync::Mutex<RecordingSinkState>>,
}

#[derive(Debug, Default)]
struct RecordingSinkState {
    operations: Vec<SinkOperation>,
    active_points: HashMap<PointId, String>,
}

impl RecordingSink {
    async fn operations(&self) -> Vec<SinkOperation> {
        self.inner.lock().await.operations.clone()
    }

    async fn upsert_point_id_batches(&self) -> Vec<Vec<PointId>> {
        self.operations()
            .await
            .into_iter()
            .filter_map(|op| match op {
                SinkOperation::Upsert { point_ids, .. } => Some(point_ids),
                SinkOperation::Delete { .. } => None,
            })
            .collect()
    }

    async fn active_point_ids(&self) -> HashSet<PointId> {
        self.inner
            .lock()
            .await
            .active_points
            .keys()
            .cloned()
            .collect()
    }
}

#[async_trait]
impl Sink for RecordingSink {
    async fn upsert_points(&self, points: Vec<VectorPoint>) -> Result<(), RagloomError> {
        let mut state = self.inner.lock().await;
        let mut point_ids = Vec::new();
        let mut canonical_paths = Vec::new();

        for point in points {
            let canonical_path = point
                .payload
                .get("canonical_path")
                .and_then(|value| value.as_str())
                .expect("point payload canonical_path")
                .to_string();
            state
                .active_points
                .insert(point.id.clone(), canonical_path.clone());
            point_ids.push(point.id);
            canonical_paths.push(canonical_path);
        }

        state.operations.push(SinkOperation::Upsert {
            point_ids,
            canonical_paths,
        });
        Ok(())
    }

    async fn delete_document_points(&self, identity: DocumentIdentity) -> Result<(), RagloomError> {
        let mut state = self.inner.lock().await;
        let canonical_path_uri = canonical_path_to_test_uri(&identity.canonical_path);
        state.active_points.retain(|_, canonical_path| {
            canonical_path != &identity.canonical_path && canonical_path != &canonical_path_uri
        });
        state.operations.push(SinkOperation::Delete {
            canonical_path: identity.canonical_path,
            doc_id: identity.doc_id,
        });
        Ok(())
    }
}

fn canonical_path_to_test_uri(canonical_path: &str) -> String {
    if canonical_path.contains("://") {
        return canonical_path.to_string();
    }

    let normalized = canonical_path.replace('\\', "/");
    if let Some((drive, rest)) = normalized.split_once(":/") {
        return format!("file:///{drive}:/{rest}");
    }
    if normalized.starts_with('/') {
        return format!("file://{normalized}");
    }
    format!("file:///{normalized}")
}

#[derive(Debug, Clone)]
struct ScriptedExecutor {
    outcomes: Arc<std::sync::Mutex<VecDeque<Result<(), RagloomErrorKind>>>>,
}

impl ScriptedExecutor {
    fn new(outcomes: Vec<Result<(), RagloomErrorKind>>) -> Self {
        Self {
            outcomes: Arc::new(std::sync::Mutex::new(VecDeque::from(outcomes))),
        }
    }
}

#[async_trait]
impl WorkExecutor for ScriptedExecutor {
    async fn execute(&self, _record: WalRecord) -> Result<(), RagloomError> {
        match self
            .outcomes
            .lock()
            .expect("lock scripted outcomes")
            .pop_front()
            .unwrap_or(Ok(()))
        {
            Ok(()) => Ok(()),
            Err(kind) => Err(RagloomError::from_kind(kind).with_context("scripted failure")),
        }
    }
}
