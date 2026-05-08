use std::fs;
use std::io::Write;
use std::sync::Arc;

use ragloom::pipeline::runtime::{AckingExecutor, Runtime, WorkExecutor, run_worker};
use ragloom::source::DirectoryScannerSource;
use ragloom::state::wal::{FileWal, WalRecord, known_live_document_paths};
use tempfile::tempdir;

#[tokio::test]
async fn delete_sync_survives_restart_when_reusing_same_wal() {
    let tmp = tempdir().expect("create tempdir");
    let source_root = tmp.path().join("docs");
    fs::create_dir(&source_root).expect("create source root");
    let wal_path = tmp.path().join("ragloom.ndjson");
    let path = source_root.join("a.txt");
    write_text_file(&path, "hello");

    let wal = Arc::new(tokio::sync::Mutex::new(
        FileWal::open(&wal_path).expect("open wal"),
    ));
    let source = DirectoryScannerSource::new(&source_root).expect("create scanner");
    let mut runtime = Runtime::with_shared_wal(source, Arc::clone(&wal));

    runtime.tick().expect("first tick");

    let initial_records = {
        let guard = wal.lock().await;
        guard.read_all().expect("read wal")
    };
    assert_eq!(initial_records.len(), 1);
    let fingerprint = match &initial_records[0] {
        WalRecord::WorkItemV2 { fingerprint } => fingerprint.clone(),
        other => panic!("expected WorkItemV2, got {other:?}"),
    };

    {
        let mut guard = wal.lock().await;
        guard
            .append(WalRecord::SinkAckV2 {
                fingerprint: fingerprint.clone(),
            })
            .expect("append ack");
    }

    fs::remove_file(&path).expect("delete file while offline");

    let seeded_paths = {
        let guard = wal.lock().await;
        let records = guard.read_all().expect("read wal");
        known_live_document_paths(&records)
    };
    let restarted_source =
        DirectoryScannerSource::with_previously_observed_paths(&source_root, seeded_paths)
            .expect("create restarted scanner");
    let mut restarted_runtime = Runtime::with_shared_wal(restarted_source, Arc::clone(&wal));

    restarted_runtime.tick().expect("restart tick");
    restarted_runtime.tick().expect("second restart tick");

    let delete_records = {
        let guard = wal.lock().await;
        guard.read_all().expect("read wal")
    };
    let delete_count = delete_records
        .iter()
        .filter(|record| {
            matches!(
                record,
                WalRecord::DeleteDocument { canonical_path }
                if canonical_path == &path.to_string_lossy()
            )
        })
        .count();
    assert!(
        delete_records.contains(&WalRecord::DeleteDocument {
            canonical_path: path.to_string_lossy().to_string()
        }),
        "expected durable delete work after restart"
    );
    assert_eq!(delete_count, 1, "delete work should not be re-emitted");

    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let executor = AckingExecutor {
        inner: DeleteOnlyExecutor::default(),
        wal: Arc::clone(&wal),
    };
    let worker = tokio::spawn(async move {
        run_worker(rx, executor).await;
    });

    tx.send(WalRecord::DeleteDocument {
        canonical_path: path.to_string_lossy().to_string(),
    })
    .await
    .expect("send delete");
    drop(tx);
    worker.await.expect("worker join");

    let final_records = {
        let guard = wal.lock().await;
        guard.read_all().expect("read wal")
    };
    assert!(
        final_records.contains(&WalRecord::DeleteAck {
            canonical_path: path.to_string_lossy().to_string()
        }),
        "expected delete acknowledgement after worker execution"
    );
}

#[derive(Debug, Default)]
struct DeleteOnlyExecutor {
    seen: Arc<tokio::sync::Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl WorkExecutor for DeleteOnlyExecutor {
    async fn execute(&self, record: WalRecord) -> Result<(), ragloom::RagloomError> {
        if let WalRecord::DeleteDocument { canonical_path } = record {
            self.seen.lock().await.push(canonical_path);
        }
        Ok(())
    }
}

fn write_text_file(path: &std::path::Path, contents: &str) {
    let mut file = fs::File::create(path).expect("create file");
    write!(file, "{contents}").expect("write file");
}
