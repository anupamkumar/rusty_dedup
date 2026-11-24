use crate::telemetry::{TelemetryRegistry, ThreadRole};
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoAttributes {
    pub size_in_bytes: u64,
    pub runtime_in_seconds: u64,
    pub video_height: u32,
    pub video_width: u32,
    pub video_codec: String,
    pub audio_codec: String,
    #[serde(default)]
    pub video_bitrate_bps: u64,
    #[serde(default)]
    pub audio_bitrate_bps: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoRecord {
    pub path: String,
    pub file_stem: String,
    pub normalized_stem: String,
    #[serde(default)]
    pub detected_creator: Option<String>,
    #[serde(default)]
    pub subject_title: String,
    #[serde(default)]
    pub sequel_markers: Vec<String>,
    pub container_ext: String,
    pub dev_id: u64,
    pub inode: u64,
    pub mtime_secs: i64,
    pub size_in_bytes: u64,
    pub runtime_exact_secs: f64,
    pub runtime_in_seconds: u64,
    pub video_height: u32,
    pub video_width: u32,
    pub video_codec: String,
    pub audio_codec: String,
    #[serde(default)]
    pub video_bitrate_bps: u64,
    #[serde(default)]
    pub audio_bitrate_bps: u64,
    #[serde(default)]
    pub bitrate_probed: bool,
    /// Optional 192-KB sample BLAKE3 hash (computed when multiple files share identical byte size)
    pub blake3_sample_hash: Option<String>,
    /// 5 sparse keyframe 64-bit dHashes at 10%, 30%, 50%, 70%, 90% of runtime
    pub sparse_hashes: Vec<u64>,
    /// First 60s 1-fps 64-bit dHashes (stored in SQLite BLOB + bounded LRU cache at runtime!)
    #[serde(default)]
    pub prefix_1fps_hashes: Option<Vec<u64>>,
    /// Full 1-fps 64-bit dHashes (stored in SQLite BLOB + bounded LRU cache at runtime!)
    pub dense_1fps_hashes: Option<Vec<u64>>,
}

impl VideoRecord {
    pub fn file_key(&self) -> String {
        make_file_key(&self.path, self.size_in_bytes, self.mtime_secs)
    }

    /// Strips heavy 1-fps hash vectors after loading/migrating so 300K records stay ~75 MB in RAM.
    pub fn strip_heavy_hashes(&mut self) {
        self.prefix_1fps_hashes = None;
        self.dense_1fps_hashes = None;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PairCategory {
    Strong,
    Weak,
    PotentiallyDistinct,
}

impl std::fmt::Display for PairCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PairCategory::Strong => write!(f, "Strong chance of duplicate pair"),
            PairCategory::Weak => write!(f, "Weak chance of duplicate pair"),
            PairCategory::PotentiallyDistinct => write!(f, "Potentially distinct file pair"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidatePair {
    pub idx1: usize,
    pub idx2: usize,
    pub category: PairCategory,
    pub name_similarity: f64,
    pub size_diff_ratio: f64,
    pub runtime_diff_secs: f64,
    pub conflicting_creators: Option<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairEvaluationLog {
    pub file1: String,
    pub file2: String,
    pub initial_category: String,
    pub final_outcome: String,
    pub confidence: f64,
    pub matched_frames: usize,
    pub compared_runtime_secs: u64,
    pub best_offset_secs: i64,
    pub detection_method: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateGroupOutput {
    pub dup_group_name: String,
    pub files: Vec<String>,
    pub attributes: BTreeMap<String, VideoAttributes>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateReportOutput {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_directories: Vec<String>,
    pub duplicates: Vec<DuplicateGroupOutput>,
}

/// Legacy JSON session state struct (used strictly for one-time automatic migration into SQLite3 WAL).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacyJsonSessionState {
    pub session_id: String,
    pub canonical_input_directories: Vec<String>,
    pub last_updated_epoch_secs: u64,
    pub file_records: BTreeMap<String, VideoRecord>,
    pub pair_scores: BTreeMap<String, PairEvaluationLog>,
}

pub fn make_file_key(path: &str, size: u64, mtime: i64) -> String {
    format!("{}|{}|{}", path, size, mtime)
}

pub fn make_pair_key(file_key_a: &str, file_key_b: &str) -> String {
    if file_key_a <= file_key_b {
        format!("{}||{}", file_key_a, file_key_b)
    } else {
        format!("{}||{}", file_key_b, file_key_a)
    }
}

pub fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Encodes a slice of 64-bit dHashes as compact little-endian raw bytes (8 bytes/hash).
pub fn encode_u64_blob(hashes: &[u64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(hashes.len() * 8);
    for &h in hashes {
        out.extend_from_slice(&h.to_le_bytes());
    }
    out
}

/// Decodes a compact little-endian byte BLOB back into `Vec<u64>`.
pub fn decode_u64_blob(bytes: &[u8]) -> Vec<u64> {
    let count = bytes.len() / 8;
    let mut out = Vec::with_capacity(count);
    for chunk in bytes.chunks_exact(8) {
        out.push(u64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]));
    }
    out
}

fn configure_wal_connection(conn: &Connection) -> Result<()> {
    conn.busy_timeout(Duration::from_secs(30))?;
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA temp_store = MEMORY;
         PRAGMA cache_size = -32000;",
    )?;
    Ok(())
}

fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS session_meta (
             key TEXT PRIMARY KEY,
             value TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS file_records (
             file_key TEXT PRIMARY KEY,
             path TEXT NOT NULL,
             file_stem TEXT NOT NULL,
             normalized_stem TEXT NOT NULL,
             detected_creator TEXT,
             subject_title TEXT NOT NULL,
             sequel_markers_json TEXT NOT NULL,
             container_ext TEXT NOT NULL,
             dev_id INTEGER NOT NULL,
             inode INTEGER NOT NULL,
             mtime_secs INTEGER NOT NULL,
             size_in_bytes INTEGER NOT NULL,
             runtime_exact_secs REAL NOT NULL,
             runtime_in_seconds INTEGER NOT NULL,
             video_height INTEGER NOT NULL,
             video_width INTEGER NOT NULL,
             video_codec TEXT NOT NULL,
             audio_codec TEXT NOT NULL,
             video_bitrate_bps INTEGER NOT NULL DEFAULT 0,
             audio_bitrate_bps INTEGER NOT NULL DEFAULT 0,
             bitrate_probed INTEGER NOT NULL DEFAULT 0,
             blake3_sample_hash TEXT,
             sparse_hashes BLOB NOT NULL,
             prefix_1fps_hashes BLOB,
             dense_1fps_hashes BLOB
         );
         CREATE TABLE IF NOT EXISTS pair_scores (
             pair_key TEXT PRIMARY KEY,
             file1 TEXT NOT NULL,
             file2 TEXT NOT NULL,
             initial_category TEXT NOT NULL,
             final_outcome TEXT NOT NULL,
             confidence REAL NOT NULL,
             matched_frames INTEGER NOT NULL,
             compared_runtime_secs INTEGER NOT NULL,
             best_offset_secs INTEGER NOT NULL,
             detection_method TEXT NOT NULL
         );",
    )?;

    // Idempotent online schema migration for pre-existing SQLite3 WAL databases
    let mut existing_cols = std::collections::HashSet::new();
    {
        let mut stmt = conn.prepare("PRAGMA table_info(file_records)")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let col_name: String = row.get(1)?;
            existing_cols.insert(col_name);
        }
    }
    if !existing_cols.contains("video_bitrate_bps") {
        conn.execute(
            "ALTER TABLE file_records ADD COLUMN video_bitrate_bps INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    if !existing_cols.contains("audio_bitrate_bps") {
        conn.execute(
            "ALTER TABLE file_records ADD COLUMN audio_bitrate_bps INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    if !existing_cols.contains("bitrate_probed") {
        conn.execute(
            "ALTER TABLE file_records ADD COLUMN bitrate_probed INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }

    Ok(())
}

/// SQLite3 WAL session store engineered for 300,000+ video files (~80 TB libraries).
pub struct SessionDb {
    db_path: PathBuf,
    conn: Connection,
}

impl SessionDb {
    pub fn open_or_create(
        db_path: &Path,
        session_id: &str,
        canonical_dirs: &[String],
        reset_session: bool,
    ) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create states directory {:?}", parent))?;
        }
        if reset_session {
            let _ = fs::remove_file(db_path);
            let _ = fs::remove_file(format!("{}-wal", db_path.display()));
            let _ = fs::remove_file(format!("{}-shm", db_path.display()));
        }

        let conn = Connection::open(db_path)
            .with_context(|| format!("Failed to open SQLite3 session DB {:?}", db_path))?;
        configure_wal_connection(&conn)?;
        init_schema(&conn)?;

        let dirs_json = serde_json::to_string(canonical_dirs)?;
        conn.execute(
            "INSERT OR REPLACE INTO session_meta (key, value) VALUES ('session_id', ?1)",
            params![session_id],
        )?;
        conn.execute(
            "INSERT OR REPLACE INTO session_meta (key, value) VALUES ('canonical_input_directories', ?1)",
            params![dirs_json],
        )?;
        conn.execute(
            "INSERT OR REPLACE INTO session_meta (key, value) VALUES ('last_updated_epoch_secs', ?1)",
            params![current_epoch_secs().to_string()],
        )?;

        Ok(Self {
            db_path: db_path.to_path_buf(),
            conn,
        })
    }

    pub fn open_readonly_handle(&self) -> Result<Connection> {
        let conn = Connection::open(&self.db_path)
            .with_context(|| format!("Failed to open SQLite3 reader handle {:?}", self.db_path))?;
        configure_wal_connection(&conn)?;
        Ok(conn)
    }

    pub fn count_stats(&self) -> Result<(usize, usize)> {
        let files: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM file_records", [], |r| r.get(0))?;
        let pairs: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM pair_scores", [], |r| r.get(0))?;
        Ok((files as usize, pairs as usize))
    }

    /// One-time automatic migration if an older `.states/.rusty_temp_{sessionid}` JSON state exists
    /// and the new SQLite3 DB currently has 0 rows.
    pub fn maybe_migrate_legacy_json(
        &mut self,
        legacy_json_path: &Path,
    ) -> Result<Option<(usize, usize)>> {
        if !legacy_json_path.exists() {
            return Ok(None);
        }
        let (existing_files, existing_pairs) = self.count_stats()?;
        if existing_files > 0 || existing_pairs > 0 {
            return Ok(None);
        }

        let mut file = File::open(legacy_json_path)?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        let parsed: LegacyJsonSessionState = match serde_json::from_str(&buf) {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };

        let tx = self.conn.transaction()?;
        for (file_key, rec) in &parsed.file_records {
            upsert_file_record_tx(&tx, file_key, rec)?;
        }
        for (pair_key, log) in &parsed.pair_scores {
            upsert_pair_score_tx(&tx, pair_key, log)?;
        }
        tx.commit()?;

        Ok(Some((parsed.file_records.len(), parsed.pair_scores.len())))
    }

    /// Loads compact `VideoRecord` (metadata + 5 sparse hashes; heavy 1-fps hashes stay on disk in BLOBs).
    pub fn get_compact_file_record(
        conn: &Connection,
        file_key: &str,
    ) -> Result<Option<VideoRecord>> {
        conn.query_row(
            "SELECT path, file_stem, normalized_stem, detected_creator, subject_title,
                    sequel_markers_json, container_ext, dev_id, inode, mtime_secs,
                    size_in_bytes, runtime_exact_secs, runtime_in_seconds, video_height,
                    video_width, video_codec, audio_codec, video_bitrate_bps, audio_bitrate_bps,
                    bitrate_probed, blake3_sample_hash, sparse_hashes
             FROM file_records WHERE file_key = ?1",
            params![file_key],
            |row| {
                let sequel_json: String = row.get(5)?;
                let sequel_markers: Vec<String> =
                    serde_json::from_str(&sequel_json).unwrap_or_default();
                let sparse_blob: Vec<u8> = row.get(21)?;
                Ok(VideoRecord {
                    path: row.get(0)?,
                    file_stem: row.get(1)?,
                    normalized_stem: row.get(2)?,
                    detected_creator: row.get(3)?,
                    subject_title: row.get(4)?,
                    sequel_markers,
                    container_ext: row.get(6)?,
                    dev_id: row.get::<_, i64>(7)? as u64,
                    inode: row.get::<_, i64>(8)? as u64,
                    mtime_secs: row.get(9)?,
                    size_in_bytes: row.get::<_, i64>(10)? as u64,
                    runtime_exact_secs: row.get(11)?,
                    runtime_in_seconds: row.get::<_, i64>(12)? as u64,
                    video_height: row.get::<_, i64>(13)? as u32,
                    video_width: row.get::<_, i64>(14)? as u32,
                    video_codec: row.get(15)?,
                    audio_codec: row.get(16)?,
                    video_bitrate_bps: row.get::<_, i64>(17)?.max(0) as u64,
                    audio_bitrate_bps: row.get::<_, i64>(18)?.max(0) as u64,
                    bitrate_probed: row.get::<_, i64>(19)? != 0,
                    blake3_sample_hash: row.get(20)?,
                    sparse_hashes: decode_u64_blob(&sparse_blob),
                    prefix_1fps_hashes: None,
                    dense_1fps_hashes: None,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn load_prefix_hashes(&self, file_key: &str) -> Result<Option<Vec<u64>>> {
        let blob: Option<Option<Vec<u8>>> = self
            .conn
            .query_row(
                "SELECT prefix_1fps_hashes FROM file_records WHERE file_key = ?1",
                params![file_key],
                |r| r.get(0),
            )
            .optional()?;
        Ok(blob.flatten().map(|b| decode_u64_blob(&b)))
    }

    pub fn load_dense_hashes(&self, file_key: &str) -> Result<Option<Vec<u64>>> {
        let blob: Option<Option<Vec<u8>>> = self
            .conn
            .query_row(
                "SELECT dense_1fps_hashes FROM file_records WHERE file_key = ?1",
                params![file_key],
                |r| r.get(0),
            )
            .optional()?;
        Ok(blob.flatten().map(|b| decode_u64_blob(&b)))
    }

    pub fn get_pair_score(&self, pair_key: &str) -> Result<Option<PairEvaluationLog>> {
        self.conn
            .query_row(
                "SELECT file1, file2, initial_category, final_outcome, confidence,
                        matched_frames, compared_runtime_secs, best_offset_secs, detection_method
                 FROM pair_scores WHERE pair_key = ?1",
                params![pair_key],
                |r| {
                    Ok(PairEvaluationLog {
                        file1: r.get(0)?,
                        file2: r.get(1)?,
                        initial_category: r.get(2)?,
                        final_outcome: r.get(3)?,
                        confidence: r.get(4)?,
                        matched_frames: r.get::<_, i64>(5)? as usize,
                        compared_runtime_secs: r.get::<_, i64>(6)? as u64,
                        best_offset_secs: r.get(7)?,
                        detection_method: r.get(8)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    /// Streams all rows from `pair_scores` directly to a JSON array file using constant RAM
    /// (avoids holding millions of `PairEvaluationLog` structs in memory on 300K-file scans).
    pub fn stream_categories_log_json(&self, out_path: &Path) -> Result<usize> {
        let file = File::create(out_path)
            .with_context(|| format!("Failed to create categories log {:?}", out_path))?;
        let mut writer = BufWriter::new(file);
        writer.write_all(b"[\n")?;

        let mut stmt = self.conn.prepare(
            "SELECT file1, file2, initial_category, final_outcome, confidence,
                    matched_frames, compared_runtime_secs, best_offset_secs, detection_method
             FROM pair_scores ORDER BY pair_key ASC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(PairEvaluationLog {
                file1: r.get(0)?,
                file2: r.get(1)?,
                initial_category: r.get(2)?,
                final_outcome: r.get(3)?,
                confidence: r.get(4)?,
                matched_frames: r.get::<_, i64>(5)? as usize,
                compared_runtime_secs: r.get::<_, i64>(6)? as u64,
                best_offset_secs: r.get(7)?,
                detection_method: r.get(8)?,
            })
        })?;

        let mut count = 0usize;
        for row in rows {
            let entry = row?;
            if count > 0 {
                writer.write_all(b",\n")?;
            }
            serde_json::to_writer_pretty(&mut writer, &entry)?;
            count += 1;
        }
        writer.write_all(b"\n]\n")?;
        writer.flush()?;
        Ok(count)
    }
}

fn upsert_file_record_tx(conn: &Connection, file_key: &str, rec: &VideoRecord) -> Result<()> {
    let sequel_json = serde_json::to_string(&rec.sequel_markers)?;
    let sparse_blob = encode_u64_blob(&rec.sparse_hashes);
    let prefix_blob = rec.prefix_1fps_hashes.as_deref().map(encode_u64_blob);
    let dense_blob = rec.dense_1fps_hashes.as_deref().map(encode_u64_blob);

    conn.execute(
        "INSERT INTO file_records (
            file_key, path, file_stem, normalized_stem, detected_creator, subject_title,
            sequel_markers_json, container_ext, dev_id, inode, mtime_secs, size_in_bytes,
            runtime_exact_secs, runtime_in_seconds, video_height, video_width, video_codec,
            audio_codec, video_bitrate_bps, audio_bitrate_bps, bitrate_probed,
            blake3_sample_hash, sparse_hashes, prefix_1fps_hashes, dense_1fps_hashes
         ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6,
            ?7, ?8, ?9, ?10, ?11, ?12,
            ?13, ?14, ?15, ?16, ?17,
            ?18, ?19, ?20, ?21,
            ?22, ?23, ?24, ?25
         )
         ON CONFLICT(file_key) DO UPDATE SET
            detected_creator = excluded.detected_creator,
            subject_title = excluded.subject_title,
            sequel_markers_json = excluded.sequel_markers_json,
            video_bitrate_bps = CASE WHEN excluded.video_bitrate_bps > 0 THEN excluded.video_bitrate_bps ELSE file_records.video_bitrate_bps END,
            audio_bitrate_bps = CASE WHEN excluded.audio_bitrate_bps > 0 THEN excluded.audio_bitrate_bps ELSE file_records.audio_bitrate_bps END,
            bitrate_probed = MAX(file_records.bitrate_probed, excluded.bitrate_probed),
            blake3_sample_hash = COALESCE(excluded.blake3_sample_hash, file_records.blake3_sample_hash),
            sparse_hashes = CASE WHEN length(excluded.sparse_hashes) > 0 THEN excluded.sparse_hashes ELSE file_records.sparse_hashes END,
            prefix_1fps_hashes = COALESCE(excluded.prefix_1fps_hashes, file_records.prefix_1fps_hashes),
            dense_1fps_hashes = COALESCE(excluded.dense_1fps_hashes, file_records.dense_1fps_hashes)",
        params![
            file_key,
            rec.path,
            rec.file_stem,
            rec.normalized_stem,
            rec.detected_creator,
            rec.subject_title,
            sequel_json,
            rec.container_ext,
            rec.dev_id as i64,
            rec.inode as i64,
            rec.mtime_secs,
            rec.size_in_bytes as i64,
            rec.runtime_exact_secs,
            rec.runtime_in_seconds as i64,
            rec.video_height as i64,
            rec.video_width as i64,
            rec.video_codec,
            rec.audio_codec,
            rec.video_bitrate_bps as i64,
            rec.audio_bitrate_bps as i64,
            if rec.bitrate_probed { 1i64 } else { 0i64 },
            rec.blake3_sample_hash,
            sparse_blob,
            prefix_blob,
            dense_blob,
        ],
    )?;
    Ok(())
}

fn upsert_pair_score_tx(conn: &Connection, pair_key: &str, log: &PairEvaluationLog) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO pair_scores (
            pair_key, file1, file2, initial_category, final_outcome, confidence,
            matched_frames, compared_runtime_secs, best_offset_secs, detection_method
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            pair_key,
            log.file1,
            log.file2,
            log.initial_category,
            log.final_outcome,
            log.confidence,
            log.matched_frames as i64,
            log.compared_runtime_secs as i64,
            log.best_offset_secs,
            log.detection_method,
        ],
    )?;
    Ok(())
}

/// Incremental write operations sent by worker threads to the dedicated Async SQLite3 WAL Writer Thread.
pub enum DbWriteOp {
    UpsertFileRecord {
        file_key: String,
        record: VideoRecord,
    },
    UpdateBitrates {
        file_key: String,
        video_bitrate_bps: u64,
        audio_bitrate_bps: u64,
    },
    UpdateSparseHashes {
        file_key: String,
        hashes: Vec<u64>,
    },
    UpdatePrefixHashes {
        file_key: String,
        hashes: Vec<u64>,
    },
    UpdateDenseHashes {
        file_key: String,
        hashes: Vec<u64>,
    },
    UpsertPairScore {
        pair_key: String,
        log: PairEvaluationLog,
    },
    Flush(Sender<()>),
    Shutdown,
}

/// Dedicated background SQLite3 WAL writer thread batching mutations into fast atomic transactions
/// (`< 2 ms` per batch, zero full-file rewrites even at 300,000 files).
pub struct AsyncDbWriter {
    tx: Sender<DbWriteOp>,
    handle: Option<JoinHandle<()>>,
}

impl AsyncDbWriter {
    pub fn spawn(
        db_path: PathBuf,
        checkpoint_interval_ms: u64,
        verbose: bool,
        telemetry: Arc<TelemetryRegistry>,
    ) -> Result<Self> {
        let (tx, rx): (Sender<DbWriteOp>, Receiver<DbWriteOp>) = mpsc::channel();
        let flush_every = Duration::from_millis(checkpoint_interval_ms.max(100));

        let handle = thread::Builder::new()
            .name("sqlite-wal-writer".to_string())
            .spawn(move || {
                let mut conn = match Connection::open(&db_path) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!(
                            "Error: SQLite3 WAL writer thread failed to open {:?}: {}",
                            db_path, e
                        );
                        telemetry.record_thread_error(ThreadRole::SqliteWalWriter);
                        return;
                    }
                };
                if let Err(e) = configure_wal_connection(&conn) {
                    eprintln!("Warning: SQLite3 WAL pragma setup failed: {}", e);
                }

                let mut pending: Vec<DbWriteOp> = Vec::with_capacity(512);
                let mut last_flush = Instant::now();
                let mut total_committed: usize = 0;

                loop {
                    let wait_start = Instant::now();
                    let recv_res = rx.recv_timeout(Duration::from_millis(100));
                    telemetry.record_thread_starved(ThreadRole::SqliteWalWriter, wait_start.elapsed());

                    match recv_res {
                        Ok(DbWriteOp::Shutdown) => {
                            if !pending.is_empty() {
                                let t0 = Instant::now();
                                let n = Self::commit_batch(&mut conn, &mut pending);
                                let dt = t0.elapsed();
                                telemetry.sqlite_pending_sub(n);
                                telemetry.record_thread_busy(ThreadRole::SqliteWalWriter, dt);
                                telemetry.record_thread_job(ThreadRole::SqliteWalWriter, dt);
                                telemetry.record_sqlite_commit(n, dt);
                                total_committed += n;
                            }
                            let cp_t0 = Instant::now();
                            let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
                            telemetry.record_thread_busy(ThreadRole::SqliteWalWriter, cp_t0.elapsed());
                            if verbose {
                                eprintln!(
                                    "  [Verbose][SQLite3 WAL] Clean shutdown after committing {} total mutation(s) to {:?}.",
                                    total_committed, db_path
                                );
                            }
                            break;
                        }
                        Ok(DbWriteOp::Flush(ack)) => {
                            if !pending.is_empty() {
                                let t0 = Instant::now();
                                let n = Self::commit_batch(&mut conn, &mut pending);
                                let dt = t0.elapsed();
                                telemetry.sqlite_pending_sub(n);
                                telemetry.record_thread_busy(ThreadRole::SqliteWalWriter, dt);
                                telemetry.record_thread_job(ThreadRole::SqliteWalWriter, dt);
                                telemetry.record_sqlite_commit(n, dt);
                                total_committed += n;
                                last_flush = Instant::now();
                            }
                            let _ = ack.send(());
                        }
                        Ok(op) => {
                            pending.push(op);
                            telemetry.sqlite_pending_inc();
                            if pending.len() >= 500 || last_flush.elapsed() >= flush_every {
                                let t0 = Instant::now();
                                let n = Self::commit_batch(&mut conn, &mut pending);
                                let dt = t0.elapsed();
                                telemetry.sqlite_pending_sub(n);
                                telemetry.record_thread_busy(ThreadRole::SqliteWalWriter, dt);
                                telemetry.record_thread_job(ThreadRole::SqliteWalWriter, dt);
                                telemetry.record_sqlite_commit(n, dt);
                                total_committed += n;
                                last_flush = Instant::now();
                                if verbose && n > 0 {
                                    eprintln!(
                                        "  [Verbose][SQLite3 WAL] Committed batch of {} mutation(s) (total: {})",
                                        n, total_committed
                                    );
                                }
                            }
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            if !pending.is_empty() && last_flush.elapsed() >= flush_every {
                                let t0 = Instant::now();
                                let n = Self::commit_batch(&mut conn, &mut pending);
                                let dt = t0.elapsed();
                                telemetry.sqlite_pending_sub(n);
                                telemetry.record_thread_busy(ThreadRole::SqliteWalWriter, dt);
                                telemetry.record_thread_job(ThreadRole::SqliteWalWriter, dt);
                                telemetry.record_sqlite_commit(n, dt);
                                total_committed += n;
                                last_flush = Instant::now();
                                if verbose && n > 0 {
                                    eprintln!(
                                        "  [Verbose][SQLite3 WAL] Periodic flush committed {} mutation(s) (total: {})",
                                        n, total_committed
                                    );
                                }
                            }
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            if !pending.is_empty() {
                                let t0 = Instant::now();
                                let n = Self::commit_batch(&mut conn, &mut pending);
                                let dt = t0.elapsed();
                                telemetry.sqlite_pending_sub(n);
                                telemetry.record_thread_busy(ThreadRole::SqliteWalWriter, dt);
                                telemetry.record_thread_job(ThreadRole::SqliteWalWriter, dt);
                                telemetry.record_sqlite_commit(n, dt);
                            }
                            let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
                            break;
                        }
                    }
                }
            })
            .context("Failed to spawn SQLite3 WAL writer thread")?;

        Ok(Self {
            tx,
            handle: Some(handle),
        })
    }

    fn commit_batch(conn: &mut Connection, pending: &mut Vec<DbWriteOp>) -> usize {
        let count = pending.len();
        if count == 0 {
            return 0;
        }
        let tx = match conn.transaction() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("Warning: Failed to begin SQLite3 WAL transaction: {}", e);
                pending.clear();
                return 0;
            }
        };

        for op in pending.drain(..) {
            let res = match op {
                DbWriteOp::UpsertFileRecord { file_key, record } => {
                    upsert_file_record_tx(&tx, &file_key, &record)
                }
                DbWriteOp::UpdateBitrates {
                    file_key,
                    video_bitrate_bps,
                    audio_bitrate_bps,
                } => tx
                    .execute(
                        "UPDATE file_records
                         SET video_bitrate_bps = ?1, audio_bitrate_bps = ?2, bitrate_probed = 1
                         WHERE file_key = ?3",
                        params![
                            video_bitrate_bps as i64,
                            audio_bitrate_bps as i64,
                            file_key
                        ],
                    )
                    .map(|_| ())
                    .map_err(Into::into),
                DbWriteOp::UpdateSparseHashes { file_key, hashes } => {
                    let blob = encode_u64_blob(&hashes);
                    tx.execute(
                        "UPDATE file_records SET sparse_hashes = ?1 WHERE file_key = ?2",
                        params![blob, file_key],
                    )
                    .map(|_| ())
                    .map_err(Into::into)
                }
                DbWriteOp::UpdatePrefixHashes { file_key, hashes } => {
                    let blob = encode_u64_blob(&hashes);
                    tx.execute(
                        "UPDATE file_records SET prefix_1fps_hashes = ?1 WHERE file_key = ?2",
                        params![blob, file_key],
                    )
                    .map(|_| ())
                    .map_err(Into::into)
                }
                DbWriteOp::UpdateDenseHashes { file_key, hashes } => {
                    let blob = encode_u64_blob(&hashes);
                    tx.execute(
                        "UPDATE file_records SET dense_1fps_hashes = ?1 WHERE file_key = ?2",
                        params![blob, file_key],
                    )
                    .map(|_| ())
                    .map_err(Into::into)
                }
                DbWriteOp::UpsertPairScore { pair_key, log } => {
                    upsert_pair_score_tx(&tx, &pair_key, &log)
                }
                DbWriteOp::Flush(_) | DbWriteOp::Shutdown => Ok(()),
            };
            if let Err(e) = res {
                eprintln!("Warning: SQLite3 WAL mutation failed: {}", e);
            }
        }

        let _ = tx.execute(
            "INSERT OR REPLACE INTO session_meta (key, value) VALUES ('last_updated_epoch_secs', ?1)",
            params![current_epoch_secs().to_string()],
        );

        if let Err(e) = tx.commit() {
            eprintln!("Warning: SQLite3 WAL commit failed: {}", e);
            return 0;
        }
        count
    }

    pub fn sender(&self) -> Sender<DbWriteOp> {
        self.tx.clone()
    }

    pub fn submit(&self, op: DbWriteOp) {
        let _ = self.tx.send(op);
    }

    /// Blocks until all previously submitted mutations are durably committed in SQLite3 WAL.
    pub fn flush(&self) {
        let (ack_tx, ack_rx) = mpsc::channel();
        if self.tx.send(DbWriteOp::Flush(ack_tx)).is_ok() {
            let _ = ack_rx.recv();
        }
    }

    pub fn shutdown(mut self) {
        let _ = self.tx.send(DbWriteOp::Shutdown);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Bounded in-memory cache for 1-fps hash vectors backed by SQLite3 BLOB storage.
/// Keeps RAM flat (~50–100 MB) even across 300,000+ video files.
pub struct BoundedHashCache {
    max_entries: usize,
    map: HashMap<String, Vec<u64>>,
    order: VecDeque<String>,
    telemetry: Option<(Arc<TelemetryRegistry>, bool)>,
}

impl BoundedHashCache {
    #[cfg(test)]
    pub fn new(max_entries: usize) -> Self {
        Self {
            max_entries: max_entries.max(16),
            map: HashMap::new(),
            order: VecDeque::new(),
            telemetry: None,
        }
    }

    pub fn with_telemetry(
        max_entries: usize,
        telemetry: Arc<TelemetryRegistry>,
        is_dense: bool,
    ) -> Self {
        Self {
            max_entries: max_entries.max(16),
            map: HashMap::new(),
            order: VecDeque::new(),
            telemetry: Some((telemetry, is_dense)),
        }
    }

    fn record_cache(&self, f: impl FnOnce(&crate::telemetry::CacheMetrics)) {
        if let Some((ref reg, is_dense)) = self.telemetry {
            let m = if is_dense {
                &reg.dense_lru
            } else {
                &reg.prefix_lru
            };
            f(m);
        }
    }

    pub fn insert(&mut self, key: String, hashes: Vec<u64>) {
        if self.map.contains_key(&key) {
            self.map.insert(key, hashes);
            return;
        }
        if self.map.len() >= self.max_entries {
            if let Some(oldest) = self.order.pop_front() {
                self.map.remove(&oldest);
                self.record_cache(|m| {
                    m.evictions.fetch_add(1, AtomicOrdering::Relaxed);
                });
            }
        }
        self.order.push_back(key.clone());
        self.map.insert(key, hashes);
    }

    pub fn get_or_load_prefix(
        &mut self,
        file_key: &str,
        db: &SessionDb,
    ) -> Result<Option<&[u64]>> {
        if self.map.contains_key(file_key) {
            self.record_cache(|m| {
                m.hits.fetch_add(1, AtomicOrdering::Relaxed);
            });
            return Ok(self.map.get(file_key).map(|v| v.as_slice()));
        }
        if let Some(loaded) = db.load_prefix_hashes(file_key)? {
            self.record_cache(|m| {
                m.sqlite_reloads.fetch_add(1, AtomicOrdering::Relaxed);
            });
            self.insert(file_key.to_string(), loaded);
            return Ok(self.map.get(file_key).map(|v| v.as_slice()));
        }
        self.record_cache(|m| {
            m.misses_decode.fetch_add(1, AtomicOrdering::Relaxed);
        });
        Ok(None)
    }

    pub fn get_or_load_dense(
        &mut self,
        file_key: &str,
        db: &SessionDb,
    ) -> Result<Option<&[u64]>> {
        if self.map.contains_key(file_key) {
            self.record_cache(|m| {
                m.hits.fetch_add(1, AtomicOrdering::Relaxed);
            });
            return Ok(self.map.get(file_key).map(|v| v.as_slice()));
        }
        if let Some(loaded) = db.load_dense_hashes(file_key)? {
            self.record_cache(|m| {
                m.sqlite_reloads.fetch_add(1, AtomicOrdering::Relaxed);
            });
            self.insert(file_key.to_string(), loaded);
            return Ok(self.map.get(file_key).map(|v| v.as_slice()));
        }
        self.record_cache(|m| {
            m.misses_decode.fetch_add(1, AtomicOrdering::Relaxed);
        });
        Ok(None)
    }

    pub fn contains(&self, file_key: &str) -> bool {
        self.map.contains_key(file_key)
    }

    pub fn get(&self, file_key: &str) -> Option<&[u64]> {
        self.map.get(file_key).map(|v| v.as_slice())
    }
}
