mod config;
mod db;
mod ffmpeg;
mod pipeline;
mod prometheus;
mod telemetry;

use anyhow::{Context, Result};
use clap::Parser;
use config::DedupConfig;
use db::{
    AsyncDbWriter, BoundedHashCache, CandidatePair, DbWriteOp, DuplicateGroupOutput,
    DuplicateReportOutput, PairCategory, PairEvaluationLog, SessionDb, VideoAttributes,
    VideoRecord, current_epoch_secs, make_file_key, make_pair_key,
};
use ffmpeg::{DecodeKind, SPARSE_SAMPLE_FRACTIONS, run_config_test};
use pipeline::{DecodeJob, DecodePipeline, FRAME_CHANNEL_CAPACITY};
use prometheus::TelemetryReporter;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Instant;
use telemetry::{RunMeta, TelemetryRegistry, ThreadRole};
use walkdir::WalkDir;

/// Supported video extensions (case-insensitive)
const VIDEO_EXTENSIONS: &[&str] = &[
    "mp4", "mkv", "avi", "wmv", "mov", "flv", "webm", "m4v", "mpg", "mpeg", "ts", "mts", "m2ts",
    "vob", "ogv", "3gp", "asf", "rm", "rmvb", "divx", "f4v",
];

/// Sample chunk size for BLAKE3 pre-filtering (64 KiB = 65,536 bytes)
const SAMPLE_CHUNK_SIZE: usize = 64 * 1024;

/// Default duration (in seconds) decoded in Pass 2's 60-Second Early-Exit stage (`-t 60`)
const EARLY_EXIT_DURATION_SECS: u64 = 60;

/// Minimum video runtime (in seconds) to trigger the 60-Second Early-Exit stage before full decoding
const EARLY_EXIT_MIN_RUNTIME_SECS: u64 = 90;

/// Bounded in-memory LRU capacity for 1-fps prefix/dense hash vectors (backed by SQLite3 BLOBs).
/// Keeps RAM flat (~50-100 MB) even across 300,000+ files (~80 TB).
const HASH_LRU_CAPACITY: usize = 2000;

#[derive(Parser, Debug)]
#[command(
    name = "rusty_dedup",
    version = "0.5.0",
    about = "High-performance SQLite3-WAL crash-recoverable duplicate video detector CLI with mandatory --config-test GPU verification, cross-platform 72x72 HW downscaling, multi-threaded orchestrated pipeline, and 300K-file scaling"
)]
struct Args {
    /// One or more directories containing video files (scanned recursively)
    #[arg(required_unless_present = "config_test")]
    directories: Vec<PathBuf>,

    /// Run FFmpeg + hardware-acceleration / CPU decode verification against dedup_config.yaml and exit
    #[arg(long = "config-test", default_value_t = false)]
    config_test: bool,

    /// Optional path to the YAML deduplication configuration file (defaults to `dedup_config.yaml` in the current directory or alongside the `rusty_dedup` binary; falls back to built-in defaults if absent)
    #[arg(short = 'c', long = "dedup-config", alias = "config")]
    dedup_config: Option<PathBuf>,

    /// Enable verbose output (-v / --verbose) to print step-by-step file discovery, creator extraction, metadata, bucketing, pair comparisons, and state flushes
    #[arg(short = 'v', long = "verbose", default_value_t = false)]
    verbose: bool,

    /// Output JSON file path for confirmed duplicate groups
    #[arg(short, long, default_value = "duplicates.json")]
    output: PathBuf,

    /// Optional output JSON file path to log all categorized pairs (streamed from SQLite3 WAL in constant RAM)
    #[arg(long)]
    categories_log: Option<PathBuf>,

    /// Directory where crash-recoverable SQLite3 WAL session databases (`rusty_session_{sessionid}.db`) and `creator_directory.json` are stored
    #[arg(long, default_value = ".states")]
    states_dir: PathBuf,

    /// Optional custom path for the persistent cross-session creator directory JSON file (defaults to `<states-dir>/creator_directory.json`)
    #[arg(long)]
    creator_dir_file: Option<PathBuf>,

    /// Minimum corpus frequency ratio (0.0 to 1.0, default 0.15 = 15%) for a repeating prefix/suffix/n-gram to be automatically learned as a creator/studio
    #[arg(long, default_value_t = 0.15)]
    tfidf_creator_threshold: f64,

    /// Interval in milliseconds for the asynchronous SQLite3 WAL writer thread to commit batched state mutations
    #[arg(long, default_value_t = 2000)]
    checkpoint_interval_ms: u64,

    /// Ignore any existing `.states/rusty_session_{sessionid}.db` database and start a fresh scan for this session
    #[arg(long, default_value_t = false)]
    reset_session: bool,

    /// Maximum concurrent disk I/O workers per physical volume/mount-point (default 1 for spinning HDDs)
    #[arg(long, default_value_t = 1)]
    hdd_concurrency: usize,

    /// Maximum Hamming distance (out of 64 bits) for two frames to be considered >=90% similar (default 6 = 90.6% bit similarity)
    #[arg(long, default_value_t = 6)]
    max_hamming_distance: u32,

    /// Minimum confirmed_duplicate_confidence threshold (0.0 to 1.0, default 0.90 = 90%)
    #[arg(long, default_value_t = 0.90)]
    min_confidence: f64,

    /// Maximum runtime difference in seconds to consider two videos as potential duplicates (default 10.0s)
    #[arg(long, default_value_t = 10.0)]
    max_runtime_diff_secs: f64,

    /// Maximum sliding window time offset in seconds when aligning 1-fps frame sequences (default 10s)
    #[arg(long, default_value_t = 10)]
    max_slide_offset_secs: i64,

    /// Opt-in local HTTP bind address for Prometheus pull scraping (GET /metrics), e.g. `127.0.0.1:9464` (overrides dedup_config.yaml; pass `none` to disable)
    #[arg(long)]
    prometheus_listen: Option<String>,

    /// Opt-in local HTTP Prometheus Pushgateway URL, e.g. `http://127.0.0.1:9091` (overrides dedup_config.yaml; pass `none` to disable)
    #[arg(long)]
    prometheus_pushgateway: Option<String>,

    /// Prometheus `job` grouping label for Pushgateway pushes (overrides dedup_config.yaml)
    #[arg(long)]
    prometheus_job: Option<String>,

    /// Interval in seconds between periodic Pushgateway pushes (overrides dedup_config.yaml)
    #[arg(long)]
    prometheus_push_interval_secs: Option<u64>,

    /// Optional `.prom` file path for node_exporter textfile collector (overrides dedup_config.yaml)
    #[arg(long)]
    prometheus_textfile: Option<PathBuf>,

    /// Live terminal telemetry heartbeat interval in seconds (0 = end-of-run dashboard only; overrides dedup_config.yaml)
    #[arg(long)]
    telemetry_interval_secs: Option<u64>,

    /// Optional path to save the end-of-run structured APM telemetry summary as JSON
    #[arg(long)]
    telemetry_out: Option<PathBuf>,

    /// Optional path to atomically flush live JSON telemetry snapshots every 500ms (used by scripts/rusty_dedup_metrics_harness.py; overrides dedup_config.yaml and RUSTY_DEDUP_TELEMETRY_SNAPSHOT)
    #[arg(long)]
    telemetry_snapshot: Option<PathBuf>,

    /// Disable telemetry reporter, heartbeats, Prometheus endpoints, and end-of-run APM dashboard
    #[arg(long, default_value_t = false)]
    no_telemetry: bool,
}

/// Entry for a known Studio / Creator / Producer / Artist in the persistent cross-session CreatorDirectory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatorEntry {
    pub canonical_name: String,
    pub aliases: BTreeSet<String>,
    pub discovery_sources: BTreeSet<String>,
    pub seen_count: u64,
}

/// Precomputed longest-first alias patterns computed ONCE before Pass 1 (avoids 300,000 redundant sorts/allocations!).
#[derive(Debug, Clone)]
pub struct PrecomputedAliasMatcher {
    /// `(canon, prefix_pat = "alias ", suffix_pat = " alias")` sorted longest alias first
    patterns: Vec<(String, String, String)>,
}

/// Cross-session persistent Creator Directory (`.states/creator_directory.json`)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CreatorDirectory {
    pub updated_epoch_secs: u64,
    pub creators: BTreeMap<String, CreatorEntry>,
    pub alias_to_canonical: BTreeMap<String, String>,
}

impl CreatorDirectory {
    pub fn load(path: &Path) -> Self {
        if let Ok(mut file) = File::open(path) {
            let mut buf = String::new();
            if file.read_to_string(&mut buf).is_ok() {
                if let Ok(parsed) = serde_json::from_str::<CreatorDirectory>(&buf) {
                    return parsed;
                }
            }
        }
        CreatorDirectory::default()
    }

    pub fn save_atomic(&mut self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        self.updated_epoch_secs = current_epoch_secs();
        let file_name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("creator_directory.json");
        let tmp_path = path.with_file_name(format!("{}.tmp.{}", file_name, std::process::id()));
        {
            let file = File::create(&tmp_path)?;
            let mut writer = BufWriter::new(file);
            serde_json::to_writer_pretty(&mut writer, self)?;
            writer.flush()?;
            writer.into_inner()?.sync_all()?;
        }
        fs::rename(&tmp_path, path)?;
        Ok(())
    }

    pub fn register_creator(&mut self, raw_or_norm_name: &str, source: &str) -> Option<String> {
        let norm = normalize_text_camel_aware(raw_or_norm_name);
        if !is_valid_creator_candidate(&norm) {
            return None;
        }
        if let Some(existing_canon) = self.alias_to_canonical.get(&norm).cloned() {
            if let Some(entry) = self.creators.get_mut(&existing_canon) {
                entry.discovery_sources.insert(source.to_string());
                entry.seen_count += 1;
            }
            return Some(existing_canon);
        }

        let mut aliases = BTreeSet::new();
        aliases.insert(norm.clone());
        let mut sources = BTreeSet::new();
        sources.insert(source.to_string());

        self.creators.insert(
            norm.clone(),
            CreatorEntry {
                canonical_name: norm.clone(),
                aliases,
                discovery_sources: sources,
                seen_count: 1,
            },
        );
        self.alias_to_canonical.insert(norm.clone(), norm.clone());
        Some(norm)
    }

    pub fn link_aliases(&mut self, creator_a: &str, creator_b: &str, source: &str) -> Option<String> {
        let can_a = self.register_creator(creator_a, source)?;
        let can_b = self.register_creator(creator_b, source)?;
        if can_a == can_b {
            return Some(can_a);
        }
        let (primary, secondary) = if can_a.len() <= can_b.len() {
            (can_a, can_b)
        } else {
            (can_b, can_a)
        };

        if let Some(sec_entry) = self.creators.remove(&secondary) {
            if let Some(prim_entry) = self.creators.get_mut(&primary) {
                for al in sec_entry.aliases {
                    prim_entry.aliases.insert(al.clone());
                    self.alias_to_canonical.insert(al, primary.clone());
                }
                for src in sec_entry.discovery_sources {
                    prim_entry.discovery_sources.insert(src);
                }
                prim_entry.discovery_sources.insert(source.to_string());
                prim_entry.seen_count += sec_entry.seen_count;
            }
        }
        self.alias_to_canonical.insert(secondary, primary.clone());
        Some(primary)
    }

    pub fn are_same_creator(&self, c1: &str, c2: &str) -> bool {
        let n1 = normalize_text_camel_aware(c1);
        let n2 = normalize_text_camel_aware(c2);
        if n1 == n2 {
            return true;
        }
        let can1 = self.alias_to_canonical.get(&n1).unwrap_or(&n1);
        let can2 = self.alias_to_canonical.get(&n2).unwrap_or(&n2);
        can1 == can2
    }

    /// Precomputes longest-first prefix/suffix patterns ONCE before Pass 1 probing.
    pub fn build_alias_matcher(&self) -> PrecomputedAliasMatcher {
        let mut list: Vec<(&String, &String)> = self.alias_to_canonical.iter().collect();
        list.sort_by(|a, b| b.0.len().cmp(&a.0.len()).then_with(|| a.0.cmp(b.0)));
        let patterns = list
            .into_iter()
            .map(|(alias, canon)| (canon.clone(), format!("{} ", alias), format!(" {}", alias)))
            .collect();
        PrecomputedAliasMatcher { patterns }
    }
}

/// Computes a deterministic 16-character hex `sessionid` from the list of input directories.
fn compute_session_id(directories: &[PathBuf]) -> (String, Vec<String>) {
    let mut normalized_dirs: Vec<String> = directories
        .iter()
        .map(|d| {
            fs::canonicalize(d)
                .unwrap_or_else(|_| d.clone())
                .to_string_lossy()
                .trim_end_matches('/')
                .to_string()
        })
        .collect();
    normalized_dirs.sort();
    normalized_dirs.dedup();

    let mut hasher = blake3::Hasher::new();
    for dir_str in &normalized_dirs {
        hasher.update(dir_str.as_bytes());
        hasher.update(b"\n");
    }
    let full_hex = hasher.finalize().to_hex().to_string();
    let short_id = full_hex[..16].to_string();
    (short_id, normalized_dirs)
}

/// Splits CamelCase / PascalCase words, strips technical encoding/resolution tags, and normalizes to lowercase space-separated words.
pub fn normalize_text_camel_aware(input: &str) -> String {
    let mut spaced = String::with_capacity(input.len() + 8);
    let chars: Vec<char> = input.chars().collect();
    for i in 0..chars.len() {
        let c = chars[i];
        if i > 0 {
            let prev = chars[i - 1];
            let next_is_lower = chars.get(i + 1).map(|nc| nc.is_lowercase()).unwrap_or(false);
            if (prev.is_lowercase() && c.is_uppercase())
                || (prev.is_uppercase() && c.is_uppercase() && next_is_lower)
            {
                spaced.push(' ');
            }
        }
        if c.is_alphanumeric() {
            for lc in c.to_lowercase() {
                spaced.push(lc);
            }
        } else {
            spaced.push(' ');
        }
    }

    let noise_tokens = [
        "1080p", "720p", "2160p", "1440p", "4k", "480p", "360p", "240p", "x264", "x265", "h264",
        "h265", "hevc", "avc", "aac", "ac3", "eac3", "mp3", "opus", "flac", "webrip", "web-dl",
        "webdl", "bluray", "brrip", "dvdrip", "hdrip", "uhd", "60fps", "30fps",
    ];

    spaced
        .split_whitespace()
        .filter(|tok| !noise_tokens.contains(tok))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Evaluates whether a normalized directory name or repeating n-gram is a valid Studio/Creator/Artist candidate.
pub fn is_valid_creator_candidate(norm: &str) -> bool {
    let trimmed = norm.trim();
    if trimmed.is_empty() {
        return false;
    }

    let alpha_count = trimmed.chars().filter(|c| c.is_alphabetic()).count();
    let digit_count = trimmed.chars().filter(|c| c.is_ascii_digit()).count();
    let compact: String = trimmed.chars().filter(|c| !c.is_whitespace()).collect();

    if compact.len() < 4 || alpha_count < 3 {
        return false;
    }

    if digit_count > alpha_count {
        return false;
    }

    let unique_chars: HashSet<char> = compact.chars().collect();
    if unique_chars.len() <= 2 {
        return false;
    }

    const KEYBOARD_MASH_SUBSTRINGS: &[&str] = &[
        "asdf", "qwer", "zxcv", "sdfg", "dfgh", "fghj", "ghjk", "hjkl", "wert", "erty", "rtyu",
        "tyui", "yuio", "uiop", "xcvb", "cvbn", "vbnm",
    ];
    if compact == "asd"
        || compact == "qwe"
        || compact == "zxc"
        || compact == "sdf"
        || compact == "ert"
        || compact == "dfg"
        || KEYBOARD_MASH_SUBSTRINGS
            .iter()
            .any(|m| compact.contains(m))
    {
        return false;
    }

    let has_vowel = compact
        .chars()
        .any(|c| matches!(c, 'a' | 'e' | 'i' | 'o' | 'u' | 'y'));
    if !has_vowel {
        return false;
    }

    const GENERIC_FOLDER_AND_MEDIA_STOPWORDS: &[&str] = &[
        "video",
        "videos",
        "vid",
        "vids",
        "home vids",
        "home videos",
        "movie",
        "movies",
        "clip",
        "clips",
        "media",
        "download",
        "downloads",
        "new",
        "old",
        "temp",
        "tmp",
        "backup",
        "backups",
        "archive",
        "archives",
        "folder",
        "new folder",
        "untitled",
        "misc",
        "other",
        "stuff",
        "test",
        "sample",
        "samples",
        "full",
        "hd",
        "uhd",
        "sd",
        "raw",
        "final",
        "edit",
        "edited",
        "remastered",
        "part",
        "scene",
        "episode",
        "volume",
        "vol",
        "disc",
        "disk",
        "cd",
        "dvd",
    ];
    if GENERIC_FOLDER_AND_MEDIA_STOPWORDS.contains(&trimmed) {
        return false;
    }

    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    if tokens.len() == 2
        && matches!(
            tokens[0],
            "part" | "pt" | "ep" | "episode" | "vol" | "volume" | "scene" | "sc" | "cd" | "disc"
        )
        && tokens[1].chars().all(|c| c.is_ascii_digit())
    {
        return false;
    }

    true
}

/// Walks up ancestor directory components until hitting an OS/mount boundary and extracts valid creator candidates.
pub fn extract_ancestor_directory_creators(file_path: &Path) -> Vec<String> {
    let raw_components: Vec<String> = file_path
        .parent()
        .map(|p| {
            p.components()
                .filter_map(|c| match c {
                    std::path::Component::Normal(os_str) => os_str.to_str().map(|s| s.to_string()),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();

    if raw_components.is_empty() {
        return Vec::new();
    }

    let mut creators = Vec::new();
    let len = raw_components.len();
    for idx in (0..len).rev() {
        let dir_name = &raw_components[idx];
        let lower = dir_name.to_lowercase();

        if dir_name.starts_with('.') {
            break;
        }

        let root_lower = raw_components[0].to_lowercase();
        if matches!(root_lower.as_str(), "volumes" | "users" | "home" | "mnt") && idx <= 1 {
            break;
        }
        if root_lower == "media" && idx <= 2 {
            break;
        }
        if matches!(
            lower.as_str(),
            "volumes" | "users" | "home" | "mnt" | "media" | "var" | "tmp" | "private"
        ) {
            break;
        }

        let norm = normalize_text_camel_aware(dir_name);
        if is_valid_creator_candidate(&norm) {
            creators.push(norm);
        }
    }
    creators.reverse();
    creators
}

/// Splits a raw file stem into major clauses delimited by `-`, `_-_`, `--`, `|`, or `[...]`.
fn split_into_clauses(file_stem: &str) -> Vec<String> {
    let mut normalized_delims = file_stem
        .replace("_-_", " | ")
        .replace(" - ", " | ")
        .replace("--", " | ")
        .replace('[', " | ")
        .replace(']', " | ");

    if !normalized_delims.contains('|') && normalized_delims.contains('-') {
        let parts: Vec<&str> = normalized_delims.split('-').collect();
        let mut rebuilt = String::new();
        for (idx, p) in parts.iter().enumerate() {
            if idx > 0 {
                let prev_trim = parts[idx - 1].trim().to_lowercase();
                let curr_trim = p.trim().to_lowercase();
                let is_sequel_hyphen = curr_trim.chars().all(|c| c.is_ascii_digit())
                    || prev_trim.ends_with("part")
                    || prev_trim.ends_with("pt")
                    || prev_trim.ends_with("ep")
                    || prev_trim.ends_with("vol");
                if is_sequel_hyphen {
                    rebuilt.push(' ');
                } else {
                    rebuilt.push_str(" | ");
                }
            }
            rebuilt.push_str(p);
        }
        normalized_delims = rebuilt;
    }

    normalized_delims
        .split('|')
        .map(|s| normalize_text_camel_aware(s))
        .filter(|s| !s.is_empty())
        .collect()
}

/// Discovers corpus-wide repeating creator names using a 15% document-frequency threshold.
pub fn discover_corpus_creators_tfidf(file_stems: &[String], threshold_ratio: f64) -> Vec<String> {
    let total_files = file_stems.len();
    if total_files < 4 {
        return Vec::new();
    }
    let min_df = ((total_files as f64 * threshold_ratio).ceil() as usize).max(3);

    let mut standalone_subjects: HashSet<String> = HashSet::new();
    let mut prefix_companions: HashMap<String, HashSet<String>> = HashMap::new();
    let mut suffix_companions: HashMap<String, HashSet<String>> = HashMap::new();
    let mut clause_df: HashMap<String, usize> = HashMap::new();

    for stem in file_stems {
        let clauses = split_into_clauses(stem);
        if clauses.len() == 1 {
            standalone_subjects.insert(clauses[0].clone());
        } else if clauses.len() >= 2 {
            let first = &clauses[0];
            let rest_after_first = clauses[1..].join(" ");
            if is_valid_creator_candidate(first) && !rest_after_first.is_empty() {
                *clause_df.entry(first.clone()).or_insert(0) += 1;
                prefix_companions
                    .entry(first.clone())
                    .or_default()
                    .insert(rest_after_first);
            }

            let last = &clauses[clauses.len() - 1];
            let rest_before_last = clauses[..clauses.len() - 1].join(" ");
            if last != first && is_valid_creator_candidate(last) && !rest_before_last.is_empty() {
                *clause_df.entry(last.clone()).or_insert(0) += 1;
                suffix_companions
                    .entry(last.clone())
                    .or_default()
                    .insert(rest_before_last);
            }
        }
    }

    let mut discovered: Vec<(String, usize)> = clause_df
        .into_iter()
        .filter(|(cand, count)| {
            if standalone_subjects.contains(cand) {
                return false;
            }
            let pref_distinct = prefix_companions.get(cand).map(|s| s.len()).unwrap_or(0);
            let suff_distinct = suffix_companions.get(cand).map(|s| s.len()).unwrap_or(0);
            *count >= min_df && (pref_distinct >= 2 || (suff_distinct >= 2 && pref_distinct == 0))
        })
        .collect();

    discovered.sort_by(|a, b| b.0.len().cmp(&a.0.len()).then_with(|| b.1.cmp(&a.1)));
    discovered.into_iter().map(|(k, _)| k).collect()
}

/// Extracts sequel / part / episode / trailing number markers from normalized subject tokens.
pub fn extract_sequel_markers(subject_tokens: &[&str]) -> Vec<String> {
    let mut markers: Vec<String> = Vec::new();
    let len = subject_tokens.len();
    let mut i = 0;
    while i < len {
        let tok = subject_tokens[i];
        if matches!(
            tok,
            "part" | "pt" | "ep" | "episode" | "vol" | "volume" | "scene" | "sc"
        ) && i + 1 < len
            && subject_tokens[i + 1].chars().all(|c| c.is_ascii_digit())
        {
            let num = subject_tokens[i + 1].trim_start_matches('0');
            let num_norm = if num.is_empty() { "0" } else { num };
            markers.push(format!("{}:{}", tok, num_norm));
            i += 2;
            continue;
        }
        for prefix in ["part", "pt", "episode", "ep", "volume", "vol", "scene", "sc"] {
            if let Some(rest) = tok.strip_prefix(prefix) {
                if !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()) {
                    let num = rest.trim_start_matches('0');
                    let num_norm = if num.is_empty() { "0" } else { num };
                    markers.push(format!("{}:{}", prefix, num_norm));
                }
            }
        }
        if tok.chars().all(|c| c.is_ascii_digit()) {
            let num = tok.trim_start_matches('0');
            let num_norm = if num.is_empty() { "0" } else { num };
            markers.push(format!("num:{}", num_norm));
        }
        i += 1;
    }
    markers
}

/// Uses precomputed `PrecomputedAliasMatcher` (zero per-file sort!) to extract creator, subject, and sequel markers.
pub fn analyze_filename_creator_and_subject(
    file_stem: &str,
    file_path: &Path,
    creator_dir: &CreatorDirectory,
    alias_matcher: &PrecomputedAliasMatcher,
) -> (Option<String>, String, Vec<String>) {
    let full_norm = normalize_text_camel_aware(file_stem);
    let clauses = split_into_clauses(file_stem);
    let ancestor_creators = extract_ancestor_directory_creators(file_path);

    let mut detected_creator: Option<String> = None;
    let mut subject_clauses = clauses.clone();

    let has_explicit_artist_delim = file_stem.contains(" - ")
        || file_stem.contains("_-_")
        || file_stem.contains("--")
        || file_stem.contains('[');

    if subject_clauses.len() >= 2 {
        let first_clause = subject_clauses.first().cloned().unwrap_or_default();
        let last_clause = subject_clauses.last().cloned().unwrap_or_default();

        if let Some(canon) = creator_dir.alias_to_canonical.get(&first_clause) {
            detected_creator = Some(canon.clone());
            subject_clauses.remove(0);
        } else if let Some(canon) = creator_dir.alias_to_canonical.get(&last_clause) {
            detected_creator = Some(canon.clone());
            subject_clauses.pop();
        } else if has_explicit_artist_delim && is_valid_creator_candidate(&first_clause) {
            detected_creator = Some(first_clause);
            subject_clauses.remove(0);
        }
    }

    let mut subject = subject_clauses.join(" ");

    if detected_creator.is_none() {
        for (canon, prefix_pat, suffix_pat) in &alias_matcher.patterns {
            if subject.starts_with(prefix_pat.as_str()) && subject.len() > prefix_pat.len() {
                detected_creator = Some(canon.clone());
                subject = subject[prefix_pat.len()..].trim().to_string();
                break;
            } else if subject.ends_with(suffix_pat.as_str()) && subject.len() > suffix_pat.len() {
                detected_creator = Some(canon.clone());
                subject = subject[..subject.len() - suffix_pat.len()].trim().to_string();
                break;
            }
        }
    }

    if detected_creator.is_none() {
        if let Some(last_anc) = ancestor_creators.last() {
            if let Some(canon) = creator_dir.alias_to_canonical.get(last_anc) {
                detected_creator = Some(canon.clone());
            } else {
                detected_creator = Some(last_anc.clone());
            }
        }
    }

    if subject.trim().is_empty() {
        subject = full_norm;
    }

    let subject_tokens: Vec<&str> = subject.split_whitespace().collect();
    let sequel_markers = extract_sequel_markers(&subject_tokens);

    (detected_creator, subject, sequel_markers)
}

/// Cross-platform `(dev_id, inode, mtime_secs)` extraction.
/// - Unix (macOS/Linux): exact OS `st_dev`, `st_ino`, `st_mtime`.
/// - Windows: deterministic 64-bit volume-prefix hash (`dev_id`), canonical path hash (`inode`),
///   and `modified()` Unix timestamp (`mtime_secs`).
#[cfg(unix)]
fn file_dev_inode_mtime(_path: &Path, meta: &fs::Metadata) -> (u64, u64, i64) {
    (meta.dev(), meta.ino(), meta.mtime())
}

#[cfg(not(unix))]
fn file_dev_inode_mtime(path: &Path, meta: &fs::Metadata) -> (u64, u64, i64) {
    let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let prefix_str = canonical
        .components()
        .next()
        .map(|c| c.as_os_str().to_string_lossy().to_lowercase())
        .unwrap_or_else(|| "default_vol".to_string());
    let dev_hash = blake3::hash(prefix_str.as_bytes());
    let dev_bytes = dev_hash.as_bytes();
    let dev_id = u64::from_le_bytes([
        dev_bytes[0], dev_bytes[1], dev_bytes[2], dev_bytes[3],
        dev_bytes[4], dev_bytes[5], dev_bytes[6], dev_bytes[7],
    ]);

    let path_norm = canonical.to_string_lossy().to_lowercase();
    let ino_hash = blake3::hash(path_norm.as_bytes());
    let ino_bytes = ino_hash.as_bytes();
    let ino = u64::from_le_bytes([
        ino_bytes[0], ino_bytes[1], ino_bytes[2], ino_bytes[3],
        ino_bytes[4], ino_bytes[5], ino_bytes[6], ino_bytes[7],
    ]);

    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    (dev_id, ino, mtime)
}

fn is_video_file(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|ext| VIDEO_EXTENSIONS.contains(&ext.to_lowercase().as_str()))
        .unwrap_or(false)
}

/// Computes BLAKE3 hash over the first 64 KB, middle 64 KB, and last 64 KB of the file (plus file size).
fn compute_blake3_sample(path: &Path, size: u64) -> Result<String> {
    let mut file = File::open(path).with_context(|| format!("Failed to open {:?}", path))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&size.to_le_bytes());

    if size <= (SAMPLE_CHUNK_SIZE as u64) * 3 {
        let mut buf = Vec::with_capacity(size as usize);
        file.read_to_end(&mut buf)?;
        hasher.update(&buf);
    } else {
        let mut buf = vec![0u8; SAMPLE_CHUNK_SIZE];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut buf)?;
        hasher.update(&buf);

        let mid_offset = (size / 2).saturating_sub((SAMPLE_CHUNK_SIZE / 2) as u64);
        file.seek(SeekFrom::Start(mid_offset))?;
        file.read_exact(&mut buf)?;
        hasher.update(&buf);

        let end_offset = size.saturating_sub(SAMPLE_CHUNK_SIZE as u64);
        file.seek(SeekFrom::Start(end_offset))?;
        file.read_exact(&mut buf)?;
        hasher.update(&buf);
    }

    Ok(hasher.finalize().to_hex().to_string())
}

fn normalize_codec_name(raw: &str) -> String {
    let lower = raw.trim().to_lowercase();
    match lower.as_str() {
        "hevc" | "h265" | "x265" => "H265".to_string(),
        "h264" | "avc" | "avc1" | "x264" => "H264".to_string(),
        "vp9" => "VP9".to_string(),
        "vp8" => "VP8".to_string(),
        "av1" => "AV1".to_string(),
        "mpeg4" => "MPEG4".to_string(),
        "wmv3" | "wmv2" | "wmv1" => lower.to_uppercase(),
        "" | "none" | "unknown" => "NONE".to_string(),
        other => other.to_uppercase(),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FfprobeMetadata {
    pub duration_secs: f64,
    pub width: u32,
    pub height: u32,
    pub video_codec: String,
    pub audio_codec: String,
    pub video_bitrate_bps: u64,
    pub audio_bitrate_bps: u64,
    pub container_bitrate_bps: u64,
    pub video_bitrate_source: &'static str,
    pub audio_bitrate_source: &'static str,
}

fn parse_u64_json_field(val: Option<&serde_json::Value>) -> u64 {
    match val {
        Some(serde_json::Value::Number(n)) => n.as_u64().unwrap_or(0),
        Some(serde_json::Value::String(s)) => s.trim().parse::<u64>().unwrap_or(0),
        _ => 0,
    }
}

fn extract_stream_bitrate_bps(stream: &serde_json::Value) -> (u64, &'static str) {
    let direct = parse_u64_json_field(stream.get("bit_rate"));
    if direct > 0 {
        return (direct, "stream");
    }
    if let Some(tags) = stream.get("tags").and_then(|t| t.as_object()) {
        // Matroska (MKV) / WebM store stream bitrates in `BPS` or `BPS-eng` (case-insensitive)
        for preferred in ["bps", "bps-eng"] {
            for (k, v) in tags {
                if k.to_lowercase() == preferred {
                    let parsed = parse_u64_json_field(Some(v));
                    if parsed > 0 {
                        return (parsed, "tag_bps");
                    }
                }
            }
        }
        for (k, v) in tags {
            if k.to_lowercase().starts_with("bps-") {
                let parsed = parse_u64_json_field(Some(v));
                if parsed > 0 {
                    return (parsed, "tag_bps");
                }
            }
        }
    }
    (0, "missing")
}

/// Pure parser for `ffprobe -of json` output supporting MP4/MOV/AVI/WMV `stream.bit_rate`,
/// MKV/WebM `stream.tags.BPS` / `BPS-eng`, container `format.bit_rate`, and file-size fallbacks.
pub fn parse_ffprobe_json(json: &serde_json::Value, size_in_bytes: u64) -> FfprobeMetadata {
    let mut duration_secs: f64 = json
        .get("format")
        .and_then(|f| f.get("duration"))
        .and_then(|d| d.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);

    let container_bitrate_bps =
        parse_u64_json_field(json.get("format").and_then(|f| f.get("bit_rate")));

    let mut width: u32 = 0;
    let mut height: u32 = 0;
    let mut video_codec = String::from("NONE");
    let mut audio_codec = String::from("NONE");
    let mut video_bitrate_bps: u64 = 0;
    let mut audio_bitrate_bps: u64 = 0;
    let mut video_bitrate_source: &'static str = "missing";
    let mut audio_bitrate_source: &'static str = "missing";

    if let Some(streams) = json.get("streams").and_then(|s| s.as_array()) {
        for stream in streams {
            let codec_type = stream
                .get("codec_type")
                .and_then(|t| t.as_str())
                .unwrap_or("");
            let codec_name = stream
                .get("codec_name")
                .and_then(|n| n.as_str())
                .unwrap_or("NONE");

            // Skip embedded album art / thumbnail attachments so they don't shadow the real video stream
            let is_attached_pic = stream
                .get("disposition")
                .and_then(|d| d.get("attached_pic"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                == 1;

            if codec_type == "video" && !is_attached_pic && video_codec == "NONE" {
                video_codec = normalize_codec_name(codec_name);
                width = stream.get("width").and_then(|w| w.as_u64()).unwrap_or(0) as u32;
                height = stream.get("height").and_then(|h| h.as_u64()).unwrap_or(0) as u32;
                if duration_secs <= 0.0 {
                    if let Some(d) = stream
                        .get("duration")
                        .and_then(|d| d.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                    {
                        duration_secs = d;
                    }
                }
                let (bps, src) = extract_stream_bitrate_bps(stream);
                video_bitrate_bps = bps;
                video_bitrate_source = src;
            } else if codec_type == "audio" && audio_codec == "NONE" {
                audio_codec = normalize_codec_name(codec_name);
                let (bps, src) = extract_stream_bitrate_bps(stream);
                audio_bitrate_bps = bps;
                audio_bitrate_source = src;
            }
        }
    }

    // Container-level & file-size fallbacks when the video stream omits explicit bitrate metadata
    if video_bitrate_bps == 0 && video_codec != "NONE" {
        if container_bitrate_bps > 0 {
            let est = container_bitrate_bps.saturating_sub(audio_bitrate_bps);
            video_bitrate_bps = if est > 0 { est } else { container_bitrate_bps };
            video_bitrate_source = "container_fallback";
        } else if duration_secs > 0.0 && size_in_bytes > 0 {
            let total_bps = ((size_in_bytes as f64 * 8.0) / duration_secs).round().max(0.0) as u64;
            let est = total_bps.saturating_sub(audio_bitrate_bps);
            video_bitrate_bps = if est > 0 { est } else { total_bps };
            video_bitrate_source = "size_fallback";
        }
    }

    FfprobeMetadata {
        duration_secs,
        width,
        height,
        video_codec,
        audio_codec,
        video_bitrate_bps,
        audio_bitrate_bps,
        container_bitrate_bps,
        video_bitrate_source,
        audio_bitrate_source,
    }
}

/// Uses ffprobe to extract runtime, width, height, codecs, and video/audio bitrates in a single header read.
fn extract_ffprobe_metadata(path: &Path, size_in_bytes: u64) -> Result<FfprobeMetadata> {
    let output = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=duration,bit_rate:stream=codec_type,codec_name,width,height,duration,bit_rate:stream_tags=BPS,BPS-eng:stream_disposition=attached_pic",
            "-of",
            "json",
        ])
        .arg(path)
        .output()
        .with_context(|| format!("Failed to execute ffprobe on {:?}", path))?;

    if !output.status.success() {
        anyhow::bail!(
            "ffprobe exited with error on {:?}: {}",
            path,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let json: serde_json::Value = serde_json::from_slice(&output.stdout)
        .with_context(|| format!("Failed to parse ffprobe JSON for {:?}", path))?;

    Ok(parse_ffprobe_json(&json, size_in_bytes))
}

/// Computes the exclusive upper bound on `r2 >= r1` such that `(r2 - r1) <= max_diff || (r2 - r1)/r2 <= 0.10`.
#[inline]
pub fn max_matching_runtime(r1: f64, max_runtime_diff_secs: f64) -> f64 {
    let by_abs = r1 + max_runtime_diff_secs;
    // (r2 - r1) / r2 <= 0.10  <=>  0.90 * r2 <= r1  <=>  r2 <= r1 / 0.90
    // Add tiny epsilon for IEEE-754 boundary safety.
    let by_ratio = (r1 / 0.90) + 1e-9;
    by_abs.max(by_ratio)
}

#[inline]
pub fn is_within_runtime_window(r1: f64, r2: f64, max_runtime_diff_secs: f64) -> bool {
    let rt_diff = (r1 - r2).abs();
    let max_rt = r1.max(r2).max(1.0);
    rt_diff <= max_runtime_diff_secs || (rt_diff / max_rt) <= 0.10
}

/// Runtime-sorted sliding window neighbor finder (replaces $O(N^2)$ 45-billion-iteration loop on 300K files!).
/// Assumes `records` is sorted ascending by `runtime_exact_secs`.
pub fn mark_videos_needing_sparse_keyframes(
    records: &[VideoRecord],
    max_runtime_diff_secs: f64,
) -> Vec<bool> {
    let n = records.len();
    let mut needs = vec![false; n];
    for i in 0..n {
        let r1 = records[i].runtime_exact_secs;
        let max_r2 = max_matching_runtime(r1, max_runtime_diff_secs);
        for j in (i + 1)..n {
            let r2 = records[j].runtime_exact_secs;
            if r2 > max_r2 {
                break;
            }
            if is_within_runtime_window(r1, r2, max_runtime_diff_secs) {
                let blake3_exact = records[i].size_in_bytes == records[j].size_in_bytes
                    && records[i].blake3_sample_hash.is_some()
                    && records[i].blake3_sample_hash == records[j].blake3_sample_hash;
                if !blake3_exact {
                    needs[i] = true;
                    needs[j] = true;
                }
            }
        }
    }
    needs
}

fn classify_pair(
    v1: &VideoRecord,
    v2: &VideoRecord,
    creator_dir: &CreatorDirectory,
) -> (PairCategory, f64, f64, f64, Option<(String, String)>) {
    let sequel_match = v1.sequel_markers == v2.sequel_markers;

    let raw_subj_sim = if v1.subject_title.is_empty() || v2.subject_title.is_empty() {
        0.0
    } else if v1.subject_title == v2.subject_title {
        1.0
    } else {
        strsim::jaro_winkler(&v1.subject_title, &v2.subject_title)
    };

    let name_sim = if sequel_match {
        raw_subj_sim
    } else {
        raw_subj_sim.min(0.70)
    };

    let subjects_almost_identical = sequel_match && name_sim >= 0.88;

    let conflicting_creators = match (&v1.detected_creator, &v2.detected_creator) {
        (Some(c1), Some(c2)) if !creator_dir.are_same_creator(c1, c2) => {
            Some((c1.clone(), c2.clone()))
        }
        _ => None,
    };

    let max_size = v1.size_in_bytes.max(v2.size_in_bytes).max(1) as f64;
    let size_diff_ratio = (v1.size_in_bytes as f64 - v2.size_in_bytes as f64).abs() / max_size;
    let sizes_nearly_same = size_diff_ratio <= 0.10;

    let runtime_diff_secs = (v1.runtime_exact_secs - v2.runtime_exact_secs).abs();
    let max_runtime = v1.runtime_exact_secs.max(v2.runtime_exact_secs).max(1.0);
    let runtime_diff_ratio = runtime_diff_secs / max_runtime;
    let runtimes_almost_same = runtime_diff_ratio <= 0.10 || runtime_diff_secs <= 10.0;

    let formats_or_codecs_distinct = v1.container_ext != v2.container_ext
        || v1.video_codec != v2.video_codec
        || v1.audio_codec != v2.audio_codec;

    let category = if subjects_almost_identical && sizes_nearly_same && runtimes_almost_same {
        PairCategory::Strong
    } else if (subjects_almost_identical && (formats_or_codecs_distinct || !sizes_nearly_same))
        || (sizes_nearly_same && !subjects_almost_identical && formats_or_codecs_distinct)
        || (sizes_nearly_same && !subjects_almost_identical)
    {
        PairCategory::Weak
    } else {
        PairCategory::PotentiallyDistinct
    };

    (
        category,
        name_sim,
        size_diff_ratio,
        runtime_diff_secs,
        conflicting_creators,
    )
}

fn evaluate_sparse_keyframes(
    v1: &VideoRecord,
    v2: &VideoRecord,
    max_hamming: u32,
) -> (usize, usize, f64) {
    let n1 = v1.sparse_hashes.len();
    let n2 = v2.sparse_hashes.len();
    let total = n1.min(n2);
    if total == 0 {
        return (0, 0, 0.0);
    }

    let tolerance = (max_hamming + 2).min(10);
    let mut matched = 0usize;
    for idx in 0..total {
        let h1 = v1.sparse_hashes[idx];
        let start_j = idx.saturating_sub(1);
        let end_j = (idx + 1).min(n2 - 1);
        let mut frame_matched = false;
        for j in start_j..=end_j {
            if (h1 ^ v2.sparse_hashes[j]).count_ones() <= tolerance {
                frame_matched = true;
                break;
            }
        }
        if frame_matched {
            matched += 1;
        }
    }
    let ratio = matched as f64 / total as f64;
    (matched, total, ratio)
}

fn check_sparse_gate(v1: &VideoRecord, v2: &VideoRecord, max_hamming: u32) -> (bool, u32) {
    if v1.runtime_exact_secs < 15.0
        || v2.runtime_exact_secs < 15.0
        || v1.sparse_hashes.is_empty()
        || v2.sparse_hashes.is_empty()
    {
        return (true, 0);
    }
    let sparse_tolerance = (max_hamming + 4).min(12);
    let mut min_dist = 64u32;
    for &h1 in &v1.sparse_hashes {
        for &h2 in &v2.sparse_hashes {
            let dist = (h1 ^ h2).count_ones();
            if dist < min_dist {
                min_dist = dist;
            }
            if dist <= sparse_tolerance {
                return (true, dist);
            }
        }
    }
    (false, min_dist)
}

fn compare_dense_1fps_hashes(
    hashes1: &[u64],
    hashes2: &[u64],
    runtime1_secs: u64,
    runtime2_secs: u64,
    max_hamming_distance: u32,
    max_slide_offset: i64,
) -> (f64, usize, u64, i64) {
    if hashes1.is_empty() || hashes2.is_empty() {
        return (0.0, 0, runtime1_secs.min(runtime2_secs).max(1), 0);
    }

    let len1 = hashes1.len() as i64;
    let len2 = hashes2.len() as i64;
    let mut best_points: usize = 0;
    let mut best_offset: i64 = 0;

    for offset in -max_slide_offset..=max_slide_offset {
        let mut points: usize = 0;
        let start1 = 0i64.max(-offset);
        let end1 = len1.min(len2 - offset);
        if start1 >= end1 {
            continue;
        }
        for i in start1..end1 {
            let j = i + offset;
            let h1 = hashes1[i as usize];
            let h2 = hashes2[j as usize];
            if (h1 ^ h2).count_ones() <= max_hamming_distance {
                points += 1;
            }
        }
        if points > best_points || (points == best_points && offset.abs() < best_offset.abs()) {
            best_points = points;
            best_offset = offset;
        }
    }

    let effective_runtime = hashes1
        .len()
        .min(hashes2.len())
        .min(runtime1_secs.min(runtime2_secs).max(1) as usize)
        .max(1) as u64;

    let confidence = (best_points as f64 / effective_runtime as f64).min(1.0);
    (confidence, best_points, effective_runtime, best_offset)
}

fn print_heavy_verbose_pair_header(
    pair_idx: usize,
    total_pairs: usize,
    pair: &CandidatePair,
    v1: &VideoRecord,
    v2: &VideoRecord,
) {
    eprintln!("================================================================================");
    eprintln!(
        "[Verbose][Pass 2][Pair {}/{}] Category: {}",
        pair_idx, total_pairs, pair.category
    );
    eprintln!("  File 1 : {:?}", v1.path);
    eprintln!(
        "           size={} B | runtime={}s ({:.2}s) | res={}x{} | vcodec={} | acodec={} | ext=.{}",
        v1.size_in_bytes,
        v1.runtime_in_seconds,
        v1.runtime_exact_secs,
        v1.video_width,
        v1.video_height,
        v1.video_codec,
        v1.audio_codec,
        v1.container_ext
    );
    eprintln!(
        "           creator={:?} | subject={:?} | sequel_markers={:?}",
        v1.detected_creator, v1.subject_title, v1.sequel_markers
    );
    eprintln!("  File 2 : {:?}", v2.path);
    eprintln!(
        "           size={} B | runtime={}s ({:.2}s) | res={}x{} | vcodec={} | acodec={} | ext=.{}",
        v2.size_in_bytes,
        v2.runtime_in_seconds,
        v2.runtime_exact_secs,
        v2.video_width,
        v2.video_height,
        v2.video_codec,
        v2.audio_codec,
        v2.container_ext
    );
    eprintln!(
        "           creator={:?} | subject={:?} | sequel_markers={:?}",
        v2.detected_creator, v2.subject_title, v2.sequel_markers
    );
    eprintln!(
        "  Deltas : subject_sim={:.2}% | size_diff={:.2}% ({} B) | runtime_diff={:.2}s | conflicting_creators={:?}",
        pair.name_similarity * 100.0,
        pair.size_diff_ratio * 100.0,
        (v1.size_in_bytes as i128 - v2.size_in_bytes as i128).abs(),
        pair.runtime_diff_secs,
        pair.conflicting_creators
    );
}

fn print_heavy_verbose_sparse_matrix(v1: &VideoRecord, v2: &VideoRecord, max_hamming: u32) {
    let sparse_tol = (max_hamming + 4).min(12);
    eprintln!(
        "  [Verbose][Stage B: 5-Keyframe Sparse Screening (tolerance <= {}/64 bits)]",
        sparse_tol
    );
    eprint!("    File 1 Sparse dHashes ({}): ", v1.sparse_hashes.len());
    for (idx, h) in v1.sparse_hashes.iter().enumerate() {
        eprint!(
            "[{}@{:.0}%]=0x{:016x} ",
            idx,
            SPARSE_SAMPLE_FRACTIONS.get(idx).copied().unwrap_or(0.0) * 100.0,
            h
        );
    }
    eprintln!();
    eprint!("    File 2 Sparse dHashes ({}): ", v2.sparse_hashes.len());
    for (idx, h) in v2.sparse_hashes.iter().enumerate() {
        eprint!(
            "[{}@{:.0}%]=0x{:016x} ",
            idx,
            SPARSE_SAMPLE_FRACTIONS.get(idx).copied().unwrap_or(0.0) * 100.0,
            h
        );
    }
    eprintln!();
    if !v1.sparse_hashes.is_empty() && !v2.sparse_hashes.is_empty() {
        eprintln!("    5x5 Pairwise Hamming Distance Matrix (bits differing out of 64):");
        for (i_idx, &h1) in v1.sparse_hashes.iter().enumerate() {
            eprint!("      F1[{}] (0x{:016x}) vs F2: ", i_idx, h1);
            for (j_idx, &h2) in v2.sparse_hashes.iter().enumerate() {
                let dist = (h1 ^ h2).count_ones();
                let mark = if dist <= sparse_tol { "✓" } else { " " };
                eprint!("[{}]={:>2}b{} ", j_idx, dist, mark);
            }
            eprintln!();
        }
    }
}

fn print_heavy_verbose_dense_alignment(
    stage_label: &str,
    hashes1: &[u64],
    hashes2: &[u64],
    runtime1_secs: u64,
    runtime2_secs: u64,
    max_hamming_distance: u32,
    max_slide_offset: i64,
) {
    let len1 = hashes1.len() as i64;
    let len2 = hashes2.len() as i64;
    let effective_runtime = hashes1
        .len()
        .min(hashes2.len())
        .min(runtime1_secs.min(runtime2_secs).max(1) as usize)
        .max(1) as u64;

    eprintln!(
        "  [Verbose][{}: Sliding-Window Offset Table (-{}s .. +{}s, frame match <= {}/64 bits, denom={}s)]",
        stage_label, max_slide_offset, max_slide_offset, max_hamming_distance, effective_runtime
    );

    let mut best_points: usize = 0;
    let mut best_offset: i64 = 0;

    for offset in -max_slide_offset..=max_slide_offset {
        let start1 = 0i64.max(-offset);
        let end1 = len1.min(len2 - offset);
        if start1 >= end1 {
            continue;
        }
        let overlap = (end1 - start1) as usize;
        let mut points: usize = 0;
        let mut total_hamming: u64 = 0;
        for t in start1..end1 {
            let u = t + offset;
            let dist = (hashes1[t as usize] ^ hashes2[u as usize]).count_ones();
            total_hamming += dist as u64;
            if dist <= max_hamming_distance {
                points += 1;
            }
        }
        let avg_hamming = total_hamming as f64 / (overlap.max(1) as f64);
        let conf = (points as f64 / effective_runtime as f64).min(1.0) * 100.0;
        if points > best_points || (points == best_points && offset.abs() < best_offset.abs()) {
            best_points = points;
            best_offset = offset;
        }
        eprintln!(
            "      offset {:+3}s : overlap={:>4}f | matched={:>4}/{}s ({:>5.1}%) | avg_hamming={:>4.1}/64 bits",
            offset, overlap, points, effective_runtime, conf, avg_hamming
        );
    }

    let start1 = 0i64.max(-best_offset);
    let end1 = len1.min(len2 - best_offset);
    if start1 < end1 {
        let total_aligned = (end1 - start1) as usize;
        let show_count = total_aligned.min(30);
        eprintln!(
            "    Frame-by-Frame Breakdown at Best Offset ({:+}s) (showing {}/{} aligned seconds):",
            best_offset, show_count, total_aligned
        );
        for k in 0..show_count {
            let t1 = start1 + k as i64;
            let t2 = t1 + best_offset;
            let h1 = hashes1[t1 as usize];
            let h2 = hashes2[t2 as usize];
            let dist = (h1 ^ h2).count_ones();
            let bit_sim = ((64.0 - dist as f64) / 64.0) * 100.0;
            let status = if dist <= max_hamming_distance {
                "MATCH (+1 pt)"
            } else {
                "DIFF  ( 0 pt)"
            };
            eprintln!(
                "      t1={:>4}s (0x{:016x}) vs t2={:>4}s (0x{:016x}) -> Hamming={:>2}/64 bits ({:>5.1}% sim) [{}]",
                t1, h1, t2, h2, dist, bit_sim, status
            );
        }
        if total_aligned > show_count {
            eprintln!(
                "      ... ({} additional aligned seconds omitted; total matched at best offset {:+}s = {}/{}s)",
                total_aligned - show_count,
                best_offset,
                best_points,
                effective_runtime
            );
        }
    }
}

/// Disjoint-Set (Union-Find) data structure to cluster confirmed duplicate pairs into duplicate groups.
struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<usize>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    fn find(&mut self, x: usize) -> usize {
        if self.parent[x] != x {
            self.parent[x] = self.find(self.parent[x]);
        }
        self.parent[x]
    }

    fn union(&mut self, x: usize, y: usize) {
        let rx = self.find(x);
        let ry = self.find(y);
        if rx == ry {
            return;
        }
        if self.rank[rx] < self.rank[ry] {
            self.parent[rx] = ry;
        } else if self.rank[rx] > self.rank[ry] {
            self.parent[ry] = rx;
        } else {
            self.parent[ry] = rx;
            self.rank[rx] += 1;
        }
    }
}

/// Result returned by the dedicated Pass 1 Pair Evaluator Thread.
struct Pass1EvalSummary {
    ordered_candidate_pairs: Vec<CandidatePair>,
    confirmed_edges: Vec<(usize, usize)>,
    total_strong_bucketed: usize,
    strong_fastpath_confirmed: usize,
    conflicting_creator_rejected_pass1: usize,
    weak_count: usize,
    distinct_count: usize,
}

/// Executes Pass 1 pair categorization & Strong-pair keyframe confirmation on a dedicated Pair Evaluator Thread
/// using the $O(N \log N + K)$ runtime-sorted sliding window!
fn run_pass1_pair_evaluator(
    records: Arc<Vec<VideoRecord>>,
    creator_dir: CreatorDirectory,
    db_tx: mpsc::Sender<DbWriteOp>,
    telemetry: Arc<TelemetryRegistry>,
    max_runtime_diff_secs: f64,
    max_hamming_distance: u32,
    verbose: bool,
) -> Result<Pass1EvalSummary> {
    let handle = thread::Builder::new()
        .name("pair-evaluator-pass1".to_string())
        .spawn(move || {
            let n = records.len();
            let mut strong_pairs_for_pass2: Vec<CandidatePair> = Vec::new();
            let mut weak_pairs: Vec<CandidatePair> = Vec::new();
            let mut distinct_pairs: Vec<CandidatePair> = Vec::new();
            let mut confirmed_edges: Vec<(usize, usize)> = Vec::new();
            let mut total_strong_bucketed: usize = 0;
            let mut strong_fastpath_confirmed: usize = 0;
            let mut conflicting_creator_rejected_pass1: usize = 0;
            let pass_t0 = Instant::now();

            for i in 0..n {
                let v1 = &records[i];
                let r1 = v1.runtime_exact_secs;
                let max_r2 = max_matching_runtime(r1, max_runtime_diff_secs);

                for j in (i + 1)..n {
                    let v2 = &records[j];
                    let r2 = v2.runtime_exact_secs;
                    if r2 > max_r2 {
                        break;
                    }
                    if !is_within_runtime_window(r1, r2, max_runtime_diff_secs) {
                        continue;
                    }
                    let pair_t0 = Instant::now();

                    let (category, name_sim, size_diff_ratio, rt_diff, conflicting_creators) =
                        classify_pair(v1, v2, &creator_dir);

                    let pair = CandidatePair {
                        idx1: i,
                        idx2: j,
                        category,
                        name_similarity: name_sim,
                        size_diff_ratio,
                        runtime_diff_secs: rt_diff,
                        conflicting_creators: conflicting_creators.clone(),
                    };

                    let key1 = v1.file_key();
                    let key2 = v2.file_key();
                    let pair_key = make_pair_key(&key1, &key2);

                    match category {
                        PairCategory::Strong => {
                            total_strong_bucketed += 1;
                            if let Some((ref c1, ref c2)) = conflicting_creators {
                                let (matched_kf, total_kf, kf_ratio) =
                                    evaluate_sparse_keyframes(v1, v2, max_hamming_distance);
                                if kf_ratio < 0.40 && total_kf > 0 {
                                    conflicting_creator_rejected_pass1 += 1;
                                    telemetry
                                        .funnel
                                        .pass1_conflict_rejected
                                        .fetch_add(1, AtomicOrdering::Relaxed);
                                    if verbose {
                                        eprintln!(
                                            "  [Verbose][Pass 1 Conflicting Creators -> CONFIRMED DISTINCT] {:?} (creator={:?}) != {:?} (creator={:?}) | keyframes matched {}/{} ({:.0}%)",
                                            v1.file_stem,
                                            c1,
                                            v2.file_stem,
                                            c2,
                                            matched_kf,
                                            total_kf,
                                            kf_ratio * 100.0
                                        );
                                    }
                                    let eval_entry = PairEvaluationLog {
                                        file1: v1.path.clone(),
                                        file2: v2.path.clone(),
                                        initial_category: format!(
                                            "{} (conflicting creators: {} vs {})",
                                            PairCategory::Strong,
                                            c1,
                                            c2
                                        ),
                                        final_outcome: "Confirmed distinct file pair".to_string(),
                                        confidence: kf_ratio,
                                        matched_frames: matched_kf,
                                        compared_runtime_secs: total_kf as u64,
                                        best_offset_secs: 0,
                                        detection_method:
                                            "Pass-1 Conflicting Creator Keyframe Rejection"
                                                .to_string(),
                                    };
                                    let _ = db_tx.send(DbWriteOp::UpsertPairScore {
                                        pair_key,
                                        log: eval_entry,
                                    });
                                } else {
                                    if verbose {
                                        eprintln!(
                                            "  [Verbose][Pass 1 Conflicting Creators -> QUEUED FOR PASS 2] {:?} (creator={:?}) vs {:?} (creator={:?}) | keyframes matched {}/{} ({:.0}%)",
                                            v1.file_stem,
                                            c1,
                                            v2.file_stem,
                                            c2,
                                            matched_kf,
                                            total_kf,
                                            kf_ratio * 100.0
                                        );
                                    }
                                    strong_pairs_for_pass2.push(pair);
                                }
                            } else {
                                let blake3_exact = v1.size_in_bytes == v2.size_in_bytes
                                    && v1.blake3_sample_hash.is_some()
                                    && v1.blake3_sample_hash == v2.blake3_sample_hash;
                                let (matched_kf, total_kf, kf_ratio) = if blake3_exact {
                                    (5, 5, 1.0)
                                } else {
                                    evaluate_sparse_keyframes(v1, v2, max_hamming_distance)
                                };

                                if blake3_exact || (total_kf >= 3 && kf_ratio >= 0.80) {
                                    strong_fastpath_confirmed += 1;
                                    telemetry
                                        .funnel
                                        .pass1_strong_confirmed
                                        .fetch_add(1, AtomicOrdering::Relaxed);
                                    let avoided_ms = ((v1.runtime_exact_secs + v2.runtime_exact_secs)
                                        * 1000.0)
                                        .round() as u64;
                                    telemetry
                                        .funnel
                                        .avoided_ms_pass1_strong
                                        .fetch_add(avoided_ms, AtomicOrdering::Relaxed);
                                    confirmed_edges.push((i, j));
                                    let method = if blake3_exact {
                                        "Pass-1 Strong Fast-Path (Exact Size + BLAKE3)"
                                    } else {
                                        "Pass-1 Strong Fast-Path (Metadata + Keyframe Verification)"
                                    };
                                    eprintln!(
                                        "  [PASS-1 CONFIRMED DUPLICATE - SKIPPING PASS 2] {:?} == {:?} (subj_sim={:.0}%, size_diff={:.1}%, keyframes={}/{})",
                                        v1.file_stem,
                                        v2.file_stem,
                                        name_sim * 100.0,
                                        size_diff_ratio * 100.0,
                                        matched_kf,
                                        total_kf
                                    );
                                    let eval_entry = PairEvaluationLog {
                                        file1: v1.path.clone(),
                                        file2: v2.path.clone(),
                                        initial_category: PairCategory::Strong.to_string(),
                                        final_outcome: "Confirmed duplicate file pair".to_string(),
                                        confidence: kf_ratio,
                                        matched_frames: matched_kf,
                                        compared_runtime_secs: v1
                                            .runtime_in_seconds
                                            .min(v2.runtime_in_seconds)
                                            .max(1),
                                        best_offset_secs: 0,
                                        detection_method: method.to_string(),
                                    };
                                    let _ = db_tx.send(DbWriteOp::UpsertPairScore {
                                        pair_key,
                                        log: eval_entry,
                                    });
                                } else {
                                    strong_pairs_for_pass2.push(pair);
                                }
                            }
                        }
                        PairCategory::Weak => weak_pairs.push(pair),
                        PairCategory::PotentiallyDistinct => distinct_pairs.push(pair),
                    }
                    telemetry.record_thread_job(ThreadRole::PairEvaluatorPass1, pair_t0.elapsed());
                }
            }
            telemetry.record_thread_busy(ThreadRole::PairEvaluatorPass1, pass_t0.elapsed());

            let weak_count = weak_pairs.len();
            let distinct_count = distinct_pairs.len();
            let mut ordered_candidate_pairs: Vec<CandidatePair> = Vec::with_capacity(
                strong_pairs_for_pass2.len() + weak_pairs.len() + distinct_pairs.len(),
            );
            ordered_candidate_pairs.extend(strong_pairs_for_pass2);
            ordered_candidate_pairs.extend(weak_pairs);
            ordered_candidate_pairs.extend(distinct_pairs);

            Pass1EvalSummary {
                ordered_candidate_pairs,
                confirmed_edges,
                total_strong_bucketed,
                strong_fastpath_confirmed,
                conflicting_creator_rejected_pass1,
                weak_count,
                distinct_count,
            }
        })
        .context("Failed to spawn Pass 1 Pair Evaluator thread")?;

    handle
        .join()
        .map_err(|_| anyhow::anyhow!("Pass 1 Pair Evaluator thread panicked"))
}

/// Request sent to the dedicated Pass 2 Pair Evaluator Thread once a pair's 1-fps hashes are ready.
struct Pass2EvalJob {
    pair_idx: usize,
    total_pairs: usize,
    pair: CandidatePair,
    pair_key: String,
    hashes1: Vec<u64>,
    hashes2: Vec<u64>,
    decoder_label: String,
}

/// Summary returned by the Pass 2 Pair Evaluator Thread on shutdown.
struct Pass2EvalThreadResult {
    confirmed_edges: Vec<(usize, usize)>,
    confirmed_count: usize,
    learned_aliases_count: usize,
    creator_dir: CreatorDirectory,
}

fn main() -> Result<()> {
    let start_time = Instant::now();
    let args = Args::parse();
    let verbose = args.verbose;

    let (mut dedup_cfg, config_source) = DedupConfig::load(args.dedup_config.as_deref())?;

    // Apply CLI telemetry / Prometheus overrides over `dedup_config.yaml`
    if args.no_telemetry {
        dedup_cfg.telemetry.enabled = false;
    }
    if let Some(secs) = args.telemetry_interval_secs {
        dedup_cfg.telemetry.heartbeat_interval_secs = secs;
    }
    if let Some(ref listen) = args.prometheus_listen {
        let lower = listen.trim().to_lowercase();
        if matches!(lower.as_str(), "none" | "off" | "disabled" | "") {
            dedup_cfg.telemetry.prometheus.listen_addr = None;
        } else {
            dedup_cfg.telemetry.prometheus.listen_addr = Some(listen.trim().to_string());
        }
    }
    if let Some(ref pg) = args.prometheus_pushgateway {
        let lower = pg.trim().to_lowercase();
        if matches!(lower.as_str(), "none" | "off" | "disabled" | "") {
            dedup_cfg.telemetry.prometheus.pushgateway_url = None;
        } else {
            dedup_cfg.telemetry.prometheus.pushgateway_url = Some(pg.trim().to_string());
        }
    }
    if let Some(ref job) = args.prometheus_job {
        if !job.trim().is_empty() {
            dedup_cfg.telemetry.prometheus.job_name = job.trim().to_string();
        }
    }
    if let Some(secs) = args.prometheus_push_interval_secs {
        dedup_cfg.telemetry.prometheus.push_interval_secs = secs;
    }
    if let Some(ref tf) = args.prometheus_textfile {
        dedup_cfg.telemetry.prometheus.textfile_path = Some(tf.clone());
    }
    if let Some(ref snap) = args.telemetry_snapshot {
        dedup_cfg.telemetry.snapshot_path = Some(snap.clone());
    } else if let Ok(env_snap) = std::env::var("RUSTY_DEDUP_TELEMETRY_SNAPSHOT") {
        if !env_snap.trim().is_empty() {
            dedup_cfg.telemetry.snapshot_path = Some(PathBuf::from(env_snap.trim()));
        }
    }

    eprintln!("=== rusty_dedup v0.5.0: High-Performance Video Duplicate Scanner ===");
    eprintln!(
        "Dedup Config     : {} (version {})",
        config_source, dedup_cfg.version
    );
    eprintln!("FFmpeg Settings  : {}", dedup_cfg.ffmpeg.flags_summary());

    // STEP 1 (Mandatory on EVERY run and standalone with --config-test):
    // Verify ffmpeg/ffprobe, probe decoders, and run live GPU (or CPU) decode + 72x72 scale -> 9x8 dHash test!
    let verified_env = Arc::new(run_config_test(&dedup_cfg, verbose)?);

    if args.config_test {
        eprintln!(
            "=== --config-test completed successfully in {:.2?}! Active decode pipeline: {} ===",
            start_time.elapsed(),
            verified_env.summary_label()
        );
        return Ok(());
    }

    // Compute deterministic `sessionid` from the list of input directories
    let (session_id, canonical_input_dirs) = compute_session_id(&args.directories);
    fs::create_dir_all(&args.states_dir).with_context(|| {
        format!(
            "Failed to create session states directory {:?}",
            args.states_dir
        )
    })?;
    let db_path = args
        .states_dir
        .join(format!("rusty_session_{}.db", session_id));
    let legacy_json_state_path = args
        .states_dir
        .join(format!(".rusty_temp_{}", session_id));
    let creator_dir_path = args
        .creator_dir_file
        .clone()
        .unwrap_or_else(|| args.states_dir.join("creator_directory.json"));

    let mut creator_dir = CreatorDirectory::load(&creator_dir_path);

    let mut session_db = SessionDb::open_or_create(
        &db_path,
        &session_id,
        &canonical_input_dirs,
        args.reset_session,
    )?;

    if !args.reset_session {
        if let Some((migrated_files, migrated_pairs)) =
            session_db.maybe_migrate_legacy_json(&legacy_json_state_path)?
        {
            eprintln!(
                "[Migration] Imported legacy JSON state {:?} into SQLite3 WAL {:?} ({} file records, {} pair scores).",
                legacy_json_state_path, db_path, migrated_files, migrated_pairs
            );
        }
    }

    let (cached_files_count, cached_pairs_count) = session_db.count_stats()?;

    eprintln!("Active Architecture:");
    eprintln!("  [1] Deferred 2-Sweep Inode-Ordered Probing (BTreeMap (dev_id, inode) per HDD volume)");
    eprintln!(
        "  [2] Decode Pipeline: {} + sync_channel({}) -> Pure-Rust 9x8 dHash Worker",
        verified_env.summary_label(),
        FRAME_CHANNEL_CAPACITY
    );
    eprintln!("  [3] State Store: SQLite3 WAL ({:?}) + Bounded {}-video 1-fps LRU Cache", db_path, HASH_LRU_CAPACITY);
    eprintln!(
        "  [4] 60-Second Early-Exit Stage (-t {}) for Long Videos (> {}s) + Dedicated Pair Evaluator Thread",
        EARLY_EXIT_DURATION_SECS, EARLY_EXIT_MIN_RUNTIME_SECS
    );
    eprintln!("Session ID       : {}", session_id);
    if cached_files_count > 0 || cached_pairs_count > 0 {
        eprintln!(
            "[Recovery] Resumed SQLite3 WAL session {} ({} cached file records, {} evaluated pair scores).",
            session_id, cached_files_count, cached_pairs_count
        );
    } else if args.reset_session {
        eprintln!(
            "[Recovery] --reset-session passed; initialized fresh SQLite3 WAL database {:?}.",
            db_path
        );
    }
    eprintln!(
        "Creator Directory: {:?} ({} known creators loaded)",
        creator_dir_path,
        creator_dir.creators.len()
    );
    eprintln!(
        "Input Dirs ({})   : {:?} (HDD concurrency/vol: {}, verbose: {})",
        canonical_input_dirs.len(),
        canonical_input_dirs,
        args.hdd_concurrency,
        verbose
    );

    // Initialize lock-free APM Telemetry Registry + optional Prometheus HTTP scrape / Pushgateway reporter
    let telemetry = Arc::new(TelemetryRegistry::new());
    let run_meta = RunMeta {
        version: env!("CARGO_PKG_VERSION").to_string(),
        session_id: session_id.clone(),
        hw_backend: verified_env
            .active_hw
            .as_ref()
            .map(|h| h.backend.as_ffmpeg_name().to_string())
            .unwrap_or_else(|| "cpu".to_string()),
        hw_scale: format!("{}x{}", verified_env.scale_width, verified_env.scale_height),
    };
    let telemetry_reporter = TelemetryReporter::start(
        dedup_cfg.telemetry.clone(),
        Arc::clone(&telemetry),
        run_meta,
        db_path.clone(),
        args.telemetry_out.clone(),
    );

    // Spawn dedicated Async SQLite3 WAL Writer Thread
    let db_writer = AsyncDbWriter::spawn(
        db_path.clone(),
        args.checkpoint_interval_ms,
        verbose,
        Arc::clone(&telemetry),
    )?;

    telemetry.set_phase("walk");

    // Step 1: Recursive directory walk with canonical (dev_id, inode) deduplication
    let mut seen_inodes: HashSet<(u64, u64)> = HashSet::new();
    let mut discovered_files: Vec<(PathBuf, u64, u64, u64, i64)> = Vec::new();
    let mut skipped_overlapping_files: usize = 0;

    for dir in &args.directories {
        if !dir.exists() {
            eprintln!("Warning: Directory {:?} does not exist, skipping.", dir);
            continue;
        }
        if verbose {
            eprintln!("[Verbose][Walk] Scanning directory recursively: {:?}", dir);
        }
        for entry in WalkDir::new(dir)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.path();
            if !is_video_file(path) {
                continue;
            }
            if let Ok(meta) = entry.metadata() {
                let (dev, ino, mtime) = file_dev_inode_mtime(path, &meta);
                let size = meta.len();
                if size == 0 {
                    continue;
                }
                if seen_inodes.insert((dev, ino)) {
                    let canonical_path =
                        fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
                    if verbose {
                        eprintln!(
                            "  [Verbose][Discovered] {:?} (size: {} bytes, dev: {}, inode: {})",
                            canonical_path, size, dev, ino
                        );
                    }
                    discovered_files.push((canonical_path, dev, ino, size, mtime));
                } else {
                    skipped_overlapping_files += 1;
                    if verbose {
                        eprintln!(
                            "  [Verbose][Skip-Overlap] Already queued file (dev: {}, inode: {}): {:?}",
                            dev, ino, path
                        );
                    }
                }
            }
        }
    }

    // Sort discovered files by physical device & inode order `(dev_id, inode, path)`
    discovered_files.sort_by(|a, b| {
        a.1.cmp(&b.1)
            .then_with(|| a.2.cmp(&b.2))
            .then_with(|| a.0.cmp(&b.0))
    });

    eprintln!(
        "[Pass 1a] Found {} unique video files across input directories (skipped {} overlapping path entries) in {:.2?}. Sorted by (dev_id, inode) for sequential platter probing.",
        discovered_files.len(),
        skipped_overlapping_files,
        start_time.elapsed()
    );

    if discovered_files.is_empty() {
        db_writer.shutdown();
        telemetry.set_phase("done");
        telemetry_reporter.shutdown();
        let empty_report = DuplicateReportOutput {
            input_directories: canonical_input_dirs.clone(),
            duplicates: Vec::new(),
        };
        let json_str = serde_json::to_string_pretty(&empty_report)?;
        fs::write(&args.output, json_str)?;
        eprintln!(
            "No video files found. Wrote empty report to {:?}.",
            args.output
        );
        return Ok(());
    }

    // Progressive Creator Directory Discovery (Ancestor Directories + Corpus TF-IDF >= 15%)
    let mut all_stems: Vec<String> = Vec::with_capacity(discovered_files.len());
    let mut new_dir_creators = 0usize;
    for (path, _, _, _, _) in &discovered_files {
        for anc in extract_ancestor_directory_creators(path) {
            let was_known = creator_dir.alias_to_canonical.contains_key(&anc);
            if creator_dir.register_creator(&anc, "ancestor_directory").is_some() && !was_known {
                new_dir_creators += 1;
                if verbose {
                    eprintln!(
                        "  [Verbose][Creator Learned - Directory] {:?} (from {:?})",
                        anc, path
                    );
                }
            }
        }
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            all_stems.push(stem.to_string());
        }
    }

    let tfidf_creators = discover_corpus_creators_tfidf(&all_stems, args.tfidf_creator_threshold);
    let mut new_tfidf_creators = 0usize;
    for tc in tfidf_creators {
        let was_known = creator_dir.alias_to_canonical.contains_key(&tc);
        if creator_dir.register_creator(&tc, "corpus_tfidf_15pct").is_some() && !was_known {
            new_tfidf_creators += 1;
            if verbose {
                eprintln!(
                    "  [Verbose][Creator Learned - TF-IDF >= {:.0}%] {:?}",
                    args.tfidf_creator_threshold * 100.0,
                    tc
                );
            }
        }
    }

    if new_dir_creators > 0 || new_tfidf_creators > 0 {
        let _ = creator_dir.save_atomic(&creator_dir_path);
        eprintln!(
            "[Creator Directory] Added {} directory creator(s) and {} corpus TF-IDF creator(s) (total known: {}).",
            new_dir_creators,
            new_tfidf_creators,
            creator_dir.creators.len()
        );
    }

    // Precompute sorted creator alias patterns ONCE before probing (critical for 300K files!)
    let alias_matcher_arc = Arc::new(creator_dir.build_alias_matcher());
    let creator_dir_arc = Arc::new(creator_dir.clone());

    let mut size_counts: HashMap<u64, usize> = HashMap::new();
    for (_, _, _, size, _) in &discovered_files {
        *size_counts.entry(*size).or_insert(0) += 1;
    }

    // Group files by physical device ID (`dev_id`) in deterministic `BTreeMap` preserving monotonic `inode` order
    let mut files_by_dev: BTreeMap<u64, Vec<(PathBuf, u64, u64, u64, i64)>> = BTreeMap::new();
    for item in discovered_files.iter().cloned() {
        files_by_dev.entry(item.1).or_default().push(item);
    }

    // Pass 1a: Per-volume `ffprobe` Worker Thread(s) streaming metadata into SQLite3 WAL
    telemetry.set_phase("pass1_ffprobe");
    let pass1_start = Instant::now();
    let per_dev_concurrency = args.hdd_concurrency.max(1);
    let total_to_scan = discovered_files.len();
    let scanned_counter = Arc::new(AtomicUsize::new(0));
    let db_writer_tx = db_writer.sender();
    let db_path_for_readers = db_path.clone();

    let all_records: Vec<VideoRecord> = files_by_dev
        .into_par_iter()
        .flat_map(|(_dev_id, dev_files)| {
            let db_tx_dev = db_writer_tx.clone();
            let cdir_ref = Arc::clone(&creator_dir_arc);
            let matcher_ref = Arc::clone(&alias_matcher_arc);
            let counter_ref = Arc::clone(&scanned_counter);
            let tel_ref = Arc::clone(&telemetry);
            let size_counts_ref = &size_counts;
            let reader_db_path = db_path_for_readers.clone();

            let probe_chunk = |chunk: Vec<(PathBuf, u64, u64, u64, i64)>| -> Vec<VideoRecord> {
                let reader_conn = SessionDb::open_or_create(
                    &reader_db_path,
                    &session_id,
                    &canonical_input_dirs,
                    false,
                )
                .ok()
                .and_then(|s| s.open_readonly_handle().ok());

                let mut out = Vec::with_capacity(chunk.len());
                for (path, dev, ino, size, mtime) in chunk {
                    let item_t0 = Instant::now();
                    let current_idx = counter_ref.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                    let path_str = path.to_string_lossy().to_string();
                    let file_key = make_file_key(&path_str, size, mtime);
                    let needs_blake3 = size_counts_ref.get(&size).copied().unwrap_or(0) > 1;

                    let file_stem = path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("unknown")
                        .to_string();
                    let (detected_creator, subject_title, sequel_markers) =
                        analyze_filename_creator_and_subject(
                            &file_stem,
                            &path,
                            &cdir_ref,
                            &matcher_ref,
                        );

                    // Check SQLite3 WAL cache first
                    if let Some(ref conn) = reader_conn {
                        let lookup_t0 = Instant::now();
                        if let Ok(Some(mut cached)) =
                            SessionDb::get_compact_file_record(conn, &file_key)
                        {
                            let lookup_dur = lookup_t0.elapsed();
                            let mut updated = false;
                            let mut b3_bytes = 0u64;
                            if needs_blake3 && cached.blake3_sample_hash.is_none() {
                                cached.blake3_sample_hash =
                                    compute_blake3_sample(&path, size).ok();
                                b3_bytes = size.min((SAMPLE_CHUNK_SIZE as u64) * 3);
                                updated = true;
                            }
                            if cached.subject_title != subject_title
                                || cached.detected_creator != detected_creator
                                || cached.sequel_markers != sequel_markers
                            {
                                cached.detected_creator = detected_creator;
                                cached.subject_title = subject_title;
                                cached.sequel_markers = sequel_markers;
                                updated = true;
                            }

                            // On-demand bitrate re-probe: if this row was cached before bitrate
                            // collection (video_bitrate_bps == 0 && !bitrate_probed), run ffprobe
                            // once and persist bitrates back to SQLite3 WAL.
                            if cached.video_bitrate_bps == 0 && !cached.bitrate_probed {
                                let reprobe_t0 = Instant::now();
                                match extract_ffprobe_metadata(&path, size) {
                                    Ok(meta) => {
                                        let ffprobe_dur = reprobe_t0.elapsed();
                                        cached.video_bitrate_bps = meta.video_bitrate_bps;
                                        cached.audio_bitrate_bps = meta.audio_bitrate_bps;
                                        cached.bitrate_probed = true;
                                        tel_ref.record_bitrate_source(
                                            "video",
                                            meta.video_bitrate_source,
                                        );
                                        tel_ref.record_bitrate_source(
                                            "audio",
                                            meta.audio_bitrate_source,
                                        );
                                        tel_ref.record_thread_busy(
                                            ThreadRole::FfprobeWorkers,
                                            ffprobe_dur,
                                        );
                                        tel_ref.record_thread_job(
                                            ThreadRole::FfprobeWorkers,
                                            ffprobe_dur,
                                        );
                                        tel_ref.record_volume_bitrate_reprobe(
                                            dev,
                                            ffprobe_dur,
                                            b3_bytes,
                                        );
                                        if updated {
                                            let _ = db_tx_dev.send(DbWriteOp::UpsertFileRecord {
                                                file_key,
                                                record: cached.clone(),
                                            });
                                        } else {
                                            let _ = db_tx_dev.send(DbWriteOp::UpdateBitrates {
                                                file_key,
                                                video_bitrate_bps: cached.video_bitrate_bps,
                                                audio_bitrate_bps: cached.audio_bitrate_bps,
                                            });
                                        }
                                        if verbose {
                                            eprintln!(
                                                "  [Verbose][Pass 1a][{}/{}][SQLITE BITRATE RE-PROBED in {:.1?}] {:?} (inode={}) -> vbitrate={}bps ({}), abitrate={}bps ({})",
                                                current_idx,
                                                total_to_scan,
                                                ffprobe_dur,
                                                cached.file_stem,
                                                cached.inode,
                                                cached.video_bitrate_bps,
                                                meta.video_bitrate_source,
                                                cached.audio_bitrate_bps,
                                                meta.audio_bitrate_source
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        let ffprobe_dur = reprobe_t0.elapsed();
                                        tel_ref.record_thread_busy(
                                            ThreadRole::FfprobeWorkers,
                                            ffprobe_dur,
                                        );
                                        tel_ref.record_thread_error(ThreadRole::FfprobeWorkers);
                                        tel_ref.record_volume_sqlite_hit(dev, lookup_dur, b3_bytes);
                                        if updated {
                                            let _ = db_tx_dev.send(DbWriteOp::UpsertFileRecord {
                                                file_key,
                                                record: cached.clone(),
                                            });
                                        }
                                        eprintln!(
                                            "Warning: Bitrate re-probe failed for cached {:?} ({})",
                                            path, e
                                        );
                                    }
                                }
                            } else {
                                if updated {
                                    let _ = db_tx_dev.send(DbWriteOp::UpsertFileRecord {
                                        file_key,
                                        record: cached.clone(),
                                    });
                                }
                                tel_ref.record_volume_sqlite_hit(dev, lookup_dur, b3_bytes);
                                if verbose {
                                    eprintln!(
                                        "  [Verbose][Pass 1a][{}/{}][SQLITE HIT] {:?} (inode={}) -> creator={:?}, subject={:?}, sequel={:?}, vbitrate={}bps, abitrate={}bps",
                                        current_idx,
                                        total_to_scan,
                                        cached.file_stem,
                                        cached.inode,
                                        cached.detected_creator,
                                        cached.subject_title,
                                        cached.sequel_markers,
                                        cached.video_bitrate_bps,
                                        cached.audio_bitrate_bps
                                    );
                                }
                            }
                            out.push(cached);
                            continue;
                        }
                    }

                    let file_timer = Instant::now();
                    let meta = match extract_ffprobe_metadata(&path, size) {
                        Ok(m) => m,
                        Err(e) => {
                            let elapsed = item_t0.elapsed();
                            tel_ref.record_thread_busy(ThreadRole::FfprobeWorkers, elapsed);
                            tel_ref.record_thread_error(ThreadRole::FfprobeWorkers);
                            tel_ref.record_volume_error(dev);
                            eprintln!("Warning: Skipping {:?} ({})", path, e);
                            continue;
                        }
                    };
                    let ffprobe_dur = file_timer.elapsed();
                    tel_ref.record_bitrate_source("video", meta.video_bitrate_source);
                    tel_ref.record_bitrate_source("audio", meta.audio_bitrate_source);

                    let (blake3_hash, b3_bytes) = if needs_blake3 {
                        (
                            compute_blake3_sample(&path, size).ok(),
                            size.min((SAMPLE_CHUNK_SIZE as u64) * 3),
                        )
                    } else {
                        (None, 0)
                    };

                    let elapsed = item_t0.elapsed();
                    tel_ref.record_thread_busy(ThreadRole::FfprobeWorkers, elapsed);
                    tel_ref.record_thread_job(ThreadRole::FfprobeWorkers, elapsed);
                    tel_ref.record_volume_probed(dev, ffprobe_dur, b3_bytes);

                    let normalized_stem = normalize_text_camel_aware(&file_stem);
                    let container_ext = path
                        .extension()
                        .and_then(|e| e.to_str())
                        .unwrap_or("")
                        .to_lowercase();

                    if verbose {
                        eprintln!(
                            "  [Verbose][Pass 1a][{}/{}][INODE-ORDERED HEADER PROBED in {:.1?}] {:?} (inode={}) | creator={:?}, subject={:?}, sequel={:?}, size={}B, runtime={}s ({:.2}s), {}x{}, vcodec={}, acodec={}, vbitrate={}bps ({}), abitrate={}bps ({})",
                            current_idx,
                            total_to_scan,
                            ffprobe_dur,
                            file_stem,
                            ino,
                            detected_creator,
                            subject_title,
                            sequel_markers,
                            size,
                            meta.duration_secs.round() as u64,
                            meta.duration_secs,
                            meta.width,
                            meta.height,
                            meta.video_codec,
                            meta.audio_codec,
                            meta.video_bitrate_bps,
                            meta.video_bitrate_source,
                            meta.audio_bitrate_bps,
                            meta.audio_bitrate_source
                        );
                    }

                    let record = VideoRecord {
                        path: path_str,
                        file_stem,
                        normalized_stem,
                        detected_creator,
                        subject_title,
                        sequel_markers,
                        container_ext,
                        dev_id: dev,
                        inode: ino,
                        mtime_secs: mtime,
                        size_in_bytes: size,
                        runtime_exact_secs: meta.duration_secs,
                        runtime_in_seconds: meta.duration_secs.round() as u64,
                        video_height: meta.height,
                        video_width: meta.width,
                        video_codec: meta.video_codec,
                        audio_codec: meta.audio_codec,
                        video_bitrate_bps: meta.video_bitrate_bps,
                        audio_bitrate_bps: meta.audio_bitrate_bps,
                        bitrate_probed: true,
                        blake3_sample_hash: blake3_hash,
                        sparse_hashes: Vec::new(),
                        prefix_1fps_hashes: None,
                        dense_1fps_hashes: None,
                    };

                    let _ = db_tx_dev.send(DbWriteOp::UpsertFileRecord {
                        file_key,
                        record: record.clone(),
                    });
                    out.push(record);
                }
                out
            };

            if per_dev_concurrency <= 1 {
                // Strict sequential iteration on 1 HDD thread preserves 100% monotonic inode order
                probe_chunk(dev_files)
            } else {
                let chunk_len = (dev_files.len() / per_dev_concurrency).max(1);
                let chunks: Vec<Vec<(PathBuf, u64, u64, u64, i64)>> =
                    dev_files.chunks(chunk_len).map(|c| c.to_vec()).collect();
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(per_dev_concurrency)
                    .build()
                    .unwrap();
                pool.install(|| chunks.into_par_iter().flat_map(probe_chunk).collect())
            }
        })
        .collect();

    // Flush Pass 1a records to SQLite3 WAL before Pass 1b updates sparse_hashes!
    db_writer.flush();

    // Sort records ascending by `(runtime_exact_secs, size_in_bytes, path)` to enable O(N log N + K) sliding window!
    let mut records = all_records;
    records.sort_by(|a, b| {
        a.runtime_exact_secs
            .total_cmp(&b.runtime_exact_secs)
            .then_with(|| a.size_in_bytes.cmp(&b.size_in_bytes))
            .then_with(|| a.path.cmp(&b.path))
    });

    // Pass 1b: Identify videos needing sparse keyframes using runtime-sorted sliding window
    telemetry.set_phase("pass1_keyframes");
    let n = records.len();
    let needs_sparse_keyframes =
        mark_videos_needing_sparse_keyframes(&records, args.max_runtime_diff_secs);

    let isolated_avoided_ms: u64 = (0..n)
        .filter(|&idx| !needs_sparse_keyframes[idx])
        .map(|idx| (records[idx].runtime_exact_secs.max(0.0) * 1000.0).round() as u64)
        .sum();
    telemetry
        .funnel
        .avoided_ms_pass1_isolated
        .fetch_add(isolated_avoided_ms, AtomicOrdering::Relaxed);

    let mut indices_needing_sparse: Vec<usize> = (0..n)
        .filter(|&idx| needs_sparse_keyframes[idx] && records[idx].sparse_hashes.is_empty())
        .collect();
    indices_needing_sparse.sort_by(|&a, &b| {
        records[a]
            .dev_id
            .cmp(&records[b].dev_id)
            .then_with(|| records[a].inode.cmp(&records[b].inode))
    });

    let skipped_no_neighbor_count =
        n.saturating_sub(needs_sparse_keyframes.iter().filter(|&&b| b).count());

    // Spawn the 3-thread Decode Pipeline (GPU Worker + CPU-Codec Worker -> sync_channel(16) -> CPU 9x8 dHash Worker)
    let pipeline = DecodePipeline::spawn(
        Arc::clone(&verified_env),
        db_writer.sender(),
        Arc::clone(&telemetry),
        verbose,
    )?;

    if !indices_needing_sparse.is_empty() {
        let kf_total = indices_needing_sparse.len();
        eprintln!(
            "[Pass 1b Keyframes] Extracting batched 5-keyframe dHashes in inode order via pipelined workers for {}/{} videos with runtime neighbors (skipped {} isolated videos)...",
            kf_total, n, skipped_no_neighbor_count
        );

        // Keep up to `FRAME_CHANNEL_CAPACITY` jobs in flight so GPU/CPU decode overlaps with Rust 9x8 dHashing & DB updates!
        let mut inflight = 0usize;
        let mut completed = 0usize;
        let mut iter = indices_needing_sparse.into_iter();

        loop {
            while inflight < FRAME_CHANNEL_CAPACITY {
                if let Some(idx) = iter.next() {
                    let job = DecodeJob {
                        record_idx: idx,
                        file_key: records[idx].file_key(),
                        path: PathBuf::from(&records[idx].path),
                        file_stem: records[idx].file_stem.clone(),
                        video_codec: records[idx].video_codec.clone(),
                        video_duration_secs: records[idx].runtime_exact_secs,
                        kind: DecodeKind::Sparse5 {
                            duration_secs: records[idx].runtime_exact_secs,
                        },
                    };
                    pipeline.submit(job)?;
                    inflight += 1;
                } else {
                    break;
                }
            }

            if inflight == 0 {
                break;
            }

            let res = pipeline.recv()?;
            inflight -= 1;
            completed += 1;
            records[res.record_idx].sparse_hashes = res.hashes;
            if verbose {
                eprintln!(
                    "  [Verbose][Pass 1b Keyframes][{}/{}][decode={:.1?}, dhash={:.2?}, {}] {:?} (inode={}) -> {} sparse dHashes",
                    completed,
                    kf_total,
                    res.decode_elapsed,
                    res.hash_elapsed,
                    res.decoder_label,
                    records[res.record_idx].file_stem,
                    records[res.record_idx].inode,
                    records[res.record_idx].sparse_hashes.len()
                );
            }
        }
    } else {
        eprintln!(
            "[Pass 1b Keyframes] 0 videos needed fresh keyframe extraction (skipped {} isolated videos with no runtime neighbors).",
            skipped_no_neighbor_count
        );
    }

    db_writer.flush();

    eprintln!(
        "[Pass 1] Completed deferred 2-sweep probing & sparse keyframes for {} valid videos in {:.2?}.",
        records.len(),
        pass1_start.elapsed()
    );

    // Run Pass 1 Pair Categorization & Strong-pair keyframe confirmation on dedicated Pair Evaluator Thread
    telemetry.set_phase("pass1_eval");
    let records_arc = Arc::new(records);
    let pass1_eval = run_pass1_pair_evaluator(
        Arc::clone(&records_arc),
        creator_dir.clone(),
        db_writer.sender(),
        Arc::clone(&telemetry),
        args.max_runtime_diff_secs,
        args.max_hamming_distance,
        verbose,
    )?;

    let mut uf = UnionFind::new(n);
    for (u, v) in pass1_eval.confirmed_edges {
        uf.union(u, v);
    }

    let ordered_candidate_pairs = pass1_eval.ordered_candidate_pairs;
    let total_pairs_to_eval = ordered_candidate_pairs.len();

    eprintln!(
        "[Pass 1 Categorization] Strong={} ({} confirmed in Pass 1 -> SKIPPED Pass 2, {} conflicting-creator rejected in Pass 1) | Queued for Pass 2: {} pairs (Strong-remaining={}, Weak={}, Potentially Distinct={}).",
        pass1_eval.total_strong_bucketed,
        pass1_eval.strong_fastpath_confirmed,
        pass1_eval.conflicting_creator_rejected_pass1,
        total_pairs_to_eval,
        total_pairs_to_eval
            .saturating_sub(pass1_eval.weak_count + pass1_eval.distinct_count),
        pass1_eval.weak_count,
        pass1_eval.distinct_count
    );

    // Pass 2: Pipelined 60s Early-Exit + Full 1-fps Decode with Bounded LRU Hash Cache & Dedicated Pair Evaluator Thread!
    telemetry.set_phase("pass2_eval");
    let pass2_start = Instant::now();
    let mut confirmed_pairs_count = pass1_eval.strong_fastpath_confirmed;
    let mut recovered_pairs_count = 0usize;
    let mut early_exit_60s_rejected_count = 0usize;

    let mut prefix_cache =
        BoundedHashCache::with_telemetry(HASH_LRU_CAPACITY, Arc::clone(&telemetry), false);
    let mut dense_cache =
        BoundedHashCache::with_telemetry(HASH_LRU_CAPACITY, Arc::clone(&telemetry), true);
    let mut decoder_labels_cache: HashMap<String, String> = HashMap::new();

    // Spawn dedicated Pass 2 Pair Evaluator Thread to run Stage E sliding-window alignment +
    // alias learning + SQLite3 pair_scores persistence concurrently while Orchestrator & DecodePipeline decode upcoming pairs!
    let (eval_tx, eval_rx) = mpsc::sync_channel::<Pass2EvalJob>(FRAME_CHANNEL_CAPACITY);
    let eval_records_ref = Arc::clone(&records_arc);
    let eval_db_tx = db_writer.sender();
    let eval_tel = Arc::clone(&telemetry);
    let eval_min_conf = args.min_confidence;
    let eval_max_hamming = args.max_hamming_distance;
    let eval_max_slide = args.max_slide_offset_secs;
    let eval_creator_dir = creator_dir;

    let pass2_eval_handle = thread::Builder::new()
        .name("pair-evaluator-pass2".to_string())
        .spawn(move || -> Pass2EvalThreadResult {
            let mut cdir = eval_creator_dir;
            let mut confirmed_edges = Vec::new();
            let mut confirmed_count = 0usize;
            let mut learned_aliases_count = 0usize;

            loop {
                let wait_t0 = Instant::now();
                let job = match eval_rx.recv() {
                    Ok(j) => j,
                    Err(_) => break,
                };
                eval_tel.record_thread_starved(ThreadRole::PairEvaluatorPass2, wait_t0.elapsed());
                eval_tel.eval_channel_pop();

                let job_t0 = Instant::now();
                let i = job.pair.idx1;
                let j = job.pair.idx2;
                let v1 = &eval_records_ref[i];
                let v2 = &eval_records_ref[j];

                if verbose {
                    print_heavy_verbose_dense_alignment(
                        "Stage E: Full 1-fps dHash Alignment",
                        &job.hashes1,
                        &job.hashes2,
                        v1.runtime_in_seconds,
                        v2.runtime_in_seconds,
                        eval_max_hamming,
                        eval_max_slide,
                    );
                }

                let (confidence, matched_frames, compared_runtime, best_offset) =
                    compare_dense_1fps_hashes(
                        &job.hashes1,
                        &job.hashes2,
                        v1.runtime_in_seconds,
                        v2.runtime_in_seconds,
                        eval_max_hamming,
                        eval_max_slide,
                    );

                let eval_entry = if confidence >= eval_min_conf {
                    confirmed_edges.push((i, j));
                    confirmed_count += 1;
                    eval_tel
                        .funnel
                        .pass2_stage_e_confirmed
                        .fetch_add(1, AtomicOrdering::Relaxed);

                    if let Some((ref c1, ref c2)) = job.pair.conflicting_creators {
                        if let Some(canon) =
                            cdir.link_aliases(c1, c2, "confirmed_duplicate_alias")
                        {
                            learned_aliases_count += 1;
                            eprintln!(
                                "  [Creator Alias Learned] Confirmed duplicate proved {:?} == {:?} -> linked in CreatorDirectory as {:?}",
                                c1, c2, canon
                            );
                        }
                    } else {
                        let clauses1 = split_into_clauses(&v1.file_stem);
                        let clauses2 = split_into_clauses(&v2.file_stem);
                        if clauses1.len() >= 2 && clauses2.len() >= 2 && clauses1[0] != clauses2[0]
                        {
                            if is_valid_creator_candidate(&clauses1[0])
                                && is_valid_creator_candidate(&clauses2[0])
                                && clauses1.last() == clauses2.last()
                            {
                                if let Some(canon) = cdir.link_aliases(
                                    &clauses1[0],
                                    &clauses2[0],
                                    "confirmed_duplicate_clause_alias",
                                ) {
                                    learned_aliases_count += 1;
                                    eprintln!(
                                        "  [Creator Alias Learned] Linked prefix clauses {:?} and {:?} -> canonical creator {:?}",
                                        clauses1[0], clauses2[0], canon
                                    );
                                }
                            }
                        }
                    }

                    eprintln!(
                        "  [CONFIRMED DUPLICATE ({}/{}) - {:.1}% ({}/{}s, offset {:+}s)] ({}) {:?} == {:?}",
                        job.pair_idx + 1,
                        job.total_pairs,
                        confidence * 100.0,
                        matched_frames,
                        compared_runtime,
                        best_offset,
                        job.pair.category,
                        v1.file_stem,
                        v2.file_stem
                    );

                    PairEvaluationLog {
                        file1: v1.path.clone(),
                        file2: v2.path.clone(),
                        initial_category: job.pair.category.to_string(),
                        final_outcome: "Confirmed duplicate file pair".to_string(),
                        confidence,
                        matched_frames,
                        compared_runtime_secs: compared_runtime,
                        best_offset_secs: best_offset,
                        detection_method: format!(
                            "1-fps 9x8 dHash + sliding window ({})",
                            job.decoder_label
                        ),
                    }
                } else {
                    eval_tel
                        .funnel
                        .pass2_stage_e_distinct
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    if verbose {
                        eprintln!(
                            "  [Verbose][Pass 2][CONFIRMED DISTINCT - {:.1}% ({}/{}s, best offset {:+}s)] {:?} != {:?}",
                            confidence * 100.0,
                            matched_frames,
                            compared_runtime,
                            best_offset,
                            v1.file_stem,
                            v2.file_stem
                        );
                    }
                    PairEvaluationLog {
                        file1: v1.path.clone(),
                        file2: v2.path.clone(),
                        initial_category: job.pair.category.to_string(),
                        final_outcome: "Confirmed distinct file pair".to_string(),
                        confidence,
                        matched_frames,
                        compared_runtime_secs: compared_runtime,
                        best_offset_secs: best_offset,
                        detection_method: format!(
                            "1-fps 9x8 dHash + sliding window ({})",
                            job.decoder_label
                        ),
                    }
                };

                let _ = eval_db_tx.send(DbWriteOp::UpsertPairScore {
                    pair_key: job.pair_key,
                    log: eval_entry,
                });
                let job_dt = job_t0.elapsed();
                eval_tel.record_thread_busy(ThreadRole::PairEvaluatorPass2, job_dt);
                eval_tel.record_thread_job(ThreadRole::PairEvaluatorPass2, job_dt);
            }

            Pass2EvalThreadResult {
                confirmed_edges,
                confirmed_count,
                learned_aliases_count,
                creator_dir: cdir,
            }
        })
        .context("Failed to spawn Pass 2 Pair Evaluator thread")?;

    // Helper closure to ensure 1-fps prefix or dense hashes are decoded (via DecodePipeline) and cached
    for (pair_idx, pair) in ordered_candidate_pairs.into_iter().enumerate() {
        let i = pair.idx1;
        let j = pair.idx2;
        let v1 = &records_arc[i];
        let v2 = &records_arc[j];
        let key1 = v1.file_key();
        let key2 = v2.file_key();
        let pair_key = make_pair_key(&key1, &key2);

        if verbose {
            print_heavy_verbose_pair_header(pair_idx + 1, total_pairs_to_eval, &pair, v1, v2);
        }

        // Recovery check against SQLite3 WAL `pair_scores`
        if let Some(cached_eval) = session_db.get_pair_score(&pair_key)? {
            recovered_pairs_count += 1;
            telemetry
                .funnel
                .pass2_sqlite_recovered
                .fetch_add(1, AtomicOrdering::Relaxed);
            let is_dup = cached_eval.final_outcome == "Confirmed duplicate file pair";
            if is_dup {
                uf.union(i, j);
                confirmed_pairs_count += 1;
            }
            if verbose {
                eprintln!(
                    "  [Verbose][Pass 2 ({}/{})][SQLITE RECOVERED] {} ({:.1}% via {})",
                    pair_idx + 1,
                    total_pairs_to_eval,
                    cached_eval.final_outcome,
                    cached_eval.confidence * 100.0,
                    cached_eval.detection_method
                );
            }
            continue;
        }

        // Transitive skip if already in same duplicate cluster
        if uf.find(i) == uf.find(j) {
            telemetry
                .funnel
                .pass2_transitive_skip
                .fetch_add(1, AtomicOrdering::Relaxed);
            if verbose {
                eprintln!(
                    "  [Verbose][Pass 2 ({}/{})][TRANSITIVE SKIP] Already confirmed in same duplicate group.",
                    pair_idx + 1,
                    total_pairs_to_eval
                );
            }
            continue;
        }

        // Stage A: Exact byte-size + 192KB BLAKE3 match
        if v1.size_in_bytes == v2.size_in_bytes {
            if let (Some(h1), Some(h2)) = (&v1.blake3_sample_hash, &v2.blake3_sample_hash) {
                if h1 == h2 {
                    uf.union(i, j);
                    confirmed_pairs_count += 1;
                    telemetry
                        .funnel
                        .pass2_blake3_exact
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    let rt = v1.runtime_in_seconds.max(1);
                    eprintln!(
                        "  [CONFIRMED DUPLICATE ({}/{}) - BLAKE3 EXACT] ({}) {:?} == {:?}",
                        pair_idx + 1,
                        total_pairs_to_eval,
                        pair.category,
                        v1.file_stem,
                        v2.file_stem
                    );
                    let eval_entry = PairEvaluationLog {
                        file1: v1.path.clone(),
                        file2: v2.path.clone(),
                        initial_category: pair.category.to_string(),
                        final_outcome: "Confirmed duplicate file pair".to_string(),
                        confidence: 1.0,
                        matched_frames: rt as usize,
                        compared_runtime_secs: rt,
                        best_offset_secs: 0,
                        detection_method: "Exact size + 192KB BLAKE3 sample match".to_string(),
                    };
                    db_writer.submit(DbWriteOp::UpsertPairScore {
                        pair_key,
                        log: eval_entry,
                    });
                    continue;
                }
            }
        }

        // Stage B: 5-Keyframe Sparse Screening
        if verbose {
            print_heavy_verbose_sparse_matrix(v1, v2, args.max_hamming_distance);
        }
        let (sparse_passed, min_sparse_dist) = check_sparse_gate(v1, v2, args.max_hamming_distance);
        if !sparse_passed {
            telemetry
                .funnel
                .pass2_stage_b_rejected
                .fetch_add(1, AtomicOrdering::Relaxed);
            let avoided_ms =
                ((v1.runtime_exact_secs + v2.runtime_exact_secs) * 1000.0).round() as u64;
            telemetry
                .funnel
                .avoided_ms_stage_b_sparse
                .fetch_add(avoided_ms, AtomicOrdering::Relaxed);
            if verbose {
                eprintln!(
                    "  [Verbose][Pass 2 ({}/{})][SPARSE REJECT] {:?} != {:?} (closest sparse keyframe Hamming={}/64)",
                    pair_idx + 1,
                    total_pairs_to_eval,
                    v1.file_stem,
                    v2.file_stem,
                    min_sparse_dist
                );
            }
            let eval_entry = PairEvaluationLog {
                file1: v1.path.clone(),
                file2: v2.path.clone(),
                initial_category: pair.category.to_string(),
                final_outcome: "Confirmed distinct file pair".to_string(),
                confidence: 0.0,
                matched_frames: 0,
                compared_runtime_secs: v1.runtime_in_seconds.min(v2.runtime_in_seconds).max(1),
                best_offset_secs: 0,
                detection_method: "Sparse keyframe filter".to_string(),
            };
            db_writer.submit(DbWriteOp::UpsertPairScore {
                pair_key,
                log: eval_entry,
            });
            continue;
        }

        // Stage C: 60-Second Early-Exit Stage (`-t 60`) for Long Videos (> 90s)
        let min_rt_secs = v1.runtime_in_seconds.min(v2.runtime_in_seconds);
        let dense1_available = dense_cache.get_or_load_dense(&key1, &session_db)?.is_some();
        let dense2_available = dense_cache.get_or_load_dense(&key2, &session_db)?.is_some();

        if min_rt_secs > EARLY_EXIT_MIN_RUNTIME_SECS && (!dense1_available || !dense2_available) {
            let mut prefix_jobs_submitted = 0usize;
            for &(idx, ref fkey, dense_avail) in
                &[(i, key1.clone(), dense1_available), (j, key2.clone(), dense2_available)]
            {
                if dense_avail && !prefix_cache.contains(fkey) {
                    if let Some(full) = dense_cache.get(fkey) {
                        let take_len = full.len().min(EARLY_EXIT_DURATION_SECS as usize);
                        prefix_cache.insert(fkey.clone(), full[..take_len].to_vec());
                    }
                } else if prefix_cache.get_or_load_prefix(fkey, &session_db)?.is_none() {
                    let rec = &records_arc[idx];
                    if verbose {
                        eprintln!(
                            "  [Verbose][Stage C: 60s Early-Exit Decode] Dispatching first {}s @ 1-fps ({}) for {:?}...",
                            EARLY_EXIT_DURATION_SECS,
                            verified_env.summary_label(),
                            rec.file_stem
                        );
                    }
                    pipeline.submit(DecodeJob {
                        record_idx: idx,
                        file_key: fkey.clone(),
                        path: PathBuf::from(&rec.path),
                        file_stem: rec.file_stem.clone(),
                        video_codec: rec.video_codec.clone(),
                        video_duration_secs: rec.runtime_exact_secs,
                        kind: DecodeKind::Prefix1Fps {
                            max_secs: EARLY_EXIT_DURATION_SECS,
                        },
                    })?;
                    prefix_jobs_submitted += 1;
                }
            }

            for _ in 0..prefix_jobs_submitted {
                let res = pipeline.recv()?;
                if verbose {
                    eprintln!(
                        "  [Verbose][Stage C: 60s Early-Exit Done] {} extracted {} prefix frames for {:?} (decode={:.2?}, dhash={:.2?})",
                        res.decoder_label,
                        res.hashes.len(),
                        records_arc[res.record_idx].file_stem,
                        res.decode_elapsed,
                        res.hash_elapsed
                    );
                }
                prefix_cache.insert(res.file_key, res.hashes);
            }

            let pref1 = prefix_cache.get(&key1).unwrap_or(&[]);
            let pref2 = prefix_cache.get(&key2).unwrap_or(&[]);
            if !pref1.is_empty() && !pref2.is_empty() {
                if verbose {
                    print_heavy_verbose_dense_alignment(
                        "Stage C: 60-Second Early-Exit Window (-t 60)",
                        pref1,
                        pref2,
                        pref1.len() as u64,
                        pref2.len() as u64,
                        args.max_hamming_distance,
                        args.max_slide_offset_secs,
                    );
                }
                let (pref_conf, pref_pts, pref_rt, pref_offset) = compare_dense_1fps_hashes(
                    pref1,
                    pref2,
                    pref1.len() as u64,
                    pref2.len() as u64,
                    args.max_hamming_distance,
                    args.max_slide_offset_secs,
                );
                if pref_conf < 0.50 {
                    early_exit_60s_rejected_count += 1;
                    telemetry
                        .funnel
                        .pass2_stage_c_60s_rejected
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    let avoided_ms = (((v1.runtime_exact_secs - EARLY_EXIT_DURATION_SECS as f64)
                        .max(0.0)
                        + (v2.runtime_exact_secs - EARLY_EXIT_DURATION_SECS as f64).max(0.0))
                        * 1000.0)
                        .round() as u64;
                    telemetry
                        .funnel
                        .avoided_ms_stage_c_60s
                        .fetch_add(avoided_ms, AtomicOrdering::Relaxed);
                    if verbose {
                        eprintln!(
                            "  [Verbose][Pass 2 ({}/{})][60s EARLY-EXIT REJECT] {:?} != {:?} (first 60s matched only {}/{}s = {:.1}% at offset {:+}s; skipping full {}s video decode!)",
                            pair_idx + 1,
                            total_pairs_to_eval,
                            v1.file_stem,
                            v2.file_stem,
                            pref_pts,
                            pref_rt,
                            pref_conf * 100.0,
                            pref_offset,
                            min_rt_secs
                        );
                    }
                    let eval_entry = PairEvaluationLog {
                        file1: v1.path.clone(),
                        file2: v2.path.clone(),
                        initial_category: pair.category.to_string(),
                        final_outcome: "Confirmed distinct file pair".to_string(),
                        confidence: pref_conf,
                        matched_frames: pref_pts,
                        compared_runtime_secs: pref_rt,
                        best_offset_secs: pref_offset,
                        detection_method: "60s Early-Exit 1-fps dHash filter (-t 60)".to_string(),
                    };
                    db_writer.submit(DbWriteOp::UpsertPairScore {
                        pair_key,
                        log: eval_entry,
                    });
                    continue;
                }
            }
        }

        // Stage D: Lazy Full 1-fps extraction dispatched concurrently to DecodePipeline (i and j overlap!)
        let mut dense_jobs_submitted = 0usize;
        for &(idx, ref fkey) in &[(i, key1.clone()), (j, key2.clone())] {
            if dense_cache.get_or_load_dense(fkey, &session_db)?.is_none() {
                let rec = &records_arc[idx];
                if verbose {
                    eprintln!(
                        "  [Verbose][Stage D: Full 1-fps Decode] Dispatching full 1-fps stream ({}) for {:?}...",
                        verified_env.summary_label(),
                        rec.file_stem
                    );
                }
                pipeline.submit(DecodeJob {
                    record_idx: idx,
                    file_key: fkey.clone(),
                    path: PathBuf::from(&rec.path),
                    file_stem: rec.file_stem.clone(),
                    video_codec: rec.video_codec.clone(),
                    video_duration_secs: rec.runtime_exact_secs,
                    kind: DecodeKind::Dense1Fps,
                })?;
                dense_jobs_submitted += 1;
            }
        }

        for _ in 0..dense_jobs_submitted {
            let res = pipeline.recv()?;
            if verbose {
                eprintln!(
                    "  [Verbose][Stage D: Full 1-fps Done] {} extracted {} frames for {:?} (decode={:.2?}, dhash={:.2?})",
                    res.decoder_label,
                    res.hashes.len(),
                    records_arc[res.record_idx].file_stem,
                    res.decode_elapsed,
                    res.hash_elapsed
                );
            }
            decoder_labels_cache.insert(res.file_key.clone(), res.decoder_label);
            dense_cache.insert(res.file_key, res.hashes);
        }

        let hashes1 = dense_cache.get(&key1).map(|s| s.to_vec()).unwrap_or_default();
        let hashes2 = dense_cache.get(&key2).map(|s| s.to_vec()).unwrap_or_default();
        let dec_label = decoder_labels_cache
            .get(&key1)
            .or_else(|| decoder_labels_cache.get(&key2))
            .cloned()
            .unwrap_or_else(|| verified_env.summary_label());

        // Hand Stage E alignment + scoring + alias learning to the dedicated Pass 2 Pair Evaluator Thread!
        telemetry.eval_channel_push();
        eval_tx
            .send(Pass2EvalJob {
                pair_idx,
                total_pairs: total_pairs_to_eval,
                pair,
                pair_key,
                hashes1,
                hashes2,
                decoder_label: dec_label,
            })
            .map_err(|_| anyhow::anyhow!("Pass 2 Pair Evaluator thread disconnected"))?;
    }

    // Close pipeline & Pair Evaluator channels cleanly and join all worker threads
    drop(eval_tx);
    let pass2_eval_res = pass2_eval_handle
        .join()
        .map_err(|_| anyhow::anyhow!("Pass 2 Pair Evaluator thread panicked"))?;

    for (u, v) in pass2_eval_res.confirmed_edges {
        uf.union(u, v);
    }
    confirmed_pairs_count += pass2_eval_res.confirmed_count;
    let mut final_creator_dir = pass2_eval_res.creator_dir;

    pipeline.shutdown();

    eprintln!(
        "[Pass 2] Completed pair confirmation in {:.2?} (skipped {} via Pass-1 Strong fast-path, rejected {} via 60s early-exit, recovered {} from SQLite3). Total confirmed duplicate pairs: {}.",
        pass2_start.elapsed(),
        pass1_eval.strong_fastpath_confirmed,
        early_exit_60s_rejected_count,
        recovered_pairs_count,
        confirmed_pairs_count
    );

    if pass2_eval_res.learned_aliases_count > 0 {
        let _ = final_creator_dir.save_atomic(&creator_dir_path);
        eprintln!(
            "[Creator Directory] Saved {} newly learned artist/studio alias link(s) to {:?}.",
            pass2_eval_res.learned_aliases_count, creator_dir_path
        );
    }

    // Commit final SQLite3 WAL mutations and checkpoint cleanly
    telemetry.set_phase("output");
    db_writer.shutdown();

    // Stream optional `--categories-log` directly from SQLite3 in constant RAM
    if let Some(cat_log_path) = &args.categories_log {
        let exported = session_db.stream_categories_log_json(cat_log_path)?;
        eprintln!(
            "Streamed {} evaluated pair record(s) from SQLite3 WAL to {:?}.",
            exported, cat_log_path
        );
    }

    // Group connected components of confirmed duplicates into final JSON output
    let mut clusters: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for idx in 0..n {
        let root = uf.find(idx);
        clusters.entry(root).or_default().push(idx);
    }

    let mut duplicates_list: Vec<DuplicateGroupOutput> = Vec::new();
    for (_root, mut member_indices) in clusters {
        if member_indices.len() < 2 {
            continue;
        }
        member_indices.sort_by(|&a, &b| records_arc[a].path.cmp(&records_arc[b].path));

        let dup_group_name = member_indices
            .iter()
            .map(|&idx| records_arc[idx].file_stem.as_str())
            .collect::<Vec<_>>()
            .join("==");

        if verbose {
            eprintln!(
                "[Verbose][Group] Formed duplicate group {:?} with {} files",
                dup_group_name,
                member_indices.len()
            );
        }

        let mut files: Vec<String> = Vec::with_capacity(member_indices.len());
        let mut attributes: BTreeMap<String, VideoAttributes> = BTreeMap::new();

        for &idx in &member_indices {
            let rec = &records_arc[idx];
            files.push(rec.path.clone());
            attributes.insert(
                rec.path.clone(),
                VideoAttributes {
                    size_in_bytes: rec.size_in_bytes,
                    runtime_in_seconds: rec.runtime_in_seconds,
                    video_height: rec.video_height,
                    video_width: rec.video_width,
                    video_codec: rec.video_codec.clone(),
                    audio_codec: rec.audio_codec.clone(),
                    video_bitrate_bps: rec.video_bitrate_bps,
                    audio_bitrate_bps: rec.audio_bitrate_bps,
                },
            );
        }

        duplicates_list.push(DuplicateGroupOutput {
            dup_group_name,
            files,
            attributes,
        });
    }

    let final_report = DuplicateReportOutput {
        input_directories: canonical_input_dirs.clone(),
        duplicates: duplicates_list,
    };

    let output_json = serde_json::to_string_pretty(&final_report)?;
    fs::write(&args.output, &output_json)
        .with_context(|| format!("Failed to write output JSON to {:?}", args.output))?;

    telemetry.set_phase("done");
    telemetry_reporter.shutdown();

    eprintln!(
        "=== Done in {:.2?}! Found {} duplicate group(s). Output saved to {:?} ===",
        start_time.elapsed(),
        final_report.duplicates.len(),
        args.output
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::{FfmpegConfig, HwBackend};
    use db::{
        LegacyJsonSessionState, decode_u64_blob, encode_u64_blob,
    };
    use ffmpeg::{
        FfmpegCapabilities, VerifiedDecodeEnv, build_gpu_ffmpeg_args, compute_dhash_from_9x8,
        downscale_luma_frame_to_9x8, downscale_luma_to_9x8_and_dhash,
        format_gpu_fast_fail_report, is_fatal_driver_error,
    };

    #[test]
    fn repo_dedup_config_yaml_parses_cleanly_with_72x72_and_all_vendors() {
        let content = fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("dedup_config.yaml"),
        )
        .expect("dedup_config.yaml should exist");
        let cfg = DedupConfig::from_yaml_str(&content).expect("repo dedup_config.yaml should parse");
        assert_eq!(cfg.version, "1.0");
        assert_eq!(cfg.ffmpeg.threads, Some(0));
        assert_eq!(cfg.ffmpeg.filter_threads, Some(0));
        assert!(cfg.ffmpeg.skip_audio);
        assert!(cfg.ffmpeg.skip_subtitles);
        assert!(cfg.ffmpeg.skip_datastreams);
        assert!(cfg.ffmpeg.hardware_acceleration.enabled);
        assert_eq!(cfg.ffmpeg.hardware_acceleration.hw_scale_width, 72);
        assert_eq!(cfg.ffmpeg.hardware_acceleration.hw_scale_height, 72);
        assert_eq!(
            cfg.ffmpeg.hardware_acceleration.fallback_order,
            HwBackend::default_fallback_order()
        );
    }

    #[test]
    fn config_rejects_odd_or_tiny_hw_scale_dimensions_and_unknown_backends() {
        let odd_yaml = r#"
version: 1.0
ffmpeg:
  hardware_acceleration:
    enabled: true
    hw_scale_width: 9
    hw_scale_height: 8
"#;
        assert!(DedupConfig::from_yaml_str(odd_yaml).is_err());

        let bad_backend_yaml = r#"
version: 1.0
ffmpeg:
  hardware_acceleration:
    enabled: true
    fallback_order: [videotoolbox, nonexistent_gpu]
"#;
        assert!(DedupConfig::from_yaml_str(bad_backend_yaml).is_err());
    }

    #[test]
    fn downscale_72x72_exact_integer_and_32x32_fractional_match_expected_dhash() {
        // Horizontal gradient from left (0) to right (255) -> every row increases strictly left->right
        // So all 64 horizontal gradient bits MUST be 1 (u64::MAX)!
        let mut frame_72 = vec![0u8; 72 * 72];
        for y in 0..72 {
            for x in 0..72 {
                frame_72[y * 72 + x] = (x * 3) as u8;
            }
        }
        let grid_72 = downscale_luma_frame_to_9x8(&frame_72, 72, 72);
        assert_eq!(compute_dhash_from_9x8(&grid_72), u64::MAX);

        let hashes = downscale_luma_to_9x8_and_dhash(&frame_72, 72, 72);
        assert_eq!(hashes, vec![u64::MAX]);

        // Also test arbitrary even dimensions (32x32 fractional path)
        let mut frame_32 = vec![0u8; 32 * 32];
        for y in 0..32 {
            for x in 0..32 {
                frame_32[y * 32 + x] = (x * 7) as u8;
            }
        }
        let grid_32 = downscale_luma_frame_to_9x8(&frame_32, 32, 32);
        assert_eq!(compute_dhash_from_9x8(&grid_32), u64::MAX);
    }

    #[test]
    fn cross_platform_decoder_and_ffmpeg_arg_selection() {
        let mut caps = FfmpegCapabilities::default();
        for dec in [
            "h264",
            "hevc",
            "av1",
            "libdav1d",
            "vp9",
            "h264_cuvid",
            "av1_qsv",
            "h264_v4l2m2m",
            "hevc_rkmpp",
            "h264_mediacodec",
        ] {
            caps.decoders.insert(dec.to_string());
        }
        for flt in [
            "hwdownload",
            "scale_vt",
            "scale_cuda",
            "scale_qsv",
            "scale_vaapi",
            "scale_vulkan",
        ] {
            caps.filters.insert(flt.to_string());
        }

        let env = VerifiedDecodeEnv {
            caps,
            active_hw: None,
            scale_width: 72,
            scale_height: 72,
            ffmpeg_cfg: FfmpegConfig::default(),
        };

        // Fastest CPU decoder picks libdav1d for AV1
        assert_eq!(env.fastest_cpu_decoder_for("AV1"), Some("libdav1d"));
        assert_eq!(env.fastest_cpu_decoder_for("VP9"), Some("vp9"));

        // NVIDIA CUDA picks h264_cuvid + NVDEC -resize 72x72
        let (cuda_args, cuda_label) = build_gpu_ffmpeg_args(
            Path::new("clip.mp4"),
            "H264",
            DecodeKind::Dense1Fps,
            HwBackend::Cuda,
            true,
            &env,
        );
        let cuda_strs: Vec<String> = cuda_args
            .iter()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert!(cuda_strs.contains(&"h264_cuvid".to_string()));
        assert!(cuda_strs.contains(&"-resize".to_string()));
        assert!(cuda_strs.contains(&"72x72".to_string()));
        assert!(cuda_label.contains("cuvid-resize(72x72)"));

        // Apple VideoToolbox picks videotoolbox_vld + scale_vt=w=72:h=72
        let (vt_args, _) = build_gpu_ffmpeg_args(
            Path::new("clip.mp4"),
            "H265",
            DecodeKind::Prefix1Fps { max_secs: 60 },
            HwBackend::VideoToolbox,
            true,
            &env,
        );
        let vt_strs: Vec<String> = vt_args
            .iter()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert!(vt_strs.contains(&"videotoolbox_vld".to_string()));
        assert!(vt_strs.iter().any(|s| s.contains("scale_vt=w=72:h=72")));

        // Linux VAAPI injects -vaapi_device and scale_vaapi
        let (vaapi_args, _) = build_gpu_ffmpeg_args(
            Path::new("clip.mp4"),
            "H264",
            DecodeKind::Dense1Fps,
            HwBackend::Vaapi,
            true,
            &env,
        );
        let vaapi_strs: Vec<String> = vaapi_args
            .iter()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert!(vaapi_strs.contains(&"-vaapi_device".to_string()));
        assert!(vaapi_strs.iter().any(|s| s.contains("scale_vaapi=w=72:h=72")));

        // Legacy WMV1 is NOT marked GPU-supported so it routes cleanly to CPU worker
        assert!(!env.is_codec_gpu_supported(HwBackend::VideoToolbox, "WMV1"));
        assert!(env.is_codec_gpu_supported(HwBackend::D3d12va, "AV1"));
        assert_eq!(
            env.gpu_decoder_for(HwBackend::Rkmpp, "H265").as_deref(),
            Some("hevc_rkmpp")
        );
        assert_eq!(
            env.gpu_decoder_for(HwBackend::MediaCodec, "H264").as_deref(),
            Some("h264_mediacodec")
        );
    }

    #[test]
    fn driver_error_classifier_and_fast_fail_warning_output() {
        assert!(ffmpeg::hwaccel_init_failed_in_stderr(
            "[h264 @ 0x1] Failed setup for format videotoolbox_vld: hwaccel initialisation returned error."
        ));
        assert!(is_fatal_driver_error(
            "[Vulkan] Unable to open the libvulkan library! Device creation failed"
        ));
        assert!(is_fatal_driver_error("Cannot load libcuda.so.1"));
        assert!(!is_fatal_driver_error(
            "[h264] Unsupported profile Hi10P for hardware decoding"
        ));

        let report = format_gpu_fast_fail_report(
            &[(HwBackend::VideoToolbox, "session failed".to_string())],
            &[HwBackend::Cuda],
            72,
            72,
        );
        assert!(report.contains("WARNING"));
        assert!(report.contains("hw_scale_width"));
        assert!(report.contains("https://trac.ffmpeg.org/wiki/HWAccelIntro#VideoToolbox"));
        assert!(report.contains("hardware_acceleration.enabled: false"));
    }

    #[test]
    fn sqlite3_wal_roundtrip_blob_lru_legacy_migration_and_streaming_export() {
        let tmp_dir = std::env::temp_dir().join(format!("rusty_dedup_db_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp_dir);
        fs::create_dir_all(&tmp_dir).unwrap();

        let blob = encode_u64_blob(&[0x1122334455667788, 0xAABBCCDDEEFF0011]);
        assert_eq!(blob.len(), 16);
        assert_eq!(
            decode_u64_blob(&blob),
            vec![0x1122334455667788, 0xAABBCCDDEEFF0011]
        );

        let legacy_path = tmp_dir.join(".rusty_temp_abc");
        let rec = VideoRecord {
            path: "/tmp/a.mp4".to_string(),
            file_stem: "a".to_string(),
            normalized_stem: "a".to_string(),
            detected_creator: None,
            subject_title: "a".to_string(),
            sequel_markers: vec![],
            container_ext: "mp4".to_string(),
            dev_id: 1,
            inode: 42,
            mtime_secs: 100,
            size_in_bytes: 5000,
            runtime_exact_secs: 120.5,
            runtime_in_seconds: 121,
            video_height: 1080,
            video_width: 1920,
            video_codec: "H264".to_string(),
            audio_codec: "AAC".to_string(),
            video_bitrate_bps: 4_500_000,
            audio_bitrate_bps: 192_000,
            bitrate_probed: true,
            blake3_sample_hash: Some("deadbeef".to_string()),
            sparse_hashes: vec![1, 2, 3, 4, 5],
            prefix_1fps_hashes: Some(vec![10, 20, 30]),
            dense_1fps_hashes: Some(vec![100, 200, 300, 400]),
        };
        let fkey = rec.file_key();
        let mut file_records = BTreeMap::new();
        file_records.insert(fkey.clone(), rec);
        let mut pair_scores = BTreeMap::new();
        pair_scores.insert(
            "p1".to_string(),
            PairEvaluationLog {
                file1: "/tmp/a.mp4".to_string(),
                file2: "/tmp/b.mp4".to_string(),
                initial_category: "Strong".to_string(),
                final_outcome: "Confirmed duplicate file pair".to_string(),
                confidence: 0.99,
                matched_frames: 120,
                compared_runtime_secs: 121,
                best_offset_secs: 0,
                detection_method: "test".to_string(),
            },
        );
        let legacy = LegacyJsonSessionState {
            session_id: "abc".to_string(),
            canonical_input_directories: vec!["/tmp".to_string()],
            last_updated_epoch_secs: 1234,
            file_records,
            pair_scores,
        };
        fs::write(&legacy_path, serde_json::to_string(&legacy).unwrap()).unwrap();

        let db_path = tmp_dir.join("rusty_session_abc.db");
        let mut db = SessionDb::open_or_create(&db_path, "abc", &["/tmp".to_string()], false)
            .expect("open db");
        let migrated = db
            .maybe_migrate_legacy_json(&legacy_path)
            .expect("migrate")
            .expect("should migrate");
        assert_eq!(migrated, (1, 1));

        let mut prefix_cache = BoundedHashCache::new(16);
        let mut dense_cache = BoundedHashCache::new(16);
        assert_eq!(
            prefix_cache.get_or_load_prefix(&fkey, &db).unwrap(),
            Some([10u64, 20, 30].as_slice())
        );
        assert_eq!(
            dense_cache.get_or_load_dense(&fkey, &db).unwrap(),
            Some([100u64, 200, 300, 400].as_slice())
        );
        // Verify bounded LRU eviction keeps capacity bounded
        for k in 0..20 {
            dense_cache.insert(format!("k{}", k), vec![k as u64]);
        }
        assert!(!dense_cache.contains(&fkey));
        // Evicted key reloads cleanly from SQLite3 BLOB
        assert_eq!(
            dense_cache.get_or_load_dense(&fkey, &db).unwrap(),
            Some([100u64, 200, 300, 400].as_slice())
        );

        let export_path = tmp_dir.join("cat_log.json");
        let exported = db.stream_categories_log_json(&export_path).unwrap();
        assert_eq!(exported, 1);
        let parsed_logs: Vec<PairEvaluationLog> =
            serde_json::from_str(&fs::read_to_string(&export_path).unwrap()).unwrap();
        assert_eq!(parsed_logs.len(), 1);
        assert_eq!(parsed_logs[0].file1, "/tmp/a.mp4");

        let _ = fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn runtime_sorted_sliding_window_matches_bruteforce_exactly() {
        let runtimes = [5.0, 12.0, 14.9, 30.0, 99.0, 108.0, 110.0, 125.0, 500.0, 549.0, 560.0];
        let mut recs: Vec<VideoRecord> = runtimes
            .iter()
            .enumerate()
            .map(|(i, &rt)| VideoRecord {
                path: format!("/v/{}.mp4", i),
                file_stem: format!("v{}", i),
                normalized_stem: format!("v{}", i),
                detected_creator: None,
                subject_title: format!("v{}", i),
                sequel_markers: vec![],
                container_ext: "mp4".to_string(),
                dev_id: 1,
                inode: i as u64,
                mtime_secs: 0,
                size_in_bytes: (i as u64 + 1) * 1000,
                runtime_exact_secs: rt,
                runtime_in_seconds: rt.round() as u64,
                video_height: 720,
                video_width: 1280,
                video_codec: "H264".to_string(),
                audio_codec: "AAC".to_string(),
                video_bitrate_bps: 2_500_000,
                audio_bitrate_bps: 128_000,
                bitrate_probed: true,
                blake3_sample_hash: None,
                sparse_hashes: vec![],
                prefix_1fps_hashes: None,
                dense_1fps_hashes: None,
            })
            .collect();
        recs.sort_by(|a, b| a.runtime_exact_secs.total_cmp(&b.runtime_exact_secs));

        let fast = mark_videos_needing_sparse_keyframes(&recs, 10.0);
        let mut brute = vec![false; recs.len()];
        for i in 0..recs.len() {
            for j in (i + 1)..recs.len() {
                if is_within_runtime_window(
                    recs[i].runtime_exact_secs,
                    recs[j].runtime_exact_secs,
                    10.0,
                ) {
                    brute[i] = true;
                    brute[j] = true;
                }
            }
        }
        assert_eq!(fast, brute);
    }

    #[test]
    fn telemetry_config_opt_in_prometheus_defaults_and_overrides() {
        // Built-in defaults keep Prometheus endpoints opt-in (disabled)
        let default_cfg = DedupConfig::default();
        assert!(default_cfg.telemetry.enabled);
        assert_eq!(default_cfg.telemetry.heartbeat_interval_secs, 30);
        assert_eq!(default_cfg.telemetry.snapshot_path, None);
        assert_eq!(default_cfg.telemetry.prometheus.listen_addr, None);
        assert_eq!(default_cfg.telemetry.prometheus.pushgateway_url, None);
        assert_eq!(default_cfg.telemetry.prometheus.push_interval_secs, 15);
        assert_eq!(default_cfg.telemetry.prometheus.job_name, "rusty_dedup");
        assert_eq!(default_cfg.telemetry.prometheus.textfile_path, None);

        // Repo dedup_config.yaml must always parse cleanly regardless of user's local listen_addr
        let content = fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("dedup_config.yaml"),
        )
        .expect("dedup_config.yaml should exist");
        let cfg = DedupConfig::from_yaml_str(&content).expect("repo dedup_config.yaml should parse");
        assert!(cfg.telemetry.enabled);
        assert_eq!(cfg.telemetry.prometheus.job_name, "rusty_dedup");

        let custom = r#"
version: 1.0
telemetry:
  enabled: true
  heartbeat_interval_secs: 10
  snapshot_path: /tmp/rusty_dedup_snap.json
  prometheus:
    listen_addr: 127.0.0.1:9464
    pushgateway_url: http://127.0.0.1:9091
    push_interval_secs: 5
    job_name: media_scanner
    textfile_path: /tmp/rusty_dedup.prom
"#;
        let cfg2 = DedupConfig::from_yaml_str(custom).expect("custom telemetry yaml");
        assert_eq!(cfg2.telemetry.heartbeat_interval_secs, 10);
        assert_eq!(
            cfg2.telemetry.snapshot_path,
            Some(PathBuf::from("/tmp/rusty_dedup_snap.json"))
        );
        assert_eq!(
            cfg2.telemetry.prometheus.listen_addr.as_deref(),
            Some("127.0.0.1:9464")
        );
        assert_eq!(
            cfg2.telemetry.prometheus.pushgateway_url.as_deref(),
            Some("http://127.0.0.1:9091")
        );
        assert_eq!(cfg2.telemetry.prometheus.push_interval_secs, 5);
        assert_eq!(cfg2.telemetry.prometheus.job_name, "media_scanner");
        assert_eq!(
            cfg2.telemetry.prometheus.textfile_path,
            Some(PathBuf::from("/tmp/rusty_dedup.prom"))
        );
    }

    #[test]
    fn atomic_prom_histogram_concurrent_accuracy_and_exposition_format() {
        use std::time::Duration;
        let hist = Arc::new(telemetry::AtomicPromHistogram::new());
        let mut threads = Vec::new();
        for _ in 0..4 {
            let h = Arc::clone(&hist);
            threads.push(thread::spawn(move || {
                for i in 0..5000u64 {
                    // Mix of 0.5ms and 10ms samples
                    let us = if i % 2 == 0 { 500 } else { 10_000 };
                    h.observe(Duration::from_micros(us));
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        let snap = hist.snapshot();
        assert_eq!(snap.count, 20_000);
        // Cumulative buckets must be monotonically non-decreasing and <= count
        for i in 1..snap.cumulative_buckets.len() {
            assert!(snap.cumulative_buckets[i] >= snap.cumulative_buckets[i - 1]);
        }
        assert_eq!(
            snap.cumulative_buckets[telemetry::PROM_BUCKET_COUNT - 1],
            20_000
        );
        assert!(snap.avg_ms() > 4.5 && snap.avg_ms() < 6.0);
        assert!(snap.quantile_ms(0.95) >= 5.0 && snap.quantile_ms(0.95) <= 10.5);

        let reg = Arc::new(TelemetryRegistry::new());
        reg.set_phase("pass2_eval");
        reg.record_thread_busy(ThreadRole::GpuDecodeWorker, Duration::from_millis(500));
        reg.record_thread_backpressured(ThreadRole::GpuDecodeWorker, Duration::from_millis(200));
        reg.record_thread_job(ThreadRole::GpuDecodeWorker, Duration::from_millis(50));
        reg.record_decode("gpu", "H264", "dense", 60, 60.0, Duration::from_millis(500), false);
        reg.record_dhash_batch(60, true, Duration::from_micros(600));

        let meta = RunMeta {
            version: "0.5.0".to_string(),
            session_id: "sess123".to_string(),
            hw_backend: "videotoolbox".to_string(),
            hw_scale: "72x72".to_string(),
        };
        let body = prometheus::render_prometheus_exposition(&reg, &meta, Path::new("/tmp/none.db"));
        assert!(body.contains("# TYPE rusty_dedup_thread_time_seconds_total counter"));
        assert!(body.contains("rusty_dedup_info{version=\"0.5.0\",session_id=\"sess123\",hw_backend=\"videotoolbox\",hw_scale=\"72x72\"} 1"));
        assert!(body.contains("rusty_dedup_phase{phase=\"pass2_eval\"} 1"));
        assert!(body.contains("rusty_dedup_thread_job_duration_seconds_bucket{thread=\"gpu_decode_worker\",le=\"+Inf\"} 1"));
        assert!(body.contains("rusty_dedup_decode_video_seconds_total{engine=\"gpu\",codec=\"H264\",kind=\"dense\"} 60.000"));
        assert!(body.contains("rusty_dedup_dhash_frames_total{path=\"integer_exact\"} 60"));

        // Bottleneck advisor should flag gpu_decode_worker backpressured > 10%
        let summary = reg.build_summary(&meta, Path::new("/tmp/none.db"));
        assert!(
            summary
                .bottleneck_advisor
                .iter()
                .any(|n| n.contains("backpressured"))
        );
    }

    #[test]
    fn prometheus_http_protocol_scrape_response_and_pushgateway_put_request() {
        let reg = Arc::new(TelemetryRegistry::new());
        reg.set_phase("pass1_ffprobe");
        let meta = RunMeta {
            version: "0.5.0".to_string(),
            session_id: "test_sess".to_string(),
            hw_backend: "cpu".to_string(),
            hw_scale: "72x72".to_string(),
        };

        let metrics_resp = prometheus::build_scrape_http_response(
            b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
            &reg,
            &meta,
            Path::new("/tmp/none.db"),
        );
        assert!(metrics_resp.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(metrics_resp.contains("text/plain; version=0.0.4"));
        assert!(metrics_resp.contains("rusty_dedup_phase{phase=\"pass1_ffprobe\"} 1"));

        let health_resp = prometheus::build_scrape_http_response(
            b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n",
            &reg,
            &meta,
            Path::new("/tmp/none.db"),
        );
        assert!(health_resp.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(health_resp.ends_with("\r\n\r\nok\n"));

        let not_found = prometheus::build_scrape_http_response(
            b"GET /unknown HTTP/1.1\r\nHost: localhost\r\n\r\n",
            &reg,
            &meta,
            Path::new("/tmp/none.db"),
        );
        assert!(not_found.starts_with("HTTP/1.1 404 Not Found\r\n"));

        let payload = prometheus::render_prometheus_exposition(&reg, &meta, Path::new("/tmp/none.db"));
        let (addr, put_req) = prometheus::build_pushgateway_put_request(
            "http://127.0.0.1:9091",
            "rusty_dedup",
            "test_sess",
            &payload,
        )
        .expect("build put request");
        assert_eq!(addr, "127.0.0.1:9091");
        assert!(put_req.starts_with("PUT /metrics/job/rusty_dedup/instance/test_sess HTTP/1.1\r\n"));
        assert!(put_req.contains("rusty_dedup_info{"));

        // HTTPS Pushgateway returns a clean explicit error (local HTTP only)
        assert!(
            prometheus::build_pushgateway_put_request(
                "https://push.example.com",
                "rusty_dedup",
                "test_sess",
                &payload
            )
            .is_err()
        );
    }

    #[test]
    #[ignore = "binds local loopback TCP socket; run with -- --ignored outside network-restricted sandboxes"]
    fn prometheus_live_loopback_scrape_and_pushgateway() {
        use std::io::{Read as IoRead, Write as IoWrite};
        use std::net::{TcpListener, TcpStream};

        let reg = Arc::new(TelemetryRegistry::new());
        reg.set_phase("pass1_ffprobe");
        let meta = RunMeta {
            version: "0.5.0".to_string(),
            session_id: "test_sess".to_string(),
            hw_backend: "cpu".to_string(),
            hw_scale: "72x72".to_string(),
        };

        let srv = prometheus::PrometheusScrapeServer::start(
            "127.0.0.1:0",
            Arc::clone(&reg),
            meta.clone(),
            PathBuf::from("/tmp/none.db"),
        )
        .expect("start scrape server");
        let addr = srv.local_addr();

        let mut client = TcpStream::connect(addr).expect("connect scrape server");
        client
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut resp = String::new();
        client.read_to_string(&mut resp).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(resp.contains("text/plain; version=0.0.4"));
        assert!(resp.contains("rusty_dedup_phase{phase=\"pass1_ffprobe\"} 1"));
        srv.shutdown();

        let mock_pg = TcpListener::bind("127.0.0.1:0").expect("bind mock pushgateway");
        let pg_addr = mock_pg.local_addr().unwrap();
        let pg_thread = thread::spawn(move || {
            let (mut s, _) = mock_pg.accept().unwrap();
            let mut buf = vec![0u8; 65536];
            let n = s.read(&mut buf).unwrap();
            let req = String::from_utf8_lossy(&buf[..n]).to_string();
            s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
            req
        });

        let payload = prometheus::render_prometheus_exposition(&reg, &meta, Path::new("/tmp/none.db"));
        let pg_url = format!("http://{}", pg_addr);
        prometheus::push_to_pushgateway(&pg_url, "rusty_dedup", "test_sess", &payload)
            .expect("push to mock pushgateway");
        let received = pg_thread.join().unwrap();
        assert!(received.starts_with("PUT /metrics/job/rusty_dedup/instance/test_sess HTTP/1.1\r\n"));
        assert!(received.contains("rusty_dedup_info{"));
    }

    #[test]
    fn ffprobe_json_bitrate_extraction_all_containers_and_fallbacks() {
        // 1. MP4/MOV explicit stream.bit_rate + attached cover art skipped
        let mp4_json = serde_json::json!({
            "format": { "duration": "120.5", "bit_rate": "5200000" },
            "streams": [
                {
                    "codec_type": "video",
                    "codec_name": "mjpeg",
                    "width": 300,
                    "height": 300,
                    "disposition": { "attached_pic": 1 }
                },
                {
                    "codec_type": "video",
                    "codec_name": "hevc",
                    "width": 1920,
                    "height": 1080,
                    "bit_rate": "4800000",
                    "disposition": { "attached_pic": 0 }
                },
                {
                    "codec_type": "audio",
                    "codec_name": "aac",
                    "bit_rate": "192000"
                }
            ]
        });
        let mp4 = parse_ffprobe_json(&mp4_json, 78_000_000);
        assert_eq!(mp4.video_codec, "H265");
        assert_eq!(mp4.audio_codec, "AAC");
        assert_eq!(mp4.video_bitrate_bps, 4_800_000);
        assert_eq!(mp4.audio_bitrate_bps, 192_000);
        assert_eq!(mp4.video_bitrate_source, "stream");
        assert_eq!(mp4.audio_bitrate_source, "stream");

        // 2. MKV/WebM stream.tags.BPS / BPS-eng fallback
        let mkv_json = serde_json::json!({
            "format": { "duration": "600.0", "bit_rate": "6400000" },
            "streams": [
                {
                    "codec_type": "video",
                    "codec_name": "h264",
                    "width": 1920,
                    "height": 1080,
                    "tags": { "BPS-eng": "6000000" }
                },
                {
                    "codec_type": "audio",
                    "codec_name": "opus",
                    "tags": { "BPS": "160000" }
                }
            ]
        });
        let mkv = parse_ffprobe_json(&mkv_json, 480_000_000);
        assert_eq!(mkv.video_bitrate_bps, 6_000_000);
        assert_eq!(mkv.audio_bitrate_bps, 160_000);
        assert_eq!(mkv.video_bitrate_source, "tag_bps");
        assert_eq!(mkv.audio_bitrate_source, "tag_bps");

        // 3. Container format.bit_rate - audio_bitrate_bps fallback
        let container_json = serde_json::json!({
            "format": { "duration": "100.0", "bit_rate": "3200000" },
            "streams": [
                { "codec_type": "video", "codec_name": "vp9", "width": 1280, "height": 720 },
                { "codec_type": "audio", "codec_name": "aac", "bit_rate": "128000" }
            ]
        });
        let cont = parse_ffprobe_json(&container_json, 40_000_000);
        assert_eq!(cont.video_bitrate_bps, 3_072_000);
        assert_eq!(cont.video_bitrate_source, "container_fallback");

        // 4. File size * 8 / duration fallback when both stream and format bit_rate are absent
        let size_json = serde_json::json!({
            "format": { "duration": "100.0" },
            "streams": [
                { "codec_type": "video", "codec_name": "mpeg4", "width": 640, "height": 480 },
                { "codec_type": "audio", "codec_name": "mp3", "bit_rate": "128000" }
            ]
        });
        // 25_000_000 bytes * 8 / 100s = 2_000_000 bps - 128_000 = 1_872_000 bps
        let sz = parse_ffprobe_json(&size_json, 25_000_000);
        assert_eq!(sz.video_bitrate_bps, 1_872_000);
        assert_eq!(sz.video_bitrate_source, "size_fallback");
    }

    #[test]
    fn sqlite3_alter_table_migration_for_bitrates_and_update_op() {
        let tmp_dir = std::env::temp_dir().join(format!(
            "rusty_dedup_bitrate_mig_{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&tmp_dir);
        fs::create_dir_all(&tmp_dir).unwrap();
        let db_path = tmp_dir.join("legacy_no_bitrate.db");

        // Create pre-enhancement schema WITHOUT video_bitrate_bps / audio_bitrate_bps / bitrate_probed
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE file_records (
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
                    blake3_sample_hash TEXT,
                    sparse_hashes BLOB NOT NULL,
                    prefix_1fps_hashes BLOB,
                    dense_1fps_hashes BLOB
                );
                INSERT INTO file_records VALUES (
                    '/old.mp4|1000|1', '/old.mp4', 'old', 'old', NULL, 'old', '[]', 'mp4',
                    1, 10, 1, 1000, 60.0, 60, 1080, 1920, 'H264', 'AAC', NULL, X'', NULL, NULL
                );",
            )
            .unwrap();
        }

        // Opening via SessionDb must idempotently ALTER TABLE and preserve existing rows with 0 bitrate & bitrate_probed=false
        let db = SessionDb::open_or_create(&db_path, "mig", &["/tmp".to_string()], false)
            .expect("open and migrate");
        let reader = db.open_readonly_handle().unwrap();
        let loaded = SessionDb::get_compact_file_record(&reader, "/old.mp4|1000|1")
            .unwrap()
            .expect("migrated row should exist");
        assert_eq!(loaded.video_bitrate_bps, 0);
        assert_eq!(loaded.audio_bitrate_bps, 0);
        assert!(!loaded.bitrate_probed);

        // UpdateBitrates persists new bitrates and flips bitrate_probed=true
        let tel = Arc::new(TelemetryRegistry::new());
        let writer = AsyncDbWriter::spawn(db_path.clone(), 100, false, tel).unwrap();
        writer.submit(DbWriteOp::UpdateBitrates {
            file_key: "/old.mp4|1000|1".to_string(),
            video_bitrate_bps: 7_500_000,
            audio_bitrate_bps: 256_000,
        });
        writer.flush();
        writer.shutdown();

        let reloaded = SessionDb::get_compact_file_record(&reader, "/old.mp4|1000|1")
            .unwrap()
            .expect("reloaded row");
        assert_eq!(reloaded.video_bitrate_bps, 7_500_000);
        assert_eq!(reloaded.audio_bitrate_bps, 256_000);
        assert!(reloaded.bitrate_probed);

        let _ = fs::remove_dir_all(&tmp_dir);
    }

    #[test]
    fn expanded_histogram_24_buckets_and_dashboard_formatting() {
        use std::time::Duration;

        // 1. Sub-millisecond SQLite hits (~20us) should NOT clamp to 0.1ms (100us)
        let fast_hist = telemetry::AtomicPromHistogram::new();
        for _ in 0..100 {
            fast_hist.observe(Duration::from_micros(20));
        }
        let fast_snap = fast_hist.snapshot();
        assert!(
            fast_snap.quantile_ms(0.50) < 0.03,
            "expected ~20us p50, got {}ms",
            fast_snap.quantile_ms(0.50)
        );

        // 2. Multi-minute GPU decodes (60s..500s) should keep p95 distinct from max!
        let slow_hist = telemetry::AtomicPromHistogram::new();
        for _ in 0..90 {
            slow_hist.observe(Duration::from_secs(90));
        }
        for _ in 0..9 {
            slow_hist.observe(Duration::from_secs(200));
        }
        slow_hist.observe(Duration::from_secs(500));
        let slow_snap = slow_hist.snapshot();
        assert!(
            slow_snap.quantile_ms(0.95) < 350_000.0,
            "p95 should not collapse to 500s max, got {}ms",
            slow_snap.quantile_ms(0.95)
        );
        assert!((slow_snap.max_secs - 500.0).abs() < 1e-6);

        // 3. Dashboard formatting checks: <0.1%, /m & /h rates, ActiveRate, and ffprobe: N/A
        assert_eq!(telemetry::format_pct(0.001, 50_000), "<0.1%");
        assert_eq!(telemetry::format_rate(280.0 / 44881.0, 280), "22.5/h");
        assert_eq!(telemetry::format_rate(1699.0 / 44881.0, 1699), "2.3/m");
        assert_eq!(telemetry::format_latency_ms(0.018), "18µs");
        assert_eq!(telemetry::format_latency_ms(13183.9), "13.18s");

        let reg = TelemetryRegistry::new();
        reg.record_thread_busy(ThreadRole::PairEvaluatorPass2, Duration::from_millis(56));
        reg.record_thread_starved(ThreadRole::PairEvaluatorPass2, Duration::from_secs(44881));
        for _ in 0..280 {
            reg.record_thread_job(ThreadRole::PairEvaluatorPass2, Duration::from_micros(200));
        }
        reg.record_volume_sqlite_hit(16777238, Duration::from_micros(20), 0);

        let meta = RunMeta {
            version: "0.5.0".to_string(),
            session_id: "fmt".to_string(),
            hw_backend: "videotoolbox".to_string(),
            hw_scale: "72x72".to_string(),
        };
        let summary = reg.build_summary(&meta, Path::new("/tmp/none.db"));
        let dash = telemetry::format_terminal_dashboard(&summary);
        assert!(dash.contains("ActiveRate"));
        assert!(dash.contains("<0.1%"));
        assert!(dash.contains("22.5/h"));
        assert!(dash.contains("ffprobe: N/A (100% SQLite cache hit)"));
    }
}
