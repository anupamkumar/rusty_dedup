use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Fixed Prometheus-aligned histogram bucket upper bounds (in seconds, 10µs .. 1800s / 30 min).
/// Covers microsecond SQLite/Pass-1 lookups through multi-minute 4K/H.265 full-video decodes.
pub const PROM_BUCKET_COUNT: usize = 24;

pub const PROM_BUCKET_BOUNDS_SECS: [f64; PROM_BUCKET_COUNT] = [
    0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 15.0, 60.0, 120.0, 300.0, 600.0, 1800.0,
];

const PROM_BUCKET_BOUNDS_NS: [u64; PROM_BUCKET_COUNT] = [
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    2_500_000_000,
    5_000_000_000,
    15_000_000_000,
    60_000_000_000,
    120_000_000_000,
    300_000_000_000,
    600_000_000_000,
    1_800_000_000_000,
];

/// Lock-free, zero-allocation Prometheus-aligned latency histogram.
/// Records in ~3 ns using `AtomicU64` with `Ordering::Relaxed`.
pub struct AtomicPromHistogram {
    /// Non-cumulative per-bucket counts (`0..24` correspond to `PROM_BUCKET_BOUNDS_NS`, `24` is `+Inf` overflow).
    buckets: [AtomicU64; PROM_BUCKET_COUNT + 1],
    count: AtomicU64,
    sum_ns: AtomicU64,
    min_ns: AtomicU64,
    max_ns: AtomicU64,
}

impl Default for AtomicPromHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl AtomicPromHistogram {
    pub fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
            min_ns: AtomicU64::new(u64::MAX),
            max_ns: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn observe(&self, d: Duration) {
        self.observe_ns(d.as_nanos().min(u64::MAX as u128) as u64);
    }

    #[inline]
    pub fn observe_ns(&self, ns: u64) {
        let mut idx = PROM_BUCKET_COUNT;
        for (i, &bound) in PROM_BUCKET_BOUNDS_NS.iter().enumerate() {
            if ns <= bound {
                idx = i;
                break;
            }
        }
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        self.min_ns.fetch_min(ns, Ordering::Relaxed);
        self.max_ns.fetch_max(ns, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut raw = [0u64; PROM_BUCKET_COUNT + 1];
        let mut cum = [0u64; PROM_BUCKET_COUNT];
        let mut running = 0u64;
        for i in 0..=PROM_BUCKET_COUNT {
            raw[i] = self.buckets[i].load(Ordering::Relaxed);
            running += raw[i];
            if i < PROM_BUCKET_COUNT {
                cum[i] = running;
            }
        }
        let count = running;
        let sum_ns = self.sum_ns.load(Ordering::Relaxed);
        let min_ns = if count == 0 {
            0
        } else {
            let m = self.min_ns.load(Ordering::Relaxed);
            if m == u64::MAX { 0 } else { m }
        };
        let max_ns = self.max_ns.load(Ordering::Relaxed);

        HistogramSnapshot {
            cumulative_buckets: cum,
            count,
            sum_secs: sum_ns as f64 / 1e9,
            min_secs: min_ns as f64 / 1e9,
            max_secs: max_ns as f64 / 1e9,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HistogramSnapshot {
    pub cumulative_buckets: [u64; PROM_BUCKET_COUNT],
    pub count: u64,
    pub sum_secs: f64,
    pub min_secs: f64,
    pub max_secs: f64,
}

impl HistogramSnapshot {
    #[cfg(test)]
    pub fn avg_ms(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            (self.sum_secs / self.count as f64) * 1000.0
        }
    }

    /// Estimates quantile `q` (in `[0.0, 1.0]`) using linear interpolation within the bucket (matching Prometheus `histogram_quantile`).
    pub fn quantile_ms(&self, q: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let rank = q.clamp(0.0, 1.0) * (self.count as f64);
        let mut prev_bound = 0.0f64;
        let mut prev_count = 0u64;
        for i in 0..PROM_BUCKET_COUNT {
            let c = self.cumulative_buckets[i];
            let bound = PROM_BUCKET_BOUNDS_SECS[i];
            if (c as f64) >= rank {
                let bucket_count = c.saturating_sub(prev_count);
                let frac = if bucket_count == 0 {
                    0.0
                } else {
                    (rank - prev_count as f64) / (bucket_count as f64)
                };
                let est_secs = prev_bound + (bound - prev_bound) * frac.clamp(0.0, 1.0);
                return est_secs.min(self.max_secs) * 1000.0;
            }
            prev_bound = bound;
            prev_count = c;
        }
        self.max_secs * 1000.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum ThreadRole {
    FfprobeWorkers = 0,
    GpuDecodeWorker = 1,
    CpuDecodeWorker = 2,
    CpuDhashWorker = 3,
    PairEvaluatorPass1 = 4,
    PairEvaluatorPass2 = 5,
    SqliteWalWriter = 6,
}

pub const ALL_THREAD_ROLES: [ThreadRole; 7] = [
    ThreadRole::FfprobeWorkers,
    ThreadRole::GpuDecodeWorker,
    ThreadRole::CpuDecodeWorker,
    ThreadRole::CpuDhashWorker,
    ThreadRole::PairEvaluatorPass1,
    ThreadRole::PairEvaluatorPass2,
    ThreadRole::SqliteWalWriter,
];

impl ThreadRole {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::FfprobeWorkers => "ffprobe_workers",
            Self::GpuDecodeWorker => "gpu_decode_worker",
            Self::CpuDecodeWorker => "cpu_decode_worker",
            Self::CpuDhashWorker => "cpu_dhash_worker",
            Self::PairEvaluatorPass1 => "pair_evaluator_pass1",
            Self::PairEvaluatorPass2 => "pair_evaluator_pass2",
            Self::SqliteWalWriter => "sqlite_wal_writer",
        }
    }
}

pub const ALL_PHASES: [&str; 8] = [
    "config_test",
    "walk",
    "pass1_ffprobe",
    "pass1_keyframes",
    "pass1_eval",
    "pass2_eval",
    "output",
    "done",
];

pub struct ThreadMetrics {
    pub busy_ns: AtomicU64,
    pub starved_ns: AtomicU64,
    pub backpressured_ns: AtomicU64,
    pub jobs_completed: AtomicU64,
    pub errors: AtomicU64,
    pub job_duration: AtomicPromHistogram,
}

impl Default for ThreadMetrics {
    fn default() -> Self {
        Self {
            busy_ns: AtomicU64::new(0),
            starved_ns: AtomicU64::new(0),
            backpressured_ns: AtomicU64::new(0),
            jobs_completed: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            job_duration: AtomicPromHistogram::new(),
        }
    }
}

#[derive(Default)]
pub struct CacheMetrics {
    pub hits: AtomicU64,
    pub sqlite_reloads: AtomicU64,
    pub misses_decode: AtomicU64,
    pub evictions: AtomicU64,
}

#[derive(Default)]
pub struct FunnelMetrics {
    pub pass1_strong_confirmed: AtomicU64,
    pub pass1_conflict_rejected: AtomicU64,
    pub pass2_sqlite_recovered: AtomicU64,
    pub pass2_transitive_skip: AtomicU64,
    pub pass2_blake3_exact: AtomicU64,
    pub pass2_stage_b_rejected: AtomicU64,
    pub pass2_stage_c_60s_rejected: AtomicU64,
    pub pass2_stage_e_confirmed: AtomicU64,
    pub pass2_stage_e_distinct: AtomicU64,
    /// Video milliseconds avoided from full 1-fps decode per short-circuit stage
    pub avoided_ms_pass1_isolated: AtomicU64,
    pub avoided_ms_pass1_strong: AtomicU64,
    pub avoided_ms_stage_b_sparse: AtomicU64,
    pub avoided_ms_stage_c_60s: AtomicU64,
}

pub struct VolumeStats {
    pub probed: u64,
    pub sqlite_hits: u64,
    pub bitrate_reprobes: u64,
    pub skipped_errors: u64,
    pub blake3_bytes: u64,
    pub ffprobe_duration: AtomicPromHistogram,
    pub sqlite_lookup_duration: AtomicPromHistogram,
}

impl Default for VolumeStats {
    fn default() -> Self {
        Self {
            probed: 0,
            sqlite_hits: 0,
            bitrate_reprobes: 0,
            skipped_errors: 0,
            blake3_bytes: 0,
            ffprobe_duration: AtomicPromHistogram::new(),
            sqlite_lookup_duration: AtomicPromHistogram::new(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct CodecDecodeStats {
    pub jobs: u64,
    pub frames: u64,
    pub video_secs_ms: u64,
    pub wall_secs_ms: u64,
    pub hw_scale_fallbacks: u64,
}

/// Static run metadata exported in `rusty_dedup_info` and APM summaries.
#[derive(Debug, Clone, Serialize)]
pub struct RunMeta {
    pub version: String,
    pub session_id: String,
    pub hw_backend: String,
    pub hw_scale: String,
}

/// OS-level process & child subprocess (`ffmpeg`/`ffprobe`) CPU and RSS snapshot.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ProcessResourceSnapshot {
    pub max_rss_bytes: u64,
    pub rust_user_cpu_secs: f64,
    pub rust_sys_cpu_secs: f64,
    pub children_user_cpu_secs: f64,
    pub children_sys_cpu_secs: f64,
}

#[cfg(unix)]
pub fn sample_process_resources() -> ProcessResourceSnapshot {
    #[repr(C)]
    #[derive(Default)]
    struct TimeVal {
        tv_sec: i64,
        tv_usec: i64,
    }

    // Standard POSIX `struct rusage` layout on 64-bit macOS & Linux.
    #[repr(C)]
    #[derive(Default)]
    struct RUsage {
        ru_utime: TimeVal,
        ru_stime: TimeVal,
        ru_maxrss: i64,
        ru_ixrss: i64,
        ru_idrss: i64,
        ru_isrss: i64,
        ru_minflt: i64,
        ru_majflt: i64,
        ru_nswap: i64,
        ru_inblock: i64,
        ru_oublock: i64,
        ru_msgsnd: i64,
        ru_msgrcv: i64,
        ru_nsignals: i64,
        ru_nvcsw: i64,
        ru_nivcsw: i64,
    }

    const RUSAGE_SELF: i32 = 0;
    const RUSAGE_CHILDREN: i32 = -1;

    unsafe extern "C" {
        fn getrusage(who: i32, usage: *mut RUsage) -> i32;
    }

    let mut self_ru = RUsage::default();
    let mut child_ru = RUsage::default();
    unsafe {
        let _ = getrusage(RUSAGE_SELF, &mut self_ru);
        let _ = getrusage(RUSAGE_CHILDREN, &mut child_ru);
    }

    // macOS reports `ru_maxrss` in BYTES; Linux reports `ru_maxrss` in KILOBYTES!
    #[cfg(target_os = "macos")]
    let max_rss_bytes = self_ru.ru_maxrss.max(0) as u64;
    #[cfg(not(target_os = "macos"))]
    let max_rss_bytes = (self_ru.ru_maxrss.max(0) as u64).saturating_mul(1024);

    let tv_to_secs = |tv: &TimeVal| -> f64 {
        (tv.tv_sec.max(0) as f64) + (tv.tv_usec.max(0) as f64 / 1_000_000.0)
    };

    ProcessResourceSnapshot {
        max_rss_bytes,
        rust_user_cpu_secs: tv_to_secs(&self_ru.ru_utime),
        rust_sys_cpu_secs: tv_to_secs(&self_ru.ru_stime),
        children_user_cpu_secs: tv_to_secs(&child_ru.ru_utime),
        children_sys_cpu_secs: tv_to_secs(&child_ru.ru_stime),
    }
}

#[cfg(not(unix))]
pub fn sample_process_resources() -> ProcessResourceSnapshot {
    ProcessResourceSnapshot::default()
}

pub fn sample_sqlite_file_bytes(db_path: &Path) -> (u64, u64) {
    let db_bytes = fs::metadata(db_path).map(|m| m.len()).unwrap_or(0);
    let wal_path = format!("{}-wal", db_path.display());
    let wal_bytes = fs::metadata(wal_path).map(|m| m.len()).unwrap_or(0);
    (db_bytes, wal_bytes)
}

/// Central lock-free APM telemetry registry shared across all worker threads via `Arc`.
pub struct TelemetryRegistry {
    pub started_at: Instant,
    pub current_phase: Mutex<String>,
    pub threads: [ThreadMetrics; 7],
    // Bounded queue & RAM gauges
    pub stage_channel_depth: AtomicU64,
    pub stage_channel_peak: AtomicU64,
    pub inflight_luma_bytes: AtomicU64,
    pub inflight_luma_bytes_peak: AtomicU64,
    pub eval_channel_depth: AtomicU64,
    pub eval_channel_peak: AtomicU64,
    pub sqlite_pending_depth: AtomicU64,
    pub sqlite_pending_peak: AtomicU64,
    // dHash domain counters
    pub dhash_frames_integer: AtomicU64,
    pub dhash_frames_fractional: AtomicU64,
    pub dhash_frame_duration: AtomicPromHistogram,
    // LRU caches
    pub prefix_lru: CacheMetrics,
    pub dense_lru: CacheMetrics,
    // Duplicate funnel
    pub funnel: FunnelMetrics,
    // SQLite3 WAL
    pub sqlite_commits: AtomicU64,
    pub sqlite_mutations: AtomicU64,
    pub sqlite_commit_duration: AtomicPromHistogram,
    // Slow-path grouped breakdowns (updated once per file/job, never per frame)
    pub per_volume: Mutex<BTreeMap<u64, VolumeStats>>,
    /// Keyed by `(engine: "gpu"|"cpu", codec, kind: "sparse5"|"prefix60"|"dense")`
    pub per_codec_decode: Mutex<BTreeMap<(String, String, String), CodecDecodeStats>>,
    /// Keyed by `(codec, reason: "legacy_codec"|"codec_rejected")`
    pub gpu_to_cpu_routes: Mutex<BTreeMap<(String, String), u64>>,
    /// Keyed by `(stream: "video"|"audio", source: "stream"|"tag_bps"|"container_fallback"|"size_fallback"|"missing")`
    pub ffprobe_bitrate_sources: Mutex<BTreeMap<(String, String), u64>>,
}

impl Default for TelemetryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TelemetryRegistry {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            current_phase: Mutex::new("config_test".to_string()),
            threads: std::array::from_fn(|_| ThreadMetrics::default()),
            stage_channel_depth: AtomicU64::new(0),
            stage_channel_peak: AtomicU64::new(0),
            inflight_luma_bytes: AtomicU64::new(0),
            inflight_luma_bytes_peak: AtomicU64::new(0),
            eval_channel_depth: AtomicU64::new(0),
            eval_channel_peak: AtomicU64::new(0),
            sqlite_pending_depth: AtomicU64::new(0),
            sqlite_pending_peak: AtomicU64::new(0),
            dhash_frames_integer: AtomicU64::new(0),
            dhash_frames_fractional: AtomicU64::new(0),
            dhash_frame_duration: AtomicPromHistogram::new(),
            prefix_lru: CacheMetrics::default(),
            dense_lru: CacheMetrics::default(),
            funnel: FunnelMetrics::default(),
            sqlite_commits: AtomicU64::new(0),
            sqlite_mutations: AtomicU64::new(0),
            sqlite_commit_duration: AtomicPromHistogram::new(),
            per_volume: Mutex::new(BTreeMap::new()),
            per_codec_decode: Mutex::new(BTreeMap::new()),
            gpu_to_cpu_routes: Mutex::new(BTreeMap::new()),
            ffprobe_bitrate_sources: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn set_phase(&self, phase: &str) {
        if let Ok(mut guard) = self.current_phase.lock() {
            *guard = phase.to_string();
        }
    }

    pub fn current_phase(&self) -> String {
        self.current_phase
            .lock()
            .map(|g| g.clone())
            .unwrap_or_else(|_| "unknown".to_string())
    }

    #[inline]
    pub fn record_thread_busy(&self, role: ThreadRole, d: Duration) {
        self.threads[role as usize]
            .busy_ns
            .fetch_add(d.as_nanos().min(u64::MAX as u128) as u64, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_thread_starved(&self, role: ThreadRole, d: Duration) {
        self.threads[role as usize]
            .starved_ns
            .fetch_add(d.as_nanos().min(u64::MAX as u128) as u64, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_thread_backpressured(&self, role: ThreadRole, d: Duration) {
        self.threads[role as usize]
            .backpressured_ns
            .fetch_add(d.as_nanos().min(u64::MAX as u128) as u64, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_thread_job(&self, role: ThreadRole, job_duration: Duration) {
        let tm = &self.threads[role as usize];
        tm.jobs_completed.fetch_add(1, Ordering::Relaxed);
        tm.job_duration.observe(job_duration);
    }

    #[inline]
    pub fn record_thread_error(&self, role: ThreadRole) {
        self.threads[role as usize]
            .errors
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn stage_channel_push(&self, luma_bytes: usize) {
        let depth = self.stage_channel_depth.fetch_add(1, Ordering::Relaxed) + 1;
        self.stage_channel_peak.fetch_max(depth, Ordering::Relaxed);
        let bytes = self
            .inflight_luma_bytes
            .fetch_add(luma_bytes as u64, Ordering::Relaxed)
            + luma_bytes as u64;
        self.inflight_luma_bytes_peak
            .fetch_max(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub fn stage_channel_pop(&self, luma_bytes: usize) {
        let _ = self
            .stage_channel_depth
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
        let _ = self
            .inflight_luma_bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(luma_bytes as u64))
            });
    }

    #[inline]
    pub fn eval_channel_push(&self) {
        let depth = self.eval_channel_depth.fetch_add(1, Ordering::Relaxed) + 1;
        self.eval_channel_peak.fetch_max(depth, Ordering::Relaxed);
    }

    #[inline]
    pub fn eval_channel_pop(&self) {
        let _ = self
            .eval_channel_depth
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
    }

    #[inline]
    pub fn sqlite_pending_inc(&self) {
        let depth = self.sqlite_pending_depth.fetch_add(1, Ordering::Relaxed) + 1;
        self.sqlite_pending_peak.fetch_max(depth, Ordering::Relaxed);
    }

    #[inline]
    pub fn sqlite_pending_sub(&self, count: usize) {
        let _ = self
            .sqlite_pending_depth
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(count as u64))
            });
    }

    pub fn record_volume_probed(
        &self,
        dev_id: u64,
        ffprobe_dur: Duration,
        blake3_bytes: u64,
    ) {
        if let Ok(mut map) = self.per_volume.lock() {
            let entry = map.entry(dev_id).or_default();
            entry.probed += 1;
            entry.blake3_bytes += blake3_bytes;
            entry.ffprobe_duration.observe(ffprobe_dur);
        }
    }

    pub fn record_volume_sqlite_hit(&self, dev_id: u64, lookup_dur: Duration, blake3_bytes: u64) {
        if let Ok(mut map) = self.per_volume.lock() {
            let entry = map.entry(dev_id).or_default();
            entry.sqlite_hits += 1;
            entry.blake3_bytes += blake3_bytes;
            entry.sqlite_lookup_duration.observe(lookup_dur);
        }
    }

    pub fn record_volume_bitrate_reprobe(
        &self,
        dev_id: u64,
        ffprobe_dur: Duration,
        blake3_bytes: u64,
    ) {
        if let Ok(mut map) = self.per_volume.lock() {
            let entry = map.entry(dev_id).or_default();
            entry.probed += 1;
            entry.bitrate_reprobes += 1;
            entry.blake3_bytes += blake3_bytes;
            entry.ffprobe_duration.observe(ffprobe_dur);
        }
    }

    pub fn record_bitrate_source(&self, stream: &str, source: &str) {
        let key = (stream.to_string(), source.to_string());
        if let Ok(mut map) = self.ffprobe_bitrate_sources.lock() {
            *map.entry(key).or_insert(0) += 1;
        }
    }

    pub fn record_volume_error(&self, dev_id: u64) {
        if let Ok(mut map) = self.per_volume.lock() {
            let entry = map.entry(dev_id).or_default();
            entry.skipped_errors += 1;
        }
    }

    pub fn record_decode(
        &self,
        engine: &str,
        codec: &str,
        kind: &str,
        frames: usize,
        video_secs: f64,
        wall_dur: Duration,
        used_hw_scale_fallback: bool,
    ) {
        let key = (
            engine.to_string(),
            codec.to_uppercase(),
            kind.to_string(),
        );
        let vid_ms = (video_secs.max(0.0) * 1000.0).round() as u64;
        let wall_ms = wall_dur.as_millis().min(u64::MAX as u128) as u64;
        if let Ok(mut map) = self.per_codec_decode.lock() {
            let entry = map.entry(key).or_default();
            entry.jobs += 1;
            entry.frames += frames as u64;
            entry.video_secs_ms += vid_ms;
            entry.wall_secs_ms += wall_ms;
            if used_hw_scale_fallback {
                entry.hw_scale_fallbacks += 1;
            }
        }
    }

    pub fn record_gpu_to_cpu_route(&self, codec: &str, reason: &str) {
        let key = (codec.to_uppercase(), reason.to_string());
        if let Ok(mut map) = self.gpu_to_cpu_routes.lock() {
            *map.entry(key).or_insert(0) += 1;
        }
    }

    pub fn record_dhash_batch(&self, frames: usize, exact_integer: bool, total_dur: Duration) {
        if frames == 0 {
            return;
        }
        if exact_integer {
            self.dhash_frames_integer
                .fetch_add(frames as u64, Ordering::Relaxed);
        } else {
            self.dhash_frames_fractional
                .fetch_add(frames as u64, Ordering::Relaxed);
        }
        let per_frame_ns = (total_dur.as_nanos() / (frames as u128)).min(u64::MAX as u128) as u64;
        self.dhash_frame_duration.observe_ns(per_frame_ns);
    }

    pub fn record_sqlite_commit(&self, mutations: usize, commit_dur: Duration) {
        self.sqlite_commits.fetch_add(1, Ordering::Relaxed);
        self.sqlite_mutations
            .fetch_add(mutations as u64, Ordering::Relaxed);
        self.sqlite_commit_duration.observe(commit_dur);
    }

    pub fn build_summary(&self, meta: &RunMeta, db_path: &Path) -> TelemetrySummary {
        let wall_secs = self.started_at.elapsed().as_secs_f64();
        let resources = sample_process_resources();
        let (db_bytes, wal_bytes) = sample_sqlite_file_bytes(db_path);

        let mut threads_summary = Vec::with_capacity(ALL_THREAD_ROLES.len());
        for role in ALL_THREAD_ROLES {
            let tm = &self.threads[role as usize];
            let busy_ns = tm.busy_ns.load(Ordering::Relaxed);
            let starved_ns = tm.starved_ns.load(Ordering::Relaxed);
            let backp_ns = tm.backpressured_ns.load(Ordering::Relaxed);
            let busy_s = busy_ns as f64 / 1e9;
            let starved_s = starved_ns as f64 / 1e9;
            let backp_s = backp_ns as f64 / 1e9;
            let total_s = (busy_s + starved_s + backp_s).max(1e-9);
            let jobs = tm.jobs_completed.load(Ordering::Relaxed);
            let hist = tm.job_duration.snapshot();
            let wall_rate = if busy_s + starved_s + backp_s > 0.0 {
                jobs as f64 / total_s
            } else {
                0.0
            };
            let active_rate = if busy_s > 0.0 {
                jobs as f64 / busy_s
            } else {
                0.0
            };
            threads_summary.push(ThreadSummary {
                thread: role.as_label().to_string(),
                busy_secs: busy_s,
                busy_ns,
                starved_ns,
                backpressured_ns: backp_ns,
                busy_pct: if busy_s + starved_s + backp_s > 0.0 {
                    (busy_s / total_s) * 100.0
                } else {
                    0.0
                },
                starved_pct: if busy_s + starved_s + backp_s > 0.0 {
                    (starved_s / total_s) * 100.0
                } else {
                    0.0
                },
                backpressured_pct: if busy_s + starved_s + backp_s > 0.0 {
                    (backp_s / total_s) * 100.0
                } else {
                    0.0
                },
                jobs_completed: jobs,
                errors: tm.errors.load(Ordering::Relaxed),
                jobs_per_sec: wall_rate,
                active_jobs_per_sec: active_rate,
                p50_ms: hist.quantile_ms(0.50),
                p95_ms: hist.quantile_ms(0.95),
                p99_ms: hist.quantile_ms(0.99),
                max_ms: hist.max_secs * 1000.0,
            });
        }

        let mut volumes = Vec::new();
        if let Ok(map) = self.per_volume.lock() {
            for (&dev_id, st) in map.iter() {
                let hist = st.ffprobe_duration.snapshot();
                let sql_hist = st.sqlite_lookup_duration.snapshot();
                volumes.push(VolumeSummary {
                    dev_id,
                    probed: st.probed,
                    sqlite_hits: st.sqlite_hits,
                    bitrate_reprobes: st.bitrate_reprobes,
                    skipped_errors: st.skipped_errors,
                    blake3_bytes: st.blake3_bytes,
                    ffprobe_p50_ms: hist.quantile_ms(0.50),
                    ffprobe_p95_ms: hist.quantile_ms(0.95),
                    ffprobe_max_ms: hist.max_secs * 1000.0,
                    sqlite_lookup_p50_ms: sql_hist.quantile_ms(0.50),
                    sqlite_lookup_p95_ms: sql_hist.quantile_ms(0.95),
                });
            }
        }

        let mut codecs = Vec::new();
        let mut gpu_vid_ms = 0u64;
        let mut gpu_wall_ms = 0u64;
        let mut cpu_vid_ms = 0u64;
        let mut cpu_wall_ms = 0u64;
        let mut total_decode_jobs = 0u64;
        if let Ok(map) = self.per_codec_decode.lock() {
            for ((engine, codec, kind), st) in map.iter() {
                total_decode_jobs += st.jobs;
                if engine == "gpu" {
                    gpu_vid_ms += st.video_secs_ms;
                    gpu_wall_ms += st.wall_secs_ms;
                } else {
                    cpu_vid_ms += st.video_secs_ms;
                    cpu_wall_ms += st.wall_secs_ms;
                }
                let rt_factor = if st.wall_secs_ms > 0 {
                    st.video_secs_ms as f64 / st.wall_secs_ms as f64
                } else {
                    0.0
                };
                codecs.push(CodecDecodeSummary {
                    engine: engine.clone(),
                    codec: codec.clone(),
                    kind: kind.clone(),
                    jobs: st.jobs,
                    frames: st.frames,
                    realtime_factor: rt_factor,
                    hw_scale_fallbacks: st.hw_scale_fallbacks,
                });
            }
        }

        let mut routed_map = BTreeMap::new();
        let mut total_routed = 0u64;
        if let Ok(map) = self.gpu_to_cpu_routes.lock() {
            for ((codec, reason), &cnt) in map.iter() {
                routed_map.insert(format!("{}[{}]", codec, reason), cnt);
                total_routed += cnt;
            }
        }

        let cache_snap = |c: &CacheMetrics| -> CacheSummary {
            let hits = c.hits.load(Ordering::Relaxed);
            let sqlite_reloads = c.sqlite_reloads.load(Ordering::Relaxed);
            let misses_decode = c.misses_decode.load(Ordering::Relaxed);
            let evictions = c.evictions.load(Ordering::Relaxed);
            let lookups = hits + sqlite_reloads + misses_decode;
            let hit_rate_pct = if lookups > 0 {
                (hits as f64 / lookups as f64) * 100.0
            } else {
                100.0
            };
            CacheSummary {
                hits,
                sqlite_reloads,
                misses_decode,
                evictions,
                hit_rate_pct,
            }
        };

        let prefix_lru = cache_snap(&self.prefix_lru);
        let dense_lru = cache_snap(&self.dense_lru);

        let sqlite_hist = self.sqlite_commit_duration.snapshot();
        let sqlite_commits = self.sqlite_commits.load(Ordering::Relaxed);
        let sqlite_mutations = self.sqlite_mutations.load(Ordering::Relaxed);

        let avoided_hours = (self.funnel.avoided_ms_pass1_isolated.load(Ordering::Relaxed)
            + self.funnel.avoided_ms_pass1_strong.load(Ordering::Relaxed)
            + self.funnel.avoided_ms_stage_b_sparse.load(Ordering::Relaxed)
            + self.funnel.avoided_ms_stage_c_60s.load(Ordering::Relaxed))
            as f64
            / 3_600_000.0;

        let funnel = FunnelSummary {
            pass1_strong_confirmed: self.funnel.pass1_strong_confirmed.load(Ordering::Relaxed),
            pass1_conflict_rejected: self.funnel.pass1_conflict_rejected.load(Ordering::Relaxed),
            pass2_sqlite_recovered: self.funnel.pass2_sqlite_recovered.load(Ordering::Relaxed),
            pass2_transitive_skip: self.funnel.pass2_transitive_skip.load(Ordering::Relaxed),
            pass2_blake3_exact: self.funnel.pass2_blake3_exact.load(Ordering::Relaxed),
            pass2_stage_b_rejected: self.funnel.pass2_stage_b_rejected.load(Ordering::Relaxed),
            pass2_stage_c_60s_rejected: self
                .funnel
                .pass2_stage_c_60s_rejected
                .load(Ordering::Relaxed),
            pass2_stage_e_confirmed: self.funnel.pass2_stage_e_confirmed.load(Ordering::Relaxed),
            pass2_stage_e_distinct: self.funnel.pass2_stage_e_distinct.load(Ordering::Relaxed),
            estimated_video_hours_avoided: avoided_hours,
        };

        let sqlite = SqliteSummary {
            db_bytes,
            wal_bytes,
            commits: sqlite_commits,
            mutations: sqlite_mutations,
            avg_batch_size: if sqlite_commits > 0 {
                sqlite_mutations as f64 / sqlite_commits as f64
            } else {
                0.0
            },
            commit_p50_ms: sqlite_hist.quantile_ms(0.50),
            commit_p95_ms: sqlite_hist.quantile_ms(0.95),
            commit_p99_ms: sqlite_hist.quantile_ms(0.99),
            commit_max_ms: sqlite_hist.max_secs * 1000.0,
            pending_peak: self.sqlite_pending_peak.load(Ordering::Relaxed),
        };

        let mut bitrate_sources_map = BTreeMap::new();
        if let Ok(map) = self.ffprobe_bitrate_sources.lock() {
            for ((stream, src), &cnt) in map.iter() {
                bitrate_sources_map.insert(format!("{}:{}", stream, src), cnt);
            }
        }

        let mut summary = TelemetrySummary {
            meta: meta.clone(),
            wall_secs,
            resources,
            threads: threads_summary,
            stage_channel_peak: self.stage_channel_peak.load(Ordering::Relaxed),
            eval_channel_peak: self.eval_channel_peak.load(Ordering::Relaxed),
            inflight_luma_bytes_peak: self.inflight_luma_bytes_peak.load(Ordering::Relaxed),
            gpu_overall_realtime_factor: if gpu_wall_ms > 0 {
                gpu_vid_ms as f64 / gpu_wall_ms as f64
            } else {
                0.0
            },
            cpu_overall_realtime_factor: if cpu_wall_ms > 0 {
                cpu_vid_ms as f64 / cpu_wall_ms as f64
            } else {
                0.0
            },
            dhash_frames_integer: self.dhash_frames_integer.load(Ordering::Relaxed),
            dhash_frames_fractional: self.dhash_frames_fractional.load(Ordering::Relaxed),
            prefix_lru,
            dense_lru,
            volumes,
            codecs,
            gpu_to_cpu_routes: routed_map,
            ffprobe_bitrate_sources: bitrate_sources_map,
            funnel,
            sqlite,
            bottleneck_advisor: Vec::new(),
        };

        summary.bottleneck_advisor =
            evaluate_bottleneck_advisor(&summary, total_decode_jobs, total_routed);
        summary
    }

    pub fn format_heartbeat_line(&self, db_path: &Path) -> String {
        let elapsed = self.started_at.elapsed().as_secs();
        let phase = self.current_phase();
        let ffp_jobs = self.threads[ThreadRole::FfprobeWorkers as usize]
            .jobs_completed
            .load(Ordering::Relaxed);
        let gpu_jobs = self.threads[ThreadRole::GpuDecodeWorker as usize]
            .jobs_completed
            .load(Ordering::Relaxed);
        let cpu_jobs = self.threads[ThreadRole::CpuDecodeWorker as usize]
            .jobs_completed
            .load(Ordering::Relaxed);
        let dhash_frames = self.dhash_frames_integer.load(Ordering::Relaxed)
            + self.dhash_frames_fractional.load(Ordering::Relaxed);
        let stage_d = self.stage_channel_depth.load(Ordering::Relaxed);
        let stage_pk = self.stage_channel_peak.load(Ordering::Relaxed);
        let luma_mb = self.inflight_luma_bytes.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0);
        let (db_bytes, wal_bytes) = sample_sqlite_file_bytes(db_path);
        let res = sample_process_resources();
        let rss_mb = res.max_rss_bytes as f64 / (1024.0 * 1024.0);
        format!(
            "[Telemetry +{elapsed}s | phase={phase}] ffprobe={ffp_jobs} | decode(gpu={gpu_jobs}, cpu={cpu_jobs}) | dhash_frames={dhash_frames} | stage_q={stage_d}/16 (peak={stage_pk}, {luma_mb:.1}MB) | rss={rss_mb:.1}MB | sqlite={:.1}MB(wal {:.1}MB)",
            db_bytes as f64 / (1024.0 * 1024.0),
            wal_bytes as f64 / (1024.0 * 1024.0),
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ThreadSummary {
    pub thread: String,
    pub busy_secs: f64,
    pub busy_ns: u64,
    pub starved_ns: u64,
    pub backpressured_ns: u64,
    pub busy_pct: f64,
    pub starved_pct: f64,
    pub backpressured_pct: f64,
    pub jobs_completed: u64,
    pub errors: u64,
    pub jobs_per_sec: f64,
    pub active_jobs_per_sec: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct VolumeSummary {
    pub dev_id: u64,
    pub probed: u64,
    pub sqlite_hits: u64,
    pub bitrate_reprobes: u64,
    pub skipped_errors: u64,
    pub blake3_bytes: u64,
    pub ffprobe_p50_ms: f64,
    pub ffprobe_p95_ms: f64,
    pub ffprobe_max_ms: f64,
    pub sqlite_lookup_p50_ms: f64,
    pub sqlite_lookup_p95_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CodecDecodeSummary {
    pub engine: String,
    pub codec: String,
    pub kind: String,
    pub jobs: u64,
    pub frames: u64,
    pub realtime_factor: f64,
    pub hw_scale_fallbacks: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheSummary {
    pub hits: u64,
    pub sqlite_reloads: u64,
    pub misses_decode: u64,
    pub evictions: u64,
    pub hit_rate_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FunnelSummary {
    pub pass1_strong_confirmed: u64,
    pub pass1_conflict_rejected: u64,
    pub pass2_sqlite_recovered: u64,
    pub pass2_transitive_skip: u64,
    pub pass2_blake3_exact: u64,
    pub pass2_stage_b_rejected: u64,
    pub pass2_stage_c_60s_rejected: u64,
    pub pass2_stage_e_confirmed: u64,
    pub pass2_stage_e_distinct: u64,
    pub estimated_video_hours_avoided: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SqliteSummary {
    pub db_bytes: u64,
    pub wal_bytes: u64,
    pub commits: u64,
    pub mutations: u64,
    pub avg_batch_size: f64,
    pub commit_p50_ms: f64,
    pub commit_p95_ms: f64,
    pub commit_p99_ms: f64,
    pub commit_max_ms: f64,
    pub pending_peak: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TelemetrySummary {
    pub meta: RunMeta,
    pub wall_secs: f64,
    pub resources: ProcessResourceSnapshot,
    pub threads: Vec<ThreadSummary>,
    pub stage_channel_peak: u64,
    pub eval_channel_peak: u64,
    pub inflight_luma_bytes_peak: u64,
    pub gpu_overall_realtime_factor: f64,
    pub cpu_overall_realtime_factor: f64,
    pub dhash_frames_integer: u64,
    pub dhash_frames_fractional: u64,
    pub prefix_lru: CacheSummary,
    pub dense_lru: CacheSummary,
    pub volumes: Vec<VolumeSummary>,
    pub codecs: Vec<CodecDecodeSummary>,
    pub gpu_to_cpu_routes: BTreeMap<String, u64>,
    pub ffprobe_bitrate_sources: BTreeMap<String, u64>,
    pub funnel: FunnelSummary,
    pub sqlite: SqliteSummary,
    pub bottleneck_advisor: Vec<String>,
}

/// Evaluates APM threshold rules to produce concrete tuning recommendations.
pub fn evaluate_bottleneck_advisor(
    summary: &TelemetrySummary,
    total_decode_jobs: u64,
    total_routed: u64,
) -> Vec<String> {
    let mut notes = Vec::new();

    if let Some(gpu) = summary
        .threads
        .iter()
        .find(|t| t.thread == "gpu_decode_worker")
    {
        if gpu.backpressured_pct > 10.0 {
            notes.push(format!(
                "WARN: gpu_decode_worker was backpressured {:.1}% of the time on sync_channel(16) — cpu_dhash_worker may be saturated.",
                gpu.backpressured_pct
            ));
        }
    }

    if summary.dense_lru.evictions > 0 && summary.dense_lru.hit_rate_pct < 80.0 {
        notes.push(format!(
            "WARN: Dense 1-fps LRU hit rate dropped to {:.1}% ({} SQLite BLOB reloads, {} evictions) — consider raising HASH_LRU_CAPACITY above 2000.",
            summary.dense_lru.hit_rate_pct,
            summary.dense_lru.sqlite_reloads,
            summary.dense_lru.evictions
        ));
    }

    if summary.sqlite.commits > 0 && summary.sqlite.commit_p99_ms > 200.0 {
        notes.push(format!(
            "WARN: SQLite3 WAL commit p99 latency was {:.1}ms — consider pointing --states-dir to a local internal SSD/NVMe rather than a spinning HDD.",
            summary.sqlite.commit_p99_ms
        ));
    }

    if total_decode_jobs > 10 && total_routed > 0 {
        let pct = (total_routed as f64 / total_decode_jobs as f64) * 100.0;
        if pct > 15.0 {
            notes.push(format!(
                "INFO: {:.1}% of decode jobs ({}/{}) routed to cpu_decode_worker (legacy/unsupported GPU codecs: {:?}).",
                pct, total_routed, total_decode_jobs, summary.gpu_to_cpu_routes
            ));
        }
    }

    if notes.is_empty() {
        notes.push(
            "OK: No queue backpressure, LRU thrashing, or SQLite WAL commit bottlenecks detected."
                .to_string(),
        );
    }

    notes
}

pub fn format_duration_compact(secs: f64) -> String {
    if secs <= 0.0 {
        return "0s".to_string();
    }
    if secs < 0.001 {
        format!("{:.0}µs", secs * 1_000_000.0)
    } else if secs < 1.0 {
        format!("{:.1}ms", secs * 1_000.0)
    } else if secs < 60.0 {
        format!("{:.2}s", secs)
    } else if secs < 3600.0 {
        let mins = (secs / 60.0).floor() as u64;
        let rem = (secs % 60.0).round() as u64;
        format!("{}m{:02}s", mins, rem)
    } else {
        let hrs = (secs / 3600.0).floor() as u64;
        let mins = ((secs % 3600.0) / 60.0).floor() as u64;
        format!("{}h{:02}m", hrs, mins)
    }
}

pub fn format_pct(pct: f64, non_zero_ns: u64) -> String {
    if non_zero_ns > 0 && pct < 0.05 {
        "<0.1%".to_string()
    } else {
        format!("{:.1}%", pct)
    }
}

pub fn format_rate(per_sec: f64, jobs: u64) -> String {
    if jobs == 0 || per_sec <= 0.0 {
        return "0/s".to_string();
    }
    if per_sec >= 100.0 {
        format!("{:.0}/s", per_sec)
    } else if per_sec >= 1.0 {
        format!("{:.1}/s", per_sec)
    } else if per_sec * 60.0 >= 1.0 {
        format!("{:.1}/m", per_sec * 60.0)
    } else {
        format!("{:.1}/h", per_sec * 3600.0)
    }
}

pub fn format_latency_ms(ms: f64) -> String {
    if ms <= 0.0 {
        return "0ms".to_string();
    }
    if ms < 0.1 {
        format!("{:.0}µs", ms * 1000.0)
    } else if ms < 1000.0 {
        format!("{:.1}ms", ms)
    } else {
        format!("{:.2}s", ms / 1000.0)
    }
}

pub fn format_terminal_dashboard(s: &TelemetrySummary) -> String {
    let mut out = String::new();
    let rust_cpu = s.resources.rust_user_cpu_secs + s.resources.rust_sys_cpu_secs;
    let child_cpu = s.resources.children_user_cpu_secs + s.resources.children_sys_cpu_secs;
    let rss_mb = s.resources.max_rss_bytes as f64 / (1024.0 * 1024.0);

    out.push_str("\n========================= RUSTY_DEDUP APM TELEMETRY SUMMARY =========================\n");
    out.push_str(&format!(
        "Wall Time: {:.2}s ({}) | Peak RSS: {:.1} MB | Rust CPU: {:.2}s | FFmpeg/Probe CPU: {:.2}s\n",
        s.wall_secs,
        format_duration_compact(s.wall_secs),
        rss_mb,
        rust_cpu,
        child_cpu
    ));
    out.push_str(&format!(
        "SQLite3 WAL: {:.2} MB DB / {:.2} MB WAL | {} commits (avg batch {:.1}, p95={}, max={})\n\n",
        s.sqlite.db_bytes as f64 / (1024.0 * 1024.0),
        s.sqlite.wal_bytes as f64 / (1024.0 * 1024.0),
        s.sqlite.commits,
        s.sqlite.avg_batch_size,
        format_latency_ms(s.sqlite.commit_p95_ms),
        format_latency_ms(s.sqlite.commit_max_ms)
    ));

    out.push_str("[Thread Utilization & Latency]\n");
    out.push_str("Thread               | BusyTime |  Busy% | Starv% | Backp% |      Jobs | WallRate | ActiveRate |     p50 |     p95 |     max\n");
    out.push_str("---------------------|----------|--------|--------|--------|-----------|----------|------------|---------|---------|--------\n");
    for t in &s.threads {
        if t.jobs_completed == 0 && t.busy_ns == 0 && t.starved_ns == 0 {
            continue;
        }
        out.push_str(&format!(
            "{:<20} | {:>8} | {:>6} | {:>6} | {:>6} | {:>9} | {:>8} | {:>10} | {:>7} | {:>7} | {:>7}\n",
            t.thread,
            format_duration_compact(t.busy_secs),
            format_pct(t.busy_pct, t.busy_ns),
            format_pct(t.starved_pct, t.starved_ns),
            format_pct(t.backpressured_pct, t.backpressured_ns),
            t.jobs_completed,
            format_rate(t.jobs_per_sec, t.jobs_completed),
            format_rate(t.active_jobs_per_sec, t.jobs_completed),
            format_latency_ms(t.p50_ms),
            format_latency_ms(t.p95_ms),
            format_latency_ms(t.max_ms)
        ));
    }

    out.push_str("\n[Queues & Bounded Caches]\n");
    out.push_str(&format!(
        "  sync_channel(16) stage queue : peak {}/16 slots | peak in-flight luma: {:.2} MB\n",
        s.stage_channel_peak,
        s.inflight_luma_bytes_peak as f64 / (1024.0 * 1024.0)
    ));
    out.push_str(&format!(
        "  eval_channel(16) pair queue  : peak {}/16 slots | sqlite pending peak: {}\n",
        s.eval_channel_peak, s.sqlite.pending_peak
    ));
    out.push_str(&format!(
        "  Prefix 1-fps LRU             : hit rate {:.1}% (hits={}, sqlite_reload={}, decode_miss={}, evict={})\n",
        s.prefix_lru.hit_rate_pct,
        s.prefix_lru.hits,
        s.prefix_lru.sqlite_reloads,
        s.prefix_lru.misses_decode,
        s.prefix_lru.evictions
    ));
    out.push_str(&format!(
        "  Dense  1-fps LRU             : hit rate {:.1}% (hits={}, sqlite_reload={}, decode_miss={}, evict={})\n",
        s.dense_lru.hit_rate_pct,
        s.dense_lru.hits,
        s.dense_lru.sqlite_reloads,
        s.dense_lru.misses_decode,
        s.dense_lru.evictions
    ));

    if !s.codecs.is_empty() || !s.gpu_to_cpu_routes.is_empty() {
        out.push_str("\n[Decode Realtime Factor & Per-Codec Breakdown]\n");
        out.push_str(&format!(
            "  GPU Overall: {:.1}x realtime | CPU Overall: {:.1}x realtime | dHash frames: {} integer / {} fractional\n",
            s.gpu_overall_realtime_factor,
            s.cpu_overall_realtime_factor,
            s.dhash_frames_integer,
            s.dhash_frames_fractional
        ));
        for c in &s.codecs {
            out.push_str(&format!(
                "  - {:<3} {:<10} {:<8}: {} jobs, {} frames, {:.1}x realtime (hw_scale_fallbacks={})\n",
                c.engine, c.codec, c.kind, c.jobs, c.frames, c.realtime_factor, c.hw_scale_fallbacks
            ));
        }
        if !s.gpu_to_cpu_routes.is_empty() {
            out.push_str(&format!("  GPU->CPU Codec Routes: {:?}\n", s.gpu_to_cpu_routes));
        }
    }

    if !s.volumes.is_empty() {
        out.push_str("\n[Per-Volume HDD Probing]\n");
        for v in &s.volumes {
            let reprobe_note = if v.bitrate_reprobes > 0 {
                format!(" (incl. {} bitrate re-probes)", v.bitrate_reprobes)
            } else {
                String::new()
            };
            let lookup_note = if v.sqlite_hits > 0 {
                format!(
                    " (lookup p50={}, p95={})",
                    format_latency_ms(v.sqlite_lookup_p50_ms),
                    format_latency_ms(v.sqlite_lookup_p95_ms)
                )
            } else {
                String::new()
            };
            if v.probed == 0 {
                out.push_str(&format!(
                    "  - dev_id={:<10}: probed=0, sqlite_hits={}{}, errors={} | ffprobe: N/A (100% SQLite cache hit)\n",
                    v.dev_id, v.sqlite_hits, lookup_note, v.skipped_errors
                ));
            } else {
                out.push_str(&format!(
                    "  - dev_id={:<10}: probed={}{}, sqlite_hits={}{}, errors={}, ffprobe p50={} p95={} max={}\n",
                    v.dev_id,
                    v.probed,
                    reprobe_note,
                    v.sqlite_hits,
                    lookup_note,
                    v.skipped_errors,
                    format_latency_ms(v.ffprobe_p50_ms),
                    format_latency_ms(v.ffprobe_p95_ms),
                    format_latency_ms(v.ffprobe_max_ms)
                ));
            }
        }
        if !s.ffprobe_bitrate_sources.is_empty() {
            out.push_str(&format!(
                "  FFprobe Bitrate Sources: {:?}\n",
                s.ffprobe_bitrate_sources
            ));
        }
    }

    out.push_str("\n[Duplicate Funnel & Work Saved]\n");
    out.push_str(&format!(
        "  Pass 1: strong_fastpath_confirmed={}, conflict_rejected={}\n",
        s.funnel.pass1_strong_confirmed, s.funnel.pass1_conflict_rejected
    ));
    out.push_str(&format!(
        "  Pass 2: sqlite_recovered={}, transitive_skip={}, blake3={}, stage_b_rej={}, stage_c_60s_rej={}, stage_e_confirmed={}, stage_e_distinct={}\n",
        s.funnel.pass2_sqlite_recovered,
        s.funnel.pass2_transitive_skip,
        s.funnel.pass2_blake3_exact,
        s.funnel.pass2_stage_b_rejected,
        s.funnel.pass2_stage_c_60s_rejected,
        s.funnel.pass2_stage_e_confirmed,
        s.funnel.pass2_stage_e_distinct
    ));
    out.push_str(&format!(
        "  Estimated full-video decode hours avoided: {:.2} hours\n",
        s.funnel.estimated_video_hours_avoided
    ));

    out.push_str("\n[Bottleneck Advisor]\n");
    for note in &s.bottleneck_advisor {
        out.push_str(&format!("  - {}\n", note));
    }
    out.push_str("=====================================================================================\n");
    out
}
