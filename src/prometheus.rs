use crate::config::TelemetryConfig;
use crate::telemetry::{
    ALL_PHASES, ALL_THREAD_ROLES, HistogramSnapshot, PROM_BUCKET_BOUNDS_SECS, RunMeta,
    TelemetryRegistry, format_terminal_dashboard, sample_process_resources,
    sample_sqlite_file_bytes,
};
use anyhow::{Context, Result};
use std::fmt::Write as FmtWrite;
use std::fs::{self, File};
use std::io::{BufWriter, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub fn escape_prom_label(val: &str) -> String {
    let mut out = String::with_capacity(val.len());
    for c in val.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

fn write_prom_histogram(
    out: &mut String,
    name: &str,
    labels_prefix: &str,
    snap: &HistogramSnapshot,
) {
    let sep = if labels_prefix.is_empty() { "" } else { "," };
    for (i, &bound) in PROM_BUCKET_BOUNDS_SECS.iter().enumerate() {
        let _ = writeln!(
            out,
            "{name}_bucket{{{labels_prefix}{sep}le=\"{bound}\"}} {}",
            snap.cumulative_buckets[i]
        );
    }
    let _ = writeln!(
        out,
        "{name}_bucket{{{labels_prefix}{sep}le=\"+Inf\"}} {}",
        snap.count
    );
    if labels_prefix.is_empty() {
        let _ = writeln!(out, "{name}_sum {:.9}", snap.sum_secs);
        let _ = writeln!(out, "{name}_count {}", snap.count);
    } else {
        let _ = writeln!(out, "{name}_sum{{{labels_prefix}}} {:.9}", snap.sum_secs);
        let _ = writeln!(out, "{name}_count{{{labels_prefix}}} {}", snap.count);
    }
}

/// Renders all APM telemetry metrics in standard **Prometheus Text Exposition Format v0.0.4**.
pub fn render_prometheus_exposition(
    reg: &TelemetryRegistry,
    meta: &RunMeta,
    db_path: &Path,
) -> String {
    let mut out = String::with_capacity(16 * 1024);

    // 1. Static session & pipeline phase gauges
    let _ = writeln!(out, "# HELP rusty_dedup_info Static build, session, and hardware decode info.");
    let _ = writeln!(out, "# TYPE rusty_dedup_info gauge");
    let _ = writeln!(
        out,
        "rusty_dedup_info{{version=\"{}\",session_id=\"{}\",hw_backend=\"{}\",hw_scale=\"{}\"}} 1",
        escape_prom_label(&meta.version),
        escape_prom_label(&meta.session_id),
        escape_prom_label(&meta.hw_backend),
        escape_prom_label(&meta.hw_scale),
    );

    let active_phase = reg.current_phase();
    let _ = writeln!(out, "# HELP rusty_dedup_phase Current active pipeline stage (1=active, 0=inactive).");
    let _ = writeln!(out, "# TYPE rusty_dedup_phase gauge");
    for phase in ALL_PHASES {
        let val = if active_phase == phase { 1 } else { 0 };
        let _ = writeln!(out, "rusty_dedup_phase{{phase=\"{}\"}} {}", phase, val);
    }

    let _ = writeln!(out, "# HELP rusty_dedup_uptime_seconds Elapsed wall-clock seconds since scanner start.");
    let _ = writeln!(out, "# TYPE rusty_dedup_uptime_seconds gauge");
    let _ = writeln!(
        out,
        "rusty_dedup_uptime_seconds {:.6}",
        reg.started_at.elapsed().as_secs_f64()
    );

    // 2. Per-thread USE + RED metrics
    let _ = writeln!(out, "# HELP rusty_dedup_thread_time_seconds_total Cumulative worker thread time by USE state.");
    let _ = writeln!(out, "# TYPE rusty_dedup_thread_time_seconds_total counter");
    for role in ALL_THREAD_ROLES {
        let tm = &reg.threads[role as usize];
        let lbl = role.as_label();
        let _ = writeln!(
            out,
            "rusty_dedup_thread_time_seconds_total{{thread=\"{lbl}\",state=\"busy\"}} {:.9}",
            tm.busy_ns.load(Ordering::Relaxed) as f64 / 1e9
        );
        let _ = writeln!(
            out,
            "rusty_dedup_thread_time_seconds_total{{thread=\"{lbl}\",state=\"starved\"}} {:.9}",
            tm.starved_ns.load(Ordering::Relaxed) as f64 / 1e9
        );
        let _ = writeln!(
            out,
            "rusty_dedup_thread_time_seconds_total{{thread=\"{lbl}\",state=\"backpressured\"}} {:.9}",
            tm.backpressured_ns.load(Ordering::Relaxed) as f64 / 1e9
        );
    }

    let _ = writeln!(out, "# HELP rusty_dedup_thread_jobs_total Total completed jobs per worker thread.");
    let _ = writeln!(out, "# TYPE rusty_dedup_thread_jobs_total counter");
    for role in ALL_THREAD_ROLES {
        let _ = writeln!(
            out,
            "rusty_dedup_thread_jobs_total{{thread=\"{}\"}} {}",
            role.as_label(),
            reg.threads[role as usize]
                .jobs_completed
                .load(Ordering::Relaxed)
        );
    }

    let _ = writeln!(out, "# HELP rusty_dedup_thread_errors_total Total errors handled per worker thread.");
    let _ = writeln!(out, "# TYPE rusty_dedup_thread_errors_total counter");
    for role in ALL_THREAD_ROLES {
        let _ = writeln!(
            out,
            "rusty_dedup_thread_errors_total{{thread=\"{}\"}} {}",
            role.as_label(),
            reg.threads[role as usize].errors.load(Ordering::Relaxed)
        );
    }

    let _ = writeln!(out, "# HELP rusty_dedup_thread_job_duration_seconds Per-job execution latency histogram.");
    let _ = writeln!(out, "# TYPE rusty_dedup_thread_job_duration_seconds histogram");
    for role in ALL_THREAD_ROLES {
        let snap = reg.threads[role as usize].job_duration.snapshot();
        if snap.count > 0 {
            write_prom_histogram(
                &mut out,
                "rusty_dedup_thread_job_duration_seconds",
                &format!("thread=\"{}\"", role.as_label()),
                &snap,
            );
        }
    }

    // 3. Per-volume HDD probing
    if let Ok(map) = reg.per_volume.lock() {
        if !map.is_empty() {
            let _ = writeln!(out, "# HELP rusty_dedup_volume_files_total Files processed per physical HDD dev_id.");
            let _ = writeln!(out, "# TYPE rusty_dedup_volume_files_total counter");
            for (&dev_id, st) in map.iter() {
                let _ = writeln!(
                    out,
                    "rusty_dedup_volume_files_total{{dev_id=\"{dev_id}\",outcome=\"probed\"}} {}",
                    st.probed
                );
                let _ = writeln!(
                    out,
                    "rusty_dedup_volume_files_total{{dev_id=\"{dev_id}\",outcome=\"sqlite_hit\"}} {}",
                    st.sqlite_hits
                );
                let _ = writeln!(
                    out,
                    "rusty_dedup_volume_files_total{{dev_id=\"{dev_id}\",outcome=\"bitrate_reprobe\"}} {}",
                    st.bitrate_reprobes
                );
                let _ = writeln!(
                    out,
                    "rusty_dedup_volume_files_total{{dev_id=\"{dev_id}\",outcome=\"skipped_error\"}} {}",
                    st.skipped_errors
                );
            }

            let _ = writeln!(out, "# HELP rusty_dedup_volume_blake3_bytes_total Sample bytes hashed via BLAKE3 per dev_id.");
            let _ = writeln!(out, "# TYPE rusty_dedup_volume_blake3_bytes_total counter");
            for (&dev_id, st) in map.iter() {
                let _ = writeln!(
                    out,
                    "rusty_dedup_volume_blake3_bytes_total{{dev_id=\"{dev_id}\"}} {}",
                    st.blake3_bytes
                );
            }

            let _ = writeln!(out, "# HELP rusty_dedup_volume_ffprobe_duration_seconds FFprobe header latency per dev_id.");
            let _ = writeln!(out, "# TYPE rusty_dedup_volume_ffprobe_duration_seconds histogram");
            for (&dev_id, st) in map.iter() {
                let snap = st.ffprobe_duration.snapshot();
                if snap.count > 0 {
                    write_prom_histogram(
                        &mut out,
                        "rusty_dedup_volume_ffprobe_duration_seconds",
                        &format!("dev_id=\"{dev_id}\""),
                        &snap,
                    );
                }
            }

            let _ = writeln!(out, "# HELP rusty_dedup_volume_sqlite_lookup_duration_seconds SQLite3 WAL cache lookup latency per dev_id.");
            let _ = writeln!(out, "# TYPE rusty_dedup_volume_sqlite_lookup_duration_seconds histogram");
            for (&dev_id, st) in map.iter() {
                let snap = st.sqlite_lookup_duration.snapshot();
                if snap.count > 0 {
                    write_prom_histogram(
                        &mut out,
                        "rusty_dedup_volume_sqlite_lookup_duration_seconds",
                        &format!("dev_id=\"{dev_id}\""),
                        &snap,
                    );
                }
            }
        }
    }

    if let Ok(map) = reg.ffprobe_bitrate_sources.lock() {
        if !map.is_empty() {
            let _ = writeln!(out, "# HELP rusty_dedup_ffprobe_bitrate_source_total Bitrate resolution source per stream type.");
            let _ = writeln!(out, "# TYPE rusty_dedup_ffprobe_bitrate_source_total counter");
            for ((stream, source), &cnt) in map.iter() {
                let _ = writeln!(
                    out,
                    "rusty_dedup_ffprobe_bitrate_source_total{{stream=\"{}\",source=\"{}\"}} {}",
                    escape_prom_label(stream),
                    escape_prom_label(source),
                    cnt
                );
            }
        }
    }

    // 4. Decode & dHash metrics
    if let Ok(map) = reg.per_codec_decode.lock() {
        if !map.is_empty() {
            let _ = writeln!(out, "# HELP rusty_dedup_decode_video_seconds_total Video duration decoded by engine/codec/kind.");
            let _ = writeln!(out, "# TYPE rusty_dedup_decode_video_seconds_total counter");
            for ((engine, codec, kind), st) in map.iter() {
                let _ = writeln!(
                    out,
                    "rusty_dedup_decode_video_seconds_total{{engine=\"{}\",codec=\"{}\",kind=\"{}\"}} {:.3}",
                    escape_prom_label(engine),
                    escape_prom_label(codec),
                    escape_prom_label(kind),
                    st.video_secs_ms as f64 / 1000.0
                );
            }

            let _ = writeln!(out, "# HELP rusty_dedup_decode_wall_seconds_total Wall time spent decoding by engine/codec/kind.");
            let _ = writeln!(out, "# TYPE rusty_dedup_decode_wall_seconds_total counter");
            for ((engine, codec, kind), st) in map.iter() {
                let _ = writeln!(
                    out,
                    "rusty_dedup_decode_wall_seconds_total{{engine=\"{}\",codec=\"{}\",kind=\"{}\"}} {:.3}",
                    escape_prom_label(engine),
                    escape_prom_label(codec),
                    escape_prom_label(kind),
                    st.wall_secs_ms as f64 / 1000.0
                );
            }

            let _ = writeln!(out, "# HELP rusty_dedup_decode_frames_total Decoded frames by engine/codec/kind.");
            let _ = writeln!(out, "# TYPE rusty_dedup_decode_frames_total counter");
            for ((engine, codec, kind), st) in map.iter() {
                let _ = writeln!(
                    out,
                    "rusty_dedup_decode_frames_total{{engine=\"{}\",codec=\"{}\",kind=\"{}\"}} {}",
                    escape_prom_label(engine),
                    escape_prom_label(codec),
                    escape_prom_label(kind),
                    st.frames
                );
            }

            let _ = writeln!(out, "# HELP rusty_dedup_gpu_hw_scale_fallbacks_total Files falling back from GPU scale to CPU scale.");
            let _ = writeln!(out, "# TYPE rusty_dedup_gpu_hw_scale_fallbacks_total counter");
            for ((engine, codec, _), st) in map.iter() {
                if engine == "gpu" && st.hw_scale_fallbacks > 0 {
                    let _ = writeln!(
                        out,
                        "rusty_dedup_gpu_hw_scale_fallbacks_total{{codec=\"{}\"}} {}",
                        escape_prom_label(codec),
                        st.hw_scale_fallbacks
                    );
                }
            }
        }
    }

    if let Ok(map) = reg.gpu_to_cpu_routes.lock() {
        if !map.is_empty() {
            let _ = writeln!(out, "# HELP rusty_dedup_gpu_to_cpu_routes_total Files routed to CPU decode worker by codec and reason.");
            let _ = writeln!(out, "# TYPE rusty_dedup_gpu_to_cpu_routes_total counter");
            for ((codec, reason), &cnt) in map.iter() {
                let _ = writeln!(
                    out,
                    "rusty_dedup_gpu_to_cpu_routes_total{{codec=\"{}\",reason=\"{}\"}} {}",
                    escape_prom_label(codec),
                    escape_prom_label(reason),
                    cnt
                );
            }
        }
    }

    let _ = writeln!(out, "# HELP rusty_dedup_dhash_frames_total Frames hashed by pure-Rust downscaler path.");
    let _ = writeln!(out, "# TYPE rusty_dedup_dhash_frames_total counter");
    let _ = writeln!(
        out,
        "rusty_dedup_dhash_frames_total{{path=\"integer_exact\"}} {}",
        reg.dhash_frames_integer.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "rusty_dedup_dhash_frames_total{{path=\"fractional\"}} {}",
        reg.dhash_frames_fractional.load(Ordering::Relaxed)
    );

    let dhash_hist = reg.dhash_frame_duration.snapshot();
    if dhash_hist.count > 0 {
        let _ = writeln!(out, "# HELP rusty_dedup_dhash_frame_duration_seconds Per-frame Rust 9x8 dHash duration.");
        let _ = writeln!(out, "# TYPE rusty_dedup_dhash_frame_duration_seconds histogram");
        write_prom_histogram(
            &mut out,
            "rusty_dedup_dhash_frame_duration_seconds",
            "",
            &dhash_hist,
        );
    }

    // 5. Channel gauges & RAM
    let _ = writeln!(out, "# HELP rusty_dedup_channel_depth Current channel queue occupancy.");
    let _ = writeln!(out, "# TYPE rusty_dedup_channel_depth gauge");
    let _ = writeln!(
        out,
        "rusty_dedup_channel_depth{{channel=\"stage_frames\"}} {}",
        reg.stage_channel_depth.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "rusty_dedup_channel_depth{{channel=\"eval_pairs\"}} {}",
        reg.eval_channel_depth.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "rusty_dedup_channel_depth{{channel=\"sqlite_pending\"}} {}",
        reg.sqlite_pending_depth.load(Ordering::Relaxed)
    );

    let _ = writeln!(out, "# HELP rusty_dedup_channel_depth_peak High-water mark channel queue occupancy.");
    let _ = writeln!(out, "# TYPE rusty_dedup_channel_depth_peak gauge");
    let _ = writeln!(
        out,
        "rusty_dedup_channel_depth_peak{{channel=\"stage_frames\"}} {}",
        reg.stage_channel_peak.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "rusty_dedup_channel_depth_peak{{channel=\"eval_pairs\"}} {}",
        reg.eval_channel_peak.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "rusty_dedup_channel_depth_peak{{channel=\"sqlite_pending\"}} {}",
        reg.sqlite_pending_peak.load(Ordering::Relaxed)
    );

    let _ = writeln!(out, "# HELP rusty_dedup_inflight_luma_bytes Current RAM bytes in staged luma frames.");
    let _ = writeln!(out, "# TYPE rusty_dedup_inflight_luma_bytes gauge");
    let _ = writeln!(
        out,
        "rusty_dedup_inflight_luma_bytes {}",
        reg.inflight_luma_bytes.load(Ordering::Relaxed)
    );
    let _ = writeln!(out, "# HELP rusty_dedup_inflight_luma_bytes_peak Peak RAM bytes in staged luma frames.");
    let _ = writeln!(out, "# TYPE rusty_dedup_inflight_luma_bytes_peak gauge");
    let _ = writeln!(
        out,
        "rusty_dedup_inflight_luma_bytes_peak {}",
        reg.inflight_luma_bytes_peak.load(Ordering::Relaxed)
    );

    // 6. Bounded LRU cache metrics
    let _ = writeln!(out, "# HELP rusty_dedup_lru_cache_ops_total Bounded 1-fps hash LRU cache operations.");
    let _ = writeln!(out, "# TYPE rusty_dedup_lru_cache_ops_total counter");
    for (cache_name, c) in [("prefix", &reg.prefix_lru), ("dense", &reg.dense_lru)] {
        let _ = writeln!(
            out,
            "rusty_dedup_lru_cache_ops_total{{cache=\"{cache_name}\",outcome=\"hit\"}} {}",
            c.hits.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "rusty_dedup_lru_cache_ops_total{{cache=\"{cache_name}\",outcome=\"sqlite_reload\"}} {}",
            c.sqlite_reloads.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "rusty_dedup_lru_cache_ops_total{{cache=\"{cache_name}\",outcome=\"miss_decode\"}} {}",
            c.misses_decode.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "rusty_dedup_lru_cache_ops_total{{cache=\"{cache_name}\",outcome=\"eviction\"}} {}",
            c.evictions.load(Ordering::Relaxed)
        );
    }

    // 7. Duplicate funnel & work avoided
    let f = &reg.funnel;
    let _ = writeln!(out, "# HELP rusty_dedup_funnel_pairs_total Pair evaluation outcomes across pipeline stages.");
    let _ = writeln!(out, "# TYPE rusty_dedup_funnel_pairs_total counter");
    for (stage, val) in [
        ("pass1_strong_confirmed", f.pass1_strong_confirmed.load(Ordering::Relaxed)),
        ("pass1_conflict_rejected", f.pass1_conflict_rejected.load(Ordering::Relaxed)),
        ("pass2_sqlite_recovered", f.pass2_sqlite_recovered.load(Ordering::Relaxed)),
        ("pass2_transitive_skip", f.pass2_transitive_skip.load(Ordering::Relaxed)),
        ("pass2_blake3_exact", f.pass2_blake3_exact.load(Ordering::Relaxed)),
        ("pass2_stage_b_rejected", f.pass2_stage_b_rejected.load(Ordering::Relaxed)),
        ("pass2_stage_c_60s_rejected", f.pass2_stage_c_60s_rejected.load(Ordering::Relaxed)),
        ("pass2_stage_e_confirmed", f.pass2_stage_e_confirmed.load(Ordering::Relaxed)),
        ("pass2_stage_e_distinct", f.pass2_stage_e_distinct.load(Ordering::Relaxed)),
    ] {
        let _ = writeln!(out, "rusty_dedup_funnel_pairs_total{{stage=\"{stage}\"}} {val}");
    }

    let _ = writeln!(out, "# HELP rusty_dedup_video_seconds_avoided_total Video seconds spared from full 1-fps decode.");
    let _ = writeln!(out, "# TYPE rusty_dedup_video_seconds_avoided_total counter");
    for (stage, ms) in [
        ("pass1_isolated", f.avoided_ms_pass1_isolated.load(Ordering::Relaxed)),
        ("pass1_strong", f.avoided_ms_pass1_strong.load(Ordering::Relaxed)),
        ("stage_b_sparse", f.avoided_ms_stage_b_sparse.load(Ordering::Relaxed)),
        ("stage_c_60s", f.avoided_ms_stage_c_60s.load(Ordering::Relaxed)),
    ] {
        let _ = writeln!(
            out,
            "rusty_dedup_video_seconds_avoided_total{{stage=\"{stage}\"}} {:.3}",
            ms as f64 / 1000.0
        );
    }

    // 8. SQLite3 WAL persistence
    let _ = writeln!(out, "# HELP rusty_dedup_sqlite_commits_total Total batched SQLite3 WAL transactions committed.");
    let _ = writeln!(out, "# TYPE rusty_dedup_sqlite_commits_total counter");
    let _ = writeln!(
        out,
        "rusty_dedup_sqlite_commits_total {}",
        reg.sqlite_commits.load(Ordering::Relaxed)
    );
    let _ = writeln!(out, "# HELP rusty_dedup_sqlite_mutations_total Total SQLite3 WAL row mutations committed.");
    let _ = writeln!(out, "# TYPE rusty_dedup_sqlite_mutations_total counter");
    let _ = writeln!(
        out,
        "rusty_dedup_sqlite_mutations_total {}",
        reg.sqlite_mutations.load(Ordering::Relaxed)
    );

    let sql_hist = reg.sqlite_commit_duration.snapshot();
    if sql_hist.count > 0 {
        let _ = writeln!(out, "# HELP rusty_dedup_sqlite_commit_duration_seconds SQLite3 WAL batch commit latency.");
        let _ = writeln!(out, "# TYPE rusty_dedup_sqlite_commit_duration_seconds histogram");
        write_prom_histogram(
            &mut out,
            "rusty_dedup_sqlite_commit_duration_seconds",
            "",
            &sql_hist,
        );
    }

    let (db_bytes, wal_bytes) = sample_sqlite_file_bytes(db_path);
    let _ = writeln!(out, "# HELP rusty_dedup_sqlite_file_bytes On-disk SQLite3 database and WAL sizes.");
    let _ = writeln!(out, "# TYPE rusty_dedup_sqlite_file_bytes gauge");
    let _ = writeln!(out, "rusty_dedup_sqlite_file_bytes{{kind=\"db\"}} {db_bytes}");
    let _ = writeln!(out, "rusty_dedup_sqlite_file_bytes{{kind=\"wal\"}} {wal_bytes}");

    // 9. Process & subprocess OS resource metrics
    let res = sample_process_resources();
    let _ = writeln!(out, "# HELP rusty_dedup_process_max_rss_bytes Peak resident memory of rusty_dedup.");
    let _ = writeln!(out, "# TYPE rusty_dedup_process_max_rss_bytes gauge");
    let _ = writeln!(out, "rusty_dedup_process_max_rss_bytes {}", res.max_rss_bytes);

    let _ = writeln!(out, "# HELP rusty_dedup_process_cpu_seconds_total User/sys CPU seconds for Rust process and FFmpeg children.");
    let _ = writeln!(out, "# TYPE rusty_dedup_process_cpu_seconds_total counter");
    let _ = writeln!(
        out,
        "rusty_dedup_process_cpu_seconds_total{{target=\"rust_self\",mode=\"user\"}} {:.6}",
        res.rust_user_cpu_secs
    );
    let _ = writeln!(
        out,
        "rusty_dedup_process_cpu_seconds_total{{target=\"rust_self\",mode=\"sys\"}} {:.6}",
        res.rust_sys_cpu_secs
    );
    let _ = writeln!(
        out,
        "rusty_dedup_process_cpu_seconds_total{{target=\"ffmpeg_children\",mode=\"user\"}} {:.6}",
        res.children_user_cpu_secs
    );
    let _ = writeln!(
        out,
        "rusty_dedup_process_cpu_seconds_total{{target=\"ffmpeg_children\",mode=\"sys\"}} {:.6}",
        res.children_sys_cpu_secs
    );

    out
}

pub fn build_scrape_http_response(
    req_bytes: &[u8],
    reg: &TelemetryRegistry,
    meta: &RunMeta,
    db_path: &Path,
) -> String {
    let req_line = String::from_utf8_lossy(req_bytes);
    let first_line = req_line.lines().next().unwrap_or("");
    let target = first_line.split_whitespace().nth(1).unwrap_or("/");

    if target.starts_with("/metrics") {
        let body = render_prometheus_exposition(reg, meta, db_path);
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
    } else if target.starts_with("/healthz") {
        let body = "ok\n";
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
    } else {
        let body = "404 Not Found (try /metrics)\n";
        format!(
            "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
    }
}

/// Lightweight local HTTP/1.1 Prometheus pull scrape server (`std::net::TcpListener`, zero dependencies).
pub struct PrometheusScrapeServer {
    bound_addr: SocketAddr,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl PrometheusScrapeServer {
    pub fn start(
        bind_addr: &str,
        reg: Arc<TelemetryRegistry>,
        meta: RunMeta,
        db_path: PathBuf,
    ) -> Result<Self> {
        let listener = TcpListener::bind(bind_addr).with_context(|| {
            format!("Failed to bind Prometheus HTTP scrape server on {}", bind_addr)
        })?;
        let bound_addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_ref = Arc::clone(&stop);

        let handle = thread::Builder::new()
            .name("prom-scrape-http".to_string())
            .spawn(move || {
                while !stop_ref.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            let _ = stream.set_nonblocking(false);
                            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                            let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
                            let mut req_buf = [0u8; 1024];
                            let n = stream.read(&mut req_buf).unwrap_or(0);
                            let resp =
                                build_scrape_http_response(&req_buf[..n], &reg, &meta, &db_path);
                            let _ = stream.write_all(resp.as_bytes());
                            let _ = stream.flush();
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(50));
                        }
                        Err(_) => {
                            thread::sleep(Duration::from_millis(50));
                        }
                    }
                }
            })
            .context("Failed to spawn Prometheus HTTP scrape thread")?;

        Ok(Self {
            bound_addr,
            stop,
            handle: Some(handle),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Builds the `(connect_addr, http_put_request)` for a local HTTP Prometheus Pushgateway push.
pub fn build_pushgateway_put_request(
    base_url: &str,
    job_name: &str,
    instance: &str,
    body: &str,
) -> Result<(String, String)> {
    let trimmed = base_url.trim().trim_end_matches('/');
    if trimmed.to_lowercase().starts_with("https://") {
        anyhow::bail!(
            "HTTPS Pushgateway URLs ({}) are not supported (local http:// Pushgateway is supported without TLS dependencies).",
            base_url
        );
    }
    let without_scheme = trimmed
        .strip_prefix("http://")
        .or_else(|| trimmed.strip_prefix("HTTP://"))
        .unwrap_or(trimmed);

    let (host_port, base_path) = match without_scheme.find('/') {
        Some(idx) => (&without_scheme[..idx], &without_scheme[idx..]),
        None => (without_scheme, ""),
    };
    let connect_addr = if host_port.contains(':') {
        host_port.to_string()
    } else {
        format!("{}:80", host_port)
    };

    let req_path = format!(
        "{}/metrics/job/{}/instance/{}",
        base_path.trim_end_matches('/'),
        job_name,
        instance
    );

    let req = format!(
        "PUT {} HTTP/1.1\r\nHost: {}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        req_path,
        host_port,
        body.len(),
        body
    );
    Ok((connect_addr, req))
}

/// Pushes Prometheus Text Exposition v0.0.4 payload to a local HTTP Prometheus Pushgateway
/// via `PUT /metrics/job/<job_name>/instance/<instance>` using pure `std::net::TcpStream`.
pub fn push_to_pushgateway(
    base_url: &str,
    job_name: &str,
    instance: &str,
    body: &str,
) -> Result<()> {
    let (connect_addr, req) = build_pushgateway_put_request(base_url, job_name, instance, body)?;

    let mut stream = TcpStream::connect(&connect_addr).with_context(|| {
        format!(
            "Failed to connect to Prometheus Pushgateway at {}",
            connect_addr
        )
    })?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    stream.write_all(req.as_bytes())?;
    stream.flush()?;

    let mut resp_buf = [0u8; 512];
    let n = stream.read(&mut resp_buf).unwrap_or(0);
    let resp = String::from_utf8_lossy(&resp_buf[..n]);
    let status_line = resp.lines().next().unwrap_or("");
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    if !(200..300).contains(&status_code) {
        anyhow::bail!(
            "Prometheus Pushgateway returned HTTP status {} ({:?})",
            status_code,
            status_line
        );
    }
    Ok(())
}

/// Atomically writes a `.prom` file for `node_exporter --collector.textfile.directory`.
pub fn write_textfile_atomic(path: &Path, body: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("rusty_dedup.prom");
    let tmp_path = path.with_file_name(format!("{}.tmp.{}", file_name, std::process::id()));
    {
        let file = File::create(&tmp_path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(body.as_bytes())?;
        writer.flush()?;
        writer.into_inner()?.sync_all()?;
    }
    fs::rename(&tmp_path, path)?;
    Ok(())
}

/// Atomically writes a JSON telemetry snapshot (structured summary + pre-rendered Prometheus v0.0.4 text)
/// for consumption by `scripts/rusty_dedup_metrics_harness.py`.
pub fn write_telemetry_snapshot_atomic(
    path: &Path,
    reg: &TelemetryRegistry,
    meta: &RunMeta,
    db_path: &Path,
    finished: bool,
) -> Result<()> {
    let summary = reg.build_summary(meta, db_path);
    let exposition = render_prometheus_exposition(reg, meta, db_path);
    let payload = serde_json::json!({
        "pid": std::process::id(),
        "finished": finished,
        "phase": reg.current_phase(),
        "uptime_secs": reg.started_at.elapsed().as_secs_f64(),
        "prometheus_exposition": exposition,
        "summary": summary,
    });
    let body = serde_json::to_string(&payload)?;
    write_textfile_atomic(path, &body)
}

/// Background telemetry reporter thread coordinating:
///   - Live terminal heartbeats (`heartbeat_interval_secs`)
///   - Atomic JSON snapshot flushes every 500ms when `snapshot_path` is active (for Python metrics harness)
///   - Opt-in Prometheus HTTP `/metrics` scrape server (`listen_addr`, skipped when `RUSTY_DEDUP_HARNESS_ACTIVE=1`)
///   - Opt-in Prometheus Pushgateway periodic + guaranteed final push (`pushgateway_url`)
///   - Opt-in `.prom` textfile atomic export (`textfile_path`)
///   - End-of-run APM dashboard + optional JSON summary (`--telemetry-out`)
pub struct TelemetryReporter {
    enabled: bool,
    harness_active: bool,
    reg: Arc<TelemetryRegistry>,
    meta: RunMeta,
    db_path: PathBuf,
    cfg: TelemetryConfig,
    telemetry_out: Option<PathBuf>,
    scrape_server: Option<PrometheusScrapeServer>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl TelemetryReporter {
    pub fn start(
        cfg: TelemetryConfig,
        reg: Arc<TelemetryRegistry>,
        meta: RunMeta,
        db_path: PathBuf,
        telemetry_out: Option<PathBuf>,
    ) -> Self {
        let harness_active = std::env::var("RUSTY_DEDUP_HARNESS_ACTIVE")
            .map(|v| matches!(v.trim().to_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);

        if !cfg.enabled {
            return Self {
                enabled: false,
                harness_active,
                reg,
                meta,
                db_path,
                cfg,
                telemetry_out,
                scrape_server: None,
                stop: Arc::new(AtomicBool::new(true)),
                handle: None,
            };
        }

        if let Some(ref snap_path) = cfg.snapshot_path {
            let _ = write_telemetry_snapshot_atomic(snap_path, &reg, &meta, &db_path, false);
        }

        let scrape_server = if harness_active {
            eprintln!(
                "[Telemetry] Running under Python metrics harness (snapshot={:?}); skipping duplicate Rust HTTP bind.",
                cfg.snapshot_path
            );
            None
        } else if let Some(ref addr) = cfg.prometheus.listen_addr {
            match PrometheusScrapeServer::start(addr, Arc::clone(&reg), meta.clone(), db_path.clone())
            {
                Ok(srv) => {
                    eprintln!(
                        "[Prometheus] HTTP scrape server listening at http://{}/metrics",
                        srv.local_addr()
                    );
                    Some(srv)
                }
                Err(e) => {
                    eprintln!("Warning: {} (continuing without HTTP scrape endpoint)", e);
                    None
                }
            }
        } else {
            None
        };

        if !harness_active {
            if let Some(ref url) = cfg.prometheus.pushgateway_url {
                eprintln!(
                    "[Prometheus] Pushgateway enabled: {} (job={}, instance={}, every {}s + final push)",
                    url, cfg.prometheus.job_name, meta.session_id, cfg.prometheus.push_interval_secs
                );
            }
            if let Some(ref tf) = cfg.prometheus.textfile_path {
                eprintln!("[Prometheus] Textfile collector output enabled: {:?}", tf);
            }
        }

        let stop = Arc::new(AtomicBool::new(false));
        let needs_bg_thread = cfg.heartbeat_interval_secs > 0
            || cfg.snapshot_path.is_some()
            || (!harness_active
                && (cfg.prometheus.pushgateway_url.is_some()
                    || cfg.prometheus.textfile_path.is_some()));

        let handle = if needs_bg_thread {
            let stop_ref = Arc::clone(&stop);
            let reg_ref = Arc::clone(&reg);
            let meta_ref = meta.clone();
            let db_ref = db_path.clone();
            let cfg_ref = cfg.clone();

            thread::Builder::new()
                .name("telemetry-reporter".to_string())
                .spawn(move || {
                    let mut last_hb = Instant::now();
                    let mut last_push = Instant::now();
                    let mut last_snap = Instant::now();
                    let hb_dur = Duration::from_secs(cfg_ref.heartbeat_interval_secs.max(1));
                    let push_dur = Duration::from_secs(cfg_ref.prometheus.push_interval_secs.max(1));
                    let snap_dur = Duration::from_millis(500);

                    while !stop_ref.load(Ordering::Relaxed) {
                        thread::sleep(Duration::from_millis(200));

                        if let Some(ref snap_path) = cfg_ref.snapshot_path {
                            if last_snap.elapsed() >= snap_dur {
                                let _ = write_telemetry_snapshot_atomic(
                                    snap_path,
                                    &reg_ref,
                                    &meta_ref,
                                    &db_ref,
                                    false,
                                );
                                last_snap = Instant::now();
                            }
                        }

                        if cfg_ref.heartbeat_interval_secs > 0 && last_hb.elapsed() >= hb_dur {
                            eprintln!("{}", reg_ref.format_heartbeat_line(&db_ref));
                            last_hb = Instant::now();
                        }

                        if !harness_active
                            && (cfg_ref.prometheus.pushgateway_url.is_some()
                                || cfg_ref.prometheus.textfile_path.is_some())
                            && last_push.elapsed() >= push_dur
                        {
                            let body =
                                render_prometheus_exposition(&reg_ref, &meta_ref, &db_ref);
                            if let Some(ref url) = cfg_ref.prometheus.pushgateway_url {
                                if let Err(e) = push_to_pushgateway(
                                    url,
                                    &cfg_ref.prometheus.job_name,
                                    &meta_ref.session_id,
                                    &body,
                                ) {
                                    eprintln!("Warning: Periodic Prometheus push failed: {}", e);
                                }
                            }
                            if let Some(ref tf) = cfg_ref.prometheus.textfile_path {
                                if let Err(e) = write_textfile_atomic(tf, &body) {
                                    eprintln!(
                                        "Warning: Failed writing Prometheus textfile {:?}: {}",
                                        tf, e
                                    );
                                }
                            }
                            last_push = Instant::now();
                        }
                    }
                })
                .ok()
        } else {
            None
        };

        Self {
            enabled: true,
            harness_active,
            reg,
            meta,
            db_path,
            cfg,
            telemetry_out,
            scrape_server,
            stop,
            handle,
        }
    }

    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }

        if self.enabled {
            if let Some(ref snap_path) = self.cfg.snapshot_path {
                let _ = write_telemetry_snapshot_atomic(
                    snap_path,
                    &self.reg,
                    &self.meta,
                    &self.db_path,
                    true,
                );
            }

            // Guaranteed final Prometheus push & textfile write before stopping scrape server
            if !self.harness_active
                && (self.cfg.prometheus.pushgateway_url.is_some()
                    || self.cfg.prometheus.textfile_path.is_some())
            {
                let body = render_prometheus_exposition(&self.reg, &self.meta, &self.db_path);
                if let Some(ref url) = self.cfg.prometheus.pushgateway_url {
                    match push_to_pushgateway(
                        url,
                        &self.cfg.prometheus.job_name,
                        &self.meta.session_id,
                        &body,
                    ) {
                        Ok(()) => eprintln!(
                            "[Prometheus] Final metrics pushed to {} (job={}, instance={}).",
                            url, self.cfg.prometheus.job_name, self.meta.session_id
                        ),
                        Err(e) => eprintln!("Warning: Final Prometheus push failed: {}", e),
                    }
                }
                if let Some(ref tf) = self.cfg.prometheus.textfile_path {
                    if let Err(e) = write_textfile_atomic(tf, &body) {
                        eprintln!("Warning: Final Prometheus textfile write failed: {}", e);
                    } else {
                        eprintln!("[Prometheus] Wrote final textfile metrics to {:?}.", tf);
                    }
                }
            }

            let summary = self.reg.build_summary(&self.meta, &self.db_path);
            eprint!("{}", format_terminal_dashboard(&summary));

            if let Some(ref out_path) = self.telemetry_out {
                match serde_json::to_string_pretty(&summary) {
                    Ok(json) => {
                        if let Err(e) = fs::write(out_path, json) {
                            eprintln!(
                                "Warning: Failed to write APM telemetry JSON to {:?}: {}",
                                out_path, e
                            );
                        } else {
                            eprintln!("[Telemetry] Saved APM summary JSON to {:?}.", out_path);
                        }
                    }
                    Err(e) => eprintln!("Warning: Failed to serialize APM telemetry JSON: {}", e),
                }
            }
        }

        if let Some(srv) = self.scrape_server.take() {
            srv.shutdown();
        }
    }
}
