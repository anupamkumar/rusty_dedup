# Dedup & Organized Vids

**High-performance, GPU-accelerated, crash-recoverable duplicate video discovery, resolution, and observability suite built in Rust.**

`rusty_dedup` finds visually identical and re-encoded duplicate videos across multi-terabyte libraries stored on external spinning HDDs or SSDs—even when filenames, resolutions, containers (`.mp4`, `.mkv`, `.wmv`, `.avi`, `.mov`), or codecs (`H.264`, `H.265/HEVC`, `AV1`, `VP9`, `WMV`) differ. Its companion tool **`rusty_clean`** scores every duplicate group against a customizable YAML rubric (`decision_config.yaml`), keeps the highest-quality copy, deletes inferior duplicates, and either links across directories or consolidates winners and unique files into a single target folder. An optional zero-dependency Python companion (**`scripts/rusty_dedup_metrics_harness.py`**) adds OS-kernel thread introspection and host CPU/RAM/GPU metrics to Prometheus & Grafana.

---

## Use Cases & Why `rusty_dedup`?

### Real-World Use Cases
- **Multi-Drive Archive Consolidation**: Scan `dirA`, `dirB`, and `dirC` across separate external USB HDDs, eliminate cross-drive duplicates, and merge all retained winners plus unique videos into one clean destination directory (`rusty_clean -t dirA`).
- **Post-Transcode Cleanup**: You re-encoded an H.264/WMV library into H.265/AV1 and want to detect the old lower-efficiency duplicates automatically while preserving the higher-quality copy.
- **Messy Filenames & Studio Prefixes**: Match `StudioX - Sunset_Clip_1080p_x265.mkv` with `SunsetClip.mp4` without falsely merging `Episode 1` and `Episode 2` (hard sequel-marker protection).
- **Unattended Overnight 80 TB Scans**: Safely interrupt and resume multi-hour scans with zero wasted work thanks to SQLite3 WAL persistence and live Grafana/Prometheus dashboards.

### Why `rusty_dedup`?
- **Hardware-Accelerated GPU Decode + `72×72` On-GPU Downscaling**: Supports 11 cross-platform GPU backends (`videotoolbox`, `cuda`, `d3d12va`, `d3d11va`, `qsv`, `vaapi`, `dxva2`, `vulkan`, `v4l2m2m`, `rkmpp`, `mediacodec`) with strict `--config-test` fast-fail verification (never silently falls back to slow CPU without telling you). Downscaling on-GPU cuts frame transfer size by **600×** vs. 1080p.
- **Spinning-HDD Friendly**: Sorts files monotonically by `(dev_id, inode)` per physical disk so drive heads sweep sequentially instead of thrashing.
- **Flat Memory Footprint at 300K-File Scale**: Bounded `sync_channel(16)` backpressure, 2,000-entry LRU caches, and SQLite3 WAL binary `u64` BLOBs keep peak RAM around **~75–150 MB** even on 80 TB libraries.
- **Massive Compute Savings**: A 6-gate cost-escalating funnel (`BLAKE3` → 5-keyframe sparse matrix → 60-second early-exit → full 1-fps sliding-window alignment) avoids **95%+** of unnecessary full-video decodes.
- **Quality-Aware Bitrate Resolution**: Single-pass `ffprobe` captures video and audio bitrates (`bps`) across MP4/MOV/MKV/WebM/AVI/WMV, and `rusty_clean` weights `video_bitrate` **2.0×** relative to `audio_bitrate` (**1.0×**) alongside codec, resolution, runtime, and file size.
- **Production APM Telemetry Out of the Box**: Lock-free 24-bucket histograms (`10µs`–`30 min`), per-thread USE+RED utilization, Bottleneck Advisor, and 3-layer Prometheus/Grafana metrics via the zero-dependency Python harness.

---

## Installation & Execution

### Prerequisites
- **Rust toolchain** (1.85+ / Cargo): [rustup.rs](https://rustup.rs)
- **FFmpeg & FFprobe** on your `PATH`:
  - macOS: `brew install ffmpeg`
  - Linux (Debian/Ubuntu): `sudo apt install ffmpeg`
  - Windows: `winget install Gyan.FFmpeg`
- **Python 3.9+** *(optional, zero `pip` packages needed)* — only required if you want the external OS-thread & host-hardware Prometheus metrics harness.


### OPTION 1:
#### 1. Download the latest release binary appropriate for your Operating System

#### 2. Download the source-code either by getting the tar.gz/zip or cloning the repo

`git clone https://codeberg.org/krazykllh/rusty_dedup.git`

#### 3. Place and configure `dedup_config.yaml` next to rusty_dedup and rusty_clean binaries. 

#### 4. Verify config-test and verify FFMPEG & GPU Acceleration
In linux terminal type
`$ ./rusty_dedup --config-test`

or 

In Windows cmd.exe or powershell.exe type
`C:\> rusty_dedup.exe --config-test`

### OPTION 2:
#### 1. Build Optimized Release Binaries

```bash
cd /path/to/rusty_dedup
cargo build --release
```

Compiled binaries are placed in:
- `target/release/rusty_dedup`
- `target/release/rusty_clean`

#### 2. Verify FFmpeg & GPU Acceleration (`--config-test`)

Run this once to confirm your GPU decoder & `72×72` hardware scaler work against `dedup_config.yaml`:

```bash
./target/release/rusty_dedup --config-test
```

#### 3. Scan for Duplicates (`rusty_dedup`)

**Standard run (standalone):**
```bash
./target/release/rusty_dedup /Volumes/DriveA /Volumes/DriveB /Volumes/DriveC \
  -o duplicates.json
```

**With full 3-layer Prometheus/Grafana telemetry (Python Metrics Harness):**
```bash
python3 scripts/rusty_dedup_metrics_harness.py -- \
  ./target/release/rusty_dedup /Volumes/DriveA /Volumes/DriveB /Volumes/DriveC \
  -o duplicates.json
```

#### 4. Resolve Duplicates & Consolidate (`rusty_clean`)

**Always preview first with `--dry-run`:**
```bash
# Preview in-place cleanup + cross-directory symlinks
./target/release/rusty_clean duplicates.json -c decision_config.yaml --dry-run

# Preview merging all winners + unique files from all scanned folders into /Volumes/DriveA
./target/release/rusty_clean duplicates.json -c decision_config.yaml -t /Volumes/DriveA --dry-run
```

Remove `--dry-run` when ready to execute on disk.

---

## How It Works (Architecture Overview)

1. **Multi-Threaded Pipeline**: Per-volume Rayon `ffprobe_workers` sweep each physical drive in monotonic `(dev_id, inode)` order and stream records to a batched `sqlite-wal-writer` thread. A dedicated 3-thread `DecodePipeline` (`gpu-decode-worker`, `cpu-decode-worker`, `cpu-dhash-worker`) connects decoders to pure-Rust exact integer `8×9` block-average `dHash` over a bounded `sync_channel(16)`, while dedicated `pair-eval-pass1` and `pair-eval-pass2` threads evaluate pairs concurrently.
2. **Multi-Stage Duplicate Detection Funnel**: Files are sorted by runtime for an $O(N \log N + K)$ sliding window (`±10s` or `±10%`). Pass 1 disentangles studio/creator prefixes via the persistent `.states/creator_directory.json` + TF-IDF discovery, enforces sequel-marker gates, and fast-paths high-confidence `Strong` pairs using 5 sparse keyframes. Remaining pairs pass through SQLite recovery → Union-Find transitive skip → Stage A (192 KB `BLAKE3`) → Stage B (5×5 sparse keyframe matrix) → Stage C (60-second `-t 60` early-exit) → Stage D (lazy full 1-fps decode cached in 2,000-entry LRUs + SQLite BLOBs) → Stage E (`[-10s..+10s]` sliding-window alignment, `>= 90%` confidence) + automatic creator alias learning.
   > **Note on 1–3 Character Acronyms**: Automatic creator discovery intentionally ignores `< 4`-char or vowel-less tokens (e.g., `DBZ`, `HBO`, `BBC`) to keep junk folder names out of `creator_directory.json`. Videos with such prefixes are still accurately matched via Pass 2 visual dHash; you can also map them manually under `"alias_to_canonical"` in `.states/creator_directory.json`.
3. **Weighted Resolution (`rusty_clean`)**: Scores every file in a duplicate group out of `9.0000` (`size_in_bytes`, `runtime_in_seconds`, `video_height`, `video_width`, `video_bitrate` weighted **2.0×**, `audio_bitrate` weighted **1.0×**, `video_codec`, `audio_codec`), retains the highest scorer, deletes losers, and either creates cross-directory symlinks or consolidates winners + non-duplicates from all scanned `input_directories` into `--target`.
4. **3-Layer APM Telemetry**: Merges lock-free in-process Rust metrics (Layer 1) with unprivileged OS-kernel thread & child-process syscalls (Layer 2) and Host CPU/RAM/GPU syscalls (Layer 3) onto a single Prometheus `/metrics` endpoint.

### Deep-Dive Documentation
- **[docs/README.md](docs/README.md)** — Documentation index and repository layout.
- **[docs/01_multithreaded_architecture.md](docs/01_multithreaded_architecture.md)** — Full thread inventory, memory/backpressure invariants, **Architecture Diagram**, and **High-Level Multi-Thread Sequence Diagram**.
- **[docs/02_duplicate_detection_algorithm.md](docs/02_duplicate_detection_algorithm.md)** — Mathematical heuristics, funnel stages A–E, `rusty_clean` 2:1 bitrate scoring, **State-Machine Diagram**, and **Algorithm Sequence Diagram**.
- **[docs/03_metrics_and_observability.md](docs/03_metrics_and_observability.md)** — Python harness internals, **Component & Architecture Diagrams**, **complete catalog of all 44 metric families** (meaning, rationale, insights), and PromQL recipes.

---

## CLI Command Reference

### 1. `rusty_dedup`

```text
rusty_dedup [OPTIONS] <DIRECTORIES>...
```

| Option | Default | Description & Usage Example |
| :--- | :--- | :--- |
| `<DIRECTORIES>...` | *(required unless `--config-test`)* | One or more directories scanned recursively (overlapping mounts deduplicated by `(dev, inode)`).<br>Example: `./target/release/rusty_dedup /Volumes/DriveA /Volumes/DriveB` |
| `--config-test` | `false` | Runs mandatory 2-step FFmpeg/FFprobe + live GPU `72×72 → 9×8` verification against `dedup_config.yaml` and exits.<br>Example: `./target/release/rusty_dedup --config-test` |
| `-c, --dedup-config <PATH>` *(alias `--config`)* | `./dedup_config.yaml` | Path to YAML config for FFmpeg, GPU acceleration, and telemetry/Prometheus.<br>Example: `./target/release/rusty_dedup -c /etc/dedup_config.yaml /Volumes/Media` |
| `-v, --verbose` | `false` | Prints detailed file discovery, creator extraction, `ffprobe` bitrates, sparse 5×5 matrices, sliding-window offset tables, and WAL commits.<br>Example: `./target/release/rusty_dedup -v /Volumes/Media` |
| `-o, --output <PATH>` | `duplicates.json` | Output JSON report path containing `input_directories` and confirmed duplicate groups with `video_bitrate_bps` / `audio_bitrate_bps`.<br>Example: `./target/release/rusty_dedup /Volumes/Media -o /tmp/dups.json` |
| `--categories-log <PATH>` | *(none)* | Streams all evaluated pair verdicts from SQLite3 WAL to a JSON array in constant RAM.<br>Example: `./target/release/rusty_dedup /Volumes/Media --categories-log pairs_audit.json` |
| `--states-dir <PATH>` | `.states` | Directory for crash-recoverable SQLite3 WAL databases (`rusty_session_{id}.db`) and `creator_directory.json` (put on local SSD/NVMe for best speed).<br>Example: `./target/release/rusty_dedup /Volumes/HDD --states-dir ~/dedup_states` |
| `--creator-dir-file <PATH>` | `<states-dir>/creator_directory.json` | Custom path for the cross-session learned Creator Directory JSON.<br>Example: `./target/release/rusty_dedup /Volumes/Media --creator-dir-file ~/creators.json` |
| `--tfidf-creator-threshold <FLOAT>` | `0.15` | Minimum corpus document frequency (`0.0`–`1.0`) for a repeating filename prefix/suffix to be auto-learned as a creator/studio.<br>Example: `./target/release/rusty_dedup /Volumes/Media --tfidf-creator-threshold 0.20` |
| `--checkpoint-interval-ms <MS>` | `2000` | Maximum milliseconds between batched SQLite3 WAL commits (also flushes every 500 mutations).<br>Example: `./target/release/rusty_dedup /Volumes/Media --checkpoint-interval-ms 5000` |
| `--reset-session` | `false` | Deletes the existing `.states/rusty_session_{id}.db` for this directory set and rescans from scratch.<br>Example: `./target/release/rusty_dedup /Volumes/Media --reset-session` |
| `--hdd-concurrency <N>` | `1` | Concurrent `ffprobe` workers per physical disk volume (`1` preserves strict sequential inode order on spinning HDDs; raise to `4`–`8` for NVMe SSDs).<br>Example: `./target/release/rusty_dedup /Volumes/NVMe --hdd-concurrency 4` |
| `--max-hamming-distance <BITS>` | `6` | Maximum differing bits out of 64 for two `9×8` dHash frames to match (`6` = `90.6%` similarity).<br>Example: `./target/release/rusty_dedup /Volumes/Media --max-hamming-distance 8` |
| `--min-confidence <FLOAT>` | `0.90` | Minimum fraction (`0.0`–`1.0`) of aligned 1-fps seconds that must match to confirm a duplicate.<br>Example: `./target/release/rusty_dedup /Volumes/Media --min-confidence 0.85` |
| `--max-runtime-diff-secs <SECS>` | `10.0` | Absolute runtime window (`±secs`, alongside `±10%` relative window) for pairing candidate videos.<br>Example: `./target/release/rusty_dedup /Volumes/Media --max-runtime-diff-secs 15.0` |
| `--max-slide-offset-secs <SECS>` | `10` | Maximum time shift (`±secs`) tested during Stage E sliding-window alignment for trimmed intros/outros.<br>Example: `./target/release/rusty_dedup /Volumes/Media --max-slide-offset-secs 15` |
| `--prometheus-listen <ADDR>` | *(from YAML)* | Override local HTTP Prometheus pull scrape bind address (`none` disables).<br>Example: `./target/release/rusty_dedup /Volumes/Media --prometheus-listen 127.0.0.1:9185` |
| `--prometheus-pushgateway <URL>` | *(from YAML)* | Override local HTTP Pushgateway URL for periodic + guaranteed final push (`none` disables).<br>Example: `./target/release/rusty_dedup /Volumes/Media --prometheus-pushgateway http://127.0.0.1:9091` |
| `--prometheus-job <NAME>` | `rusty_dedup` | Override Pushgateway `job` grouping label.<br>Example: `./target/release/rusty_dedup /Volumes/Media --prometheus-job media_nightly` |
| `--prometheus-push-interval-secs <SECS>` | `15` | Override interval in seconds between Pushgateway pushes.<br>Example: `./target/release/rusty_dedup /Volumes/Media --prometheus-push-interval-secs 30` |
| `--prometheus-textfile <PATH>` | *(from YAML)* | Atomically export `.prom` metrics file for `node_exporter --collector.textfile.directory`.<br>Example: `./target/release/rusty_dedup /Volumes/Media --prometheus-textfile /var/lib/node_exporter/rusty_dedup.prom` |
| `--telemetry-interval-secs <SECS>` | `30` | Live terminal heartbeat cadence (`0` = end-of-run APM dashboard only).<br>Example: `./target/release/rusty_dedup /Volumes/Media --telemetry-interval-secs 60` |
| `--telemetry-out <PATH>` | *(none)* | Save the structured end-of-run APM summary as JSON.<br>Example: `./target/release/rusty_dedup /Volumes/Media --telemetry-out run_apm.json` |
| `--telemetry-snapshot <PATH>` | *(env/YAML)* | Atomically write 500 ms live telemetry JSON snapshots (auto-wired by the Python metrics harness).<br>Example: `./target/release/rusty_dedup /Volumes/Media --telemetry-snapshot /tmp/dedup_snap.json` |
| `--no-telemetry` | `false` | Disables telemetry reporter, heartbeats, Prometheus endpoints, and end-of-run APM summary.<br>Example: `./target/release/rusty_dedup /Volumes/Media --no-telemetry` |

---

### 2. `rusty_clean`

```text
rusty_clean [OPTIONS] <DUPLICATES_JSON>
```

| Option | Default | Description & Usage Example |
| :--- | :--- | :--- |
| `<DUPLICATES_JSON>` | *(required)* | Path to the `duplicates.json` file generated by `rusty_dedup`.<br>Example: `./target/release/rusty_clean duplicates.json --dry-run` |
| `-c, --decision-config <PATH>` | `<binary_dir>/decision_config.yaml` | YAML scoring rules (`size_in_bytes`, `runtime_in_seconds`, `video_height`, `video_width`, `video_bitrate` [2.0×], `audio_bitrate` [1.0×], `video_codec`, `audio_codec`).<br>Example: `./target/release/rusty_clean duplicates.json -c ./decision_config.yaml` |
| `-t, --target <DIRECTORY_PATH>` | *(none)* | Consolidation mode: deletes duplicates (skips symlinks), moves retained winners and all non-duplicate videos from every scanned `input_directories` entry into `<DIRECTORY_PATH>` (with `_1`/`_2` collision safety and 4 MiB cross-volume progress bars), and updates `duplicates.json`.<br>Example: `./target/release/rusty_clean duplicates.json -c decision_config.yaml -t /Volumes/Consolidated` |
| `--dry-run` | `false` | Simulates all scoring, deletions, moves, and symlinks without modifying a single file on disk.<br>Example: `./target/release/rusty_clean duplicates.json -t /Volumes/DriveA --dry-run` |
| `-v, --verbose` | `false` | Prints timestamped per-file directory discovery, rename/copy diagnostics, and JSON update steps.<br>Example: `./target/release/rusty_clean duplicates.json -v --dry-run` |
| `-o, --report-out <PATH>` | *(none)* | Saves the complete per-group scoring breakdown and action audit trail as JSON.<br>Example: `./target/release/rusty_clean duplicates.json --dry-run -o clean_audit.json` |

---

### 3. `scripts/rusty_dedup_metrics_harness.py`

```text
python3 scripts/rusty_dedup_metrics_harness.py [OPTIONS] [-- <RUSTY_DEDUP_COMMAND...>]
```

| Option | Default | Description & Usage Example |
| :--- | :--- | :--- |
| `-- <COMMAND...>` | *(Harness Mode)* | Launches `rusty_dedup` as a monitored child process, automatically passing `RUSTY_DEDUP_HARNESS_ACTIVE=1` and a per-PID snapshot path.<br>Example: `python3 scripts/rusty_dedup_metrics_harness.py -- ./target/release/rusty_dedup /Volumes/Media` |
| `--pid <PID>` | *(Attach Mode)* | Attaches to an already-running `rusty_dedup` PID instead of launching a child.<br>Example: `python3 scripts/rusty_dedup_metrics_harness.py --pid 41230 --snapshot /tmp/snap.json` |
| `--self-test` | `false` | Empirically verifies OS-kernel child thread introspection, host hardware syscalls, and Prometheus v0.0.4 output, then exits.<br>Example: `python3 scripts/rusty_dedup_metrics_harness.py --self-test` |
| `-c, --config <PATH>` | `./dedup_config.yaml` | Loads default Prometheus settings (`listen_addr`, `pushgateway_url`, `job_name`, `textfile_path`) from `dedup_config.yaml`.<br>Example: `python3 scripts/rusty_dedup_metrics_harness.py -c custom_dedup.yaml -- ./target/release/rusty_dedup /Volumes/Media` |
| `--listen <HOST:PORT>` | *(from YAML)* | Overrides Prometheus HTTP scrape bind address (`none` disables).<br>Example: `python3 scripts/rusty_dedup_metrics_harness.py --listen 0.0.0.0:9185 -- ./target/release/rusty_dedup /Volumes/Media` |
| `--pushgateway-url <URL>` | *(from YAML)* | Overrides local HTTP Pushgateway URL.<br>Example: `python3 scripts/rusty_dedup_metrics_harness.py --pushgateway-url http://127.0.0.1:9091 -- ./target/release/rusty_dedup /Volumes/Media` |
| `--push-interval-secs <SECS>` | *(from YAML)* | Overrides seconds between Pushgateway pushes.<br>Example: `python3 scripts/rusty_dedup_metrics_harness.py --push-interval-secs 10 -- ./target/release/rusty_dedup /Volumes/Media` |
| `--job-name <NAME>` | `rusty_dedup` | Overrides Pushgateway `job` grouping label.<br>Example: `python3 scripts/rusty_dedup_metrics_harness.py --job-name archive_scan -- ./target/release/rusty_dedup /Volumes/Media` |
| `--textfile-path <PATH>` | *(from YAML)* | Overrides atomic `.prom` textfile export path.<br>Example: `python3 scripts/rusty_dedup_metrics_harness.py --textfile-path /tmp/rusty_dedup.prom -- ./target/release/rusty_dedup /Volumes/Media` |
| `--nodename <NAME>` | Hostname | Sets `nodename` Prometheus label for Grafana parity with `rusty_h265_converter`.<br>Example: `python3 scripts/rusty_dedup_metrics_harness.py --nodename mac-m4 -- ./target/release/rusty_dedup /Volumes/Media` |
| `--snapshot <PATH>` | `/tmp/rusty_dedup_telemetry_<pid>.json` | Overrides the atomic JSON snapshot path shared between `rusty_dedup` and the harness.<br>Example: `python3 scripts/rusty_dedup_metrics_harness.py --snapshot /tmp/my_snap.json -- ./target/release/rusty_dedup /Volumes/Media` |
| `--poll-interval <SECS>` | `1.0` | Polling interval in seconds for OS thread syscalls, host hardware, and snapshot reads.<br>Example: `python3 scripts/rusty_dedup_metrics_harness.py --poll-interval 0.5 -- ./target/release/rusty_dedup /Volumes/Media` |
| `--linger-secs <SECS>` | `20.0` | Holds `GET /metrics` alive after `rusty_dedup` exits so Prometheus captures final counters.<br>Example: `python3 scripts/rusty_dedup_metrics_harness.py --linger-secs 30 -- ./target/release/rusty_dedup /Volumes/Media` |
| `-v, --verbose` | `false` | Logs harness startup config, target PID, and shutdown drain diagnostics to stderr.<br>Example: `python3 scripts/rusty_dedup_metrics_harness.py -v -- ./target/release/rusty_dedup /Volumes/Media` |

---

## Setting Up Prometheus & Grafana APM Tracking

### Prerequisites
1. **Prometheus** installed and running locally:
   - macOS: `brew install prometheus && brew services start prometheus`
   - Linux: `sudo apt install prometheus` (or official Prometheus tarball)
2. **Grafana** installed with Prometheus configured as a data source (`http://localhost:9090`):
   - macOS: `brew install grafana && brew services start grafana` (UI at `http://localhost:3000`)
3. **Python 3** *(recommended)* for full 3-layer metrics (Rust domain + OS kernel threads + Host CPU/RAM/GPU) with zero `pip` dependencies.
4. *(Optional)* **Prometheus Pushgateway** (`http://127.0.0.1:9091`) if you prefer push-based collection for short-lived scans, or **`node_exporter`** with `--collector.textfile.directory` for `.prom` file export.

### Step 1: Enable Prometheus in `dedup_config.yaml`

Ensure `telemetry.prometheus.listen_addr` is set to an unused local port (default `0.0.0.0:9185` avoids colliding with `rusty_h265_converter` on `9184`):

```yaml
telemetry:
  enabled: true
  heartbeat_interval_secs: 30
  prometheus:
    listen_addr: 0.0.0.0:9185
    pushgateway_url: none
    push_interval_secs: 15
    job_name: rusty_dedup
    textfile_path: none
```

### Step 2: Add the Scrape Job to `prometheus.yml`

Edit your Prometheus configuration (e.g., `/opt/homebrew/etc/prometheus.yml` on Apple Silicon macOS or `/etc/prometheus/prometheus.yml` on Linux) and add a `rusty_dedup` scrape job under `scrape_configs:`:

```yaml
scrape_configs:
  - job_name: "rusty_dedup"
    scrape_interval: 5s
    static_configs:
      - targets: ["127.0.0.1:9185"]
        labels:
          app: "rusty_dedup"
```

Validate and reload Prometheus:
```bash
promtool check config /opt/homebrew/etc/prometheus.yml
brew services restart prometheus
```

### Step 3: Launch Under the Harness & Verify in Grafana

1. Start your scan via the Python harness:
   ```bash
   python3 scripts/rusty_dedup_metrics_harness.py -- \
     ./target/release/rusty_dedup /Volumes/DriveA /Volumes/DriveB -o duplicates.json
   ```
2. Verify live metrics directly:
   ```bash
   curl -s http://127.0.0.1:9185/metrics | head -n 30
   ```
3. In Grafana (**Explore → Prometheus**), query any metric with `{app="rusty_dedup"}` (see [docs/03_metrics_and_observability.md](docs/03_metrics_and_observability.md) for the full 44-metric catalog and ready-to-use PromQL queries).

---

## Credits

Built with Rust and powered by outstanding open-source projects:

- **[Rust](https://www.rust-lang.org/)** — Systems language enabling zero-cost concurrency and flat memory footprints.
- **[FFmpeg & FFprobe](https://ffmpeg.org/)** — Cross-platform hardware-accelerated video decoding, GPU downscaling, and container metadata probing.
- **[SQLite](https://www.sqlite.org/) & [`rusqlite`](https://github.com/rusqlite/rusqlite)** — Embedded crash-recoverable WAL persistence and compact binary hash storage.
- **[`blake3`](https://github.com/BLAKE3-team/BLAKE3)** — Ultra-fast cryptographic pre-filtering for exact-size files.
- **[`rayon`](https://github.com/rayon-rs/rayon)** — Parallel multi-volume probing pools.
- **[`clap`](https://github.com/clap-rs/clap)**, **[`serde`](https://serde.rs/)**, **[`anyhow`](https://github.com/dtolnay/anyhow)**, **[`strsim`](https://github.com/rapidfuzz/strsim-rs)**, and **[`walkdir`](https://github.com/BurntSushi/walkdir)** — CLI ergonomics, serialization, fuzzy subject matching, and filesystem traversal.
- **[Prometheus](https://prometheus.io/) & [Grafana](https://grafana.com/)** — Time-series APM metrics collection and visualization.
