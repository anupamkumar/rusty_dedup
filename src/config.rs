use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};

/// Supported cross-platform FFmpeg hardware acceleration backends covering:
/// Apple (VideoToolbox), NVIDIA (CUDA/NVDEC), AMD/Intel/Qualcomm Windows (D3D12VA/D3D11VA/DXVA2),
/// Intel (QSV), AMD/Intel Linux (VAAPI), cross-platform Vulkan Video, ARM/Qualcomm Linux (V4L2M2M),
/// ARM Rockchip (RKMPP), and ARM/Qualcomm Android (MediaCodec).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HwBackend {
    VideoToolbox,
    Cuda,
    D3d12va,
    D3d11va,
    Qsv,
    Vaapi,
    Dxva2,
    Vulkan,
    V4l2m2m,
    Rkmpp,
    MediaCodec,
}

impl HwBackend {
    pub fn from_str_name(s: &str) -> Option<Self> {
        match s.trim().to_lowercase().as_str() {
            "videotoolbox" | "vt" => Some(Self::VideoToolbox),
            "cuda" | "nvdec" | "cuvid" => Some(Self::Cuda),
            "d3d12va" => Some(Self::D3d12va),
            "d3d11va" => Some(Self::D3d11va),
            "qsv" => Some(Self::Qsv),
            "vaapi" => Some(Self::Vaapi),
            "dxva2" => Some(Self::Dxva2),
            "vulkan" => Some(Self::Vulkan),
            "v4l2m2m" => Some(Self::V4l2m2m),
            "rkmpp" => Some(Self::Rkmpp),
            "mediacodec" => Some(Self::MediaCodec),
            _ => None,
        }
    }

    pub fn as_ffmpeg_name(&self) -> &'static str {
        match self {
            Self::VideoToolbox => "videotoolbox",
            Self::Cuda => "cuda",
            Self::D3d12va => "d3d12va",
            Self::D3d11va => "d3d11va",
            Self::Qsv => "qsv",
            Self::Vaapi => "vaapi",
            Self::Dxva2 => "dxva2",
            Self::Vulkan => "vulkan",
            Self::V4l2m2m => "v4l2m2m",
            Self::Rkmpp => "rkmpp",
            Self::MediaCodec => "mediacodec",
        }
    }

    pub fn vendor_label(&self) -> &'static str {
        match self {
            Self::VideoToolbox => "Apple Silicon / macOS (VideoToolbox)",
            Self::Cuda => "NVIDIA GPU (CUDA / NVDEC)",
            Self::D3d12va => "Windows Direct3D 12 (AMD / Intel / NVIDIA / Qualcomm)",
            Self::D3d11va => "Windows Direct3D 11 (AMD / Intel / NVIDIA / Qualcomm)",
            Self::Qsv => "Intel QuickSync Video (Arc / Iris Xe / UHD)",
            Self::Vaapi => "Linux VA-API (AMD Radeon / Intel Mesa / iHD)",
            Self::Dxva2 => "Windows DXVA2 (Legacy DirectX Video Acceleration)",
            Self::Vulkan => "Cross-Platform Vulkan Video (NVIDIA / AMD / Intel / ARM / Qualcomm)",
            Self::V4l2m2m => "Linux ARM / Qualcomm V4L2 M2M (Raspberry Pi / Snapdragon Venus)",
            Self::Rkmpp => "ARM Rockchip MPP (RK3588 / Radxa / Orange Pi)",
            Self::MediaCodec => "Android / ARM / Qualcomm MediaCodec",
        }
    }

    pub fn troubleshooting_resources(&self) -> &'static [&'static str] {
        match self {
            Self::VideoToolbox => &[
                "https://trac.ffmpeg.org/wiki/HWAccelIntro#VideoToolbox",
                "https://developer.apple.com/documentation/videotoolbox",
            ],
            Self::Cuda => &[
                "https://trac.ffmpeg.org/wiki/HWAccelIntro#CUDANVENCNVDEC",
                "https://docs.nvidia.com/video-technologies/video-codec-sdk/",
            ],
            Self::D3d12va => &[
                "https://trac.ffmpeg.org/wiki/HWAccelIntro#D3D12VA",
                "https://learn.microsoft.com/en-us/windows/win32/medfound/direct3d-12-video-overview",
            ],
            Self::D3d11va => &[
                "https://trac.ffmpeg.org/wiki/HWAccelIntro#D3D11VA",
                "https://trac.ffmpeg.org/wiki/HWAccelIntro#PlatformAPIAvailability",
            ],
            Self::Qsv => &[
                "https://trac.ffmpeg.org/wiki/Hardware/QuickSync",
                "https://github.com/intel/media-driver",
            ],
            Self::Vaapi => &[
                "https://trac.ffmpeg.org/wiki/Hardware/VAAPI",
                "https://wiki.archlinux.org/title/Hardware_video_acceleration",
            ],
            Self::Dxva2 => &["https://trac.ffmpeg.org/wiki/HWAccelIntro#DXVA2"],
            Self::Vulkan => &[
                "https://trac.ffmpeg.org/wiki/HWAccelIntro#Vulkan",
                "https://vulkan.lunarg.com/sdk/home",
            ],
            Self::V4l2m2m => &[
                "https://trac.ffmpeg.org/wiki/HWAccelIntro#V4L2M2M",
                "https://www.kernel.org/doc/html/latest/userspace-api/media/v4l/dev-decoder.html",
            ],
            Self::Rkmpp => &["https://github.com/nyanmisaka/ffmpeg-rockchip/wiki"],
            Self::MediaCodec => &["https://trac.ffmpeg.org/wiki/HWAccelIntro#MediaCodec"],
        }
    }

    pub fn default_fallback_order() -> Vec<Self> {
        vec![
            Self::VideoToolbox,
            Self::Cuda,
            Self::D3d12va,
            Self::D3d11va,
            Self::Qsv,
            Self::Vaapi,
            Self::Dxva2,
            Self::Vulkan,
            Self::V4l2m2m,
            Self::Rkmpp,
            Self::MediaCodec,
        ]
    }
}

impl std::fmt::Display for HwBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ffmpeg_name())
    }
}

/// Hardware-accelerated video decode & GPU downscale settings from `dedup_config.yaml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardwareAccelerationConfig {
    pub enabled: bool,
    pub fallback_order: Vec<HwBackend>,
    pub vaapi_device: Option<PathBuf>,
    pub hw_scale_width: u32,
    pub hw_scale_height: u32,
}

impl Default for HardwareAccelerationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            fallback_order: HwBackend::default_fallback_order(),
            vaapi_device: Some(PathBuf::from("/dev/dri/renderD128")),
            hw_scale_width: 72,
            hw_scale_height: 72,
        }
    }
}

/// FFmpeg decode, threading, stream-skipping, and hardware-acceleration settings from `dedup_config.yaml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FfmpegConfig {
    pub threads: Option<u32>,
    pub filter_threads: Option<u32>,
    pub skip_audio: bool,
    pub skip_subtitles: bool,
    pub skip_datastreams: bool,
    pub hardware_acceleration: HardwareAccelerationConfig,
}

impl Default for FfmpegConfig {
    fn default() -> Self {
        Self {
            threads: Some(0),
            filter_threads: Some(0),
            skip_audio: true,
            skip_subtitles: true,
            skip_datastreams: true,
            hardware_acceleration: HardwareAccelerationConfig::default(),
        }
    }
}

impl FfmpegConfig {
    pub fn push_global_filter_thread_args(&self, args: &mut Vec<std::ffi::OsString>) {
        if let Some(ft) = self.filter_threads {
            args.push("-filter_threads".into());
            args.push(ft.to_string().into());
        }
    }

    pub fn push_thread_args(&self, args: &mut Vec<std::ffi::OsString>) {
        if let Some(t) = self.threads {
            args.push("-threads".into());
            args.push(t.to_string().into());
        }
    }

    pub fn push_skip_stream_args(&self, args: &mut Vec<std::ffi::OsString>) {
        if self.skip_audio {
            args.push("-an".into());
        }
        if self.skip_subtitles {
            args.push("-sn".into());
        }
        if self.skip_datastreams {
            args.push("-dn".into());
        }
    }

    pub fn flags_summary(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if self.hardware_acceleration.enabled {
            let order: Vec<&str> = self
                .hardware_acceleration
                .fallback_order
                .iter()
                .map(|b| b.as_ffmpeg_name())
                .collect();
            parts.push(format!(
                "hwaccel=enabled([{}], {}x{})",
                order.join(","),
                self.hardware_acceleration.hw_scale_width,
                self.hardware_acceleration.hw_scale_height
            ));
        } else {
            parts.push(format!(
                "hwaccel=disabled (CPU {}x{})",
                self.hardware_acceleration.hw_scale_width,
                self.hardware_acceleration.hw_scale_height
            ));
        }
        match self.threads {
            Some(t) => parts.push(format!("-threads {}", t)),
            None => parts.push("threads=default".to_string()),
        }
        match self.filter_threads {
            Some(ft) => parts.push(format!("-filter_threads {}", ft)),
            None => parts.push("filter_threads=default".to_string()),
        }
        let mut skipped = Vec::new();
        if self.skip_audio {
            skipped.push("-an");
        }
        if self.skip_subtitles {
            skipped.push("-sn");
        }
        if self.skip_datastreams {
            skipped.push("-dn");
        }
        if skipped.is_empty() {
            parts.push("skip_streams=none".to_string());
        } else {
            parts.push(skipped.join(" "));
        }
        parts.join(", ")
    }
}

/// Opt-in local HTTP Prometheus telemetry configuration loaded from `dedup_config.yaml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrometheusConfig {
    /// Local HTTP scrape bind address (e.g. `127.0.0.1:9464`), `None` = disabled (opt-in).
    pub listen_addr: Option<String>,
    /// Local HTTP Pushgateway base URL (e.g. `http://127.0.0.1:9091`), `None` = disabled (opt-in).
    pub pushgateway_url: Option<String>,
    /// Interval in seconds between Pushgateway pushes (also sends a final push on shutdown).
    pub push_interval_secs: u64,
    /// Prometheus `job` grouping label for Pushgateway pushes.
    pub job_name: String,
    /// Optional `.prom` textfile collector output path for `node_exporter`.
    pub textfile_path: Option<PathBuf>,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            listen_addr: None,
            pushgateway_url: None,
            push_interval_secs: 15,
            job_name: "rusty_dedup".to_string(),
            textfile_path: None,
        }
    }
}

/// APM telemetry configuration loaded from `dedup_config.yaml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryConfig {
    pub enabled: bool,
    pub heartbeat_interval_secs: u64,
    /// Optional path to atomically flush live JSON telemetry snapshots (for companion metrics harness).
    pub snapshot_path: Option<PathBuf>,
    pub prometheus: PrometheusConfig,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            heartbeat_interval_secs: 30,
            snapshot_path: None,
            prometheus: PrometheusConfig::default(),
        }
    }
}

/// Top-level configuration loaded from `dedup_config.yaml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupConfig {
    pub version: String,
    pub ffmpeg: FfmpegConfig,
    pub telemetry: TelemetryConfig,
}

impl Default for DedupConfig {
    fn default() -> Self {
        Self {
            version: "1.0".to_string(),
            ffmpeg: FfmpegConfig::default(),
            telemetry: TelemetryConfig::default(),
        }
    }
}

impl DedupConfig {
    fn parse_bool(val: &str, field: &str) -> Result<bool> {
        match val.trim().to_lowercase().as_str() {
            "true" | "yes" | "on" | "1" => Ok(true),
            "false" | "no" | "off" | "0" => Ok(false),
            _ => anyhow::bail!(
                "Invalid boolean {:?} for '{}' in dedup_config.yaml (expected true/false)",
                val,
                field
            ),
        }
    }

    fn parse_optional_u32(val: &str, field: &str) -> Result<Option<u32>> {
        let trimmed = val.trim();
        let lower = trimmed.to_lowercase();
        if trimmed.is_empty() || matches!(lower.as_str(), "null" | "none" | "~" | "default" | "off")
        {
            return Ok(None);
        }
        let parsed = trimmed.parse::<u32>().with_context(|| {
            format!(
                "Invalid non-negative integer {:?} for '{}' in dedup_config.yaml",
                val, field
            )
        })?;
        Ok(Some(parsed))
    }

    fn parse_optional_string(val: &str) -> Option<String> {
        let trimmed = val.trim();
        let lower = trimmed.to_lowercase();
        if trimmed.is_empty() || matches!(lower.as_str(), "null" | "none" | "~" | "off" | "disabled")
        {
            None
        } else {
            Some(trimmed.to_string())
        }
    }

    fn parse_optional_path(val: &str) -> Option<PathBuf> {
        Self::parse_optional_string(val).map(PathBuf::from)
    }

    fn parse_backend(token: &str, field: &str) -> Result<HwBackend> {
        HwBackend::from_str_name(token).ok_or_else(|| {
            anyhow::anyhow!(
                "Unrecognized hardware acceleration backend {:?} in '{}'. Supported backends: videotoolbox, cuda, d3d12va, d3d11va, qsv, vaapi, dxva2, vulkan, v4l2m2m, rkmpp, mediacodec.",
                token,
                field
            )
        })
    }

    fn validate_dimensions(w: u32, h: u32) -> Result<()> {
        if w < 16 || h < 16 {
            anyhow::bail!(
                "Invalid hw_scale_width/hw_scale_height ({}x{}): dimensions must be >= 16 for GPU NV12/P010 hardware surfaces (recommended: 72x72).",
                w,
                h
            );
        }
        if w % 2 != 0 || h % 2 != 0 {
            anyhow::bail!(
                "Invalid hw_scale_width/hw_scale_height ({}x{}): dimensions must be even numbers for GPU NV12/YUV420 surfaces (recommended: 72x72).",
                w,
                h
            );
        }
        Ok(())
    }

    /// Parses `dedup_config.yaml` without external YAML crate dependencies (consistent with `rusty_clean`).
    pub fn from_yaml_str(content: &str) -> Result<Self> {
        let mut cfg = DedupConfig::default();

        #[derive(PartialEq)]
        enum Section {
            Root,
            Ffmpeg,
            HardwareAcceleration,
            FallbackOrderList,
            Telemetry,
            Prometheus,
        }

        let mut section = Section::Root;
        let mut ffmpeg_indent = 0usize;
        let mut hwaccel_indent = 0usize;
        let mut fallback_indent = 0usize;
        let mut telemetry_indent = 0usize;
        let mut prometheus_indent = 0usize;
        let mut parsed_fallback_order: Vec<HwBackend> = Vec::new();
        let mut saw_fallback_order = false;

        for raw_line in content.lines() {
            let code_part = match raw_line.find('#') {
                Some(idx) => &raw_line[..idx],
                None => raw_line,
            };
            let trimmed = code_part.trim();
            if trimmed.is_empty() {
                continue;
            }

            let indent = code_part.chars().take_while(|c| c.is_whitespace()).count();

            match section {
                Section::FallbackOrderList => {
                    let is_list_item = trimmed.starts_with('-');
                    if !is_list_item && indent <= fallback_indent {
                        if indent <= hwaccel_indent {
                            if indent <= ffmpeg_indent {
                                section = Section::Root;
                            } else {
                                section = Section::Ffmpeg;
                            }
                        } else {
                            section = Section::HardwareAcceleration;
                        }
                    }
                }
                Section::HardwareAcceleration => {
                    if indent <= hwaccel_indent {
                        if indent <= ffmpeg_indent {
                            section = Section::Root;
                        } else {
                            section = Section::Ffmpeg;
                        }
                    }
                }
                Section::Ffmpeg => {
                    if indent <= ffmpeg_indent {
                        section = Section::Root;
                    }
                }
                Section::Prometheus => {
                    if indent <= prometheus_indent {
                        if indent <= telemetry_indent {
                            section = Section::Root;
                        } else {
                            section = Section::Telemetry;
                        }
                    }
                }
                Section::Telemetry => {
                    if indent <= telemetry_indent {
                        section = Section::Root;
                    }
                }
                Section::Root => {}
            }

            if section == Section::FallbackOrderList && trimmed.starts_with('-') {
                let item = trimmed
                    .trim_start_matches('-')
                    .trim()
                    .trim_matches('"')
                    .trim_matches('\'');
                if !item.is_empty() {
                    let backend = Self::parse_backend(item, "hardware_acceleration.fallback_order")?;
                    if !parsed_fallback_order.contains(&backend) {
                        parsed_fallback_order.push(backend);
                    }
                }
                continue;
            }

            let Some(colon_idx) = trimmed.find(':') else {
                continue;
            };
            let key = trimmed[..colon_idx]
                .trim()
                .trim_matches('"')
                .trim_matches('\'');
            let val = trimmed[colon_idx + 1..]
                .trim()
                .trim_matches('"')
                .trim_matches('\'');

            match section {
                Section::Root => match key {
                    "version" if !val.is_empty() => cfg.version = val.to_string(),
                    "ffmpeg" => {
                        ffmpeg_indent = indent;
                        section = Section::Ffmpeg;
                    }
                    "telemetry" => {
                        telemetry_indent = indent;
                        if !val.is_empty() {
                            cfg.telemetry.enabled = Self::parse_bool(val, "telemetry")?;
                        } else {
                            section = Section::Telemetry;
                        }
                    }
                    "hardware_acceleration" => {
                        hwaccel_indent = indent;
                        if !val.is_empty() {
                            cfg.ffmpeg.hardware_acceleration.enabled =
                                Self::parse_bool(val, "hardware_acceleration")?;
                        } else {
                            section = Section::HardwareAcceleration;
                        }
                    }
                    "threads" => cfg.ffmpeg.threads = Self::parse_optional_u32(val, "threads")?,
                    "filter_threads" => {
                        cfg.ffmpeg.filter_threads =
                            Self::parse_optional_u32(val, "filter_threads")?;
                    }
                    "skip_audio" => {
                        cfg.ffmpeg.skip_audio = Self::parse_bool(val, "skip_audio")?;
                    }
                    "skip_subtitles" => {
                        cfg.ffmpeg.skip_subtitles = Self::parse_bool(val, "skip_subtitles")?;
                    }
                    "skip_datastreams" | "skip_data" => {
                        cfg.ffmpeg.skip_datastreams = Self::parse_bool(val, key)?;
                    }
                    _ => {}
                },
                Section::Telemetry => match key {
                    "enabled" => {
                        cfg.telemetry.enabled = Self::parse_bool(val, "telemetry.enabled")?;
                    }
                    "heartbeat_interval_secs" => {
                        cfg.telemetry.heartbeat_interval_secs =
                            val.parse::<u64>().with_context(|| {
                                format!(
                                    "Invalid integer {:?} for 'telemetry.heartbeat_interval_secs'",
                                    val
                                )
                            })?;
                    }
                    "snapshot_path" => {
                        cfg.telemetry.snapshot_path = Self::parse_optional_path(val);
                    }
                    "prometheus" => {
                        prometheus_indent = indent;
                        section = Section::Prometheus;
                    }
                    _ => {}
                },
                Section::Prometheus => match key {
                    "listen_addr" => {
                        cfg.telemetry.prometheus.listen_addr = Self::parse_optional_string(val);
                    }
                    "pushgateway_url" => {
                        cfg.telemetry.prometheus.pushgateway_url = Self::parse_optional_string(val);
                    }
                    "push_interval_secs" => {
                        cfg.telemetry.prometheus.push_interval_secs =
                            val.parse::<u64>().with_context(|| {
                                format!(
                                    "Invalid integer {:?} for 'telemetry.prometheus.push_interval_secs'",
                                    val
                                )
                            })?;
                    }
                    "job_name" => {
                        if let Some(j) = Self::parse_optional_string(val) {
                            cfg.telemetry.prometheus.job_name = j;
                        }
                    }
                    "textfile_path" => {
                        cfg.telemetry.prometheus.textfile_path = Self::parse_optional_path(val);
                    }
                    _ => {}
                },
                Section::Ffmpeg => match key {
                    "threads" => cfg.ffmpeg.threads = Self::parse_optional_u32(val, "threads")?,
                    "filter_threads" => {
                        cfg.ffmpeg.filter_threads =
                            Self::parse_optional_u32(val, "filter_threads")?;
                    }
                    "skip_audio" => {
                        cfg.ffmpeg.skip_audio = Self::parse_bool(val, "skip_audio")?;
                    }
                    "skip_subtitles" => {
                        cfg.ffmpeg.skip_subtitles = Self::parse_bool(val, "skip_subtitles")?;
                    }
                    "skip_datastreams" | "skip_data" => {
                        cfg.ffmpeg.skip_datastreams = Self::parse_bool(val, key)?;
                    }
                    "hardware_acceleration" => {
                        hwaccel_indent = indent;
                        if !val.is_empty() {
                            cfg.ffmpeg.hardware_acceleration.enabled =
                                Self::parse_bool(val, "hardware_acceleration")?;
                        } else {
                            section = Section::HardwareAcceleration;
                        }
                    }
                    _ => {}
                },
                Section::HardwareAcceleration => match key {
                    "enabled" => {
                        cfg.ffmpeg.hardware_acceleration.enabled =
                            Self::parse_bool(val, "hardware_acceleration.enabled")?;
                    }
                    "fallback_order" => {
                        saw_fallback_order = true;
                        parsed_fallback_order.clear();
                        if val.is_empty() {
                            fallback_indent = indent;
                            section = Section::FallbackOrderList;
                        } else {
                            let inner = val.trim_start_matches('[').trim_end_matches(']');
                            for tok in inner.split(',') {
                                let clean = tok.trim().trim_matches('"').trim_matches('\'');
                                if !clean.is_empty() {
                                    let backend = Self::parse_backend(
                                        clean,
                                        "hardware_acceleration.fallback_order",
                                    )?;
                                    if !parsed_fallback_order.contains(&backend) {
                                        parsed_fallback_order.push(backend);
                                    }
                                }
                            }
                        }
                    }
                    "vaapi_device" => {
                        cfg.ffmpeg.hardware_acceleration.vaapi_device =
                            Self::parse_optional_path(val);
                    }
                    "hw_scale_width" => {
                        let w = val.parse::<u32>().with_context(|| {
                            format!(
                                "Invalid integer {:?} for 'hw_scale_width' in dedup_config.yaml",
                                val
                            )
                        })?;
                        cfg.ffmpeg.hardware_acceleration.hw_scale_width = w;
                    }
                    "hw_scale_height" => {
                        let h = val.parse::<u32>().with_context(|| {
                            format!(
                                "Invalid integer {:?} for 'hw_scale_height' in dedup_config.yaml",
                                val
                            )
                        })?;
                        cfg.ffmpeg.hardware_acceleration.hw_scale_height = h;
                    }
                    _ => {}
                },
                Section::FallbackOrderList => {}
            }
        }

        if saw_fallback_order {
            if parsed_fallback_order.is_empty() && cfg.ffmpeg.hardware_acceleration.enabled {
                anyhow::bail!(
                    "hardware_acceleration.fallback_order cannot be empty when hardware_acceleration.enabled is true"
                );
            }
            cfg.ffmpeg.hardware_acceleration.fallback_order = parsed_fallback_order;
        }

        Self::validate_dimensions(
            cfg.ffmpeg.hardware_acceleration.hw_scale_width,
            cfg.ffmpeg.hardware_acceleration.hw_scale_height,
        )?;

        Ok(cfg)
    }

    /// Resolves and loads `dedup_config.yaml`:
    /// 1. If `explicit_path` is passed via `--dedup-config` / `-c`, it must exist and parse cleanly.
    /// 2. Otherwise checks `./dedup_config.yaml` and `<binary_dir>/dedup_config.yaml`.
    /// 3. Falls back to built-in defaults if no config file is found.
    pub fn load(explicit_path: Option<&Path>) -> Result<(Self, String)> {
        if let Some(p) = explicit_path {
            let content = fs::read_to_string(p)
                .with_context(|| format!("Failed to read dedup configuration file {:?}", p))?;
            let cfg = Self::from_yaml_str(&content)
                .with_context(|| format!("Failed to parse dedup configuration file {:?}", p))?;
            return Ok((cfg, format!("{:?}", p)));
        }

        let mut candidates: Vec<PathBuf> = Vec::new();
        if let Ok(cwd) = std::env::current_dir() {
            candidates.push(cwd.join("dedup_config.yaml"));
        }
        if let Ok(exe_path) = std::env::current_exe() {
            if let Some(exe_dir) = exe_path.parent() {
                let exe_cfg = exe_dir.join("dedup_config.yaml");
                if !candidates.contains(&exe_cfg) {
                    candidates.push(exe_cfg);
                }
            }
        }

        for cand in candidates {
            if cand.exists() {
                let content = fs::read_to_string(&cand).with_context(|| {
                    format!("Failed to read dedup configuration file {:?}", cand)
                })?;
                let cfg = Self::from_yaml_str(&content).with_context(|| {
                    format!("Failed to parse dedup configuration file {:?}", cand)
                })?;
                return Ok((cfg, format!("{:?}", cand)));
            }
        }

        Ok((
            Self::default(),
            "built-in defaults (dedup_config.yaml not found)".to_string(),
        ))
    }
}
