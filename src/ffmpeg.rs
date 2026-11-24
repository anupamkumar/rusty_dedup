use crate::config::{DedupConfig, FfmpegConfig, HwBackend};
use anyhow::{Context, Result};
use std::collections::HashSet;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Embedded 128x128 2-frame H.264 Baseline MP4 used for live `--config-test` verification
/// without requiring external files or `libx264` in the user's FFmpeg build.
pub const CONFIG_TEST_H264_MP4: &[u8] = include_bytes!("assets/config_test_128x128.mp4");

/// Sparse sample positions as fractions of video duration.
pub const SPARSE_SAMPLE_FRACTIONS: [f64; 5] = [0.10, 0.30, 0.50, 0.70, 0.90];

/// Probed FFmpeg/FFprobe capabilities on the host machine.
#[derive(Debug, Clone, Default)]
pub struct FfmpegCapabilities {
    pub ffmpeg_version: String,
    pub ffprobe_version: String,
    pub decoders: HashSet<String>,
    /// Populated ONLY when `hardware_acceleration.enabled == true`.
    pub hwaccels: HashSet<String>,
    /// Populated ONLY when `hardware_acceleration.enabled == true`.
    pub filters: HashSet<String>,
}

impl FfmpegCapabilities {
    /// Step 1 of `--config-test`: verifies `ffmpeg -version` and `ffprobe -version` execute cleanly,
    /// fast-failing with install instructions if missing/broken, then probes `ffmpeg -decoders`.
    pub fn probe_binaries_and_decoders() -> Result<Self> {
        let ffmpeg_ver_out = Command::new("ffmpeg").arg("-version").output().map_err(|e| {
            anyhow::anyhow!(
                "FAST-FAIL: Failed to execute `ffmpeg -version` ({}).\n\
                 Please install FFmpeg and ensure `ffmpeg` is on your PATH:\n\
                   - macOS  : brew install ffmpeg\n\
                   - Linux  : sudo apt install ffmpeg  (or your distro package manager)\n\
                   - Windows: winget install Gyan.FFmpeg\n\
                   - Official downloads: https://ffmpeg.org/download.html",
                e
            )
        })?;
        if !ffmpeg_ver_out.status.success() {
            anyhow::bail!(
                "FAST-FAIL: `ffmpeg -version` exited with status {}: {}\n\
                 Please verify your FFmpeg installation: https://ffmpeg.org/download.html",
                ffmpeg_ver_out.status,
                String::from_utf8_lossy(&ffmpeg_ver_out.stderr).trim()
            );
        }
        let ffmpeg_version = String::from_utf8_lossy(&ffmpeg_ver_out.stdout)
            .lines()
            .next()
            .unwrap_or("ffmpeg (unknown version)")
            .trim()
            .to_string();

        let ffprobe_ver_out = Command::new("ffprobe").arg("-version").output().map_err(|e| {
            anyhow::anyhow!(
                "FAST-FAIL: Failed to execute `ffprobe -version` ({}).\n\
                 `ffprobe` ships alongside FFmpeg and is required for Pass 1 metadata probing.\n\
                 Please install FFmpeg/FFprobe and ensure both are on your PATH: https://ffmpeg.org/download.html",
                e
            )
        })?;
        if !ffprobe_ver_out.status.success() {
            anyhow::bail!(
                "FAST-FAIL: `ffprobe -version` exited with status {}: {}\n\
                 Please verify your FFmpeg/FFprobe installation: https://ffmpeg.org/download.html",
                ffprobe_ver_out.status,
                String::from_utf8_lossy(&ffprobe_ver_out.stderr).trim()
            );
        }
        let ffprobe_version = String::from_utf8_lossy(&ffprobe_ver_out.stdout)
            .lines()
            .next()
            .unwrap_or("ffprobe (unknown version)")
            .trim()
            .to_string();

        let dec_out = Command::new("ffmpeg")
            .args(["-hide_banner", "-decoders"])
            .output()
            .context("Failed to probe `ffmpeg -decoders`")?;
        if !dec_out.status.success() {
            anyhow::bail!(
                "FAST-FAIL: `ffmpeg -decoders` failed: {}",
                String::from_utf8_lossy(&dec_out.stderr).trim()
            );
        }
        let decoders = Self::parse_decoders_output(&String::from_utf8_lossy(&dec_out.stdout));

        Ok(Self {
            ffmpeg_version,
            ffprobe_version,
            decoders,
            hwaccels: HashSet::new(),
            filters: HashSet::new(),
        })
    }

    /// Step 2a (ONLY called when `hardware_acceleration.enabled == true`):
    /// probes `ffmpeg -hwaccels` and `ffmpeg -filters`.
    pub fn probe_hw_capabilities(&mut self) -> Result<()> {
        let hw_out = Command::new("ffmpeg")
            .args(["-hide_banner", "-hwaccels"])
            .output()
            .context("Failed to probe `ffmpeg -hwaccels`")?;
        if !hw_out.status.success() {
            anyhow::bail!(
                "FAST-FAIL: `ffmpeg -hwaccels` failed: {}",
                String::from_utf8_lossy(&hw_out.stderr).trim()
            );
        }
        self.hwaccels = Self::parse_hwaccels_output(&String::from_utf8_lossy(&hw_out.stdout));

        let flt_out = Command::new("ffmpeg")
            .args(["-hide_banner", "-filters"])
            .output()
            .context("Failed to probe `ffmpeg -filters`")?;
        if !flt_out.status.success() {
            anyhow::bail!(
                "FAST-FAIL: `ffmpeg -filters` failed: {}",
                String::from_utf8_lossy(&flt_out.stderr).trim()
            );
        }
        self.filters = Self::parse_filters_output(&String::from_utf8_lossy(&flt_out.stdout));
        Ok(())
    }

    pub fn parse_decoders_output(stdout: &str) -> HashSet<String> {
        let mut set = HashSet::new();
        let mut past_header = false;
        for line in stdout.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("------") {
                past_header = true;
                continue;
            }
            if !past_header || trimmed.is_empty() {
                continue;
            }
            let mut parts = trimmed.split_whitespace();
            let flags = parts.next().unwrap_or("");
            let name = parts.next().unwrap_or("");
            // Only collect video decoders (`V` in position 0)
            if flags.starts_with('V') && !name.is_empty() {
                set.insert(name.to_lowercase());
            }
        }
        set
    }

    pub fn parse_hwaccels_output(stdout: &str) -> HashSet<String> {
        let mut set = HashSet::new();
        for line in stdout.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty()
                || trimmed.to_lowercase().starts_with("hardware acceleration")
            {
                continue;
            }
            set.insert(trimmed.to_lowercase());
        }
        set
    }

    pub fn parse_filters_output(stdout: &str) -> HashSet<String> {
        let mut set = HashSet::new();
        for line in stdout.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty()
                || trimmed.starts_with("Filters:")
                || trimmed.starts_with("T..")
                || trimmed.starts_with(".S.")
                || trimmed.starts_with("..C")
                || trimmed.starts_with("A =")
                || trimmed.starts_with("V =")
                || trimmed.starts_with("N =")
                || trimmed.starts_with("| =")
            {
                continue;
            }
            let mut parts = trimmed.split_whitespace();
            let _flags = parts.next();
            if let Some(name) = parts.next() {
                set.insert(name.to_lowercase());
            }
        }
        set
    }

    pub fn has_hwaccel(&self, backend: HwBackend) -> bool {
        self.hwaccels.contains(backend.as_ffmpeg_name())
    }

    pub fn has_decoder(&self, name: &str) -> bool {
        self.decoders.contains(&name.to_lowercase())
    }

    pub fn has_filter(&self, name: &str) -> bool {
        self.filters.contains(&name.to_lowercase())
    }
}

/// Verified hardware backend locked in by `--config-test` for the session.
#[derive(Debug, Clone)]
pub struct VerifiedHwBackend {
    pub backend: HwBackend,
    pub uses_hw_scaler: bool,
    pub scale_description: String,
}

/// Immutable, verified decoding environment created by `run_config_test` and shared across all worker threads.
#[derive(Debug, Clone)]
pub struct VerifiedDecodeEnv {
    pub caps: FfmpegCapabilities,
    pub active_hw: Option<VerifiedHwBackend>,
    pub scale_width: u32,
    pub scale_height: u32,
    pub ffmpeg_cfg: FfmpegConfig,
}

impl VerifiedDecodeEnv {
    /// Picks the fastest available CPU software decoder for a normalized `video_codec`
    /// (e.g., `libdav1d` for AV1, native multi-threaded SIMD `vp9`/`vp8`/`h264`/`hevc`).
    pub fn fastest_cpu_decoder_for(&self, video_codec: &str) -> Option<&'static str> {
        match video_codec.trim().to_uppercase().as_str() {
            "AV1" => {
                if self.caps.has_decoder("libdav1d") {
                    Some("libdav1d")
                } else if self.caps.has_decoder("av1") {
                    Some("av1")
                } else if self.caps.has_decoder("libaom-av1") {
                    Some("libaom-av1")
                } else {
                    None
                }
            }
            "VP9" => {
                if self.caps.has_decoder("vp9") {
                    Some("vp9")
                } else if self.caps.has_decoder("libvpx-vp9") {
                    Some("libvpx-vp9")
                } else {
                    None
                }
            }
            "VP8" => {
                if self.caps.has_decoder("vp8") {
                    Some("vp8")
                } else if self.caps.has_decoder("libvpx") {
                    Some("libvpx")
                } else {
                    None
                }
            }
            "H264" => self.caps.has_decoder("h264").then_some("h264"),
            "H265" | "HEVC" => self.caps.has_decoder("hevc").then_some("hevc"),
            "MPEG4" => self.caps.has_decoder("mpeg4").then_some("mpeg4"),
            "MPEG2VIDEO" => self.caps.has_decoder("mpeg2video").then_some("mpeg2video"),
            _ => None,
        }
    }

    /// Returns whether `video_codec` is plausibly supported by `backend`'s GPU silicon.
    /// Legacy CPU-only codecs (WMV1/2/3, FLV1, Theora, etc.) return `false` so they route
    /// directly to the CPU-Codec Decode Worker without wasting a doomed GPU spawn.
    pub fn is_codec_gpu_supported(&self, backend: HwBackend, video_codec: &str) -> bool {
        let upper = video_codec.trim().to_uppercase();
        match backend {
            HwBackend::VideoToolbox => matches!(
                upper.as_str(),
                "H264" | "H265" | "HEVC" | "AV1" | "VP9" | "PRORES" | "MPEG2VIDEO" | "MPEG4"
            ),
            HwBackend::Cuda => matches!(
                upper.as_str(),
                "H264"
                    | "H265"
                    | "HEVC"
                    | "AV1"
                    | "VP9"
                    | "VP8"
                    | "MPEG2VIDEO"
                    | "MPEG4"
                    | "VC1"
                    | "MJPEG"
            ),
            HwBackend::Qsv => matches!(
                upper.as_str(),
                "H264" | "H265" | "HEVC" | "AV1" | "VP9" | "VP8" | "MPEG2VIDEO" | "VC1" | "MJPEG"
            ),
            HwBackend::Vaapi => matches!(
                upper.as_str(),
                "H264" | "H265" | "HEVC" | "AV1" | "VP9" | "VP8" | "MPEG2VIDEO" | "VC1" | "MJPEG"
            ),
            HwBackend::D3d12va | HwBackend::D3d11va | HwBackend::Dxva2 => matches!(
                upper.as_str(),
                "H264" | "H265" | "HEVC" | "AV1" | "VP9" | "MPEG2VIDEO" | "VC1"
            ),
            HwBackend::Vulkan => matches!(upper.as_str(), "H264" | "H265" | "HEVC" | "AV1"),
            HwBackend::V4l2m2m => matches!(
                upper.as_str(),
                "H264" | "H265" | "HEVC" | "VP8" | "VP9" | "MPEG2VIDEO" | "MPEG4"
            ),
            HwBackend::Rkmpp => matches!(
                upper.as_str(),
                "H264" | "H265" | "HEVC" | "AV1" | "VP9" | "VP8" | "MPEG2VIDEO" | "MPEG4"
            ),
            HwBackend::MediaCodec => matches!(
                upper.as_str(),
                "H264" | "H265" | "HEVC" | "AV1" | "VP9" | "VP8" | "MPEG2VIDEO" | "MPEG4"
            ),
        }
    }

    /// Picks the fastest dedicated or native FFmpeg decoder (`-c:v`) for `(backend, video_codec)`.
    pub fn gpu_decoder_for(&self, backend: HwBackend, video_codec: &str) -> Option<String> {
        let base = match video_codec.trim().to_uppercase().as_str() {
            "H264" => "h264",
            "H265" | "HEVC" => "hevc",
            "AV1" => "av1",
            "VP9" => "vp9",
            "VP8" => "vp8",
            "MPEG2VIDEO" => "mpeg2",
            "MPEG4" => "mpeg4",
            "VC1" => "vc1",
            "MJPEG" => "mjpeg",
            "PRORES" => "prores",
            _ => return None,
        };

        let dedicated_suffix = match backend {
            HwBackend::Cuda => Some("_cuvid"),
            HwBackend::Qsv => Some("_qsv"),
            HwBackend::V4l2m2m => Some("_v4l2m2m"),
            HwBackend::Rkmpp => Some("_rkmpp"),
            HwBackend::MediaCodec => Some("_mediacodec"),
            _ => None,
        };

        if let Some(suffix) = dedicated_suffix {
            let dedicated = format!("{}{}", base, suffix);
            if self.caps.has_decoder(&dedicated) {
                return Some(dedicated);
            }
        }

        let native = if base == "mpeg2" { "mpeg2video" } else { base };
        if self.caps.has_decoder(native) {
            Some(native.to_string())
        } else {
            None
        }
    }

    /// Returns true if the active GPU backend supports a hardware scaling filter or NVDEC `-resize`.
    pub fn backend_supports_hw_scaler(&self, backend: HwBackend, decoder: Option<&str>) -> bool {
        if matches!(backend, HwBackend::Cuda)
            && decoder.map(|d| d.ends_with("_cuvid")).unwrap_or(false)
        {
            return true;
        }
        let has_hwdownload = self.caps.has_filter("hwdownload");
        if !has_hwdownload {
            return false;
        }
        match backend {
            HwBackend::VideoToolbox => self.caps.has_filter("scale_vt"),
            HwBackend::Cuda => self.caps.has_filter("scale_cuda") || self.caps.has_filter("scale_npp"),
            HwBackend::Qsv => self.caps.has_filter("scale_qsv"),
            HwBackend::Vaapi => self.caps.has_filter("scale_vaapi"),
            HwBackend::Vulkan => self.caps.has_filter("scale_vulkan"),
            HwBackend::Rkmpp => self.caps.has_filter("scale_rkrga"),
            HwBackend::D3d12va
            | HwBackend::D3d11va
            | HwBackend::Dxva2
            | HwBackend::V4l2m2m
            | HwBackend::MediaCodec => false,
        }
    }

    pub fn summary_label(&self) -> String {
        match &self.active_hw {
            Some(hw) => format!(
                "{} [{}] ({}x{} -> Rust 9x8)",
                hw.backend, hw.scale_description, self.scale_width, self.scale_height
            ),
            None => format!(
                "CPU software ({}x{} -> Rust 9x8, AV1={})",
                self.scale_width,
                self.scale_height,
                self.fastest_cpu_decoder_for("AV1").unwrap_or("default")
            ),
        }
    }
}

/// Kind of frame extraction job dispatched through FFmpeg.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DecodeKind {
    Sparse5 { duration_secs: f64 },
    Prefix1Fps { max_secs: u64 },
    Dense1Fps,
}

/// Raw `W x H` 8-bit luma frames emitted by FFmpeg before pure-Rust `9x8` area downscaling + dHash.
#[derive(Debug, Clone)]
pub struct RawFrameBatch {
    pub width: usize,
    pub height: usize,
    pub luma_bytes: Vec<u8>,
    pub decoder_label: String,
    pub used_hw_scale_fallback: bool,
}

/// Outcome of a GPU decode attempt, distinguishing fatal driver errors from per-file codec rejects.
#[derive(Debug)]
pub enum RawDecodeOutcome {
    Ok(RawFrameBatch),
    /// Non-driver, per-file codec/profile/pixel-format limitation (e.g., Hi10P or unsupported codec).
    CodecRejected(String),
    /// Fatal GPU/driver/device failure — MUST fast-fail instead of silently falling back to CPU!
    FatalDriverError(String),
}

/// Detects when FFmpeg failed to initialize the requested GPU hardware decoder (even if FFmpeg
/// attempted to silently fall back to CPU internally without `-hwaccel_output_format`).
pub fn hwaccel_init_failed_in_stderr(stderr: &str) -> bool {
    let lower = stderr.to_lowercase();
    const HW_INIT_FAIL_PATTERNS: &[&str] = &[
        "hwaccel initialisation returned error",
        "hwaccel initialization returned error",
        "failed setup for format",
        "hardware device setup failed",
        "videotoolbox decoder for this format not found",
        "hwaccel_output_format",
    ];
    is_fatal_driver_error(stderr) || HW_INIT_FAIL_PATTERNS.iter().any(|p| lower.contains(p))
}

/// Classifies FFmpeg stderr into fatal GPU/driver/device errors vs. per-file codec/profile errors.
pub fn is_fatal_driver_error(stderr: &str) -> bool {
    let lower = stderr.to_lowercase();
    const FATAL_PATTERNS: &[&str] = &[
        "unable to open the libvulkan",
        "cannot load libcuda",
        "cannot load nvcuda",
        "cuda_error_no_device",
        "no cuda-capable device",
        "no device available for decoder",
        "device creation failed",
        "hardware device setup failed",
        "failed to create a vaapi device",
        "failed to initialise vaapi",
        "failed to initialize vaapi",
        "no va display found",
        "cannot initialize vaapi",
        "failed to create direct3d",
        "failed to create d3d11",
        "failed to create d3d12",
        "error creating a mfx session",
        "mfx_err_unsupported",
        "failed to create a qsv device",
        "videotoolbox session creation failed",
        "vtdecompressionsessioncreate",
        "out of memory",
        "cuda_error_out_of_memory",
    ];
    FATAL_PATTERNS.iter().any(|pat| lower.contains(pat))
}

/// Computes a 64-bit horizontal gradient difference hash (dHash) from a 9x8 grayscale pixel buffer (72 bytes).
#[inline]
pub fn compute_dhash_from_9x8(pixels: &[u8]) -> u64 {
    debug_assert_eq!(pixels.len(), 72);
    let mut hash: u64 = 0;
    let mut bit_idx: u32 = 0;
    for row in 0..8 {
        let row_offset = row * 9;
        for col in 0..8 {
            if pixels[row_offset + col] < pixels[row_offset + col + 1] {
                hash |= 1u64 << bit_idx;
            }
            bit_idx += 1;
        }
    }
    hash
}

/// Downscales a single `src_w x src_h` 8-bit luma frame to `9 x 8` grayscale using exact area-averaging.
/// Uses a zero-float exact integer block-average fast path when `src_w % 9 == 0 && src_h % 8 == 0`
/// (e.g., `72x72` -> exact `8x9` blocks of 72 pixels per cell!).
#[inline]
pub fn downscale_luma_frame_to_9x8(frame: &[u8], src_w: usize, src_h: usize) -> [u8; 72] {
    let mut out = [0u8; 72];
    if src_w == 9 && src_h == 8 && frame.len() >= 72 {
        out.copy_from_slice(&frame[..72]);
        return out;
    }
    if src_w == 0 || src_h == 0 || frame.len() < src_w * src_h {
        return out;
    }

    if src_w % 9 == 0 && src_h % 8 == 0 {
        // Exact integer block average (e.g., 72x72 -> 8x9 = 72 pixels per cell, zero floating-point math!)
        let bw = src_w / 9;
        let bh = src_h / 8;
        let area = (bw * bh) as u32;
        let half = area / 2;
        for dy in 0..8 {
            let y0 = dy * bh;
            for dx in 0..9 {
                let x0 = dx * bw;
                let mut sum: u32 = 0;
                for y in y0..(y0 + bh) {
                    let row_start = y * src_w + x0;
                    for x in 0..bw {
                        sum += frame[row_start + x] as u32;
                    }
                }
                out[dy * 9 + dx] = ((sum + half) / area) as u8;
            }
        }
    } else {
        // Exact fractional area-averaging fallback for arbitrary even dimensions (e.g., 32x32, 64x64)
        let scale_x = src_w as f64 / 9.0;
        let scale_y = src_h as f64 / 8.0;
        for dy in 0..8 {
            let sy0 = dy as f64 * scale_y;
            let sy1 = (dy + 1) as f64 * scale_y;
            let iy0 = sy0.floor() as usize;
            let iy1 = (sy1.ceil() as usize).min(src_h);
            for dx in 0..9 {
                let sx0 = dx as f64 * scale_x;
                let sx1 = (dx + 1) as f64 * scale_x;
                let ix0 = sx0.floor() as usize;
                let ix1 = (sx1.ceil() as usize).min(src_w);
                let mut acc = 0.0f64;
                let mut wsum = 0.0f64;
                for y in iy0..iy1 {
                    let wy = ((y + 1) as f64).min(sy1) - (y as f64).max(sy0);
                    if wy <= 0.0 {
                        continue;
                    }
                    let row_off = y * src_w;
                    for x in ix0..ix1 {
                        let wx = ((x + 1) as f64).min(sx1) - (x as f64).max(sx0);
                        if wx <= 0.0 {
                            continue;
                        }
                        let w = wx * wy;
                        acc += frame[row_off + x] as f64 * w;
                        wsum += w;
                    }
                }
                let val = if wsum > 0.0 {
                    (acc / wsum).round().clamp(0.0, 255.0) as u8
                } else {
                    0
                };
                out[dy * 9 + dx] = val;
            }
        }
    }
    out
}

/// Returns true when `(src_w, src_h)` uses the zero-float exact integer block-average fast path.
#[inline]
pub fn is_exact_integer_downscale(src_w: usize, src_h: usize) -> bool {
    src_w > 0 && src_h > 0 && src_w % 9 == 0 && src_h % 8 == 0
}

/// Converts a concatenated buffer of raw `src_w x src_h` 8-bit luma frames into 64-bit dHashes.
pub fn downscale_luma_to_9x8_and_dhash(
    frames_buf: &[u8],
    src_w: usize,
    src_h: usize,
) -> Vec<u64> {
    let frame_bytes = src_w.saturating_mul(src_h);
    if frame_bytes == 0 {
        return Vec::new();
    }
    let num_frames = frames_buf.len() / frame_bytes;
    let mut hashes = Vec::with_capacity(num_frames);
    for idx in 0..num_frames {
        let start = idx * frame_bytes;
        let grid = downscale_luma_frame_to_9x8(&frames_buf[start..start + frame_bytes], src_w, src_h);
        hashes.push(compute_dhash_from_9x8(&grid));
    }
    hashes
}

/// Builds the FFmpeg GPU decode + GPU downscale filter string for 1-fps extraction.
fn build_hw_1fps_vf(
    backend: HwBackend,
    use_hw_scaler: bool,
    cuvid_resized: bool,
    w: u32,
    h: u32,
    caps: &FfmpegCapabilities,
) -> (Option<&'static str>, String) {
    if cuvid_resized {
        // NVDEC `*_cuvid -resize WxH` already scales directly on NVDEC silicon before output!
        return (None, "fps=1,format=gray".to_string());
    }
    if !use_hw_scaler {
        return (
            None,
            format!("fps=1,scale={}:{}:flags=area,format=gray", w, h),
        );
    }

    match backend {
        HwBackend::VideoToolbox => (
            Some("videotoolbox_vld"),
            format!(
                "fps=1,scale_vt=w={}:h={},hwdownload,format=nv12,format=gray",
                w, h
            ),
        ),
        HwBackend::Cuda => {
            if caps.has_filter("scale_cuda") {
                (
                    Some("cuda"),
                    format!(
                        "fps=1,scale_cuda=w={}:h={}:format=nv12,hwdownload,format=nv12,format=gray",
                        w, h
                    ),
                )
            } else {
                (
                    Some("cuda"),
                    format!(
                        "fps=1,scale_npp=w={}:h={}:format=nv12,hwdownload,format=nv12,format=gray",
                        w, h
                    ),
                )
            }
        }
        HwBackend::Qsv => (
            Some("qsv"),
            format!(
                "fps=1,scale_qsv=w={}:h={}:format=nv12,hwdownload,format=nv12,format=gray",
                w, h
            ),
        ),
        HwBackend::Vaapi => (
            Some("vaapi"),
            format!(
                "fps=1,scale_vaapi=w={}:h={}:format=nv12,hwdownload,format=nv12,format=gray",
                w, h
            ),
        ),
        HwBackend::Vulkan => (
            Some("vulkan"),
            format!(
                "fps=1,scale_vulkan=w={}:h={}:format=nv12,hwdownload,format=nv12,format=gray",
                w, h
            ),
        ),
        HwBackend::Rkmpp => (
            Some("drm_prime"),
            format!(
                "fps=1,scale_rkrga=w={}:h={}:format=nv12,hwdownload,format=nv12,format=gray",
                w, h
            ),
        ),
        HwBackend::D3d12va
        | HwBackend::D3d11va
        | HwBackend::Dxva2
        | HwBackend::V4l2m2m
        | HwBackend::MediaCodec => (
            None,
            format!("fps=1,scale={}:{}:flags=area,format=gray", w, h),
        ),
    }
}

/// Builds FFmpeg CLI arguments for a specific GPU backend + codec + extraction kind.
pub fn build_gpu_ffmpeg_args(
    path: &Path,
    video_codec: &str,
    kind: DecodeKind,
    backend: HwBackend,
    use_hw_scaler: bool,
    env: &VerifiedDecodeEnv,
) -> (Vec<OsString>, String) {
    let w = env.scale_width;
    let h = env.scale_height;
    let decoder = env.gpu_decoder_for(backend, video_codec);
    let cuvid_resized = use_hw_scaler
        && backend == HwBackend::Cuda
        && decoder
            .as_deref()
            .map(|d| d.ends_with("_cuvid"))
            .unwrap_or(false);

    let mut args: Vec<OsString> = vec!["-nostdin".into(), "-v".into(), "error".into()];

    if backend == HwBackend::Vaapi {
        if let Some(ref dev) = env.ffmpeg_cfg.hardware_acceleration.vaapi_device {
            args.push("-vaapi_device".into());
            args.push(dev.as_os_str().to_os_string());
        }
    }

    env.ffmpeg_cfg.push_global_filter_thread_args(&mut args);

    match kind {
        DecodeKind::Sparse5 { duration_secs } => {
            // 5-input batched seek: uses HW decode -> CPU area scale to avoid hitting consumer GPU
            // concurrent session limits (e.g., GeForce NVDEC session cap) across 5 simultaneous inputs.
            let filter = format!(
                "[0:v]scale={w}:{h}:flags=area,format=gray,trim=end_frame=1[v0];\
                 [1:v]scale={w}:{h}:flags=area,format=gray,trim=end_frame=1[v1];\
                 [2:v]scale={w}:{h}:flags=area,format=gray,trim=end_frame=1[v2];\
                 [3:v]scale={w}:{h}:flags=area,format=gray,trim=end_frame=1[v3];\
                 [4:v]scale={w}:{h}:flags=area,format=gray,trim=end_frame=1[v4];\
                 [v0][v1][v2][v3][v4]concat=n=5:v=1:a=0"
            );
            for frac in SPARSE_SAMPLE_FRACTIONS {
                let ts = (duration_secs * frac).min((duration_secs - 0.1).max(0.0));
                let ts_str = format!("{:.3}", ts.max(0.0));
                args.push("-hwaccel".into());
                args.push(backend.as_ffmpeg_name().into());
                env.ffmpeg_cfg.push_thread_args(&mut args);
                if let Some(ref dec) = decoder {
                    args.push("-c:v".into());
                    args.push(dec.into());
                }
                args.push("-ss".into());
                args.push(ts_str.into());
                args.push("-i".into());
                args.push(path.as_os_str().to_os_string());
            }
            env.ffmpeg_cfg.push_skip_stream_args(&mut args);
            args.extend(
                ["-filter_complex", &filter, "-f", "rawvideo", "pipe:1"]
                    .into_iter()
                    .map(OsString::from),
            );
            let label = format!(
                "{}({})",
                backend.as_ffmpeg_name(),
                decoder.as_deref().unwrap_or("native")
            );
            (args, label)
        }
        DecodeKind::Prefix1Fps { max_secs } => {
            let (hw_out_fmt, vf) =
                build_hw_1fps_vf(backend, use_hw_scaler, cuvid_resized, w, h, &env.caps);
            args.push("-hwaccel".into());
            args.push(backend.as_ffmpeg_name().into());
            if let Some(fmt) = hw_out_fmt {
                args.push("-hwaccel_output_format".into());
                args.push(fmt.into());
            }
            env.ffmpeg_cfg.push_thread_args(&mut args);
            if let Some(ref dec) = decoder {
                args.push("-c:v".into());
                args.push(dec.into());
            }
            if cuvid_resized {
                args.push("-resize".into());
                args.push(format!("{}x{}", w, h).into());
            }
            args.push("-t".into());
            args.push(max_secs.to_string().into());
            args.push("-i".into());
            args.push(path.as_os_str().to_os_string());
            env.ffmpeg_cfg.push_skip_stream_args(&mut args);
            args.extend(
                ["-vf", &vf, "-f", "rawvideo", "pipe:1"]
                    .into_iter()
                    .map(OsString::from),
            );
            let scale_tag = if cuvid_resized {
                format!("cuvid-resize({}x{})", w, h)
            } else if hw_out_fmt.is_some() {
                format!("hw-scale({}x{})", w, h)
            } else {
                format!("cpu-scale({}x{})", w, h)
            };
            let label = format!(
                "{}({}+{})",
                backend.as_ffmpeg_name(),
                decoder.as_deref().unwrap_or("native"),
                scale_tag
            );
            (args, label)
        }
        DecodeKind::Dense1Fps => {
            let (hw_out_fmt, vf) =
                build_hw_1fps_vf(backend, use_hw_scaler, cuvid_resized, w, h, &env.caps);
            args.push("-hwaccel".into());
            args.push(backend.as_ffmpeg_name().into());
            if let Some(fmt) = hw_out_fmt {
                args.push("-hwaccel_output_format".into());
                args.push(fmt.into());
            }
            env.ffmpeg_cfg.push_thread_args(&mut args);
            if let Some(ref dec) = decoder {
                args.push("-c:v".into());
                args.push(dec.into());
            }
            if cuvid_resized {
                args.push("-resize".into());
                args.push(format!("{}x{}", w, h).into());
            }
            args.push("-i".into());
            args.push(path.as_os_str().to_os_string());
            env.ffmpeg_cfg.push_skip_stream_args(&mut args);
            args.extend(
                ["-vf", &vf, "-f", "rawvideo", "pipe:1"]
                    .into_iter()
                    .map(OsString::from),
            );
            let scale_tag = if cuvid_resized {
                format!("cuvid-resize({}x{})", w, h)
            } else if hw_out_fmt.is_some() {
                format!("hw-scale({}x{})", w, h)
            } else {
                format!("cpu-scale({}x{})", w, h)
            };
            let label = format!(
                "{}({}+{})",
                backend.as_ffmpeg_name(),
                decoder.as_deref().unwrap_or("native"),
                scale_tag
            );
            (args, label)
        }
    }
}

/// Builds FFmpeg CLI arguments for the fastest CPU software decoder + `W x H` luma scaling.
pub fn build_cpu_ffmpeg_args(
    path: &Path,
    video_codec: &str,
    kind: DecodeKind,
    env: &VerifiedDecodeEnv,
) -> (Vec<OsString>, String) {
    let w = env.scale_width;
    let h = env.scale_height;
    let cpu_dec = env.fastest_cpu_decoder_for(video_codec);

    let mut args: Vec<OsString> = vec!["-nostdin".into(), "-v".into(), "error".into()];
    env.ffmpeg_cfg.push_global_filter_thread_args(&mut args);

    match kind {
        DecodeKind::Sparse5 { duration_secs } => {
            let filter = format!(
                "[0:v]scale={w}:{h}:flags=area,format=gray,trim=end_frame=1[v0];\
                 [1:v]scale={w}:{h}:flags=area,format=gray,trim=end_frame=1[v1];\
                 [2:v]scale={w}:{h}:flags=area,format=gray,trim=end_frame=1[v2];\
                 [3:v]scale={w}:{h}:flags=area,format=gray,trim=end_frame=1[v3];\
                 [4:v]scale={w}:{h}:flags=area,format=gray,trim=end_frame=1[v4];\
                 [v0][v1][v2][v3][v4]concat=n=5:v=1:a=0"
            );
            for frac in SPARSE_SAMPLE_FRACTIONS {
                let ts = (duration_secs * frac).min((duration_secs - 0.1).max(0.0));
                let ts_str = format!("{:.3}", ts.max(0.0));
                env.ffmpeg_cfg.push_thread_args(&mut args);
                if let Some(dec) = cpu_dec {
                    args.push("-c:v".into());
                    args.push(dec.into());
                }
                args.push("-ss".into());
                args.push(ts_str.into());
                args.push("-i".into());
                args.push(path.as_os_str().to_os_string());
            }
            env.ffmpeg_cfg.push_skip_stream_args(&mut args);
            args.extend(
                ["-filter_complex", &filter, "-f", "rawvideo", "pipe:1"]
                    .into_iter()
                    .map(OsString::from),
            );
        }
        DecodeKind::Prefix1Fps { max_secs } => {
            env.ffmpeg_cfg.push_thread_args(&mut args);
            if let Some(dec) = cpu_dec {
                args.push("-c:v".into());
                args.push(dec.into());
            }
            args.push("-t".into());
            args.push(max_secs.to_string().into());
            args.push("-i".into());
            args.push(path.as_os_str().to_os_string());
            env.ffmpeg_cfg.push_skip_stream_args(&mut args);
            let vf = format!("fps=1,scale={}:{}:flags=area,format=gray", w, h);
            args.extend(
                ["-vf", &vf, "-f", "rawvideo", "pipe:1"]
                    .into_iter()
                    .map(OsString::from),
            );
        }
        DecodeKind::Dense1Fps => {
            env.ffmpeg_cfg.push_thread_args(&mut args);
            if let Some(dec) = cpu_dec {
                args.push("-c:v".into());
                args.push(dec.into());
            }
            args.push("-i".into());
            args.push(path.as_os_str().to_os_string());
            env.ffmpeg_cfg.push_skip_stream_args(&mut args);
            let vf = format!("fps=1,scale={}:{}:flags=area,format=gray", w, h);
            args.extend(
                ["-vf", &vf, "-f", "rawvideo", "pipe:1"]
                    .into_iter()
                    .map(OsString::from),
            );
        }
    }

    let label = format!("cpu({})", cpu_dec.unwrap_or("native"));
    (args, label)
}

fn extract_single_frame_raw_cpu(
    path: &Path,
    timestamp_secs: f64,
    video_codec: &str,
    env: &VerifiedDecodeEnv,
) -> Option<Vec<u8>> {
    let w = env.scale_width as usize;
    let h = env.scale_height as usize;
    let frame_bytes = w * h;
    let ts_str = format!("{:.3}", timestamp_secs.max(0.0));
    let mut args: Vec<OsString> = vec!["-nostdin".into(), "-v".into(), "error".into()];
    env.ffmpeg_cfg.push_thread_args(&mut args);
    if let Some(dec) = env.fastest_cpu_decoder_for(video_codec) {
        args.push("-c:v".into());
        args.push(dec.into());
    }
    args.push("-ss".into());
    args.push(ts_str.into());
    args.push("-i".into());
    args.push(path.as_os_str().to_os_string());
    env.ffmpeg_cfg.push_skip_stream_args(&mut args);
    let vf = format!("scale={}:{}:flags=area,format=gray", w, h);
    args.extend(
        [
            "-frames:v",
            "1",
            "-vf",
            &vf,
            "-f",
            "rawvideo",
            "pipe:1",
        ]
        .into_iter()
        .map(OsString::from),
    );
    let out = Command::new("ffmpeg").args(&args).output().ok()?;
    if out.stdout.len() >= frame_bytes {
        Some(out.stdout[..frame_bytes].to_vec())
    } else {
        None
    }
}

/// Runs GPU-accelerated FFmpeg decode + downscale to raw `W x H` luma frames.
/// Distinguishes fatal driver/GPU errors (which fast-fail!) from per-file codec/profile rejects.
pub fn decode_raw_luma_gpu(
    path: &Path,
    video_codec: &str,
    kind: DecodeKind,
    env: &VerifiedDecodeEnv,
) -> RawDecodeOutcome {
    let Some(ref active_hw) = env.active_hw else {
        return RawDecodeOutcome::CodecRejected("No active GPU backend".to_string());
    };
    let w = env.scale_width as usize;
    let h = env.scale_height as usize;
    let frame_bytes = w * h;

    if let DecodeKind::Sparse5 { duration_secs } = kind {
        if duration_secs <= 0.5 {
            return RawDecodeOutcome::CodecRejected(
                "Very short clip (<= 0.5s); routing single-frame seek to CPU worker".to_string(),
            );
        }
    }

    let (args, label) = build_gpu_ffmpeg_args(
        path,
        video_codec,
        kind,
        active_hw.backend,
        active_hw.uses_hw_scaler,
        env,
    );

    match Command::new("ffmpeg").args(&args).output() {
        Ok(out) => {
            let count = out.stdout.len() / frame_bytes;
            let sufficient = match kind {
                DecodeKind::Sparse5 { .. } => count == SPARSE_SAMPLE_FRACTIONS.len(),
                DecodeKind::Prefix1Fps { .. } | DecodeKind::Dense1Fps => count > 0,
            };
            if sufficient {
                return RawDecodeOutcome::Ok(RawFrameBatch {
                    width: w,
                    height: h,
                    luma_bytes: out.stdout,
                    decoder_label: label,
                    used_hw_scale_fallback: false,
                });
            }
            let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
            if is_fatal_driver_error(&stderr) {
                return RawDecodeOutcome::FatalDriverError(format!(
                    "Fatal GPU/driver error on backend '{}' for {:?}: {}",
                    active_hw.backend, path, stderr
                ));
            }
            // If GPU hardware scaling failed on an unusual 10-bit/HDR pixel format for THIS file,
            // try GPU decode + CPU scale on the same verified GPU backend before classifying as CodecRejected.
            if active_hw.uses_hw_scaler && !matches!(kind, DecodeKind::Sparse5 { .. }) {
                let (retry_args, retry_label) = build_gpu_ffmpeg_args(
                    path,
                    video_codec,
                    kind,
                    active_hw.backend,
                    false,
                    env,
                );
                if let Ok(retry_out) = Command::new("ffmpeg").args(&retry_args).output() {
                    if retry_out.stdout.len() / frame_bytes > 0 {
                        return RawDecodeOutcome::Ok(RawFrameBatch {
                            width: w,
                            height: h,
                            luma_bytes: retry_out.stdout,
                            decoder_label: retry_label,
                            used_hw_scale_fallback: true,
                        });
                    }
                    let retry_stderr = String::from_utf8_lossy(&retry_out.stderr).trim().to_string();
                    if is_fatal_driver_error(&retry_stderr) {
                        return RawDecodeOutcome::FatalDriverError(format!(
                            "Fatal GPU/driver error on backend '{}' for {:?}: {}",
                            active_hw.backend, path, retry_stderr
                        ));
                    }
                }
            }

            RawDecodeOutcome::CodecRejected(if stderr.is_empty() {
                format!(
                    "GPU backend '{}' yielded {} frames for codec {} on {:?}",
                    active_hw.backend, count, video_codec, path
                )
            } else {
                stderr
            })
        }
        Err(e) => RawDecodeOutcome::FatalDriverError(format!(
            "Failed to spawn ffmpeg GPU decode process on {:?}: {}",
            path, e
        )),
    }
}

/// Runs fastest CPU software FFmpeg decode + `W x H` luma scaling.
pub fn decode_raw_luma_cpu(
    path: &Path,
    video_codec: &str,
    kind: DecodeKind,
    env: &VerifiedDecodeEnv,
) -> Result<RawFrameBatch> {
    let w = env.scale_width as usize;
    let h = env.scale_height as usize;
    let frame_bytes = w * h;

    if let DecodeKind::Sparse5 { duration_secs } = kind {
        if duration_secs <= 0.5 {
            let mut luma = Vec::new();
            if let Some(f) = extract_single_frame_raw_cpu(path, 0.0, video_codec, env) {
                luma.extend_from_slice(&f);
            }
            return Ok(RawFrameBatch {
                width: w,
                height: h,
                luma_bytes: luma,
                decoder_label: format!(
                    "cpu({})",
                    env.fastest_cpu_decoder_for(video_codec).unwrap_or("native")
                ),
                used_hw_scale_fallback: false,
            });
        }
    }

    let (args, label) = build_cpu_ffmpeg_args(path, video_codec, kind, env);
    let output = Command::new("ffmpeg")
        .args(&args)
        .output()
        .with_context(|| format!("Failed to run CPU ffmpeg extraction on {:?}", path))?;

    let count = output.stdout.len() / frame_bytes;
    if let DecodeKind::Sparse5 { duration_secs } = kind {
        if count != SPARSE_SAMPLE_FRACTIONS.len() {
            // Fallback to individual single-frame seeks near EOF
            let mut luma = Vec::with_capacity(SPARSE_SAMPLE_FRACTIONS.len() * frame_bytes);
            for frac in SPARSE_SAMPLE_FRACTIONS {
                let ts = (duration_secs * frac).min((duration_secs - 0.1).max(0.0));
                if let Some(f) = extract_single_frame_raw_cpu(path, ts, video_codec, env) {
                    luma.extend_from_slice(&f);
                }
            }
            return Ok(RawFrameBatch {
                width: w,
                height: h,
                luma_bytes: luma,
                decoder_label: label,
                used_hw_scale_fallback: false,
            });
        }
    }

    Ok(RawFrameBatch {
        width: w,
        height: h,
        luma_bytes: output.stdout,
        decoder_label: label,
        used_hw_scale_fallback: false,
    })
}

/// RAII temp file guard for the embedded `--config-test` clip so it is always cleaned up.
struct TempTestClip {
    path: PathBuf,
}

impl TempTestClip {
    fn create() -> Result<Self> {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rusty_dedup_config_test_{}.mp4",
            std::process::id()
        ));
        fs::write(&path, CONFIG_TEST_H264_MP4)
            .with_context(|| format!("Failed to write temporary config-test clip to {:?}", path))?;
        Ok(Self { path })
    }
}

impl Drop for TempTestClip {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Formats actionable, vendor-specific troubleshooting guidance + hw_scale WARNING + CPU-disable advice.
pub fn format_gpu_fast_fail_report(
    tested_failures: &[(HwBackend, String)],
    unavailable_backends: &[HwBackend],
    scale_w: u32,
    scale_h: u32,
) -> String {
    let mut msg = String::new();
    msg.push_str("\n================================================================================\n");
    msg.push_str("FAST-FAIL: Hardware acceleration is enabled in dedup_config.yaml, but NO working\n");
    msg.push_str("GPU hardware decoder passed the live --config-test!\n");
    msg.push_str("rusty_dedup will NOT silently fall back to slow CPU software decoding.\n");
    msg.push_str("================================================================================\n\n");

    if !tested_failures.is_empty() {
        msg.push_str("Tested GPU Backends (ALL FAILED):\n");
        for (backend, err) in tested_failures {
            let short_err = err
                .lines()
                .take(3)
                .collect::<Vec<_>>()
                .join(" | ");
            msg.push_str(&format!(
                "  - {} [{}]: {}\n",
                backend.as_ffmpeg_name(),
                backend.vendor_label(),
                if short_err.is_empty() {
                    "0 frames decoded"
                } else {
                    &short_err
                }
            ));
        }
        msg.push('\n');
    }

    if !unavailable_backends.is_empty() {
        let names: Vec<&str> = unavailable_backends
            .iter()
            .map(|b| b.as_ffmpeg_name())
            .collect();
        msg.push_str(&format!(
            "Configured backends NOT compiled into your FFmpeg build (`ffmpeg -hwaccels`):\n  {}\n\n",
            names.join(", ")
        ));
    }

    msg.push_str(&format!(
        "WARNING: GPU hardware scalers can reject certain target resolutions (currently {}x{}).\n\
         If your GPU driver enforces strict 16-pixel alignment or a higher minimum surface size,\n\
         try tuning `hw_scale_width` and `hw_scale_height` in `dedup_config.yaml` (e.g., 144x144\n\
         for 16-pixel-aligned exact 9x8 divisibility, or 64x64 / 32x32) and re-run `--config-test`.\n\n",
        scale_w, scale_h
    ));

    msg.push_str("Online GPU Hardware Troubleshooting Resources:\n");
    let mut shown_backends: Vec<HwBackend> = tested_failures.iter().map(|(b, _)| *b).collect();
    if shown_backends.is_empty() {
        #[cfg(target_os = "macos")]
        shown_backends.push(HwBackend::VideoToolbox);
        #[cfg(target_os = "windows")]
        {
            shown_backends.push(HwBackend::Cuda);
            shown_backends.push(HwBackend::D3d12va);
            shown_backends.push(HwBackend::D3d11va);
            shown_backends.push(HwBackend::Qsv);
        }
        #[cfg(target_os = "linux")]
        {
            shown_backends.push(HwBackend::Cuda);
            shown_backends.push(HwBackend::Vaapi);
            shown_backends.push(HwBackend::Qsv);
            shown_backends.push(HwBackend::Vulkan);
            shown_backends.push(HwBackend::V4l2m2m);
            shown_backends.push(HwBackend::Rkmpp);
        }
    }
    for backend in shown_backends {
        msg.push_str(&format!("  * {}:\n", backend.vendor_label()));
        for url in backend.troubleshooting_resources() {
            msg.push_str(&format!("      - {}\n", url));
        }
    }

    msg.push_str("\nAlternative (CPU-Only Mode):\n");
    msg.push_str("  If this machine has no supported GPU or you prefer to decode on the CPU,\n");
    msg.push_str("  explicitly set `hardware_acceleration.enabled: false` in `dedup_config.yaml`.\n");
    msg.push_str("  WARNING: CPU software decoding is significantly slower than GPU hardware decoding,\n");
    msg.push_str("  especially across large high-resolution video libraries.\n");
    msg
}

/// Runs the mandatory 2-step `--config-test` verification (both standalone and as Step 1 of every session).
pub fn run_config_test(dedup_cfg: &DedupConfig, verbose: bool) -> Result<VerifiedDecodeEnv> {
    eprintln!("[Config-Test] Step 1/2: Verifying `ffmpeg` & `ffprobe` binaries and probing enabled video decoders...");
    let mut caps = FfmpegCapabilities::probe_binaries_and_decoders()?;
    let w = dedup_cfg.ffmpeg.hardware_acceleration.hw_scale_width;
    let h = dedup_cfg.ffmpeg.hardware_acceleration.hw_scale_height;

    if verbose {
        eprintln!("  [Verbose][Config-Test] {}", caps.ffmpeg_version);
        eprintln!("  [Verbose][Config-Test] {}", caps.ffprobe_version);
        eprintln!(
            "  [Verbose][Config-Test] Discovered {} video decoders (libdav1d={}, vp9={}, h264={}, hevc={})",
            caps.decoders.len(),
            caps.has_decoder("libdav1d"),
            caps.has_decoder("vp9"),
            caps.has_decoder("h264"),
            caps.has_decoder("hevc")
        );
    }

    let test_clip = TempTestClip::create()?;

    if !dedup_cfg.ffmpeg.hardware_acceleration.enabled {
        eprintln!(
            "[Config-Test] Step 2/2: `hardware_acceleration.enabled: false` — running live CPU decode + {}x{} CPU scale -> Rust 9x8 dHash test...",
            w, h
        );
        let env = VerifiedDecodeEnv {
            caps,
            active_hw: None,
            scale_width: w,
            scale_height: h,
            ffmpeg_cfg: dedup_cfg.ffmpeg.clone(),
        };

        let batch = decode_raw_luma_cpu(&test_clip.path, "H264", DecodeKind::Dense1Fps, &env)?;
        let hashes = downscale_luma_to_9x8_and_dhash(&batch.luma_bytes, batch.width, batch.height);
        if hashes.len() < 2 {
            anyhow::bail!(
                "FAST-FAIL: Live CPU decode + {}x{} scale config-test failed (expected >= 2 frames from embedded H.264 clip, got {}).\n\
                 Please verify your FFmpeg installation: https://ffmpeg.org/download.html",
                w,
                h,
                hashes.len()
            );
        }

        eprintln!(
            "[Config-Test] PASSED (CPU Software Mode: {} decoded {} frames @ {}x{} -> {} dHashes).",
            batch.decoder_label,
            hashes.len(),
            w,
            h,
            hashes.len()
        );
        eprintln!("⚠️  NOTICE: Hardware acceleration is disabled (`hardware_acceleration.enabled: false`).");
        eprintln!("    CPU decoding is significantly slower on large/high-resolution video libraries.");
        eprintln!("    Enabling GPU hardware acceleration (`enabled: true` in `dedup_config.yaml`) is");
        eprintln!("    strongly recommended once you ensure compatible GPU hardware & drivers are");
        eprintln!("    available and accessible to FFmpeg on this system.");
        return Ok(env);
    }

    // Step 2a: Hardware acceleration enabled -> probe -hwaccels & -filters ONLY here
    eprintln!(
        "[Config-Test] Step 2/2: Probing FFmpeg `-hwaccels` & `-filters` and running live GPU decode + {}x{} scale test...",
        w, h
    );
    caps.probe_hw_capabilities()?;
    if verbose {
        let mut hws: Vec<&String> = caps.hwaccels.iter().collect();
        hws.sort();
        eprintln!("  [Verbose][Config-Test] FFmpeg enabled hwaccels: {:?}", hws);
        let hw_scalers: Vec<&str> = [
            "scale_vt",
            "scale_cuda",
            "scale_npp",
            "scale_qsv",
            "scale_vaapi",
            "scale_vulkan",
            "scale_rkrga",
            "hwdownload",
        ]
        .into_iter()
        .filter(|f| caps.has_filter(f))
        .collect();
        eprintln!(
            "  [Verbose][Config-Test] FFmpeg enabled GPU scale filters: {:?}",
            hw_scalers
        );
    }

    let base_env = VerifiedDecodeEnv {
        caps,
        active_hw: None,
        scale_width: w,
        scale_height: h,
        ffmpeg_cfg: dedup_cfg.ffmpeg.clone(),
    };

    let mut tested_failures: Vec<(HwBackend, String)> = Vec::new();
    let mut unavailable_backends: Vec<HwBackend> = Vec::new();
    let frame_bytes = (w as usize) * (h as usize);

    for &backend in &dedup_cfg.ffmpeg.hardware_acceleration.fallback_order {
        if !base_env.caps.has_hwaccel(backend) {
            unavailable_backends.push(backend);
            continue;
        }
        if backend == HwBackend::Vaapi {
            if let Some(ref dev) = dedup_cfg.ffmpeg.hardware_acceleration.vaapi_device {
                if !dev.exists() {
                    tested_failures.push((
                        backend,
                        format!("VAAPI render node {:?} does not exist on this host", dev),
                    ));
                    continue;
                }
            }
        }

        let decoder = base_env.gpu_decoder_for(backend, "H264");
        let supports_hw_scaler =
            base_env.backend_supports_hw_scaler(backend, decoder.as_deref());

        // Attempt live GPU decode (+ GPU downscale if supported by backend)
        let (args, label) = build_gpu_ffmpeg_args(
            &test_clip.path,
            "H264",
            DecodeKind::Dense1Fps,
            backend,
            supports_hw_scaler,
            &base_env,
        );
        if verbose {
            eprintln!(
                "  [Verbose][Config-Test] Testing backend '{}' ({})...",
                backend, label
            );
        }

        let out = Command::new("ffmpeg").args(&args).output();
        match out {
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                let hashes =
                    downscale_luma_to_9x8_and_dhash(&output.stdout, w as usize, h as usize);
                // Guard against FFmpeg silently falling back to CPU internally:
                // verify BOTH decoded frames AND zero hwaccel initialization errors in stderr!
                if output.stdout.len() >= 2 * frame_bytes
                    && hashes.len() >= 2
                    && !hwaccel_init_failed_in_stderr(&stderr)
                {
                    let scale_desc = if supports_hw_scaler {
                        format!("GPU downscale {}x{}", w, h)
                    } else {
                        format!("HW decode + CPU scale {}x{}", w, h)
                    };
                    eprintln!(
                        "[Config-Test] PASSED! Active GPU backend: '{}' [{}] ({}, verified {} frames -> {} dHashes).",
                        backend.as_ffmpeg_name(),
                        backend.vendor_label(),
                        label,
                        hashes.len(),
                        hashes.len()
                    );
                    if w % 9 != 0 || h % 8 != 0 {
                        eprintln!(
                            "⚠️  WARNING: Configured hw_scale ({}x{}) is not divisible by 9x8; using fractional area-averaging. Consider 72x72 or 144x144 for exact integer block averaging.",
                            w, h
                        );
                    }
                    let mut final_env = base_env;
                    final_env.active_hw = Some(VerifiedHwBackend {
                        backend,
                        uses_hw_scaler: supports_hw_scaler,
                        scale_description: scale_desc,
                    });
                    return Ok(final_env);
                }

                // If HW scaling specifically failed (NOT a GPU decode/init failure!),
                // check whether HW decode + CPU scale succeeds WITHOUT hwaccel init errors,
                // and surface a clear WARNING advising the user to tune hw_scale_width/hw_scale_height.
                if supports_hw_scaler && !hwaccel_init_failed_in_stderr(&stderr) {
                    let (cpu_scale_args, cpu_scale_label) = build_gpu_ffmpeg_args(
                        &test_clip.path,
                        "H264",
                        DecodeKind::Dense1Fps,
                        backend,
                        false,
                        &base_env,
                    );
                    if let Ok(fallback_out) = Command::new("ffmpeg").args(&cpu_scale_args).output()
                    {
                        let fb_stderr =
                            String::from_utf8_lossy(&fallback_out.stderr).trim().to_string();
                        let fb_hashes = downscale_luma_to_9x8_and_dhash(
                            &fallback_out.stdout,
                            w as usize,
                            h as usize,
                        );
                        if fallback_out.stdout.len() >= 2 * frame_bytes
                            && fb_hashes.len() >= 2
                            && !hwaccel_init_failed_in_stderr(&fb_stderr)
                        {
                            eprintln!(
                                "⚠️  WARNING: GPU backend '{}' decoded video in hardware, but GPU-side scaling at {}x{} failed ({}).",
                                backend.as_ffmpeg_name(),
                                w,
                                h,
                                stderr.lines().next().unwrap_or("scale filter rejected resolution")
                            );
                            eprintln!(
                                "    Consider tuning `hw_scale_width` / `hw_scale_height` in `dedup_config.yaml` (e.g., 144x144 or 64x64 for 16-pixel-aligned drivers) to enable full GPU-side scaling."
                            );
                            eprintln!(
                                "[Config-Test] PASSED (with HW decode + CPU scale): '{}' ({}).",
                                backend.as_ffmpeg_name(),
                                cpu_scale_label
                            );
                            let mut final_env = base_env;
                            final_env.active_hw = Some(VerifiedHwBackend {
                                backend,
                                uses_hw_scaler: false,
                                scale_description: format!("HW decode + CPU scale {}x{}", w, h),
                            });
                            return Ok(final_env);
                        }
                    }
                }

                tested_failures.push((backend, stderr));
            }
            Err(e) => {
                tested_failures.push((backend, format!("Failed to spawn ffmpeg: {}", e)));
            }
        }
    }

    let report = format_gpu_fast_fail_report(&tested_failures, &unavailable_backends, w, h);
    anyhow::bail!("{}", report);
}
