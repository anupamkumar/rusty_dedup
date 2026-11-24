use crate::db::DbWriteOp;
use crate::ffmpeg::{
    DecodeKind, RawDecodeOutcome, VerifiedDecodeEnv, decode_raw_luma_cpu, decode_raw_luma_gpu,
    downscale_luma_to_9x8_and_dhash, is_exact_integer_downscale,
};
use crate::telemetry::{TelemetryRegistry, ThreadRole};
use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Bounded in-memory channel capacity between Decode Workers and the CPU `9x8` dHash Worker
/// (16 in-flight video frame buffers for clean RAM backpressure).
pub const FRAME_CHANNEL_CAPACITY: usize = 16;

#[derive(Debug, Clone)]
pub struct DecodeJob {
    pub record_idx: usize,
    pub file_key: String,
    pub path: PathBuf,
    pub file_stem: String,
    pub video_codec: String,
    pub video_duration_secs: f64,
    pub kind: DecodeKind,
}

impl DecodeJob {
    fn kind_label(&self) -> &'static str {
        match self.kind {
            DecodeKind::Sparse5 { .. } => "sparse5",
            DecodeKind::Prefix1Fps { .. } => "prefix60",
            DecodeKind::Dense1Fps => "dense",
        }
    }

    fn decoded_video_secs(&self) -> f64 {
        match self.kind {
            DecodeKind::Sparse5 { .. } => 5.0,
            DecodeKind::Prefix1Fps { max_secs } => self.video_duration_secs.min(max_secs as f64),
            DecodeKind::Dense1Fps => self.video_duration_secs,
        }
    }
}

struct StagedLumaBatch {
    job: DecodeJob,
    width: usize,
    height: usize,
    luma_bytes: Vec<u8>,
    decoder_label: String,
    decode_elapsed: Duration,
}

enum HashWorkerMsg {
    Frames(StagedLumaBatch),
    FatalError(String),
}

#[derive(Debug, Clone)]
pub struct DecodeResult {
    pub record_idx: usize,
    pub file_key: String,
    pub hashes: Vec<u64>,
    pub decoder_label: String,
    pub decode_elapsed: Duration,
    pub hash_elapsed: Duration,
}

/// Orchestrated 3-thread video decode & perceptual-hashing pipeline with full USE+RED telemetry:
///   1. GPU HW-Decode & HW-Downscale Worker Thread (active when `hardware_acceleration.enabled == true`)
///   2. CPU-Codec Decode Worker Thread (active when HW disabled, for legacy non-GPU codecs, or for
///      non-driver per-file codec/profile rejects forwarded by the GPU worker)
///   3. CPU `72x72 -> 9x8` Area-Downscale & 64-bit dHash Worker Thread (drains `sync_channel(16)`)
pub struct DecodePipeline {
    env: Arc<VerifiedDecodeEnv>,
    telemetry: Arc<TelemetryRegistry>,
    gpu_tx: Option<Sender<DecodeJob>>,
    cpu_tx: Sender<DecodeJob>,
    result_rx: Receiver<Result<DecodeResult>>,
    handles: Vec<JoinHandle<()>>,
}

impl DecodePipeline {
    pub fn spawn(
        env: Arc<VerifiedDecodeEnv>,
        db_tx: Sender<DbWriteOp>,
        telemetry: Arc<TelemetryRegistry>,
        verbose: bool,
    ) -> Result<Self> {
        let (stage_tx, stage_rx): (SyncSender<HashWorkerMsg>, Receiver<HashWorkerMsg>) =
            mpsc::sync_channel(FRAME_CHANNEL_CAPACITY);
        let (result_tx, result_rx): (Sender<Result<DecodeResult>>, Receiver<Result<DecodeResult>>) =
            mpsc::channel();
        let (cpu_tx, cpu_rx): (Sender<DecodeJob>, Receiver<DecodeJob>) = mpsc::channel();

        let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(3);

        // Thread A: CPU `W x H -> 9x8` Area-Downscale & 64-bit dHash Worker
        {
            let res_tx = result_tx.clone();
            let db_sender = db_tx.clone();
            let tel = Arc::clone(&telemetry);
            let h = thread::Builder::new()
                .name("cpu-dhash-worker".to_string())
                .spawn(move || loop {
                    let wait_t0 = Instant::now();
                    let msg = match stage_rx.recv() {
                        Ok(m) => m,
                        Err(_) => break,
                    };
                    tel.record_thread_starved(ThreadRole::CpuDhashWorker, wait_t0.elapsed());

                    match msg {
                        HashWorkerMsg::FatalError(err_msg) => {
                            tel.record_thread_error(ThreadRole::CpuDhashWorker);
                            let _ = res_tx.send(Err(anyhow::anyhow!("{}", err_msg)));
                        }
                        HashWorkerMsg::Frames(batch) => {
                            let luma_len = batch.luma_bytes.len();
                            tel.stage_channel_pop(luma_len);

                            let hash_start = Instant::now();
                            let hashes = downscale_luma_to_9x8_and_dhash(
                                &batch.luma_bytes,
                                batch.width,
                                batch.height,
                            );
                            let hash_elapsed = hash_start.elapsed();
                            let exact_int = is_exact_integer_downscale(batch.width, batch.height);
                            tel.record_thread_busy(ThreadRole::CpuDhashWorker, hash_elapsed);
                            tel.record_thread_job(ThreadRole::CpuDhashWorker, hash_elapsed);
                            tel.record_dhash_batch(hashes.len(), exact_int, hash_elapsed);

                            let file_key = batch.job.file_key.clone();
                            let kind = batch.job.kind;

                            let db_op = match kind {
                                DecodeKind::Sparse5 { .. } => DbWriteOp::UpdateSparseHashes {
                                    file_key: file_key.clone(),
                                    hashes: hashes.clone(),
                                },
                                DecodeKind::Prefix1Fps { .. } => DbWriteOp::UpdatePrefixHashes {
                                    file_key: file_key.clone(),
                                    hashes: hashes.clone(),
                                },
                                DecodeKind::Dense1Fps => DbWriteOp::UpdateDenseHashes {
                                    file_key: file_key.clone(),
                                    hashes: hashes.clone(),
                                },
                            };
                            let _ = db_sender.send(db_op);

                            let _ = res_tx.send(Ok(DecodeResult {
                                record_idx: batch.job.record_idx,
                                file_key,
                                hashes,
                                decoder_label: batch.decoder_label,
                                decode_elapsed: batch.decode_elapsed,
                                hash_elapsed,
                            }));
                        }
                    }
                })
                .context("Failed to spawn CPU 9x8 dHash worker thread")?;
            handles.push(h);
        }

        // Thread B: CPU-Codec Decode Worker
        {
            let env_ref = Arc::clone(&env);
            let st_tx = stage_tx.clone();
            let tel = Arc::clone(&telemetry);
            let h = thread::Builder::new()
                .name("cpu-decode-worker".to_string())
                .spawn(move || loop {
                    let wait_t0 = Instant::now();
                    let job = match cpu_rx.recv() {
                        Ok(j) => j,
                        Err(_) => break,
                    };
                    tel.record_thread_starved(ThreadRole::CpuDecodeWorker, wait_t0.elapsed());

                    let decode_start = Instant::now();
                    let kind_lbl = job.kind_label();
                    let vid_secs = job.decoded_video_secs();
                    match decode_raw_luma_cpu(&job.path, &job.video_codec, job.kind, &env_ref) {
                        Ok(batch) => {
                            let decode_elapsed = decode_start.elapsed();
                            let frame_bytes = (batch.width * batch.height).max(1);
                            let frames = batch.luma_bytes.len() / frame_bytes;
                            let luma_len = batch.luma_bytes.len();
                            tel.record_thread_busy(ThreadRole::CpuDecodeWorker, decode_elapsed);
                            tel.record_thread_job(ThreadRole::CpuDecodeWorker, decode_elapsed);
                            tel.record_decode(
                                "cpu",
                                &job.video_codec,
                                kind_lbl,
                                frames,
                                vid_secs,
                                decode_elapsed,
                                false,
                            );

                            let send_t0 = Instant::now();
                            tel.stage_channel_push(luma_len);
                            let _ = st_tx.send(HashWorkerMsg::Frames(StagedLumaBatch {
                                job,
                                width: batch.width,
                                height: batch.height,
                                luma_bytes: batch.luma_bytes,
                                decoder_label: batch.decoder_label,
                                decode_elapsed,
                            }));
                            tel.record_thread_backpressured(
                                ThreadRole::CpuDecodeWorker,
                                send_t0.elapsed(),
                            );
                        }
                        Err(e) => {
                            let decode_elapsed = decode_start.elapsed();
                            tel.record_thread_busy(ThreadRole::CpuDecodeWorker, decode_elapsed);
                            tel.record_thread_error(ThreadRole::CpuDecodeWorker);
                            eprintln!("Warning: CPU decode failed for {:?}: {}", job.path, e);
                            let send_t0 = Instant::now();
                            tel.stage_channel_push(0);
                            let _ = st_tx.send(HashWorkerMsg::Frames(StagedLumaBatch {
                                job,
                                width: env_ref.scale_width as usize,
                                height: env_ref.scale_height as usize,
                                luma_bytes: Vec::new(),
                                decoder_label: "cpu(error)".to_string(),
                                decode_elapsed,
                            }));
                            tel.record_thread_backpressured(
                                ThreadRole::CpuDecodeWorker,
                                send_t0.elapsed(),
                            );
                        }
                    }
                })
                .context("Failed to spawn CPU-codec decode worker thread")?;
            handles.push(h);
        }

        // Thread C: GPU HW-Decode & HW-Downscale Worker (spawned when active_hw is verified)
        let gpu_tx = if env.active_hw.is_some() {
            let (tx, rx): (Sender<DecodeJob>, Receiver<DecodeJob>) = mpsc::channel();
            let env_ref = Arc::clone(&env);
            let st_tx = stage_tx;
            let cpu_forward_tx = cpu_tx.clone();
            let tel = Arc::clone(&telemetry);
            let h = thread::Builder::new()
                .name("gpu-decode-worker".to_string())
                .spawn(move || loop {
                    let wait_t0 = Instant::now();
                    let job = match rx.recv() {
                        Ok(j) => j,
                        Err(_) => break,
                    };
                    tel.record_thread_starved(ThreadRole::GpuDecodeWorker, wait_t0.elapsed());

                    let decode_start = Instant::now();
                    let kind_lbl = job.kind_label();
                    let vid_secs = job.decoded_video_secs();
                    match decode_raw_luma_gpu(&job.path, &job.video_codec, job.kind, &env_ref) {
                        RawDecodeOutcome::Ok(batch) => {
                            let decode_elapsed = decode_start.elapsed();
                            let frame_bytes = (batch.width * batch.height).max(1);
                            let frames = batch.luma_bytes.len() / frame_bytes;
                            let luma_len = batch.luma_bytes.len();
                            tel.record_thread_busy(ThreadRole::GpuDecodeWorker, decode_elapsed);
                            tel.record_thread_job(ThreadRole::GpuDecodeWorker, decode_elapsed);
                            tel.record_decode(
                                "gpu",
                                &job.video_codec,
                                kind_lbl,
                                frames,
                                vid_secs,
                                decode_elapsed,
                                batch.used_hw_scale_fallback,
                            );

                            let send_t0 = Instant::now();
                            tel.stage_channel_push(luma_len);
                            let _ = st_tx.send(HashWorkerMsg::Frames(StagedLumaBatch {
                                job,
                                width: batch.width,
                                height: batch.height,
                                luma_bytes: batch.luma_bytes,
                                decoder_label: batch.decoder_label,
                                decode_elapsed,
                            }));
                            tel.record_thread_backpressured(
                                ThreadRole::GpuDecodeWorker,
                                send_t0.elapsed(),
                            );
                        }
                        RawDecodeOutcome::CodecRejected(reason) => {
                            let decode_elapsed = decode_start.elapsed();
                            tel.record_thread_busy(ThreadRole::GpuDecodeWorker, decode_elapsed);
                            tel.record_gpu_to_cpu_route(&job.video_codec, "codec_rejected");
                            if verbose {
                                eprintln!(
                                    "  [Verbose][GPU->CPU Codec Route] {:?} (codec={}) routed to CPU-codec worker: {}",
                                    job.file_stem,
                                    job.video_codec,
                                    reason.lines().next().unwrap_or("codec unsupported by GPU")
                                );
                            }
                            let _ = cpu_forward_tx.send(job);
                        }
                        RawDecodeOutcome::FatalDriverError(err_msg) => {
                            tel.record_thread_busy(
                                ThreadRole::GpuDecodeWorker,
                                decode_start.elapsed(),
                            );
                            tel.record_thread_error(ThreadRole::GpuDecodeWorker);
                            let _ = st_tx.send(HashWorkerMsg::FatalError(err_msg));
                        }
                    }
                })
                .context("Failed to spawn GPU HW-decode worker thread")?;
            handles.push(h);
            Some(tx)
        } else {
            drop(stage_tx);
            None
        };

        Ok(Self {
            env,
            telemetry,
            gpu_tx,
            cpu_tx,
            result_rx,
            handles,
        })
    }

    pub fn submit(&self, job: DecodeJob) -> Result<()> {
        if let (Some(hw), Some(gpu_tx)) = (&self.env.active_hw, &self.gpu_tx) {
            if self.env.is_codec_gpu_supported(hw.backend, &job.video_codec) {
                gpu_tx
                    .send(job)
                    .map_err(|_| anyhow::anyhow!("GPU decode worker channel closed"))?;
                return Ok(());
            } else {
                self.telemetry
                    .record_gpu_to_cpu_route(&job.video_codec, "legacy_codec");
            }
        }
        self.cpu_tx
            .send(job)
            .map_err(|_| anyhow::anyhow!("CPU decode worker channel closed"))?;
        Ok(())
    }

    pub fn recv(&self) -> Result<DecodeResult> {
        self.result_rx
            .recv()
            .map_err(|_| anyhow::anyhow!("Decode pipeline result channel closed unexpectedly"))?
    }

    pub fn shutdown(self) {
        drop(self.gpu_tx);
        drop(self.cpu_tx);
        drop(self.result_rx);
        for h in self.handles {
            let _ = h.join();
        }
    }
}
