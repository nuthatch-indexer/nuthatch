//! `nuthatch bench backfill` — honest, reproducible backfill throughput measurement (RFC-0004).
//!
//! Runs the real fetch → decode → store path over a *pinned* block range and reports sustained
//! events/sec, wall-clock, peak RSS, and RPC requests, emitting a `bench-report.json`. Measure
//! first, optimise second: nothing here optimises anything — it establishes the baseline that the
//! seal-direct / adaptive-chunker / pipeline work (later slices) must each beat on a before/after.
//!
//! The house rule (RFC-0004): every published number traces to a report artifact produced here, with
//! date, provider, hardware, and commit. No hand-typed numbers on the website.

use anyhow::{bail, Context, Result};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::chains;
use crate::cli::BackfillBenchArgs;
use crate::config::Config;
use crate::registry::DecodeRegistry;
use crate::rpc::RpcClient;
use crate::source::Source;
use crate::store::Store;

/// One run's raw measurements.
struct Run {
    events: u64,
    wall_clock_s: f64,
    events_per_sec: f64,
    peak_rss_mb: u64,
    rpc_requests: u64,
}

/// The published artifact: medians across runs plus the pinned inputs and provenance.
#[derive(Debug, Serialize)]
pub struct BenchReport {
    pub bench: &'static str,
    pub label: Option<String>,
    pub nest: String,
    pub chain: String,
    pub from_block: u64,
    pub to_block: u64,
    pub blocks: u64,
    pub window: u64,
    pub runs: usize,
    /// Medians across runs.
    pub events: u64,
    pub wall_clock_s: f64,
    pub events_per_sec: f64,
    pub peak_rss_mb: u64,
    pub rpc_requests: u64,
    pub commit: Option<String>,
}

pub async fn backfill(args: BackfillBenchArgs) -> Result<()> {
    if args.to < args.from {
        bail!("--to ({}) is before --from ({})", args.to, args.from);
    }
    let dir = PathBuf::from(&args.dir);
    let config = Config::load(&dir)?;
    let registry = Arc::new(DecodeRegistry::from_nest(&dir, &config)?);

    let rpc_urls = match &args.rpc {
        Some(u) => vec![u.clone()],
        None => config.nest.rpc_urls.clone(),
    };
    let window = chains::lookup(&config.nest.chain)
        .map(|c| c.log_window)
        .unwrap_or(20);
    let addresses: Vec<String> = registry
        .addresses()
        .iter()
        .map(|a| format!("0x{}", hex::encode(a)))
        .collect();
    let topic0s: Vec<String> = registry
        .topic0s()
        .iter()
        .map(|t| format!("0x{}", hex::encode(t)))
        .collect();

    println!(
        "bench backfill: nest '{}' on {}, blocks {}..={} ({} blocks), window {}, {} run(s)",
        config.nest.name,
        config.nest.chain,
        args.from,
        args.to,
        args.to - args.from + 1,
        window,
        args.runs,
    );

    let mut runs = Vec::with_capacity(args.runs);
    for run in 1..=args.runs {
        let r = one_run(
            &rpc_urls, &registry, &addresses, &topic0s, args.from, args.to, window, run,
        )
        .await?;
        println!(
            "  run {run}/{}: {} events in {:.1}s = {:.0} ev/s, peak {} MB, {} rpc req",
            args.runs, r.events, r.wall_clock_s, r.events_per_sec, r.peak_rss_mb, r.rpc_requests
        );
        runs.push(r);
    }

    let report = BenchReport {
        bench: "backfill",
        label: args.label.clone(),
        nest: config.nest.name.clone(),
        chain: config.nest.chain.clone(),
        from_block: args.from,
        to_block: args.to,
        blocks: args.to - args.from + 1,
        window,
        runs: args.runs,
        events: median_u64(runs.iter().map(|r| r.events)),
        wall_clock_s: round2(median_f64(runs.iter().map(|r| r.wall_clock_s))),
        events_per_sec: round2(median_f64(runs.iter().map(|r| r.events_per_sec))),
        peak_rss_mb: median_u64(runs.iter().map(|r| r.peak_rss_mb)),
        rpc_requests: median_u64(runs.iter().map(|r| r.rpc_requests)),
        commit: git_commit(),
    };

    let json = serde_json::to_string_pretty(&report)?;
    println!("\n{json}");
    if let Some(out) = &args.out {
        std::fs::write(out, &json).with_context(|| format!("failed to write {out}"))?;
        println!("\nwrote {out}");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn one_run(
    rpc_urls: &[String],
    registry: &DecodeRegistry,
    addresses: &[String],
    topic0s: &[String],
    from: u64,
    to: u64,
    window: u64,
    run: usize,
) -> Result<Run> {
    let source = RpcClient::new(rpc_urls.to_vec())?;
    // A throwaway store per run: we measure the real decode + redb-write path, but never touch the
    // nest's own database.
    let store_path =
        std::env::temp_dir().join(format!("nuthatch-bench-{}-{run}", std::process::id()));
    let _ = std::fs::remove_dir_all(&store_path);
    std::fs::create_dir_all(&store_path)?;
    let store = Store::open(&store_path.join("bench.redb"))?;

    let rss = RssSampler::start();
    let start = Instant::now();
    let mut events = 0u64;
    let mut next = from;
    while next <= to {
        let chunk_to = (next + window - 1).min(to);
        let logs = source
            .logs(addresses, topic0s, next, chunk_to)
            .await
            .with_context(|| format!("getLogs {next}..={chunk_to}"))?;
        for log in &logs {
            if let Ok(Some(row)) = registry.decode(log) {
                let key = Store::entity_key(row.block_number, row.log_index);
                store.put_entity(&key, &row.to_json().to_string())?;
                events += 1;
            }
        }
        next = chunk_to + 1;
    }
    let wall_clock_s = start.elapsed().as_secs_f64();
    let peak_rss_mb = rss.stop();
    let _ = std::fs::remove_dir_all(&store_path);

    Ok(Run {
        events,
        wall_clock_s,
        events_per_sec: if wall_clock_s > 0.0 {
            events as f64 / wall_clock_s
        } else {
            0.0
        },
        peak_rss_mb,
        rpc_requests: source.request_count(),
    })
}

/// Samples this process's resident set size on a background thread, tracking the peak.
struct RssSampler {
    stop: Arc<AtomicBool>,
    peak_kb: Arc<AtomicU64>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl RssSampler {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak_kb = Arc::new(AtomicU64::new(0));
        let (s, p) = (stop.clone(), peak_kb.clone());
        let handle = std::thread::spawn(move || {
            while !s.load(Ordering::Relaxed) {
                if let Some(kb) = current_rss_kb() {
                    p.fetch_max(kb, Ordering::Relaxed);
                }
                std::thread::sleep(Duration::from_millis(120));
            }
        });
        Self {
            stop,
            peak_kb,
            handle: Some(handle),
        }
    }

    /// Stop sampling and return the peak RSS in MB.
    fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        self.peak_kb.load(Ordering::Relaxed) / 1024
    }
}

/// This process's RSS in KB. Linux via `/proc/self/status`; else `ps` (macOS/BSD). None if neither.
fn current_rss_kb() -> Option<u64> {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                return rest.trim().trim_end_matches("kB").trim().parse().ok();
            }
        }
    }
    let pid = std::process::id().to_string();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

fn median_f64(vals: impl Iterator<Item = f64>) -> f64 {
    let mut v: Vec<f64> = vals.collect();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    median_of_sorted(&v).unwrap_or(0.0)
}

fn median_u64(vals: impl Iterator<Item = u64>) -> u64 {
    let mut v: Vec<f64> = vals.map(|x| x as f64).collect();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    median_of_sorted(&v).unwrap_or(0.0).round() as u64
}

fn median_of_sorted(v: &[f64]) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    let mid = v.len() / 2;
    Some(if v.len() % 2 == 0 {
        (v[mid - 1] + v[mid]) / 2.0
    } else {
        v[mid]
    })
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// The short commit the bench ran at (provenance for the report), or None outside a git checkout.
fn git_commit() -> Option<String> {
    let out = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!s.is_empty()).then_some(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn medians() {
        assert_eq!(median_u64([10u64, 30, 20].into_iter()), 20);
        assert_eq!(median_u64([10u64, 20, 30, 40].into_iter()), 25); // even → mean of middle two
        assert_eq!(median_u64(std::iter::empty()), 0);
        assert!((median_f64([1.0, 3.0, 2.0].into_iter()) - 2.0).abs() < 1e-9);
    }

    #[test]
    fn round_two_dp() {
        assert_eq!(round2(1234.5678), 1234.57);
    }
}
