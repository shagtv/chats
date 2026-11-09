//! `stress-report.md` and `stress-report.json` (design D11).

use std::fmt::Write as _;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::sys::Machine;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RunConfig {
    pub server_binary: String,
    pub gen_threads: usize,
    pub step_secs: u64,
    pub warmup_secs: u64,
    pub seed: u64,
    pub sources: Vec<String>,
    pub port_capacity: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StepResult {
    pub load: u64,
    pub online: f64,
    pub posts_per_s: f64,
    pub frames_per_s: f64,
    pub reads_per_s: f64,
    pub delivery_p50_ms: Option<f64>,
    pub delivery_p99_ms: Option<f64>,
    pub http_p50_ms: Option<f64>,
    pub http_p99_ms: Option<f64>,
    pub error_rate: f64,
    pub resync_rate: f64,
    pub server_cpu: f64,
    pub gen_cpu: f64,
    pub shard_queue_max: u64,
    pub meta_queue_max: u64,
    pub passed: bool,
    pub reasons: Vec<String>,
    pub early: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PhaseResult {
    pub id: String,
    pub title: String,
    pub unit: String,
    pub seed_s: f64,
    pub steps: Vec<StepResult>,
    pub max: Option<u64>,
    pub limited_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MaxRow {
    pub measure: String,
    pub phase: String,
    pub value: String,
    pub limited_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Validity {
    pub gen_cpu_peak_share: f64,
    pub ports_peak: usize,
    pub port_capacity: usize,
    pub generator_bound: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Report {
    pub started_at: String,
    pub duration_s: u64,
    pub interrupted: bool,
    pub machine: MachineInfo,
    pub config: RunConfig,
    pub maximums: Vec<MaxRow>,
    pub validity: Validity,
    pub phases: Vec<PhaseResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MachineInfo {
    pub cpu: String,
    pub cores: usize,
    pub memory_gb: f64,
    pub os: String,
}

impl From<Machine> for MachineInfo {
    fn from(m: Machine) -> Self {
        MachineInfo { cpu: m.cpu, cores: m.cores, memory_gb: m.memory_gb, os: m.os }
    }
}

fn opt_ms(v: Option<f64>) -> String {
    v.map_or("-".into(), |v| format!("{v:.0}"))
}

fn pct(v: f64) -> String {
    format!("{:.2}%", v * 100.0)
}

pub fn render_md(r: &Report) -> String {
    let mut s = String::new();
    let m = &r.machine;
    let _ = writeln!(s, "# Stress report {}", r.started_at);
    let _ = writeln!(s);
    let _ = writeln!(s, "Machine: {}, {} cores, {:.0} GB RAM ({}).", m.cpu, m.cores, m.memory_gb, m.os);
    let _ = writeln!(
        s,
        "Run: {} s, generator {} threads, {} s windows after {} s warm-up, seed {}, {} source address(es), {} WebSocket ports.",
        r.duration_s, r.config.gen_threads, r.config.step_secs, r.config.warmup_secs, r.config.seed, r.config.sources.len(), r.config.port_capacity
    );
    if r.interrupted {
        let _ = writeln!(s, "\n**Interrupted**: results cover only the steps completed before Ctrl-C.");
    }
    let _ = writeln!(s, "\nA step passes when delivery p99 <= 1 s, HTTP p99 <= 500 ms, errors <= 1% and resyncs <= 1%.");

    let _ = writeln!(s, "\n## Maximums\n");
    let _ = writeln!(s, "| Measure | Phase | Value | Limited by |");
    let _ = writeln!(s, "|---|---|---|---|");
    for row in &r.maximums {
        let _ = writeln!(s, "| {} | {} | {} | {} |", row.measure, row.phase, row.value, row.limited_by);
    }

    let v = &r.validity;
    let _ = writeln!(s, "\n## Run validity\n");
    let _ = writeln!(s, "- Generator CPU peak: {:.0}% of its {}-thread budget (results count as generator-bound above 70%).", v.gen_cpu_peak_share * 100.0, r.config.gen_threads);
    let _ = writeln!(s, "- WebSocket ports peak: {} of {}.", v.ports_peak, v.port_capacity);
    if v.generator_bound.is_empty() {
        let _ = writeln!(s, "- No maximum was limited by the load generator.");
    } else {
        let _ = writeln!(s, "- Limited by the load generator: {}.", v.generator_bound.join(", "));
    }
    for note in &v.notes {
        let _ = writeln!(s, "- {note}");
    }

    for p in &r.phases {
        let _ = writeln!(s, "\n## Phase {}: {}\n", p.id, p.title);
        let max = p.max.map_or("none passed".into(), |m| format!("{m} {}", p.unit));
        let _ = writeln!(s, "Maximum: **{max}**, limited by {}. Seeding took {:.0} s.\n", p.limited_by, p.seed_s);
        let _ = writeln!(s, "| Load ({}) | Online | Posts/s | Frames/s | Reads/s | Delivery p50/p99 ms | HTTP p50/p99 ms | Errors | Resyncs | Server CPU | Gen CPU | Shard q | Meta q | Result |", p.unit);
        let _ = writeln!(s, "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
        for st in &p.steps {
            let result = if st.passed {
                "pass".to_string()
            } else {
                format!("FAIL{}: {}", if st.early { " (early)" } else { "" }, st.reasons.join("; "))
            };
            let _ = writeln!(
                s,
                "| {} | {:.0} | {:.0} | {:.0} | {:.0} | {} / {} | {} / {} | {} | {} | {:.0}% | {:.0}% | {} | {} | {} |",
                st.load, st.online, st.posts_per_s, st.frames_per_s, st.reads_per_s,
                opt_ms(st.delivery_p50_ms), opt_ms(st.delivery_p99_ms), opt_ms(st.http_p50_ms), opt_ms(st.http_p99_ms),
                pct(st.error_rate), pct(st.resync_rate), st.server_cpu, st.gen_cpu, st.shard_queue_max, st.meta_queue_max, result
            );
        }
    }
    s
}

pub fn write(dir: &Path, r: &Report) -> std::io::Result<()> {
    std::fs::write(dir.join("stress-report.json"), serde_json::to_string_pretty(r).expect("report serializes"))?;
    std::fs::write(dir.join("stress-report.md"), render_md(r))
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn fixture() -> Report {
        let step = |load, passed: bool, reasons: Vec<&str>| StepResult {
            load,
            online: load as f64,
            posts_per_s: 12.0,
            frames_per_s: 3400.0,
            reads_per_s: 55.0,
            delivery_p50_ms: Some(8.0),
            delivery_p99_ms: Some(if passed { 90.0 } else { 1400.0 }),
            http_p50_ms: Some(2.0),
            http_p99_ms: Some(30.0),
            error_rate: 0.0,
            resync_rate: 0.0,
            server_cpu: 250.0,
            gen_cpu: 120.0,
            shard_queue_max: 3,
            meta_queue_max: 1,
            passed,
            reasons: reasons.into_iter().map(String::from).collect(),
            early: false,
        };
        Report {
            started_at: "2026-11-09 10:00".into(),
            duration_s: 1500,
            interrupted: false,
            machine: MachineInfo { cpu: "Test CPU".into(), cores: 8, memory_gb: 16.0, os: "macos".into() },
            config: RunConfig {
                server_binary: "target/release/chats-openspec".into(),
                gen_threads: 6,
                step_secs: 45,
                warmup_secs: 10,
                seed: 42,
                sources: vec!["127.0.0.1".into()],
                port_capacity: 13721,
            },
            maximums: vec![MaxRow {
                measure: "Most online users (realistic mix)".into(),
                phase: "A".into(),
                value: "8000 users".into(),
                limited_by: "delivery latency target; likely bottleneck: server CPU".into(),
            }],
            validity: Validity { gen_cpu_peak_share: 0.2, ports_peak: 16000, port_capacity: 13721, generator_bound: vec![], notes: vec![] },
            phases: vec![PhaseResult {
                id: "A".into(),
                title: "realistic ramp".into(),
                unit: "users".into(),
                seed_s: 31.0,
                steps: vec![step(8000, true, vec![]), step(16000, false, vec!["delivery p99 1400 ms > 1000 ms"])],
                max: Some(8000),
                limited_by: "delivery latency target; likely bottleneck: server CPU".into(),
            }],
        }
    }

    const GOLDEN: &str = "# Stress report 2026-11-09 10:00

Machine: Test CPU, 8 cores, 16 GB RAM (macos).
Run: 1500 s, generator 6 threads, 45 s windows after 10 s warm-up, seed 42, 1 source address(es), 13721 WebSocket ports.

A step passes when delivery p99 <= 1 s, HTTP p99 <= 500 ms, errors <= 1% and resyncs <= 1%.

## Maximums

| Measure | Phase | Value | Limited by |
|---|---|---|---|
| Most online users (realistic mix) | A | 8000 users | delivery latency target; likely bottleneck: server CPU |

## Run validity

- Generator CPU peak: 20% of its 6-thread budget (results count as generator-bound above 70%).
- WebSocket ports peak: 16000 of 13721.
- No maximum was limited by the load generator.

## Phase A: realistic ramp

Maximum: **8000 users**, limited by delivery latency target; likely bottleneck: server CPU. Seeding took 31 s.

| Load (users) | Online | Posts/s | Frames/s | Reads/s | Delivery p50/p99 ms | HTTP p50/p99 ms | Errors | Resyncs | Server CPU | Gen CPU | Shard q | Meta q | Result |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 8000 | 8000 | 12 | 3400 | 55 | 8 / 90 | 2 / 30 | 0.00% | 0.00% | 250% | 120% | 3 | 1 | pass |
| 16000 | 16000 | 12 | 3400 | 55 | 8 / 1400 | 2 / 30 | 0.00% | 0.00% | 250% | 120% | 3 | 1 | FAIL: delivery p99 1400 ms > 1000 ms |
";

    #[test]
    fn markdown_golden() {
        assert_eq!(render_md(&fixture()), GOLDEN);
    }

    #[test]
    fn json_round_trip_and_files() {
        let r = Report { interrupted: true, ..fixture() };
        let back: Report = serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(back, r);
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), &r).unwrap();
        let md = std::fs::read_to_string(dir.path().join("stress-report.md")).unwrap();
        assert!(md.contains("**Interrupted**"));
        let json: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(dir.path().join("stress-report.json")).unwrap()).unwrap();
        assert_eq!(json["interrupted"], true);
    }
}
