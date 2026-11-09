//! Machine details and per-process CPU/memory sampling (design D8).

use std::collections::HashMap;
use std::process::Command;
use std::time::Instant;

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Machine {
    pub cpu: String,
    pub cores: usize,
    pub memory_gb: f64,
    pub os: String,
}

fn cmd(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    out.status.success().then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

pub fn machine() -> Machine {
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0);
    let os = std::env::consts::OS.to_string();
    if os == "macos" {
        let cpu = cmd("sysctl", &["-n", "machdep.cpu.brand_string"]).unwrap_or_else(|| "unknown".into());
        let mem = cmd("sysctl", &["-n", "hw.memsize"]).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        return Machine { cpu, cores, memory_gb: mem / 1024f64.powi(3), os };
    }
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("model name")).and_then(|l| l.split_once(':')).map(|(_, v)| v.trim().to_string()))
        .unwrap_or_else(|| "unknown".into());
    let mem_kb = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("MemTotal")).and_then(|l| l.split_whitespace().nth(1)?.parse::<f64>().ok()))
        .unwrap_or(0.0);
    Machine { cpu, cores, memory_gb: mem_kb / 1024f64.powi(2), os }
}

/// Parses `ps -o time=` output: `[[dd-]hh:]mm:ss[.ff]` into seconds.
pub fn parse_cpu_time(s: &str) -> Option<f64> {
    let (days, rest) = match s.trim().split_once('-') {
        Some((d, r)) => (d.parse::<f64>().ok()?, r),
        None => (0.0, s.trim()),
    };
    let mut secs = 0.0;
    for part in rest.split(':') {
        secs = secs * 60.0 + part.parse::<f64>().ok()?;
    }
    Some(days * 86_400.0 + secs)
}

/// Parses `ps -o pid=,time=,rss=` lines into `pid -> (cpu seconds, rss bytes)`.
pub fn parse_ps(out: &str) -> HashMap<u32, (f64, u64)> {
    out.lines()
        .filter_map(|line| {
            let mut f = line.split_whitespace();
            let pid = f.next()?.parse().ok()?;
            let cpu = parse_cpu_time(f.next()?)?;
            let rss_kb: u64 = f.next()?.parse().ok()?;
            Some((pid, (cpu, rss_kb * 1024)))
        })
        .collect()
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
pub struct ProcUsage {
    /// 100 = one core fully busy.
    pub cpu_percent: f64,
    pub rss_bytes: u64,
}

/// Turns cumulative CPU time into per-interval CPU% (the `%cpu` column on
/// macOS is a decaying average, so it isn't used).
#[derive(Default)]
pub struct CpuSampler {
    last: HashMap<u32, (Instant, f64)>,
}

impl CpuSampler {
    pub fn sample(&mut self, pids: &[u32]) -> HashMap<u32, ProcUsage> {
        let list = pids.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(",");
        let Some(out) = cmd("ps", &["-o", "pid=,time=,rss=", "-p", &list]) else { return HashMap::new() };
        let now = Instant::now();
        let mut result = HashMap::new();
        for (pid, (cpu, rss)) in parse_ps(&out) {
            let cpu_percent = match self.last.insert(pid, (now, cpu)) {
                Some((t, prev)) => {
                    let wall = now.duration_since(t).as_secs_f64();
                    if wall > 0.0 { ((cpu - prev) / wall * 100.0).max(0.0) } else { 0.0 }
                }
                None => 0.0,
            };
            result.insert(pid, ProcUsage { cpu_percent, rss_bytes: rss });
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_time_formats() {
        assert_eq!(parse_cpu_time("0:01.50"), Some(1.5)); // macOS mm:ss.ff
        assert_eq!(parse_cpu_time("12:03.25"), Some(723.25));
        assert_eq!(parse_cpu_time("01:02:03"), Some(3723.0)); // Linux hh:mm:ss
        assert_eq!(parse_cpu_time("1-00:00:10"), Some(86_410.0));
        assert_eq!(parse_cpu_time("x"), None);
    }

    #[test]
    fn ps_lines() {
        let m = parse_ps("  101   0:02.00  2048\n202 1:00.00 10\ngarbage\n");
        assert_eq!(m[&101], (2.0, 2048 * 1024));
        assert_eq!(m[&202], (60.0, 10 * 1024));
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn samples_this_process() {
        let pid = std::process::id();
        let mut s = CpuSampler::default();
        let first = s.sample(&[pid]);
        assert!(first[&pid].rss_bytes > 0);
        // Burn some CPU so the second sample shows usage.
        let t = Instant::now();
        let mut x = 0u64;
        while t.elapsed().as_millis() < 300 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
        }
        std::hint::black_box(x);
        let second = s.sample(&[pid]);
        assert!(second[&pid].cpu_percent > 10.0, "{:?}", second[&pid]);
    }

    #[test]
    fn machine_details() {
        let m = machine();
        assert!(m.cores > 0);
        if m.os == "macos" {
            assert!(m.memory_gb > 1.0);
            assert_ne!(m.cpu, "unknown");
        }
    }
}
