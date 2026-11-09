//! Step evaluation, the adaptive ramp, and limiting-factor rules
//! (specs/stress-testing; design D7, D10).

use serde::Serialize;

pub const DELIVERY_P99_MS: f64 = 1000.0;
pub const HTTP_P99_MS: f64 = 500.0;
pub const MAX_ERROR_RATE: f64 = 0.01;
pub const MAX_RESYNC_RATE: f64 = 0.01;
pub const EARLY_FAIL_ERROR_RATE: f64 = 0.10;
pub const MAX_BISECTIONS: u32 = 4;
pub const CONVERGED_WITHIN: f64 = 0.05;

/// What a step measured, as far as pass/fail is concerned.
#[derive(Debug, Clone, Copy, Default)]
pub struct Measured {
    pub delivery_p99_ms: Option<f64>,
    pub http_p99_ms: Option<f64>,
    pub error_rate: f64,
    pub resync_rate: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Target {
    Delivery,
    Http,
    Errors,
    Resyncs,
}

impl Target {
    pub fn describe(self, m: &Measured) -> String {
        match self {
            Target::Delivery => format!("delivery p99 {:.0} ms > {DELIVERY_P99_MS:.0} ms", m.delivery_p99_ms.unwrap_or(0.0)),
            Target::Http => format!("HTTP p99 {:.0} ms > {HTTP_P99_MS:.0} ms", m.http_p99_ms.unwrap_or(0.0)),
            Target::Errors => format!("errors {:.2}% > {:.0}%", m.error_rate * 100.0, MAX_ERROR_RATE * 100.0),
            Target::Resyncs => format!("resyncs {:.2}% > {:.0}%", m.resync_rate * 100.0, MAX_RESYNC_RATE * 100.0),
        }
    }
}

/// The targets a step missed; empty means it passed. Missing samples (no
/// messages in an idle phase) don't fail a target.
pub fn evaluate(m: &Measured) -> Vec<Target> {
    let mut failed = Vec::new();
    if m.delivery_p99_ms.is_some_and(|p| p > DELIVERY_P99_MS) {
        failed.push(Target::Delivery);
    }
    if m.http_p99_ms.is_some_and(|p| p > HTTP_P99_MS) {
        failed.push(Target::Http);
    }
    if m.error_rate > MAX_ERROR_RATE {
        failed.push(Target::Errors);
    }
    if m.resync_rate > MAX_RESYNC_RATE {
        failed.push(Target::Resyncs);
    }
    failed
}

/// More than 10% failures in the first 10 s ends the step at once.
pub fn early_fail(error_rate_so_far: f64, attempts_so_far: u64) -> bool {
    attempts_so_far >= 50 && error_rate_so_far > EARLY_FAIL_ERROR_RATE
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Stop {
    /// A step failed and the search converged.
    Failed,
    /// The product cap was reached while passing.
    Cap,
    /// The next load wouldn't fit in the available ephemeral ports.
    Ports,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Next {
    Step(u64),
    Done(Stop),
}

/// Doubles until a failure, then bisects (design D7).
#[derive(Debug, Clone)]
pub struct Ramp {
    start: u64,
    cap: Option<u64>,
    limit: Option<u64>,
    pub last_pass: Option<u64>,
    pub first_fail: Option<u64>,
    bisections: u32,
}

impl Ramp {
    /// `cap`: product cap; `limit`: most load the ports allow.
    pub fn new(start: u64, cap: Option<u64>, limit: Option<u64>) -> Ramp {
        Ramp { start: start.max(1), cap, limit, last_pass: None, first_fail: None, bisections: 0 }
    }

    fn ceiling(&self) -> u64 {
        self.cap.unwrap_or(u64::MAX).min(self.limit.unwrap_or(u64::MAX))
    }

    pub fn first(&self) -> Next {
        if self.limit == Some(0) {
            return Next::Done(Stop::Ports);
        }
        Next::Step(self.start.min(self.ceiling()))
    }

    pub fn record(&mut self, load: u64, passed: bool) -> Next {
        if passed {
            self.last_pass = Some(self.last_pass.map_or(load, |p| p.max(load)));
        } else {
            self.first_fail = Some(self.first_fail.map_or(load, |f| f.min(load)));
        }
        match (self.last_pass, self.first_fail) {
            (Some(pass), None) => {
                if self.cap.is_some_and(|c| pass >= c) {
                    return Next::Done(Stop::Cap);
                }
                if self.limit.is_some_and(|l| pass >= l) {
                    return Next::Done(Stop::Ports);
                }
                Next::Step((pass * 2).min(self.ceiling()))
            }
            (pass, Some(fail)) => {
                let lo = pass.unwrap_or(0);
                let converged = fail.saturating_sub(lo) as f64 <= CONVERGED_WITHIN * lo.max(1) as f64;
                if self.bisections >= MAX_BISECTIONS || converged || fail <= 1 {
                    return Next::Done(Stop::Failed);
                }
                let mid = (lo + fail) / 2;
                if mid <= lo || mid >= fail {
                    return Next::Done(Stop::Failed);
                }
                self.bisections += 1;
                Next::Step(mid)
            }
            (None, None) => self.first(),
        }
    }
}

/// Evidence about the failing step used to name the bottleneck.
#[derive(Debug, Clone, Default)]
pub struct Evidence {
    pub failed: Vec<Target>,
    /// Generator CPU as a share of its thread budget (1.0 = all threads busy).
    pub gen_cpu_share: f64,
    /// Server CPU as a share of all cores.
    pub server_cpu_share: f64,
    pub shard_queue_max: u64,
    pub shard_queue_max_shard: u64,
    pub shard_queue_growing: bool,
    pub meta_queue_max: u64,
}

pub const GEN_BOUND_SHARE: f64 = 0.70;
pub const SERVER_CPU_SHARE: f64 = 0.85;
pub const QUEUE_HOT: u64 = 500;

/// Names what stopped a phase (design D10); the first matching rule wins.
pub fn limiting_factor(stop: Stop, e: Option<&Evidence>, alias_hint: &str) -> String {
    match stop {
        Stop::Cap => return "product cap (no failure)".into(),
        Stop::Ports => return format!("OS ephemeral ports (add a loopback alias: `{alias_hint}`)"),
        Stop::Failed => {}
    }
    let Some(e) = e else { return "unknown".into() };
    if e.gen_cpu_share > GEN_BOUND_SHARE {
        return "load generator (its CPU was above 70% of its threads)".into();
    }
    let target = e.failed.first().map_or("a target", |t| match t {
        Target::Delivery => "delivery latency",
        Target::Http => "HTTP latency",
        Target::Errors => "error rate",
        Target::Resyncs => "resync rate",
    });
    let hint = if e.shard_queue_max > QUEUE_HOT || e.shard_queue_growing {
        format!("hot shard writer (shard #{})", e.shard_queue_max_shard)
    } else if e.meta_queue_max > QUEUE_HOT {
        "meta writer".into()
    } else if e.failed.contains(&Target::Resyncs) {
        "WebSocket fan-out / slow consumers".into()
    } else if e.server_cpu_share > SERVER_CPU_SHARE {
        "server CPU".into()
    } else {
        "unknown".into()
    };
    format!("{target} target; likely bottleneck: {hint}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok() -> Measured {
        Measured { delivery_p99_ms: Some(100.0), http_p99_ms: Some(50.0), error_rate: 0.0, resync_rate: 0.0 }
    }

    #[test]
    fn each_target_fails_alone() {
        assert!(evaluate(&ok()).is_empty());
        let cases = [
            (Measured { delivery_p99_ms: Some(1400.0), ..ok() }, Target::Delivery),
            (Measured { http_p99_ms: Some(501.0), ..ok() }, Target::Http),
            (Measured { error_rate: 0.02, ..ok() }, Target::Errors),
            (Measured { resync_rate: 0.011, ..ok() }, Target::Resyncs),
        ];
        for (m, t) in &cases {
            assert_eq!(evaluate(m), vec![*t]);
        }
        assert_eq!(Target::Delivery.describe(&cases[0].0), "delivery p99 1400 ms > 1000 ms");
        // No samples (idle phase) is not a failure.
        assert!(evaluate(&Measured::default()).is_empty());
    }

    #[test]
    fn early_failure() {
        assert!(early_fail(0.2, 100));
        assert!(!early_fail(0.05, 100));
        assert!(!early_fail(0.5, 10), "too few attempts to judge");
    }

    fn drive(mut ramp: Ramp, limit: u64) -> (Vec<u64>, Stop, Option<u64>) {
        let mut loads = Vec::new();
        let mut next = ramp.first();
        loop {
            match next {
                Next::Step(load) => {
                    loads.push(load);
                    next = ramp.record(load, load <= limit);
                }
                Next::Done(stop) => return (loads, stop, ramp.last_pass),
            }
        }
    }

    #[test]
    fn converges_on_the_limit() {
        let (loads, stop, max) = drive(Ramp::new(1000, None, None), 27_000);
        assert_eq!(loads, vec![1000, 2000, 4000, 8000, 16000, 32000, 24000, 28000, 26000, 27000]);
        assert_eq!((stop, max), (Stop::Failed, Some(27_000)));
    }

    #[test]
    fn stops_at_the_cap() {
        let (loads, stop, max) = drive(Ramp::new(1000, Some(10_000), None), u64::MAX);
        assert_eq!(loads, vec![1000, 2000, 4000, 8000, 10000]);
        assert_eq!((stop, max), (Stop::Cap, Some(10_000)));
    }

    #[test]
    fn stops_at_the_port_limit() {
        let (loads, stop, max) = drive(Ramp::new(2000, None, Some(13_720)), u64::MAX);
        assert_eq!(loads, vec![2000, 4000, 8000, 13720]);
        assert_eq!((stop, max), (Stop::Ports, Some(13_720)));
        assert_eq!(Ramp::new(10, None, Some(0)).first(), Next::Done(Stop::Ports));
    }

    #[test]
    fn first_step_failing_bisects_down() {
        let (loads, stop, max) = drive(Ramp::new(1000, None, None), 300);
        assert_eq!(loads[0], 1000);
        assert_eq!(stop, Stop::Failed);
        assert!(max.is_some_and(|m| m <= 300) || max.is_none());
        assert!(loads.len() <= 5);
    }

    #[test]
    fn limiting_factor_rules() {
        let hint = "sudo ifconfig lo0 alias 127.0.0.2 up";
        assert_eq!(limiting_factor(Stop::Cap, None, hint), "product cap (no failure)");
        assert!(limiting_factor(Stop::Ports, None, hint).contains(hint));
        let base = Evidence { failed: vec![Target::Delivery], ..Default::default() };
        let gen_bound = Evidence { gen_cpu_share: 0.9, shard_queue_max: 9999, ..base.clone() };
        assert!(limiting_factor(Stop::Failed, Some(&gen_bound), hint).starts_with("load generator"), "generator rule wins");
        let shard = Evidence { shard_queue_max: 800, shard_queue_max_shard: 12, ..base.clone() };
        assert_eq!(limiting_factor(Stop::Failed, Some(&shard), hint), "delivery latency target; likely bottleneck: hot shard writer (shard #12)");
        let meta = Evidence { meta_queue_max: 900, ..base.clone() };
        assert!(limiting_factor(Stop::Failed, Some(&meta), hint).ends_with("meta writer"));
        let fan = Evidence { failed: vec![Target::Resyncs], ..base.clone() };
        assert!(limiting_factor(Stop::Failed, Some(&fan), hint).contains("fan-out"));
        let cpu = Evidence { server_cpu_share: 0.95, ..base.clone() };
        assert!(limiting_factor(Stop::Failed, Some(&cpu), hint).ends_with("server CPU"));
        assert!(limiting_factor(Stop::Failed, Some(&base), hint).ends_with("unknown"));
    }
}
