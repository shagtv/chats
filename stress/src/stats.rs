//! Shared counters and latency histograms (design D4).
//!
//! Recording is lock-sharded; a 1-second ticker drains the histograms and
//! diffs the counters, and the step controller accumulates those seconds into
//! a measurement window.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use hdrhistogram::Histogram;
use serde::Serialize;

pub const SHARDS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum Ep {
    Register,
    Home,
    History,
    Post,
    Join,
    Leave,
    Create,
    Read,
    WsConnect,
}

impl Ep {
    pub const ALL: [Ep; 9] =
        [Ep::Register, Ep::Home, Ep::History, Ep::Post, Ep::Join, Ep::Leave, Ep::Create, Ep::Read, Ep::WsConnect];

    fn idx(self) -> usize {
        self as usize
    }

    pub fn name(self) -> &'static str {
        match self {
            Ep::Register => "register",
            Ep::Home => "home",
            Ep::History => "history",
            Ep::Post => "post",
            Ep::Join => "join",
            Ep::Leave => "leave",
            Ep::Create => "create",
            Ep::Read => "read",
            Ep::WsConnect => "ws_connect",
        }
    }
}

pub fn new_hist() -> Histogram<u64> {
    // 1 µs/ms .. 1 hour, 3 significant figures.
    Histogram::new_with_bounds(1, 3_600_000_000, 3).expect("histogram bounds")
}

/// A histogram split into lock shards; `drain` merges and resets them.
pub struct Hist {
    shards: Vec<Mutex<Histogram<u64>>>,
}

impl Default for Hist {
    fn default() -> Self {
        Hist { shards: (0..SHARDS).map(|_| Mutex::new(new_hist())).collect() }
    }
}

impl Hist {
    pub fn record(&self, key: usize, value: u64) {
        let _ = self.shards[key % SHARDS].lock().unwrap().record(value.max(1));
    }

    pub fn drain(&self) -> Histogram<u64> {
        let mut out = new_hist();
        for s in &self.shards {
            let mut h = s.lock().unwrap();
            let _ = out.add(&*h);
            h.reset();
        }
        out
    }
}

#[derive(Default)]
struct EpCounters {
    ok: AtomicU64,
    err: AtomicU64,
    accepted: AtomicU64,
}

/// Process-wide statistics shared by every virtual user.
pub struct Stats {
    /// Post-to-receive latency in ms.
    pub delivery_ms: Hist,
    http_us: Vec<Hist>,
    eps: Vec<EpCounters>,
    pub frames: AtomicU64,
    pub ws_disconnects: AtomicU64,
    pub resync_frames: AtomicU64,
    pub choice_my: AtomicU64,
    pub choice_top: AtomicU64,
    pub choice_create: AtomicU64,
    pub online: AtomicI64,
    pub connecting: AtomicI64,
    pub registered: AtomicU64,
    window_id: AtomicU64,
    window_posters: AtomicU64,
    window_resynced: AtomicU64,
    pub measuring: AtomicBool,
}

impl Default for Stats {
    fn default() -> Self {
        Stats {
            delivery_ms: Hist::default(),
            http_us: Ep::ALL.iter().map(|_| Hist::default()).collect(),
            eps: Ep::ALL.iter().map(|_| EpCounters::default()).collect(),
            frames: AtomicU64::new(0),
            ws_disconnects: AtomicU64::new(0),
            resync_frames: AtomicU64::new(0),
            choice_my: AtomicU64::new(0),
            choice_top: AtomicU64::new(0),
            choice_create: AtomicU64::new(0),
            online: AtomicI64::new(0),
            connecting: AtomicI64::new(0),
            registered: AtomicU64::new(0),
            window_id: AtomicU64::new(1),
            window_posters: AtomicU64::new(0),
            window_resynced: AtomicU64::new(0),
            measuring: AtomicBool::new(false),
        }
    }
}

impl Stats {
    /// `healthy`: the service worked (anything but 5xx/401/429); `accepted`: a 2xx.
    pub fn record_call(&self, ep: Ep, key: usize, micros: u64, healthy: bool, accepted: bool) {
        self.http_us[ep.idx()].record(key, micros);
        let c = &self.eps[ep.idx()];
        if healthy { &c.ok } else { &c.err }.fetch_add(1, Ordering::Relaxed);
        if accepted {
            c.accepted.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Calls that never got an answer (timeouts, connection errors).
    pub fn record_failure(&self, ep: Ep) {
        self.eps[ep.idx()].err.fetch_add(1, Ordering::Relaxed);
    }

    pub fn window_id(&self) -> u64 {
        self.window_id.load(Ordering::Relaxed)
    }

    /// Counts a virtual user once per window; `seen` is the user's own marker.
    fn once_per_window(&self, seen: &mut u64, counter: &AtomicU64) {
        let id = self.window_id();
        if *seen != id {
            *seen = id;
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn note_poster(&self, seen: &mut u64) {
        self.once_per_window(seen, &self.window_posters);
    }

    pub fn note_resynced(&self, seen: &mut u64) {
        self.once_per_window(seen, &self.window_resynced);
    }

    /// Starts a new window: per-window distinct counts restart from zero.
    pub fn begin_window(&self) {
        self.window_id.fetch_add(1, Ordering::Relaxed);
        self.window_posters.store(0, Ordering::Relaxed);
        self.window_resynced.store(0, Ordering::Relaxed);
    }

    pub fn window_posters(&self) -> u64 {
        self.window_posters.load(Ordering::Relaxed)
    }

    pub fn window_resynced(&self) -> u64 {
        self.window_resynced.load(Ordering::Relaxed)
    }

    pub fn counters(&self) -> Counters {
        let mut ok = [0u64; 9];
        let mut err = [0u64; 9];
        let mut accepted = [0u64; 9];
        for (i, c) in self.eps.iter().enumerate() {
            ok[i] = c.ok.load(Ordering::Relaxed);
            err[i] = c.err.load(Ordering::Relaxed);
            accepted[i] = c.accepted.load(Ordering::Relaxed);
        }
        Counters {
            ok,
            err,
            accepted,
            frames: self.frames.load(Ordering::Relaxed),
            ws_disconnects: self.ws_disconnects.load(Ordering::Relaxed),
            resync_frames: self.resync_frames.load(Ordering::Relaxed),
            choice_my: self.choice_my.load(Ordering::Relaxed),
            choice_top: self.choice_top.load(Ordering::Relaxed),
            choice_create: self.choice_create.load(Ordering::Relaxed),
        }
    }

    /// Drains one second's histograms: delivery, all HTTP calls, and per endpoint.
    pub fn drain(&self) -> Interval {
        let per_ep: Vec<Histogram<u64>> = self.http_us.iter().map(|h| h.drain()).collect();
        let mut http = new_hist();
        for (ep, h) in Ep::ALL.iter().zip(&per_ep) {
            // Registration is seeding, not part of any step's HTTP target.
            if *ep != Ep::Register {
                let _ = http.add(h);
            }
        }
        Interval { delivery: self.delivery_ms.drain(), http, per_ep }
    }
}

/// Monotonic counter values; subtract two to get a delta.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Counters {
    pub ok: [u64; 9],
    pub err: [u64; 9],
    pub accepted: [u64; 9],
    pub frames: u64,
    pub ws_disconnects: u64,
    pub resync_frames: u64,
    pub choice_my: u64,
    pub choice_top: u64,
    pub choice_create: u64,
}

impl Counters {
    pub fn since(&self, earlier: &Counters) -> Counters {
        let mut d = *self;
        for i in 0..9 {
            d.ok[i] -= earlier.ok[i];
            d.err[i] -= earlier.err[i];
            d.accepted[i] -= earlier.accepted[i];
        }
        d.frames -= earlier.frames;
        d.ws_disconnects -= earlier.ws_disconnects;
        d.resync_frames -= earlier.resync_frames;
        d.choice_my -= earlier.choice_my;
        d.choice_top -= earlier.choice_top;
        d.choice_create -= earlier.choice_create;
        d
    }

    pub fn ok(&self, ep: Ep) -> u64 {
        self.ok[ep.idx()]
    }

    pub fn err(&self, ep: Ep) -> u64 {
        self.err[ep.idx()]
    }

    /// 2xx answers only.
    pub fn accepted(&self, ep: Ep) -> u64 {
        self.accepted[ep.idx()]
    }

    /// Calls and connection attempts, excluding seeding registrations.
    pub fn attempts(&self) -> u64 {
        Ep::ALL.iter().filter(|e| **e != Ep::Register).map(|e| self.ok(*e) + self.err(*e)).sum()
    }

    pub fn failures(&self) -> u64 {
        Ep::ALL.iter().filter(|e| **e != Ep::Register).map(|e| self.err(*e)).sum()
    }

    pub fn error_rate(&self) -> f64 {
        ratio(self.failures(), self.attempts())
    }

    pub fn reads(&self) -> u64 {
        self.accepted(Ep::Home) + self.accepted(Ep::History)
    }
}

pub fn ratio(a: u64, b: u64) -> f64 {
    if b == 0 { 0.0 } else { a as f64 / b as f64 }
}

/// One drained second of histograms.
pub struct Interval {
    pub delivery: Histogram<u64>,
    pub http: Histogram<u64>,
    pub per_ep: Vec<Histogram<u64>>,
}

impl Interval {
    pub fn empty() -> Interval {
        Interval { delivery: new_hist(), http: new_hist(), per_ep: Ep::ALL.iter().map(|_| new_hist()).collect() }
    }

    pub fn add(&mut self, other: &Interval) {
        let _ = self.delivery.add(&other.delivery);
        let _ = self.http.add(&other.http);
        for (a, b) in self.per_ep.iter_mut().zip(&other.per_ep) {
            let _ = a.add(b);
        }
    }

    pub fn ep(&self, ep: Ep) -> &Histogram<u64> {
        &self.per_ep[ep.idx()]
    }
}

/// p50 and p99 in ms; `None` without samples. `scale` divides recorded units
/// (1 for ms histograms, 1000 for µs).
pub fn p50_p99(h: &Histogram<u64>, scale: f64) -> Option<(f64, f64)> {
    if h.is_empty() {
        return None;
    }
    Some((h.value_at_quantile(0.50) as f64 / scale, h.value_at_quantile(0.99) as f64 / scale))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shards_merge_and_reset() {
        let h = Hist::default();
        for i in 0..1000u64 {
            h.record(i as usize, i + 1);
        }
        let d = h.drain();
        assert_eq!(d.len(), 1000);
        let (p50, p99) = p50_p99(&d, 1.0).unwrap();
        assert!((499.0..=501.0).contains(&p50), "{p50}");
        assert!((989.0..=991.0).contains(&p99), "{p99}");
        assert!(h.drain().is_empty(), "drain resets");
        assert!(p50_p99(&h.drain(), 1.0).is_none());
    }

    #[test]
    fn http_merge_excludes_registration() {
        let s = Stats::default();
        s.record_call(Ep::Register, 0, 5_000_000, true, true);
        s.record_call(Ep::Post, 0, 2_000, true, false); // e.g. a 403
        s.record_call(Ep::Home, 1, 4_000, false, false);
        let i = s.drain();
        assert_eq!(i.http.len(), 2);
        assert_eq!(i.ep(Ep::Register).len(), 1);
        let c = s.counters();
        assert_eq!((c.attempts(), c.failures()), (2, 1));
        assert!((c.error_rate() - 0.5).abs() < 1e-9);
        assert_eq!((c.ok(Ep::Post), c.accepted(Ep::Post)), (1, 0), "healthy but not accepted");
    }

    #[test]
    fn counters_diff() {
        let s = Stats::default();
        let a = s.counters();
        s.record_call(Ep::History, 0, 10, true, true);
        s.frames.fetch_add(7, Ordering::Relaxed);
        let d = s.counters().since(&a);
        assert_eq!((d.ok(Ep::History), d.frames, d.reads()), (1, 7, 1));
    }

    #[test]
    fn window_distinct_counts_reset() {
        let s = Stats::default();
        let (mut u1, mut u2) = (0, 0);
        s.note_poster(&mut u1);
        s.note_poster(&mut u1);
        s.note_poster(&mut u2);
        assert_eq!(s.window_posters(), 2, "each user counted once");
        s.begin_window();
        assert_eq!(s.window_posters(), 0);
        s.note_poster(&mut u1);
        assert_eq!(s.window_posters(), 1, "counted again in a new window");
    }

    #[test]
    fn intervals_accumulate() {
        let mut acc = Interval::empty();
        let s = Stats::default();
        s.delivery_ms.record(0, 10);
        acc.add(&s.drain());
        s.delivery_ms.record(0, 20);
        acc.add(&s.drain());
        assert_eq!(acc.delivery.len(), 2);
    }
}
