//! Live panel, plain progress line and the timeline CSV (design D9).

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use serde::Serialize;

use crate::ramp::{DELIVERY_P99_MS, HTTP_P99_MS, MAX_ERROR_RATE, MAX_RESYNC_RATE};

/// One second of the run, as shown in the panel and written to the CSV.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Snapshot {
    pub t_s: u64,
    pub phase: String,
    pub step: u32,
    pub load: u64,
    pub unit: String,
    pub state: String,
    pub window_s: u64,
    pub window_total_s: u64,
    pub online: i64,
    pub connecting: i64,
    pub registered: u64,
    pub groups: u64,
    pub channels: u64,
    pub max_members_group: u64,
    pub max_members_channel: u64,
    pub max_online_group: u64,
    pub max_online_group_id: i64,
    pub max_online_channel: u64,
    pub max_online_channel_id: i64,
    pub posts_per_s: f64,
    pub frames_per_s: f64,
    pub reads_per_s: f64,
    pub joins_per_s: f64,
    pub creates_per_s: f64,
    pub delivery_p50_ms: Option<f64>,
    pub delivery_p99_ms: Option<f64>,
    pub http_p50_ms: Option<f64>,
    pub http_p99_ms: Option<f64>,
    pub post_p99_ms: Option<f64>,
    pub history_p99_ms: Option<f64>,
    pub home_p99_ms: Option<f64>,
    pub error_rate: f64,
    pub resync_rate: f64,
    pub shard_queue_max: u64,
    pub shard_queue_max_shard: u64,
    pub meta_queue: u64,
    pub server_cpu: f64,
    pub server_rss_mb: f64,
    pub gen_cpu: f64,
    pub gen_budget: f64,
    pub ports_used: usize,
    pub ports_capacity: usize,
    pub sources: usize,
    pub last_pass: Option<u64>,
    pub first_fail: Option<u64>,
}

fn mark(ok: bool) -> &'static str {
    if ok { "ok" } else { "FAIL" }
}

fn ms(v: Option<f64>) -> String {
    v.map_or("-".into(), |v| format!("{v:.0} ms"))
}

fn lat(p50: Option<f64>, p99: Option<f64>, limit: f64) -> String {
    match p99 {
        None => "     -     ".into(),
        Some(p) => format!("{:>6} / {:>7}  {}", ms(p50), ms(Some(p)), mark(p <= limit)),
    }
}

fn clock(secs: u64) -> String {
    format!("{:02}:{:02}", secs / 60, secs % 60)
}

fn opt(v: Option<u64>) -> String {
    v.map_or("--".into(), |v| v.to_string())
}

const LEFT: usize = 35;
const RIGHT: usize = 43;

fn rule() -> String {
    format!(" +{}+{}+", "-".repeat(LEFT), "-".repeat(RIGHT))
}

fn row(left: String, right: String) -> String {
    format!(" |{:<LEFT$}|{:<RIGHT$}|", truncate(left, LEFT), truncate(right, RIGHT))
}

fn truncate(mut s: String, width: usize) -> String {
    s.truncate(width);
    s
}

fn largest(id: i64, online: u64) -> String {
    if id == 0 { "-".into() } else { format!("#{id} ({online} online)") }
}

/// The full-screen panel, without the clear-screen prefix.
pub fn panel(s: &Snapshot) -> String {
    let gen_share = if s.gen_budget > 0.0 { s.gen_cpu / s.gen_budget * 100.0 } else { 0.0 };
    let rows = [
        format!(" chats-stress  phase {}   step {}   load {} {}", s.phase, s.step, s.load, s.unit),
        format!(" elapsed {}   [{}] {}/{} s", clock(s.t_s), s.state, s.window_s, s.window_total_s),
        rule(),
        row(" USERS".into(), " CHATS".into()),
        row(format!("   online now    {:>10}", s.online), format!("   groups   {:>8}   max members {:>6}", s.groups, s.max_members_group)),
        row(format!("   connecting    {:>10}", s.connecting), format!("   channels {:>8}   max members {:>6}", s.channels, s.max_members_channel)),
        row(format!("   registered    {:>10}", s.registered), format!("   largest group:   {}", largest(s.max_online_group_id, s.max_online_group))),
        row(String::new(), format!("   largest channel: {}", largest(s.max_online_channel_id, s.max_online_channel))),
        rule(),
        row(" TRAFFIC (per second)".into(), " LATENCY p50 / p99".into()),
        row(format!("   posts         {:>10.0}", s.posts_per_s), format!("   delivery {}", lat(s.delivery_p50_ms, s.delivery_p99_ms, DELIVERY_P99_MS))),
        row(format!("   frames out    {:>10.0}", s.frames_per_s), format!("   http     {}", lat(s.http_p50_ms, s.http_p99_ms, HTTP_P99_MS))),
        row(format!("   reads         {:>10.0}", s.reads_per_s), format!("   p99 post {}  history {}", ms(s.post_p99_ms), ms(s.history_p99_ms))),
        row(format!("   joins/creates {:>5.0} / {:<4.0}", s.joins_per_s, s.creates_per_s), format!("   p99 home {}", ms(s.home_p99_ms))),
        rule(),
        row(" HEALTH".into(), " MACHINE".into()),
        row(format!("   errors   {:>7.2}%  {}", s.error_rate * 100.0, mark(s.error_rate <= MAX_ERROR_RATE)), format!("   server CPU {:>6.0}%  RSS {:>7.0} MB", s.server_cpu, s.server_rss_mb)),
        row(format!("   resyncs  {:>7.2}%  {}", s.resync_rate * 100.0, mark(s.resync_rate <= MAX_RESYNC_RATE)), format!("   gen CPU    {:>6.0}%  ({gen_share:.0}% of budget)", s.gen_cpu)),
        row(format!("   shard queue max {:>6} (#{})", s.shard_queue_max, s.shard_queue_max_shard), format!("   ports {} / {} ({} addr)", s.ports_used, s.ports_capacity, s.sources)),
        row(format!("   meta queue      {:>6}", s.meta_queue), String::new()),
        rule(),
        format!(" last passed: {}   first failed: {}", opt(s.last_pass), opt(s.first_fail)),
    ];
    rows.join("\n")
}

/// One plain line for logs and pipes (no control codes).
pub fn line(s: &Snapshot) -> String {
    format!(
        "[{}] {} step {} load {} {} {} {}/{}s | online {} | posts {:.0}/s frames {:.0}/s | delivery p99 {} | http p99 {} | err {:.2}% resync {:.2}% | srv {:.0}% gen {:.0}% | groups {} channels {}",
        clock(s.t_s), s.phase, s.step, s.load, s.unit, s.state, s.window_s, s.window_total_s, s.online, s.posts_per_s, s.frames_per_s,
        ms(s.delivery_p99_ms), ms(s.http_p99_ms), s.error_rate * 100.0, s.resync_rate * 100.0, s.server_cpu, s.gen_cpu, s.groups, s.channels
    )
}

/// Writes `stress-timeline.csv`: a header from the field names, then one row per call.
pub struct Timeline {
    out: BufWriter<File>,
    header: Option<Vec<String>>,
}

impl Timeline {
    pub fn create(path: &Path) -> std::io::Result<Timeline> {
        Ok(Timeline { out: BufWriter::new(File::create(path)?), header: None })
    }

    pub fn write(&mut self, s: &Snapshot) -> std::io::Result<()> {
        let serde_json::Value::Object(map) = serde_json::to_value(s).expect("snapshot serializes") else { unreachable!() };
        if self.header.is_none() {
            let keys: Vec<String> = map.keys().cloned().collect();
            writeln!(self.out, "{}", keys.join(","))?;
            self.header = Some(keys);
        }
        let cells: Vec<String> = self
            .header
            .as_ref()
            .unwrap()
            .iter()
            .map(|k| match &map[k] {
                serde_json::Value::Null => String::new(),
                serde_json::Value::String(v) => v.replace(',', ";"),
                serde_json::Value::Number(n) => n.as_f64().map_or(n.to_string(), |f| if f.fract() == 0.0 { format!("{f:.0}") } else { format!("{f:.3}") }),
                other => other.to_string(),
            })
            .collect();
        writeln!(self.out, "{}", cells.join(","))?;
        self.out.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Snapshot {
        Snapshot {
            t_s: 822,
            phase: "A".into(),
            step: 7,
            load: 24_000,
            unit: "users".into(),
            state: "measuring".into(),
            window_s: 31,
            window_total_s: 45,
            online: 23_812,
            groups: 4103,
            channels: 962,
            delivery_p50_ms: Some(38.0),
            delivery_p99_ms: Some(1410.0),
            http_p99_ms: Some(190.0),
            error_rate: 0.0002,
            gen_cpu: 380.0,
            gen_budget: 600.0,
            ..Default::default()
        }
    }

    #[test]
    fn panel_shows_state_and_marks_failures() {
        let p = panel(&sample());
        assert!(p.contains("phase A") && p.contains("load 24000 users"));
        assert!(p.contains("23812"));
        assert!(p.contains("4103") && p.contains("962"));
        assert!(p.contains("1410 ms  FAIL"), "{p}");
        assert!(p.contains("190 ms  ok"));
        assert!(p.lines().all(|l| l.is_ascii()));
        // Every boxed row has its borders in the same columns.
        let boxed: Vec<&str> = p.lines().filter(|l| l.starts_with(" |") || l.starts_with(" +")).collect();
        assert!(boxed.iter().all(|l| l.len() == boxed[0].len()), "{p}");
    }

    #[test]
    fn plain_line_has_no_escape_codes() {
        let l = line(&sample());
        assert!(!l.contains('\x1b'));
        assert!(!l.contains('\n'));
        assert!(l.starts_with("[13:42] A step 7 load 24000 users measuring 31/45s"));
    }

    #[test]
    fn timeline_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.csv");
        let mut t = Timeline::create(&path).unwrap();
        t.write(&sample()).unwrap();
        t.write(&Snapshot { t_s: 823, ..sample() }).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3);
        let header: Vec<&str> = lines[0].split(',').collect();
        let row: Vec<&str> = lines[2].split(',').collect();
        assert_eq!(header.len(), row.len());
        let col = |name: &str| row[header.iter().position(|h| *h == name).unwrap()];
        assert_eq!(col("t_s"), "823");
        assert_eq!(col("online"), "23812");
        assert_eq!(col("http_p50_ms"), "", "missing values are empty");
    }
}
