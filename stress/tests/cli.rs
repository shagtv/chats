//! Drives the `chats-stress` binary end to end with `--smoke` loads.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

fn server_binary() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..").join("target");
    ["release", "debug"]
        .iter()
        .map(|p| root.join(p).join("chats-openspec"))
        .find(|p| p.is_file())
        .expect("server binary not found; run `cargo build -p chats-openspec` (or `cargo test --workspace`)")
}

fn stress(out: &Path, extra: &[&str]) -> Command {
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let mut c = Command::new(env!("CARGO_BIN_EXE_chats-stress"));
    c.arg("--server")
        .arg(server_binary())
        .args(["--addr", &format!("127.0.0.1:{port}"), "--smoke", "--warmup-secs", "1"])
        .arg("--out")
        .arg(out)
        .args(extra)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    c
}

fn report(out: &Path) -> Value {
    serde_json::from_str(&std::fs::read_to_string(out.join("stress-report.json")).unwrap()).unwrap()
}

#[test]
fn every_phase_completes_a_step() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("run");
    let result = stress(&out, &["--step-secs", "3"]).output().unwrap();
    assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    let r = report(&out);
    let phases = r["phases"].as_array().unwrap();
    let ids: Vec<&str> = phases.iter().map(|p| p["id"].as_str().unwrap()).collect();
    assert_eq!(ids, ["A", "B1", "B2", "B3", "B4"]);
    for p in phases {
        assert!(!p["steps"].as_array().unwrap().is_empty(), "phase {} ran no step", p["id"]);
    }
    assert_eq!(r["maximums"].as_array().unwrap().len(), 5);
    let md = std::fs::read_to_string(out.join("stress-report.md")).unwrap();
    assert!(md.contains("## Maximums") && md.contains("## Run validity"));
    assert!(!out.join("data").exists(), "data dir removed");
    // Piped output: plain lines, no terminal control codes.
    assert!(!result.stdout.contains(&0x1b));
}

#[test]
fn timeline_has_a_row_per_second() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("run");
    let result = stress(&out, &["--only", "b4", "--step-secs", "2"]).output().unwrap();
    assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    let r = report(&out);
    assert_eq!(r["phases"].as_array().unwrap().len(), 1, "only the selected phase");
    let csv = std::fs::read_to_string(out.join("stress-timeline.csv")).unwrap();
    let lines: Vec<&str> = csv.lines().collect();
    assert!(lines[0].starts_with("channels,") && lines[0].contains(",online,"), "header: {}", lines[0]);
    let rows = lines.len() as i64 - 1;
    let secs = r["duration_s"].as_i64().unwrap();
    assert!((rows - secs).abs() <= 2, "{rows} rows for a {secs} s run");
}

#[test]
fn ctrl_c_writes_an_interrupted_report() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("run");
    let mut child = stress(&out, &["--only", "a", "--step-secs", "60"]).spawn().unwrap();
    // Wait until the first step is being measured.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let csv = std::fs::read_to_string(out.join("stress-timeline.csv")).unwrap_or_default();
        if csv.contains(",measuring,") {
            break;
        }
        assert!(Instant::now() < deadline, "never started measuring");
        std::thread::sleep(Duration::from_millis(200));
    }
    Command::new("kill").args(["-INT", &child.id().to_string()]).status().unwrap();
    let status = child.wait().unwrap();
    assert!(status.success(), "exit {status:?}");
    let r = report(&out);
    assert_eq!(r["interrupted"], true);
    assert!(std::fs::read_to_string(out.join("stress-report.md")).unwrap().contains("**Interrupted**"));
    assert!(!out.join("data").exists(), "server stopped and data removed");
}
