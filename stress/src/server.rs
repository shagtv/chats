//! Starts and stops the server under test (specs/stress-testing "Self-contained run").

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::process::{Child, Command};

pub struct ServerProc {
    child: Child,
    pub addr: SocketAddr,
    pub pid: u32,
    pub data_dir: PathBuf,
    keep_data: bool,
}

impl ServerProc {
    /// Spawns `binary --addr ADDR --data-dir OUT/data --metrics` and waits until
    /// `/api/metrics` answers. Server output goes to `OUT/server.log`.
    pub async fn start(binary: &Path, addr: SocketAddr, out: &Path, keep_data: bool) -> Result<ServerProc, String> {
        if !binary.is_file() {
            return Err(format!(
                "server binary not found at {} (build it with `cargo build --release`)",
                binary.display()
            ));
        }
        if binary.components().any(|c| c.as_os_str() == "debug") {
            eprintln!("warning: {} looks like a debug build; results will be far below release performance", binary.display());
        }
        TcpListener::bind(addr).map_err(|e| format!("address {addr} is not free: {e}"))?;

        let data_dir = out.join("data");
        if data_dir.exists() {
            return Err(format!("{} already exists; use a fresh --out directory", data_dir.display()));
        }
        let log = std::fs::File::create(out.join("server.log")).map_err(|e| e.to_string())?;
        let child = Command::new(binary)
            .arg("--addr")
            .arg(addr.to_string())
            .arg("--data-dir")
            .arg(&data_dir)
            .arg("--metrics")
            .stdin(Stdio::null())
            .stdout(log.try_clone().map_err(|e| e.to_string())?)
            .stderr(log)
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("spawn {}: {e}", binary.display()))?;
        let pid = child.id().ok_or("server exited immediately")?;
        let mut proc = ServerProc { child, addr, pid, data_dir, keep_data };

        let url = format!("http://{addr}/api/metrics");
        let client = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(r) = client.get(&url).send().await
                && r.status().is_success()
            {
                return Ok(proc);
            }
            if let Ok(Some(status)) = proc.child.try_wait() {
                return Err(format!("server exited during startup ({status}); see {}", out.join("server.log").display()));
            }
            if Instant::now() > deadline {
                return Err("server did not answer /api/metrics within 30 s".into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// SIGINT, wait up to 15 s, then kill. Removes the data dir unless kept.
    pub async fn stop(mut self) -> Result<(), String> {
        let _ = Command::new("kill").args(["-INT", &self.pid.to_string()]).status().await;
        match tokio::time::timeout(Duration::from_secs(15), self.child.wait()).await {
            Ok(_) => {}
            Err(_) => {
                eprintln!("warning: server did not exit after SIGINT; killing it");
                let _ = self.child.kill().await;
            }
        }
        if !self.keep_data {
            std::fs::remove_dir_all(&self.data_dir).map_err(|e| format!("remove {}: {e}", self.data_dir.display()))?;
        }
        Ok(())
    }
}
