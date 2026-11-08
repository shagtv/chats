use std::net::SocketAddr;
use std::path::PathBuf;

pub const USAGE: &str = "usage: chats-openspec [--addr HOST:PORT] [--data-dir DIR] [--static-dir DIR] [--shards N]

  --addr        listen address            (env CHATS_ADDR,       default 127.0.0.1:8080)
  --data-dir    SQLite files directory    (env CHATS_DATA_DIR,   default ./data)
  --static-dir  static files directory    (env CHATS_STATIC_DIR, default <exe dir>/static or ./static)
  --shards      message shard count, used only when the database is first created
                                          (env CHATS_SHARDS,     default 32)";

#[derive(Debug, Clone)]
pub struct Config {
    pub addr: SocketAddr,
    pub data_dir: PathBuf,
    pub static_dir: PathBuf,
    /// `None` means "use the stored count, or the default on first start".
    pub shards: Option<u32>,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        Self::parse(std::env::args().skip(1), |k| std::env::var(k).ok())
    }

    /// Command-line flags take precedence over environment variables.
    pub fn parse<I, F>(args: I, env: F) -> Result<Self, String>
    where
        I: IntoIterator<Item = String>,
        F: Fn(&str) -> Option<String>,
    {
        let mut addr = env("CHATS_ADDR");
        let mut data_dir = env("CHATS_DATA_DIR");
        let mut static_dir = env("CHATS_STATIC_DIR");
        let mut shards = env("CHATS_SHARDS");

        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            let (key, inline) = match arg.split_once('=') {
                Some((k, v)) => (k.to_string(), Some(v.to_string())),
                None => (arg.clone(), None),
            };
            let slot = match key.as_str() {
                "--addr" => &mut addr,
                "--data-dir" => &mut data_dir,
                "--static-dir" => &mut static_dir,
                "--shards" => &mut shards,
                "-h" | "--help" => return Err(USAGE.to_string()),
                other => return Err(format!("unknown argument: {other}\n\n{USAGE}")),
            };
            let value = match inline {
                Some(v) => v,
                None => args.next().ok_or_else(|| format!("{key} needs a value"))?,
            };
            *slot = Some(value);
        }

        let addr = addr
            .as_deref()
            .unwrap_or("127.0.0.1:8080")
            .parse()
            .map_err(|e| format!("invalid --addr: {e}"))?;
        let shards = shards
            .map(|s| match s.parse::<u32>() {
                Ok(n) if (1..=1024).contains(&n) => Ok(n),
                _ => Err(format!("invalid --shards: {s} (expected 1..=1024)")),
            })
            .transpose()?;

        Ok(Config {
            addr,
            data_dir: data_dir.map(PathBuf::from).unwrap_or_else(|| PathBuf::from("data")),
            static_dir: static_dir.map(PathBuf::from).unwrap_or_else(default_static_dir),
            shards,
        })
    }
}

fn default_static_dir() -> PathBuf {
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        let candidate = dir.join("static");
        if candidate.is_dir() {
            return candidate;
        }
    }
    PathBuf::from("static")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn defaults() {
        let c = Config::parse(args(&[]), |_| None).unwrap();
        assert_eq!(c.addr.to_string(), "127.0.0.1:8080");
        assert_eq!(c.data_dir, PathBuf::from("data"));
        assert_eq!(c.shards, None);
    }

    #[test]
    fn env_then_flags_override() {
        let env = |k: &str| match k {
            "CHATS_ADDR" => Some("0.0.0.0:9000".to_string()),
            "CHATS_SHARDS" => Some("8".to_string()),
            "CHATS_DATA_DIR" => Some("/tmp/env".to_string()),
            _ => None,
        };
        let c = Config::parse(args(&[]), env).unwrap();
        assert_eq!(c.addr.to_string(), "0.0.0.0:9000");
        assert_eq!(c.shards, Some(8));

        let c = Config::parse(args(&["--shards", "100", "--data-dir=/tmp/x", "--static-dir", "s"]), env).unwrap();
        assert_eq!(c.shards, Some(100));
        assert_eq!(c.data_dir, PathBuf::from("/tmp/x"));
        assert_eq!(c.static_dir, PathBuf::from("s"));
        assert_eq!(c.addr.to_string(), "0.0.0.0:9000");
    }

    #[test]
    fn rejects_bad_input() {
        assert!(Config::parse(args(&["--shards", "0"]), |_| None).is_err());
        assert!(Config::parse(args(&["--bogus"]), |_| None).is_err());
        assert!(Config::parse(args(&["--addr"]), |_| None).is_err());
    }
}
