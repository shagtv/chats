use std::net::SocketAddr;
use std::path::PathBuf;

pub const USAGE: &str = "usage: chats-stress [options]

Starts the chat server with --metrics, ramps realistic and targeted load until
the service misses its targets, and writes a report.

  --server PATH       server binary              (default target/release/chats-openspec)
  --addr HOST:PORT    server listen address      (default 127.0.0.1:18080)
  --out DIR           output directory           (default stress-out/<timestamp>)
  --only PHASE        run one phase: a, b1, b2, b3, b4
  --gen-threads N     load generator threads     (default 6)
  --step-secs N       measured window per step   (default 45)
  --warmup-secs N     warm-up after connecting   (default 10)
  --seed N            random seed                (default 42)
  --keep-data         keep the server's data directory
  --smoke             tiny loads, at most 2 steps per phase (a quick check that everything runs)";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    A,
    B1,
    B2,
    B3,
    B4,
}

impl Phase {
    pub const ALL: [Phase; 5] = [Phase::A, Phase::B1, Phase::B2, Phase::B3, Phase::B4];

    pub fn parse(s: &str) -> Option<Phase> {
        match s.to_ascii_lowercase().as_str() {
            "a" => Some(Phase::A),
            "b1" => Some(Phase::B1),
            "b2" => Some(Phase::B2),
            "b3" => Some(Phase::B3),
            "b4" => Some(Phase::B4),
            _ => None,
        }
    }

    pub fn id(self) -> &'static str {
        match self {
            Phase::A => "A",
            Phase::B1 => "B1",
            Phase::B2 => "B2",
            Phase::B3 => "B3",
            Phase::B4 => "B4",
        }
    }

    pub fn title(self) -> &'static str {
        match self {
            Phase::A => "realistic ramp",
            Phase::B1 => "one channel",
            Phase::B2 => "one group",
            Phase::B3 => "write ceiling",
            Phase::B4 => "idle connections",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Args {
    pub server: PathBuf,
    pub addr: SocketAddr,
    pub out: Option<PathBuf>,
    pub phases: Vec<Phase>,
    pub gen_threads: usize,
    pub step_secs: u64,
    pub warmup_secs: u64,
    pub seed: u64,
    pub keep_data: bool,
    pub smoke: bool,
}

impl Args {
    pub fn parse<I: IntoIterator<Item = String>>(args: I) -> Result<Args, String> {
        let mut a = Args {
            server: PathBuf::from("target/release/chats-openspec"),
            addr: "127.0.0.1:18080".parse().unwrap(),
            out: None,
            phases: Phase::ALL.to_vec(),
            gen_threads: 6,
            step_secs: 45,
            warmup_secs: 10,
            seed: 42,
            keep_data: false,
            smoke: false,
        };
        let mut it = args.into_iter();
        while let Some(arg) = it.next() {
            let (key, inline) = match arg.split_once('=') {
                Some((k, v)) => (k.to_string(), Some(v.to_string())),
                None => (arg.clone(), None),
            };
            match key.as_str() {
                "-h" | "--help" => return Err(USAGE.to_string()),
                "--keep-data" => {
                    a.keep_data = true;
                    continue;
                }
                "--smoke" => {
                    a.smoke = true;
                    continue;
                }
                _ => {}
            }
            let mut value = || inline.clone().or_else(|| it.next()).ok_or_else(|| format!("{key} needs a value"));
            match key.as_str() {
                "--server" => a.server = PathBuf::from(value()?),
                "--addr" => a.addr = value()?.parse().map_err(|e| format!("invalid --addr: {e}"))?,
                "--out" => a.out = Some(PathBuf::from(value()?)),
                "--only" => {
                    let v = value()?;
                    a.phases = vec![Phase::parse(&v).ok_or_else(|| format!("unknown phase: {v} (a, b1, b2, b3, b4)"))?];
                }
                "--gen-threads" => a.gen_threads = positive(&key, &value()?)?,
                "--step-secs" => a.step_secs = positive(&key, &value()?)? as u64,
                "--warmup-secs" => a.warmup_secs = value()?.parse().map_err(|_| format!("invalid {key}"))?,
                "--seed" => a.seed = value()?.parse().map_err(|_| format!("invalid {key}"))?,
                other => return Err(format!("unknown argument: {other}\n\n{USAGE}")),
            }
        }
        Ok(a)
    }
}

fn positive(key: &str, v: &str) -> Result<usize, String> {
    match v.parse::<usize>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err(format!("invalid {key}: {v}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(list: &[&str]) -> Result<Args, String> {
        Args::parse(list.iter().map(|s| s.to_string()))
    }

    #[test]
    fn defaults() {
        let a = parse(&[]).unwrap();
        assert_eq!(a.server, PathBuf::from("target/release/chats-openspec"));
        assert_eq!(a.addr.to_string(), "127.0.0.1:18080");
        assert_eq!(a.phases, Phase::ALL.to_vec());
        assert_eq!((a.gen_threads, a.step_secs, a.warmup_secs, a.seed), (6, 45, 10, 42));
        assert!(a.out.is_none() && !a.keep_data && !a.smoke);
    }

    #[test]
    fn every_flag() {
        let a = parse(&[
            "--server", "/bin/x", "--addr=127.0.0.1:9", "--out", "o", "--only", "B2", "--gen-threads", "3",
            "--step-secs", "5", "--warmup-secs=0", "--seed", "7", "--keep-data", "--smoke",
        ])
        .unwrap();
        assert_eq!(a.server, PathBuf::from("/bin/x"));
        assert_eq!(a.addr.port(), 9);
        assert_eq!(a.out, Some(PathBuf::from("o")));
        assert_eq!(a.phases, vec![Phase::B2]);
        assert_eq!((a.gen_threads, a.step_secs, a.warmup_secs, a.seed), (3, 5, 0, 7));
        assert!(a.keep_data && a.smoke);
    }

    #[test]
    fn rejects_bad_input() {
        assert!(parse(&["--only", "c9"]).unwrap_err().contains("unknown phase"));
        assert!(parse(&["--gen-threads", "0"]).is_err());
        assert!(parse(&["--step-secs"]).is_err());
        assert!(parse(&["--frobnicate"]).is_err());
        assert!(parse(&["--help"]).unwrap_err().contains("usage"));
    }
}
