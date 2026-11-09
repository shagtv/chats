//! Loopback source addresses and ephemeral-port capacity (design D3).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Ports kept free on the first address for the shared HTTP pool and slack.
pub const HTTP_RESERVE: usize = 1024;
const MAX_ALIASES: u8 = 16;

/// Which loopback addresses can be bound as a source (127.0.0.1 always; the
/// rest only when aliased on the loopback interface).
pub fn probe_sources() -> Vec<IpAddr> {
    (1..=MAX_ALIASES)
        .map(|n| IpAddr::V4(Ipv4Addr::new(127, 0, 0, n)))
        .filter(|ip| std::net::TcpListener::bind(SocketAddr::new(*ip, 0)).is_ok())
        .collect()
}

/// Ephemeral ports per source address, from the OS range, minus a 10% margin.
pub fn ports_per_address() -> usize {
    let range = ephemeral_range().unwrap_or((49152, 65535));
    capacity_for_range(range.0, range.1)
}

pub fn capacity_for_range(first: u32, last: u32) -> usize {
    let size = last.saturating_sub(first) + 1;
    (size as usize) * 9 / 10
}

fn ephemeral_range() -> Option<(u32, u32)> {
    let read = |key: &str| {
        let out = std::process::Command::new("sysctl").args(["-n", key]).output().ok()?;
        String::from_utf8_lossy(&out.stdout).trim().parse::<u32>().ok()
    };
    if let (Some(a), Some(b)) = (read("net.inet.ip.portrange.first"), read("net.inet.ip.portrange.last")) {
        return Some((a, b));
    }
    let linux = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").ok()?;
    let mut parts = linux.split_whitespace().filter_map(|p| p.parse().ok());
    Some((parts.next()?, parts.next()?))
}

/// The command a user can run to add the next loopback alias (macOS).
pub fn alias_hint(sources: &[IpAddr]) -> String {
    let next = (2..=MAX_ALIASES)
        .map(|n| IpAddr::V4(Ipv4Addr::new(127, 0, 0, n)))
        .find(|ip| !sources.contains(ip))
        .unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)));
    format!("sudo ifconfig lo0 alias {next} up")
}

/// Hands out source addresses round-robin while tracking ports in use.
pub struct Ports {
    pub sources: Vec<IpAddr>,
    pub per_address: usize,
    used: AtomicUsize,
    next: AtomicUsize,
}

impl Ports {
    pub fn new(sources: Vec<IpAddr>, per_address: usize) -> Ports {
        assert!(!sources.is_empty(), "at least 127.0.0.1 must be bindable");
        Ports { sources, per_address, used: AtomicUsize::new(0), next: AtomicUsize::new(0) }
    }

    pub fn detect() -> Ports {
        Ports::new(probe_sources(), ports_per_address())
    }

    /// WebSocket connections that fit, keeping the HTTP reserve.
    pub fn capacity(&self) -> usize {
        (self.sources.len() * self.per_address).saturating_sub(HTTP_RESERVE)
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// Reserves a port; `None` when capacity is exhausted.
    pub fn acquire(&self) -> Option<IpAddr> {
        let prev = self.used.fetch_add(1, Ordering::Relaxed);
        if prev >= self.capacity() {
            self.used.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.sources.len();
        Some(self.sources[i])
    }

    pub fn release(&self) {
        self.used.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_maths() {
        assert_eq!(capacity_for_range(49152, 65535), 14745); // floor(16384 * 0.9)
        let p = Ports::new(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)], 14744);
        assert_eq!(p.capacity(), 14744 - HTTP_RESERVE);
        let two = Ports::new(
            vec![IpAddr::V4(Ipv4Addr::LOCALHOST), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2))],
            14744,
        );
        assert_eq!(two.capacity(), 2 * 14744 - HTTP_RESERVE);
    }

    #[test]
    fn acquire_round_robin_until_full() {
        let a = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let b = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2));
        let p = Ports::new(vec![a, b], HTTP_RESERVE / 2 + 2); // capacity 4
        assert_eq!(p.capacity(), 4);
        let got: Vec<_> = (0..4).map(|_| p.acquire().unwrap()).collect();
        assert_eq!(got, vec![a, b, a, b]);
        assert!(p.acquire().is_none());
        p.release();
        assert!(p.acquire().is_some());
        assert_eq!(p.used(), 4);
    }

    #[test]
    fn probe_finds_localhost() {
        let s = probe_sources();
        assert_eq!(s[0], IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert!(ports_per_address() > 1000);
        assert!(alias_hint(&s).starts_with("sudo ifconfig lo0 alias 127.0.0."));
    }
}
