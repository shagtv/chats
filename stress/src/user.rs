//! Virtual-user behaviour (specs/stress-testing "Realistic session loop",
//! "Realistic chat choice", "Realistic posting mix").

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::api::{Api, Home, Session};
use crate::net::Ports;
use crate::seed::Kind;
use crate::ws::{self, ConnectError, Ws};

pub const SHARE_MINE: f64 = 0.70;
pub const SHARE_TOP: f64 = 0.25;
pub const NEW_GROUP_SHARE: f64 = 0.80;
pub const SCROLL_SHARE: f64 = 0.30;
pub const JOIN_SHARE: f64 = 0.10;
pub const LEAVE_SHARE: f64 = 0.20;
pub const OCCASIONAL_EVERY: Duration = Duration::from_secs(120);
pub const HEAVY_EVERY: Duration = Duration::from_secs(10);
const BODY: &str = "stress test message of a typical chat length, around sixty characters";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    Lurker,
    Occasional,
    Heavy,
}

/// 90% lurkers, 9% occasional, 1% heavy; fixed per user index and seed.
pub fn profile_for(index: usize, seed: u64) -> Profile {
    let mut rng = StdRng::seed_from_u64(seed ^ (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    match rng.random_range(0..100) {
        0..90 => Profile::Lurker,
        90..99 => Profile::Occasional,
        _ => Profile::Heavy,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Choice {
    Mine,
    Top,
    Create(Kind),
}

/// The random decisions of a virtual user, separate from I/O so they can be tested.
pub struct Behaviour {
    pub rng: StdRng,
}

impl Behaviour {
    pub fn new(seed: u64, index: usize) -> Behaviour {
        Behaviour { rng: StdRng::seed_from_u64(seed.wrapping_add(index as u64).wrapping_mul(31)) }
    }

    pub fn choose(&mut self) -> Choice {
        let u: f64 = self.rng.random();
        if u < SHARE_MINE {
            Choice::Mine
        } else if u < SHARE_MINE + SHARE_TOP {
            Choice::Top
        } else if self.rng.random_bool(NEW_GROUP_SHARE) {
            Choice::Create(Kind::Group)
        } else {
            Choice::Create(Kind::Channel)
        }
    }

    /// Older pages to load after the latest one: 0, or 1-3 about 30% of the time.
    pub fn scroll_pages(&mut self) -> usize {
        if self.rng.random_bool(SCROLL_SHARE) { self.rng.random_range(1..=3) } else { 0 }
    }

    pub fn join(&mut self) -> bool {
        self.rng.random_bool(JOIN_SHARE)
    }

    pub fn leave(&mut self) -> bool {
        self.rng.random_bool(LEAVE_SHARE)
    }

    pub fn dwell(&mut self) -> Duration {
        Duration::from_secs(self.rng.random_range(60..=300))
    }

    /// Time to the next post (exponential around the profile's mean), or
    /// `None` for lurkers.
    pub fn next_post(&mut self, profile: Profile) -> Option<Duration> {
        let mean = match profile {
            Profile::Lurker => return None,
            Profile::Occasional => OCCASIONAL_EVERY,
            Profile::Heavy => HEAVY_EVERY,
        };
        let u: f64 = self.rng.random_range(f64::EPSILON..1.0);
        Some(mean.mul_f64(-u.ln()).min(mean * 5))
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        (!items.is_empty()).then(|| &items[self.rng.random_range(0..items.len())])
    }
}

/// Everything a virtual user needs.
#[derive(Clone)]
pub struct Env {
    pub api: Arc<Api>,
    pub ports: Arc<Ports>,
    pub seed: u64,
}

/// Sleeps unless stopped first; returns `false` when stopped.
pub async fn pause(stop: &mut watch::Receiver<bool>, d: Duration) -> bool {
    if *stop.borrow() {
        return false;
    }
    tokio::select! {
        _ = tokio::time::sleep(d) => !*stop.borrow(),
        _ = stop.changed() => false,
    }
}

/// Connects, retrying every second; `None` once stopped.
pub async fn connect(env: &Env, session: &Session, stop: &mut watch::Receiver<bool>) -> Option<Ws> {
    loop {
        match ws::connect(&env.api, &env.ports, session).await {
            Ok(w) => return Some(w),
            Err(ConnectError::NoPorts | ConnectError::Failed) => {
                if !pause(stop, Duration::from_secs(1)).await {
                    return None;
                }
            }
        }
    }
}

struct Picked {
    id: i64,
    member: bool,
    can_post: bool,
}

fn pick_chat(b: &mut Behaviour, home: &Home, choice: Choice) -> Option<Picked> {
    let mine = |id: i64| home.my_chats.iter().find(|c| c.id == id);
    match choice {
        Choice::Mine => b.pick(&home.my_chats).map(|c| Picked { id: c.id, member: true, can_post: c.kind == "group" || c.is_owner }),
        Choice::Top => {
            let top: Vec<_> = home.top_groups.iter().chain(&home.top_channels).collect();
            let c = *b.pick(&top)?;
            Some(match mine(c.id) {
                Some(m) => Picked { id: m.id, member: true, can_post: m.kind == "group" || m.is_owner },
                None => Picked { id: c.id, member: false, can_post: false },
            })
        }
        Choice::Create(_) => None,
    }
}

/// The realistic user: home, pick, open, read, maybe join, dwell and post, switch.
pub async fn realistic(env: Env, index: usize, session: Session, mut stop: watch::Receiver<bool>) {
    let stats = env.api.stats.clone();
    let api = env.api.clone();
    let mut b = Behaviour::new(env.seed, index);
    let profile = profile_for(index, env.seed);
    let mut posted_window = 0;
    // Spread arrivals so a step doesn't start with a thundering herd.
    if !pause(&mut stop, Duration::from_millis(b.rng.random_range(0..3000))).await {
        return;
    }
    let Some(mut ws) = connect(&env, &session, &mut stop).await else { return };
    let mut created = 0;
    'session: while !*stop.borrow() {
        if !ws.alive() {
            match connect(&env, &session, &mut stop).await {
                Some(w) => ws = w,
                None => break,
            }
        }
        let Ok(home) = api.home(&session).await else {
            if !pause(&mut stop, Duration::from_secs(1)).await {
                break;
            }
            continue;
        };
        let choice = b.choose();
        let picked = match choice {
            Choice::Mine => {
                stats.choice_my.fetch_add(1, Ordering::Relaxed);
                pick_chat(&mut b, &home, choice)
            }
            Choice::Top => {
                stats.choice_top.fetch_add(1, Ordering::Relaxed);
                pick_chat(&mut b, &home, choice)
            }
            Choice::Create(kind) => {
                stats.choice_create.fetch_add(1, Ordering::Relaxed);
                created += 1;
                let title = format!("vu{index}-{created}");
                api.create(&session, kind.as_str(), &title)
                    .await
                    .ok()
                    .map(|id| Picked { id, member: true, can_post: true })
            }
        };
        let Some(mut chat) = picked else {
            if !pause(&mut stop, Duration::from_secs(5)).await {
                break;
            }
            continue;
        };

        ws.open(chat.id).await;
        if let Ok(mut page) = api.history(&session, chat.id, None, None).await {
            let latest = page.messages.first().map_or(0, |m| m.seq);
            for _ in 0..b.scroll_pages() {
                let Some(oldest) = page.messages.last().map(|m| m.seq) else { break };
                if !page.has_more {
                    break;
                }
                match api.history(&session, chat.id, Some(oldest), None).await {
                    Ok(p) => page = p,
                    Err(_) => break,
                }
            }
            if chat.member && latest > 0 {
                let _ = api.read(&session, chat.id, latest).await;
            }
        }
        if !chat.member && b.join() && api.join(&session, chat.id).await.is_ok() {
            chat.member = true;
            chat.can_post = api.chat(&session, chat.id).await.is_ok_and(|c| c.can_post);
        }

        let until = Instant::now() + b.dwell();
        loop {
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            let wait = match (chat.can_post, b.next_post(profile)) {
                (true, Some(d)) => d.min(left),
                _ => left,
            };
            if !pause(&mut stop, wait).await {
                break 'session;
            }
            if chat.can_post
                && Instant::now() < until
                && profile != Profile::Lurker
                && api.post(&session, chat.id, BODY).await.is_ok()
            {
                stats.note_poster(&mut posted_window);
            }
        }
        if chat.member && b.leave() {
            let _ = api.leave(&session, chat.id).await;
        }
    }
    ws.close().await;
}

/// Connects, opens `chat`, loads its latest page, then optionally posts every
/// `post_every` until stopped.
pub async fn viewer(env: Env, session: Session, chat: i64, post_every: Option<Duration>, mut stop: watch::Receiver<bool>) {
    let mut jitter = StdRng::seed_from_u64(env.seed ^ session.id as u64);
    if !pause(&mut stop, Duration::from_millis(jitter.random_range(0..2000))).await {
        return;
    }
    let Some(mut ws) = connect(&env, &session, &mut stop).await else { return };
    ws.open(chat).await;
    let _ = env.api.history(&session, chat, None, None).await;
    let mut posted_window = 0;
    loop {
        if !ws.alive() {
            match connect(&env, &session, &mut stop).await {
                Some(w) => {
                    ws = w;
                    ws.open(chat).await;
                }
                None => break,
            }
        }
        let wait = post_every.unwrap_or(Duration::from_secs(3600));
        if !pause(&mut stop, wait).await {
            break;
        }
        if post_every.is_some() && env.api.post(&session, chat, BODY).await.is_ok() {
            env.api.stats.note_poster(&mut posted_window);
        }
    }
    ws.close().await;
}

/// Posts to `chat` at `rate_mhz` (messages per 1000 s, read on every
/// iteration so the ramp can change it) without a WebSocket.
pub async fn poster(env: Env, session: Session, chat: i64, rate_mhz: Arc<std::sync::atomic::AtomicU64>, mut stop: watch::Receiver<bool>) {
    let mut jitter = StdRng::seed_from_u64(env.seed ^ (session.id as u64) << 1);
    let mut posted_window = 0;
    // Stagger the first post across one interval.
    let first = rate_mhz.load(Ordering::Relaxed).max(1);
    if !pause(&mut stop, Duration::from_secs_f64(jitter.random_range(0.0..1000.0 / first as f64))).await {
        return;
    }
    loop {
        let rate = rate_mhz.load(Ordering::Relaxed);
        if rate == 0 {
            if !pause(&mut stop, Duration::from_millis(200)).await {
                break;
            }
            continue;
        }
        let started = Instant::now();
        if env.api.post(&session, chat, BODY).await.is_ok() {
            env.api.stats.note_poster(&mut posted_window);
        }
        let interval = Duration::from_secs_f64(1000.0 / rate as f64);
        if !pause(&mut stop, interval.saturating_sub(started.elapsed())).await {
            break;
        }
    }
}

/// Connects and does nothing else.
pub async fn idle(env: Env, session: Session, mut stop: watch::Receiver<bool>) {
    let mut jitter = StdRng::seed_from_u64(env.seed ^ session.id as u64);
    if !pause(&mut stop, Duration::from_millis(jitter.random_range(0..2000))).await {
        return;
    }
    let Some(mut ws) = connect(&env, &session, &mut stop).await else { return };
    while pause(&mut stop, Duration::from_secs(5)).await {
        if !ws.alive() {
            match connect(&env, &session, &mut stop).await {
                Some(w) => ws = w,
                None => return,
            }
        }
    }
    ws.close().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decisions_follow_the_configured_mix() {
        const N: usize = 100_000;
        let mut b = Behaviour::new(42, 0);
        let (mut mine, mut top, mut groups, mut channels) = (0, 0, 0, 0);
        let (mut scrolls, mut joins, mut leaves) = (0, 0, 0);
        for _ in 0..N {
            match b.choose() {
                Choice::Mine => mine += 1,
                Choice::Top => top += 1,
                Choice::Create(Kind::Group) => groups += 1,
                Choice::Create(Kind::Channel) => channels += 1,
            }
            scrolls += (b.scroll_pages() > 0) as usize;
            joins += b.join() as usize;
            leaves += b.leave() as usize;
        }
        let share = |n: usize| n as f64 / N as f64;
        let near = |got: f64, want: f64| (got - want).abs() < 0.01;
        assert!(near(share(mine), 0.70), "mine {}", share(mine));
        assert!(near(share(top), 0.25), "top {}", share(top));
        assert!(near(share(groups + channels), 0.05));
        assert!(near(groups as f64 / (groups + channels) as f64, 0.80));
        assert!(near(share(scrolls), 0.30) && near(share(joins), 0.10) && near(share(leaves), 0.20));
    }

    #[test]
    fn profiles_are_90_9_1() {
        const N: usize = 100_000;
        let mut counts = [0usize; 3];
        for i in 0..N {
            counts[profile_for(i, 7) as usize] += 1;
        }
        let share = |n: usize| n as f64 / N as f64;
        assert!((share(counts[0]) - 0.90).abs() < 0.01);
        assert!((share(counts[1]) - 0.09).abs() < 0.01);
        assert!((share(counts[2]) - 0.01).abs() < 0.01);
        assert_eq!(profile_for(5, 7), profile_for(5, 7), "stable per user");
    }

    #[test]
    fn post_intervals_match_profiles() {
        let mut b = Behaviour::new(1, 1);
        assert!(b.next_post(Profile::Lurker).is_none());
        let mean = |p: Profile, b: &mut Behaviour| {
            (0..20_000).map(|_| b.next_post(p).unwrap().as_secs_f64()).sum::<f64>() / 20_000.0
        };
        let heavy = mean(Profile::Heavy, &mut b);
        let occasional = mean(Profile::Occasional, &mut b);
        assert!((8.5..11.0).contains(&heavy), "heavy {heavy}");
        assert!((100.0..130.0).contains(&occasional), "occasional {occasional}");
        let d = b.dwell();
        assert!(d >= Duration::from_secs(60) && d <= Duration::from_secs(300));
    }
}
