//! Users and chats created before measurement (design D6).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures_util::StreamExt;
use futures_util::stream;
use rand::RngExt;
use rand::rngs::StdRng;

use crate::api::{Api, CallError, Session};

pub const GROUP_CAP: usize = 1_000;
pub const CHANNEL_CAP: usize = 10_000;
/// Zipf exponent for chat popularity.
pub const ZIPF_S: f64 = 1.2;
const JOIN_CONCURRENCY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Group,
    Channel,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Group => "group",
            Kind::Channel => "channel",
        }
    }

    pub fn cap(self) -> usize {
        match self {
            Kind::Group => GROUP_CAP,
            Kind::Channel => CHANNEL_CAP,
        }
    }
}

/// A chat to create; users are indexes into the session pool.
#[derive(Debug, Clone)]
pub struct PlannedChat {
    pub kind: Kind,
    pub owner: usize,
    /// Members besides the owner.
    pub members: Vec<usize>,
}

impl PlannedChat {
    pub fn size(&self) -> usize {
        self.members.len() + 1
    }
}

#[derive(Debug, Clone)]
pub struct SeededChat {
    pub id: i64,
    pub kind: Kind,
    pub owner: usize,
    pub members: Vec<usize>,
}

/// Samples ranks 0..n with probability proportional to 1/(rank+1)^s.
pub struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    pub fn new(n: usize, s: f64) -> Zipf {
        let mut acc = 0.0;
        let mut cdf: Vec<f64> = (1..=n).map(|r| {
            acc += 1.0 / (r as f64).powf(s);
            acc
        }).collect();
        for c in &mut cdf {
            *c /= acc;
        }
        Zipf { cdf }
    }

    pub fn sample(&self, rng: &mut StdRng) -> usize {
        let u: f64 = rng.random();
        self.cdf.partition_point(|c| *c < u).min(self.cdf.len() - 1)
    }

    pub fn weight(&self, rank: usize) -> f64 {
        self.cdf[rank] - if rank == 0 { 0.0 } else { self.cdf[rank - 1] }
    }
}

/// Zipf-sized chats for `users` users: `users / 20` chats, each user in 3-8 of
/// them. Popular ranks are channels so the head can pass the group cap.
pub fn realistic_plan(users: usize, rng: &mut StdRng) -> Vec<PlannedChat> {
    let n = (users / 20).max(1);
    let zipf = Zipf::new(n, ZIPF_S);
    let avg_picks = 5.5;
    let head = (n / 100).max(1);
    let mut chats: Vec<PlannedChat> = (0..n)
        .map(|rank| {
            let expected = users as f64 * avg_picks * zipf.weight(rank);
            let channel = rank < head || expected > 0.8 * GROUP_CAP as f64 || rng.random_bool(0.15);
            PlannedChat { kind: if channel { Kind::Channel } else { Kind::Group }, owner: rng.random_range(0..users), members: Vec::new() }
        })
        .collect();
    let mut sizes: Vec<usize> = vec![1; n];
    for user in 0..users {
        let picks = rng.random_range(3..=8).min(n);
        let mut mine = HashSet::new();
        let mut attempts = 0;
        while mine.len() < picks && attempts < 200 {
            attempts += 1;
            let rank = zipf.sample(rng);
            let chat = &chats[rank];
            if chat.owner == user || mine.contains(&rank) || sizes[rank] >= chat.kind.cap() {
                continue;
            }
            mine.insert(rank);
        }
        for rank in mine {
            sizes[rank] += 1;
            chats[rank].members.push(user);
        }
    }
    chats
}

/// One chat owned by user 0 with users 1..size as members.
pub fn single_chat(kind: Kind, size: usize) -> Vec<PlannedChat> {
    vec![PlannedChat { kind, owner: 0, members: (1..size).collect() }]
}

/// `count` groups of `size` consecutive users each.
pub fn small_groups(count: usize, size: usize) -> Vec<PlannedChat> {
    (0..count)
        .map(|c| {
            let first = c * size;
            PlannedChat { kind: Kind::Group, owner: first, members: (first + 1..first + size).collect() }
        })
        .collect()
}

pub fn users_needed(plan: &[PlannedChat]) -> usize {
    plan.iter().flat_map(|c| c.members.iter().copied().chain([c.owner])).max().map_or(0, |m| m + 1)
}

/// Registers users until `pool` has `upto` sessions. Logins are `user<index>`.
pub async fn register(api: &Arc<Api>, pool: &mut Vec<Session>, upto: usize, concurrency: usize) -> Result<(), String> {
    let start = pool.len();
    if upto <= start {
        return Ok(());
    }
    let results: Vec<(usize, Result<Session, CallError>)> = stream::iter(start..upto)
        .map(|i| {
            let api = api.clone();
            async move {
                let mut last = Err(CallError::Transport);
                for _ in 0..3 {
                    last = api.register(&format!("user{i}")).await;
                    if last.is_ok() {
                        api.stats.registered.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
                (i, last)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;
    let mut sorted: Vec<Option<Session>> = vec![None; upto - start];
    for (i, r) in results {
        sorted[i - start] = Some(r.map_err(|e| format!("register user{i}: {e:?}"))?);
    }
    pool.extend(sorted.into_iter().map(|s| s.expect("every index registered")));
    Ok(())
}

/// Creates the planned chats and their memberships through the API.
pub async fn apply(api: &Arc<Api>, pool: &[Session], plan: Vec<PlannedChat>, tag: &str) -> Result<Vec<SeededChat>, String> {
    let created: Vec<Result<SeededChat, String>> = stream::iter(plan.into_iter().enumerate())
        .map(|(i, c)| {
            let api = api.clone();
            let owner = pool[c.owner].clone();
            async move {
                let id = api
                    .create(&owner, c.kind.as_str(), &format!("{tag}-{i}"))
                    .await
                    .map_err(|e| format!("create chat: {e:?}"))?;
                Ok(SeededChat { id, kind: c.kind, owner: c.owner, members: c.members })
            }
        })
        // `buffered` keeps plan order, so callers can index chats by plan position.
        .buffered(64)
        .collect()
        .await;
    let chats: Vec<SeededChat> = created.into_iter().collect::<Result<_, _>>()?;
    let joins: Vec<(i64, usize)> = chats.iter().flat_map(|c| c.members.iter().map(move |m| (c.id, *m))).collect();
    let failed: usize = stream::iter(joins)
        .map(|(chat, user)| {
            let api = api.clone();
            let s = pool[user].clone();
            async move { api.join(&s, chat).await.is_err() as usize }
        })
        .buffer_unordered(JOIN_CONCURRENCY)
        .fold(0, |acc, f| async move { acc + f })
        .await;
    if failed > 0 {
        return Err(format!("{failed} seeding joins failed"));
    }
    Ok(chats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn realistic_plan_respects_caps_and_is_skewed() {
        let mut rng = StdRng::seed_from_u64(42);
        let plan = realistic_plan(10_000, &mut rng);
        assert_eq!(plan.len(), 500);
        for c in &plan {
            assert!(c.size() <= c.kind.cap(), "{:?} has {}", c.kind, c.size());
            assert!(!c.members.contains(&c.owner));
        }
        let mut sizes: Vec<usize> = plan.iter().map(|c| c.size()).collect();
        sizes.sort_unstable_by(|a, b| b.cmp(a));
        let total: usize = sizes.iter().sum();
        let top: usize = sizes[..plan.len() / 100].iter().sum();
        assert!(top as f64 > 0.30 * total as f64, "top 1% hold {top} of {total}");
        assert!(plan.iter().any(|c| c.kind == Kind::Group));
        assert!(plan.iter().any(|c| c.kind == Kind::Channel));
    }

    #[test]
    fn every_user_has_chats() {
        let mut rng = StdRng::seed_from_u64(1);
        let plan = realistic_plan(2_000, &mut rng);
        let mut counts = vec![0; 2_000];
        for c in &plan {
            for m in c.members.iter().chain([&c.owner]) {
                counts[*m] += 1;
            }
        }
        assert!(counts.iter().all(|n| *n >= 3), "min {}", counts.iter().min().unwrap());
    }

    #[test]
    fn fixed_layouts() {
        let one = single_chat(Kind::Channel, 10_000);
        assert_eq!((one.len(), one[0].size(), users_needed(&one)), (1, 10_000, 10_000));
        let small = small_groups(3, 5);
        assert_eq!(small[2].owner, 10);
        assert_eq!(small[2].members, vec![11, 12, 13, 14]);
        assert_eq!(users_needed(&small), 15);
    }

    #[test]
    fn zipf_is_normalised() {
        let z = Zipf::new(100, ZIPF_S);
        let sum: f64 = (0..100).map(|r| z.weight(r)).sum();
        assert!((sum - 1.0).abs() < 1e-9);
        assert!(z.weight(0) > z.weight(1));
        let mut rng = StdRng::seed_from_u64(3);
        let hits = (0..10_000).filter(|_| z.sample(&mut rng) == 0).count();
        assert!((hits as f64 / 10_000.0 - z.weight(0)).abs() < 0.02);
    }
}
