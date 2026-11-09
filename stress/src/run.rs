//! Orchestrates a run: server, seeding, phases, per-second sampling, report.

use std::collections::VecDeque;
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::SeedableRng;
use rand::rngs::StdRng;
use serde_json::Value;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::api::{Api, Session};
use crate::cli::{Args, Phase};
use crate::net::{self, Ports};
use crate::ramp::{self, Evidence, Measured, Next, Ramp, Stop, Target};
use crate::report::{self, MaxRow, PhaseResult, Report, RunConfig, StepResult, Validity};
use crate::seed::{self, Kind};
use crate::server::ServerProc;
use crate::stats::{Counters, Ep, Interval, Stats, p50_p99};
use crate::sys::{self, CpuSampler};
use crate::ui::{self, Snapshot, Timeline};
use crate::user::{self, Env};

/// Most users phases A and B4 will register, whatever the port capacity.
const MAX_USERS: usize = 60_000;
const B1_POST_MHZ: u64 = 2_000; // channel owner: 2 msg/s
const B2_POST_MHZ: u64 = 1_000; // each group poster: 1 msg/s
const B3_CHATS: usize = 1_000;
const B3_SIZE: usize = 5;
/// `--smoke` limits: users seeded, B1/B3 sizes, steps per phase.
const SMOKE_USERS: usize = 1_000;
const SMOKE_B1_SIZE: usize = 300;
const SMOKE_B3_CHATS: usize = 50;
const SMOKE_STEPS: u32 = 2;

/// A set of virtual-user tasks that can grow or shrink (newest stop first).
pub struct Population {
    tasks: Vec<(watch::Sender<bool>, JoinHandle<()>)>,
    make: Box<dyn FnMut(usize, watch::Receiver<bool>) -> JoinHandle<()> + Send>,
}

impl Population {
    pub fn new(make: impl FnMut(usize, watch::Receiver<bool>) -> JoinHandle<()> + Send + 'static) -> Population {
        Population { tasks: Vec::new(), make: Box::new(make) }
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub async fn set(&mut self, n: usize) {
        while self.tasks.len() < n {
            let (tx, rx) = watch::channel(false);
            let i = self.tasks.len();
            let handle = (self.make)(i, rx);
            self.tasks.push((tx, handle));
        }
        let mut stopping = Vec::new();
        while self.tasks.len() > n {
            let (tx, handle) = self.tasks.pop().unwrap();
            let _ = tx.send(true);
            stopping.push(handle);
        }
        for mut h in stopping {
            if tokio::time::timeout(Duration::from_secs(15), &mut h).await.is_err() {
                h.abort();
            }
        }
    }
}

/// How each phase turns a load into running virtual users.
enum Driver {
    Users(Population),
    ChannelViewers { viewers: Population, owner: Population },
    GroupPosters { viewers: Population, posters: Population },
    WriteRate { viewers: Population, posters: Population, rate_mhz: Arc<AtomicU64>, chats: usize },
}

impl Driver {
    /// Applies `load` and returns the WebSocket connections it needs.
    async fn apply(&mut self, load: u64) -> usize {
        match self {
            Driver::Users(p) => {
                p.set(load as usize).await;
                load as usize
            }
            Driver::ChannelViewers { viewers, owner } => {
                owner.set(1).await;
                viewers.set(load as usize).await;
                load as usize
            }
            Driver::GroupPosters { viewers, posters } => {
                viewers.set(seed::GROUP_CAP).await;
                posters.set(load as usize).await;
                seed::GROUP_CAP
            }
            Driver::WriteRate { viewers, posters, rate_mhz, chats } => {
                viewers.set(*chats * B3_SIZE).await;
                // Total msg/s spread over one poster per chat: per poster = load / chats.
                rate_mhz.store(load * 1000 / *chats as u64, Ordering::Relaxed);
                posters.set(*chats).await;
                *chats * B3_SIZE
            }
        }
    }

    async fn clear(&mut self) {
        match self {
            Driver::Users(p) => p.set(0).await,
            Driver::ChannelViewers { viewers, owner } => {
                owner.set(0).await;
                viewers.set(0).await;
            }
            Driver::GroupPosters { viewers, posters } | Driver::WriteRate { viewers, posters, .. } => {
                posters.set(0).await;
                viewers.set(0).await;
            }
        }
    }
}

#[derive(Default, Clone)]
struct Live {
    phase: String,
    unit: String,
    step: u32,
    load: u64,
    state: String,
    window_s: u64,
    window_total_s: u64,
    last_pass: Option<u64>,
    first_fail: Option<u64>,
}

#[derive(Clone, Copy, Default)]
struct Second {
    shard_queue_max: u64,
    shard_queue_max_shard: u64,
    meta_queue: u64,
    server_cpu: f64,
    gen_cpu: f64,
    online: i64,
}

struct Window {
    counters_start: Counters,
    interval: Interval,
    seconds: Vec<Second>,
}

struct Shared {
    live: Live,
    window: Option<Window>,
    recent: VecDeque<Interval>,
    ports_peak: usize,
    gen_peak_share: f64,
}

struct Ctx {
    args: Args,
    env: Env,
    stats: Arc<Stats>,
    shared: Arc<Mutex<Shared>>,
    interrupted: Arc<AtomicBool>,
    machine_cores: usize,
    alias_hint: String,
}

fn n(v: &Value, path: &str) -> u64 {
    path.split('.').fold(v, |v, k| &v[k]).as_u64().unwrap_or(0)
}

fn timestamp() -> String {
    std::process::Command::new("date")
        .arg("+%Y%m%d-%H%M%S")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|_| "run".into())
}

/// Runs the whole stress test; returns the output directory.
pub async fn run(args: Args) -> Result<PathBuf, String> {
    let started = Instant::now();
    let started_at = timestamp();
    let out = args.out.clone().unwrap_or_else(|| PathBuf::from("stress-out").join(&started_at));
    std::fs::create_dir_all(&out).map_err(|e| format!("create {}: {e}", out.display()))?;
    let machine = sys::machine();
    let ports = Arc::new(Ports::detect());
    let alias_hint = net::alias_hint(&ports.sources);
    eprintln!(
        "chats-stress: {} source address(es), {} WebSocket ports, {} generator threads; output in {}",
        ports.sources.len(),
        ports.capacity(),
        args.gen_threads,
        out.display()
    );
    if ports.sources.len() == 1 {
        eprintln!("  (only 127.0.0.1: about {} connections max; `{alias_hint}` adds more)", ports.capacity());
    }

    let server = ServerProc::start(&args.server, args.addr, &out, args.keep_data).await?;
    let stats = Arc::new(Stats::default());
    let api = Arc::new(Api::new(server.addr, stats.clone()));
    let env = Env { api: api.clone(), ports: ports.clone(), seed: args.seed };
    let interrupted = Arc::new(AtomicBool::new(false));
    {
        let flag = interrupted.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                flag.store(true, Ordering::Relaxed);
            }
        });
    }
    let shared = Arc::new(Mutex::new(Shared {
        live: Live { state: "starting".into(), ..Default::default() },
        window: None,
        recent: VecDeque::new(),
        ports_peak: 0,
        gen_peak_share: 0.0,
    }));
    let ctx = Arc::new(Ctx {
        args: args.clone(),
        env,
        stats: stats.clone(),
        shared: shared.clone(),
        interrupted: interrupted.clone(),
        machine_cores: machine.cores.max(1),
        alias_hint: alias_hint.clone(),
    });
    let ticker = tokio::spawn(sample_loop(ctx.clone(), server.pid, out.join("stress-timeline.csv"), started));

    let mut pool: Vec<Session> = Vec::new();
    let mut phases = Vec::new();
    for phase in &args.phases {
        if interrupted.load(Ordering::Relaxed) {
            break;
        }
        match run_phase(&ctx, *phase, &mut pool).await {
            Ok(p) => phases.push(p),
            Err(e) => {
                ticker.abort();
                let _ = server.stop().await;
                return Err(format!("phase {}: {e}", phase.id()));
            }
        }
    }
    ticker.abort();
    let _ = ticker.await;
    if std::io::stdout().is_terminal() {
        println!();
    }
    let stop_result = server.stop().await;

    let (ports_peak, gen_peak) = {
        let s = shared.lock().unwrap();
        (s.ports_peak, s.gen_peak_share)
    };
    let report = build_report(&args, machine, &ports, &phases, started_at, started.elapsed().as_secs(), interrupted.load(Ordering::Relaxed), ports_peak, gen_peak, &alias_hint);
    report::write(&out, &report).map_err(|e| format!("write report: {e}"))?;
    stop_result?;
    eprintln!("\nreport: {}", out.join("stress-report.md").display());
    for row in &report.maximums {
        eprintln!("  {:<55} {:>24}  ({})", row.measure, row.value, row.limited_by);
    }
    Ok(out)
}

fn set_live(ctx: &Ctx, f: impl FnOnce(&mut Live)) {
    f(&mut ctx.shared.lock().unwrap().live);
}

async fn run_phase(ctx: &Arc<Ctx>, phase: Phase, pool: &mut Vec<Session>) -> Result<PhaseResult, String> {
    let api = &ctx.env.api;
    let ports = &ctx.env.ports;
    let concurrency = ctx.machine_cores * 2;
    set_live(ctx, |l| {
        *l = Live { phase: phase.id().into(), state: "seeding".into(), ..Default::default() };
    });
    let seed_start = Instant::now();
    let mut rng = StdRng::seed_from_u64(ctx.args.seed ^ phase as u64);
    let env = ctx.env.clone();
    let tag = phase.id().to_lowercase();
    let smoke = ctx.args.smoke;
    let max_users = if smoke { SMOKE_USERS } else { MAX_USERS };
    let b1_size = if smoke { SMOKE_B1_SIZE } else { seed::CHANNEL_CAP };
    let b3_chats = if smoke { SMOKE_B3_CHATS } else { B3_CHATS };

    let (mut driver, start, cap, limit, unit) = match phase {
        Phase::A => {
            let users = ports.capacity().min(max_users);
            seed::register(api, pool, users, concurrency).await?;
            seed::apply(api, pool, seed::realistic_plan(users, &mut rng), &tag).await?;
            let sessions = pool[..users].to_vec();
            let pop = Population::new(move |i, stop| tokio::spawn(user::realistic(env.clone(), i, sessions[i].clone(), stop)));
            (Driver::Users(pop), 1_000, None, Some(users as u64), "users")
        }
        Phase::B1 => {
            let size = b1_size.min(ports.capacity());
            seed::register(api, pool, size, concurrency).await?;
            let chat = seed::apply(api, pool, seed::single_chat(Kind::Channel, size), &tag).await?[0].id;
            let sessions = pool[..size].to_vec();
            let owner_session = pool[0].clone();
            let rate = Arc::new(AtomicU64::new(B1_POST_MHZ));
            let env2 = env.clone();
            let viewers = Population::new(move |i, stop| tokio::spawn(user::viewer(env.clone(), sessions[i].clone(), chat, None, stop)));
            let owner = Population::new(move |_, stop| tokio::spawn(user::poster(env2.clone(), owner_session.clone(), chat, rate.clone(), stop)));
            (Driver::ChannelViewers { viewers, owner }, 1_000, Some(b1_size as u64), Some(size as u64), "viewers")
        }
        Phase::B2 => {
            seed::register(api, pool, seed::GROUP_CAP, concurrency).await?;
            let chat = seed::apply(api, pool, seed::single_chat(Kind::Group, seed::GROUP_CAP), &tag).await?[0].id;
            let sessions = pool[..seed::GROUP_CAP].to_vec();
            let (s2, env2) = (sessions.clone(), env.clone());
            let rate = Arc::new(AtomicU64::new(B2_POST_MHZ));
            let viewers = Population::new(move |i, stop| tokio::spawn(user::viewer(env.clone(), sessions[i].clone(), chat, None, stop)));
            let posters = Population::new(move |i, stop| tokio::spawn(user::poster(env2.clone(), s2[i].clone(), chat, rate.clone(), stop)));
            (Driver::GroupPosters { viewers, posters }, 10, Some(seed::GROUP_CAP as u64), None, "posters")
        }
        Phase::B3 => {
            let users = b3_chats * B3_SIZE;
            seed::register(api, pool, users, concurrency).await?;
            let chats = seed::apply(api, pool, seed::small_groups(b3_chats, B3_SIZE), &tag).await?;
            let ids: Arc<Vec<i64>> = Arc::new(chats.iter().map(|c| c.id).collect());
            let sessions = pool[..users].to_vec();
            let (s2, ids2, env2) = (sessions.clone(), ids.clone(), env.clone());
            let rate = Arc::new(AtomicU64::new(0));
            let r2 = rate.clone();
            let viewers = Population::new(move |i, stop| tokio::spawn(user::viewer(env.clone(), sessions[i].clone(), ids[i / B3_SIZE], None, stop)));
            let posters = Population::new(move |c, stop| {
                tokio::spawn(user::poster(env2.clone(), s2[c * B3_SIZE].clone(), ids2[c], r2.clone(), stop))
            });
            (Driver::WriteRate { viewers, posters, rate_mhz: rate, chats: b3_chats }, 500, None, None, "msg/s")
        }
        Phase::B4 => {
            let users = ports.capacity().min(max_users);
            seed::register(api, pool, users, concurrency).await?;
            let sessions = pool[..users].to_vec();
            let pop = Population::new(move |i, stop| tokio::spawn(user::idle(env.clone(), sessions[i].clone(), stop)));
            (Driver::Users(pop), 2_000, None, Some(users as u64), "connections")
        }
    };
    let seed_s = seed_start.elapsed().as_secs_f64();
    set_live(ctx, |l| l.unit = unit.into());

    let start = if smoke { (start / 10).max(5) } else { start };
    let mut ramp = Ramp::new(start, cap, limit);
    let mut next = ramp.first();
    let mut steps = Vec::new();
    let mut evidence: Option<Evidence> = None;
    let mut step_no = 0;
    let stop = loop {
        match next {
            Next::Done(stop) => break stop,
            Next::Step(_) if smoke && step_no >= SMOKE_STEPS => break Stop::Failed,
            Next::Step(load) => {
                step_no += 1;
                set_live(ctx, |l| {
                    l.step = step_no;
                    l.load = load;
                    l.state = "ramping".into();
                });
                let conns = driver.apply(load).await;
                let Some((result, ev)) = measure_step(ctx, conns).await else { break Stop::Failed };
                next = ramp.record(load, result.passed);
                if !result.passed {
                    evidence = Some(ev);
                }
                steps.push(StepResult { load, ..result });
                set_live(ctx, |l| {
                    l.last_pass = ramp.last_pass;
                    l.first_fail = ramp.first_fail;
                });
            }
        }
    };
    set_live(ctx, |l| l.state = "stopping".into());
    driver.clear().await;
    // Let disconnects settle before the next phase.
    for _ in 0..100 {
        if ctx.stats.online.load(Ordering::Relaxed) == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let interrupted = ctx.interrupted.load(Ordering::Relaxed);
    let limited_by = if smoke && ramp.first_fail.is_none() && !matches!(stop, Stop::Cap | Stop::Ports) {
        "smoke run step limit".into()
    } else if interrupted && ramp.first_fail.is_none() && !matches!(stop, Stop::Cap | Stop::Ports) {
        "interrupted before a limit was found".into()
    } else {
        ramp::limiting_factor(stop, evidence.as_ref(), &ctx.alias_hint)
    };
    Ok(PhaseResult {
        id: phase.id().into(),
        title: phase.title().into(),
        unit: unit.into(),
        seed_s,
        steps,
        max: ramp.last_pass,
        limited_by,
    })
}

/// Waits for connections, warms up, measures one window. `None` if interrupted.
async fn measure_step(ctx: &Arc<Ctx>, conns: usize) -> Option<(StepResult, Evidence)> {
    let stats = &ctx.stats;
    let interrupted = || ctx.interrupted.load(Ordering::Relaxed);
    set_live(ctx, |l| l.state = "connecting".into());
    let deadline = Instant::now() + Duration::from_secs(60 + conns as u64 / 200);
    let want = (conns as f64 * 0.95) as i64;
    while stats.online.load(Ordering::Relaxed) < want && Instant::now() < deadline {
        if interrupted() {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    set_live(ctx, |l| {
        l.state = "warmup".into();
        l.window_s = 0;
        l.window_total_s = ctx.args.warmup_secs;
    });
    for s in 1..=ctx.args.warmup_secs {
        tokio::time::sleep(Duration::from_secs(1)).await;
        set_live(ctx, |l| l.window_s = s);
        if interrupted() {
            return None;
        }
    }

    stats.begin_window();
    let counters_start = stats.counters();
    {
        let mut sh = ctx.shared.lock().unwrap();
        sh.window = Some(Window { counters_start, interval: Interval::empty(), seconds: Vec::new() });
        sh.live.state = "measuring".into();
        sh.live.window_s = 0;
        sh.live.window_total_s = ctx.args.step_secs;
    }
    let started = Instant::now();
    let mut early = false;
    for s in 1..=ctx.args.step_secs {
        tokio::time::sleep(Duration::from_secs(1)).await;
        set_live(ctx, |l| l.window_s = s);
        if interrupted() {
            ctx.shared.lock().unwrap().window = None;
            return None;
        }
        if s == 10 {
            let c = stats.counters().since(&counters_start);
            if ramp::early_fail(c.error_rate(), c.attempts()) {
                early = true;
                break;
            }
        }
    }
    let window = ctx.shared.lock().unwrap().window.take()?;
    let secs = started.elapsed().as_secs_f64().max(1.0);
    let delta = stats.counters().since(&window.counters_start);
    let seconds = &window.seconds;
    let avg = |f: fn(&Second) -> f64| if seconds.is_empty() { 0.0 } else { seconds.iter().map(f).sum::<f64>() / seconds.len() as f64 };
    let online = avg(|s| s.online as f64);
    let delivery = p50_p99(&window.interval.delivery, 1.0);
    let http = p50_p99(&window.interval.http, 1000.0);
    let measured = Measured {
        delivery_p99_ms: delivery.map(|d| d.1),
        http_p99_ms: http.map(|h| h.1),
        error_rate: delta.error_rate(),
        resync_rate: if online > 0.0 { stats.window_resynced() as f64 / online } else { 0.0 },
    };
    let mut failed = ramp::evaluate(&measured);
    if early && !failed.contains(&Target::Errors) {
        failed.push(Target::Errors);
    }
    let queue_max = seconds.iter().map(|s| s.shard_queue_max).max().unwrap_or(0);
    let queue_shard = seconds.iter().max_by_key(|s| s.shard_queue_max).map_or(0, |s| s.shard_queue_max_shard);
    let third = seconds.len() / 3;
    let growing = third > 0 && {
        let first: u64 = seconds[..third].iter().map(|s| s.shard_queue_max).sum();
        let last: u64 = seconds[seconds.len() - third..].iter().map(|s| s.shard_queue_max).sum();
        last > 100 * third as u64 && last > first * 2
    };
    let gen_cpu = avg(|s| s.gen_cpu);
    let server_cpu = avg(|s| s.server_cpu);
    let budget = ctx.args.gen_threads as f64 * 100.0;
    {
        let mut sh = ctx.shared.lock().unwrap();
        let peak = seconds.iter().map(|s| s.gen_cpu / budget).fold(0.0, f64::max);
        sh.gen_peak_share = sh.gen_peak_share.max(peak);
    }
    let evidence = Evidence {
        failed: failed.clone(),
        gen_cpu_share: gen_cpu / budget,
        server_cpu_share: server_cpu / (ctx.machine_cores as f64 * 100.0),
        shard_queue_max: queue_max,
        shard_queue_max_shard: queue_shard,
        shard_queue_growing: growing,
        meta_queue_max: seconds.iter().map(|s| s.meta_queue).max().unwrap_or(0),
    };
    let result = StepResult {
        load: 0,
        online,
        posts_per_s: delta.accepted(Ep::Post) as f64 / secs,
        frames_per_s: delta.frames as f64 / secs,
        reads_per_s: delta.reads() as f64 / secs,
        delivery_p50_ms: delivery.map(|d| d.0),
        delivery_p99_ms: delivery.map(|d| d.1),
        http_p50_ms: http.map(|h| h.0),
        http_p99_ms: http.map(|h| h.1),
        error_rate: measured.error_rate,
        resync_rate: measured.resync_rate,
        server_cpu,
        gen_cpu,
        shard_queue_max: queue_max,
        meta_queue_max: evidence.meta_queue_max,
        passed: failed.is_empty(),
        reasons: failed.iter().map(|t| t.describe(&measured)).collect(),
        early,
    };
    Some((result, evidence))
}

/// Once a second: drain statistics, poll server metrics and CPU, redraw, log.
async fn sample_loop(ctx: Arc<Ctx>, server_pid: u32, timeline_path: PathBuf, started: Instant) {
    let mut timeline = Timeline::create(&timeline_path).ok();
    let mut sampler = CpuSampler::default();
    let gen_pid = std::process::id();
    let tty = std::io::stdout().is_terminal();
    let stats = ctx.stats.clone();
    let api = ctx.env.api.clone();
    let ports = ctx.env.ports.clone();
    let mut prev = stats.counters();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut n_tick: u64 = 0;
    loop {
        tick.tick().await;
        n_tick += 1;
        let interval = stats.drain();
        let counters = stats.counters();
        let delta = counters.since(&prev);
        prev = counters;
        let metrics = tokio::time::timeout(Duration::from_secs(2), api.metrics()).await.ok().flatten().unwrap_or(Value::Null);
        let usage = tokio::task::spawn_blocking({
            let mut s = std::mem::take(&mut sampler);
            move || {
                let u = s.sample(&[server_pid, gen_pid]);
                (s, u)
            }
        })
        .await;
        let usage = match usage {
            Ok((s, u)) => {
                sampler = s;
                u
            }
            Err(_) => Default::default(),
        };
        let srv = usage.get(&server_pid).copied().unwrap_or_default();
        let generator = usage.get(&gen_pid).copied().unwrap_or_default();
        let online = stats.online.load(Ordering::Relaxed);
        let second = Second {
            shard_queue_max: n(&metrics, "writers.shards.queue_max"),
            shard_queue_max_shard: n(&metrics, "writers.shards.queue_max_shard"),
            meta_queue: n(&metrics, "writers.meta.queue"),
            server_cpu: srv.cpu_percent,
            gen_cpu: generator.cpu_percent,
            online,
        };

        let (live, latency) = {
            let mut sh = ctx.shared.lock().unwrap();
            sh.ports_peak = sh.ports_peak.max(ports.used());
            if let Some(w) = sh.window.as_mut() {
                w.interval.add(&interval);
                w.seconds.push(second);
            }
            sh.recent.push_back(interval);
            if sh.recent.len() > 5 {
                sh.recent.pop_front();
            }
            let latency = match &sh.window {
                Some(w) => latencies(&w.interval),
                None => {
                    let mut acc = Interval::empty();
                    for i in &sh.recent {
                        acc.add(i);
                    }
                    latencies(&acc)
                }
            };
            (sh.live.clone(), latency)
        };
        let max_online = |kind: &str, field: &str| metrics["chats"]["max_online"][kind][field].as_u64().unwrap_or(0);
        let snap = Snapshot {
            t_s: started.elapsed().as_secs(),
            phase: live.phase.clone(),
            step: live.step,
            load: live.load,
            unit: live.unit.clone(),
            state: live.state.clone(),
            window_s: live.window_s,
            window_total_s: live.window_total_s,
            online,
            connecting: stats.connecting.load(Ordering::Relaxed),
            registered: stats.registered.load(Ordering::Relaxed),
            groups: n(&metrics, "chats.groups"),
            channels: n(&metrics, "chats.channels"),
            max_members_group: n(&metrics, "chats.max_members.group"),
            max_members_channel: n(&metrics, "chats.max_members.channel"),
            max_online_group: max_online("group", "online"),
            max_online_group_id: max_online("group", "id") as i64,
            max_online_channel: max_online("channel", "online"),
            max_online_channel_id: max_online("channel", "id") as i64,
            posts_per_s: delta.accepted(Ep::Post) as f64,
            frames_per_s: delta.frames as f64,
            reads_per_s: delta.reads() as f64,
            joins_per_s: delta.accepted(Ep::Join) as f64,
            creates_per_s: delta.accepted(Ep::Create) as f64,
            delivery_p50_ms: latency.delivery.map(|v| v.0),
            delivery_p99_ms: latency.delivery.map(|v| v.1),
            http_p50_ms: latency.http.map(|v| v.0),
            http_p99_ms: latency.http.map(|v| v.1),
            post_p99_ms: latency.post,
            history_p99_ms: latency.history,
            home_p99_ms: latency.home,
            error_rate: delta.error_rate(),
            resync_rate: if online > 0 { stats.window_resynced() as f64 / online as f64 } else { 0.0 },
            shard_queue_max: second.shard_queue_max,
            shard_queue_max_shard: second.shard_queue_max_shard,
            meta_queue: second.meta_queue,
            server_cpu: srv.cpu_percent,
            server_rss_mb: srv.rss_bytes as f64 / 1_048_576.0,
            gen_cpu: generator.cpu_percent,
            gen_budget: ctx.args.gen_threads as f64 * 100.0,
            ports_used: ports.used(),
            ports_capacity: ports.capacity(),
            sources: ports.sources.len(),
            last_pass: live.last_pass,
            first_fail: live.first_fail,
        };
        if tty {
            let mut out = std::io::stdout().lock();
            let _ = writeln!(out, "\x1b[H\x1b[2J{}", ui::panel(&snap));
            let _ = out.flush();
        } else if n_tick.is_multiple_of(5) {
            println!("{}", ui::line(&snap));
        }
        if let Some(t) = timeline.as_mut() {
            let _ = t.write(&snap);
        }
    }
}

struct Latencies {
    delivery: Option<(f64, f64)>,
    http: Option<(f64, f64)>,
    post: Option<f64>,
    history: Option<f64>,
    home: Option<f64>,
}

fn latencies(i: &Interval) -> Latencies {
    let p99 = |ep: Ep| p50_p99(i.ep(ep), 1000.0).map(|v| v.1);
    Latencies {
        delivery: p50_p99(&i.delivery, 1.0),
        http: p50_p99(&i.http, 1000.0),
        post: p99(Ep::Post),
        history: p99(Ep::History),
        home: p99(Ep::Home),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_report(
    args: &Args,
    machine: sys::Machine,
    ports: &Ports,
    phases: &[PhaseResult],
    started_at: String,
    duration_s: u64,
    interrupted: bool,
    ports_peak: usize,
    gen_peak_share: f64,
    alias_hint: &str,
) -> Report {
    let maximums: Vec<MaxRow> = phases
        .iter()
        .map(|p| {
            let (measure, value) = match p.id.as_str() {
                "A" => ("Most online users (realistic mix)", p.max.map(|m| format!("{m} users"))),
                "B1" => ("Most online viewers in one channel", p.max.map(|m| format!("{m} viewers"))),
                "B2" => (
                    "Most online members / concurrent posters in one group",
                    p.max.map(|m| format!("{} members / {m} posters", seed::GROUP_CAP)),
                ),
                "B3" => (
                    "Highest sustained messages per second",
                    p.steps
                        .iter()
                        .filter(|s| s.passed)
                        .map(|s| s.posts_per_s)
                        .fold(None, |a: Option<f64>, v| Some(a.map_or(v, |a| a.max(v))))
                        .map(|v| format!("{v:.0} msg/s")),
                ),
                _ => ("Most idle connections", p.max.map(|m| format!("{m} connections"))),
            };
            MaxRow {
                measure: measure.into(),
                phase: p.id.clone(),
                value: value.unwrap_or_else(|| "none passed".into()),
                limited_by: p.limited_by.clone(),
            }
        })
        .collect();
    let mut notes = Vec::new();
    if args.server.components().any(|c| c.as_os_str() == "debug") {
        notes.push("The server was a debug build; numbers are far below release performance.".into());
    }
    if phases.iter().any(|p| p.limited_by.starts_with("OS ephemeral ports")) {
        notes.push(format!("Some maximums hit the port limit, not the server; `{alias_hint}` adds another source address."));
    }
    if interrupted {
        notes.push("The run was interrupted with Ctrl-C.".into());
    }
    Report {
        started_at,
        duration_s,
        interrupted,
        machine: machine.into(),
        config: RunConfig {
            server_binary: args.server.display().to_string(),
            gen_threads: args.gen_threads,
            step_secs: args.step_secs,
            warmup_secs: args.warmup_secs,
            seed: args.seed,
            sources: ports.sources.iter().map(|s| s.to_string()).collect(),
            port_capacity: ports.capacity(),
        },
        validity: Validity {
            gen_cpu_peak_share: gen_peak_share,
            ports_peak,
            port_capacity: ports.capacity(),
            generator_bound: phases.iter().filter(|p| p.limited_by.starts_with("load generator")).map(|p| p.id.clone()).collect(),
            notes,
        },
        maximums,
        phases: phases.to_vec(),
    }
}
