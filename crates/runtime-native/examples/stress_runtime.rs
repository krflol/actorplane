use actorplane_core::{Config, EndpointKind, Payload, PublicationStatus, World};
use actorplane_native::NativeRuntime;
use std::collections::VecDeque;
use std::env;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

const TICKET_WINDOW: usize = 128;

#[derive(Default)]
struct Counters {
    submitted: AtomicU64,
    rejected: AtomicU64,
    claimed: AtomicU64,
    duplicate_claims: AtomicU64,
}

#[derive(Debug)]
struct WorkloadResult {
    world_id: u64,
    rounds: u64,
    submitted: u64,
    rejected: u64,
    claimed: u64,
    duplicate_claims: u64,
    ticket_errors: u64,
    rejected_routes: u64,
    rejected_ingress: u64,
}

fn next_seed(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *seed
}

fn run_world(seed: u64, duration: Duration, ci: bool) -> Result<WorkloadResult, String> {
    let config = Config {
        max_actors: 8,
        max_subscriptions: 8,
        mailbox_capacity: 8,
        mailbox_bytes: 256,
        native_payload_budget: 512,
        max_publications: 256,
        max_publications_per_source: 128,
        max_publication_fanout: 4,
        max_routing_snapshot_entries: 4,
        routing_batch_size: 1,
        ..Config::default()
    };
    let max_publications = config.max_publications;
    let max_payload_budget = config.native_payload_budget;
    let world = World::new(config.clone()).map_err(|error| error.to_string())?;
    let mut runtime = NativeRuntime::new(world.clone(), 16)?;
    let sources = [
        world
            .allocate(EndpointKind::Native, None)
            .map_err(|error| error.to_string())?,
        world
            .allocate(EndpointKind::Native, None)
            .map_err(|error| error.to_string())?,
    ];
    let consumer = world
        .allocate(EndpointKind::Native, None)
        .map_err(|error| error.to_string())?;
    let slow = world
        .allocate(EndpointKind::Native, None)
        .map_err(|error| error.to_string())?;
    for actor in [sources[0], sources[1], consumer, slow] {
        world.activate(actor).map_err(|error| error.to_string())?;
    }
    for source in sources {
        world
            .subscribe(source, consumer)
            .map_err(|error| error.to_string())?;
        world
            .subscribe(source, slow)
            .map_err(|error| error.to_string())?;
    }

    let stop = Arc::new(AtomicBool::new(false));
    let source_stop = [
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    ];
    let counters = Arc::new(Counters::default());
    let tickets = Arc::new(Mutex::new(VecDeque::with_capacity(TICKET_WINDOW)));
    let mut publishers = Vec::new();
    for (index, source) in sources.into_iter().enumerate() {
        let world = world.clone();
        let stop = stop.clone();
        let source_stop = source_stop[index].clone();
        let counters = counters.clone();
        let tickets = tickets.clone();
        publishers.push(thread::spawn(move || {
            let mut state = seed ^ ((index as u64 + 1) * 0x9e37_79b9);
            let mut sequence = 0_u64;
            while !stop.load(Ordering::Acquire) && !source_stop.load(Ordering::Acquire) {
                let _ = next_seed(&mut state);
                sequence = sequence.saturating_add(1);
                let value = ((index as i64) << 48) | sequence as i64;
                match world.publish(source, Payload::Pulse(value)) {
                    Ok(ticket) => {
                        counters.submitted.fetch_add(1, Ordering::Relaxed);
                        let mut retained = tickets.lock().unwrap();
                        retained.push_back(ticket);
                        while retained.len() > TICKET_WINDOW {
                            let _ = retained.pop_front();
                        }
                    }
                    Err(_) => {
                        counters.rejected.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if next_seed(&mut state) & 0x3f == 0 {
                    thread::yield_now();
                }
            }
        }));
    }

    let consumer_stop = stop.clone();
    let consumer_world = world.clone();
    let consumer_counters = counters.clone();
    let consumer_thread = thread::spawn(move || {
        let mut last_sequence = [0_u64; 2];
        while !consumer_stop.load(Ordering::Acquire) {
            match consumer_world.claim(consumer) {
                Ok(Some(lease)) => {
                    let valid = match lease.payload() {
                        Payload::Pulse(value) if *value >= 0 => {
                            let source = (*value as u64 >> 48) as usize;
                            let sequence = *value as u64 & ((1 << 48) - 1);
                            source < last_sequence.len() && sequence > last_sequence[source]
                        }
                        _ => false,
                    };
                    if !valid {
                        consumer_counters
                            .duplicate_claims
                            .fetch_add(1, Ordering::Relaxed);
                    } else if let Payload::Pulse(value) = lease.payload() {
                        last_sequence[(*value as u64 >> 48) as usize] =
                            *value as u64 & ((1 << 48) - 1);
                    }
                    consumer_counters.claimed.fetch_add(1, Ordering::Relaxed);
                    lease.finish(true);
                }
                Ok(None) => thread::yield_now(),
                Err(_) => break,
            }
        }
        // Drain already-admitted consumer work after publishers stop.
        for _ in 0..100_000 {
            match consumer_world.claim(consumer) {
                Ok(Some(lease)) => {
                    let valid = match lease.payload() {
                        Payload::Pulse(value) if *value >= 0 => {
                            let source = (*value as u64 >> 48) as usize;
                            let sequence = *value as u64 & ((1 << 48) - 1);
                            source < last_sequence.len() && sequence > last_sequence[source]
                        }
                        _ => false,
                    };
                    if !valid {
                        consumer_counters
                            .duplicate_claims
                            .fetch_add(1, Ordering::Relaxed);
                    } else if let Payload::Pulse(value) = lease.payload() {
                        last_sequence[(*value as u64 >> 48) as usize] =
                            *value as u64 & ((1 << 48) - 1);
                    }
                    consumer_counters.claimed.fetch_add(1, Ordering::Relaxed);
                    lease.finish(true);
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
    });

    let started = Instant::now();
    let drain_at = started + duration / 2;
    let deadline = started + duration;
    let mut drained = false;
    let mut cancelled = false;
    let mut samples = 0;
    while Instant::now() < deadline {
        samples += 1;
        if !drained && Instant::now() >= drain_at {
            source_stop[0].store(true, Ordering::Release);
            world
                .request_drain(sources[0], Instant::now() + Duration::from_secs(2))
                .map_err(|error| error.to_string())?;
            drained = true;
        }
        if !cancelled && Instant::now() >= started + duration * 3 / 4 {
            // Keep the publisher running briefly across the stop fence. Its
            // later attempts must reject while the other source drains.
            world.stop(sources[1]).map_err(|error| error.to_string())?;
            cancelled = true;
        }
        if samples & 0x3ff == 0 {
            let snapshot = world.snapshot();
            let ticket_count = tickets.lock().unwrap().len();
            if snapshot.actors.len() > config.max_actors
                || snapshot.publications_pending + snapshot.publications_retained > max_publications
                || snapshot.retained_payload_bytes > max_payload_budget
                || snapshot.routing_snapshot_entries > config.max_routing_snapshot_entries
                || snapshot.actors.iter().any(|actor| {
                    actor.queue_entries > config.mailbox_capacity
                        || actor.queue_bytes > config.mailbox_bytes
                })
                || ticket_count > TICKET_WINDOW
            {
                return Err(format!("world {} exceeded bounded load state", world.id()));
            }
            thread::yield_now();
        }
    }
    stop.store(true, Ordering::Release);
    source_stop[1].store(true, Ordering::Release);
    for publisher in publishers {
        publisher
            .join()
            .map_err(|_| "publisher panicked".to_string())?;
    }
    consumer_thread
        .join()
        .map_err(|_| "consumer panicked".to_string())?;

    world.stop(sources[1]).map_err(|error| error.to_string())?;
    if !runtime.wait_native_idle(Duration::from_secs(2)) {
        return Err(format!("world {} did not become idle", world.id()));
    }
    world.maintain(Instant::now());
    for source in sources {
        if !matches!(world.state(source), Ok(actorplane_core::Lifecycle::Stopped)) {
            return Err(format!("world {} source did not stop", world.id()));
        }
    }
    let replacement = world
        .allocate(EndpointKind::Native, None)
        .map_err(|error| error.to_string())?;
    if replacement.slot != sources[1].slot || replacement.generation == sources[1].generation {
        return Err(format!("world {} failed source slot reuse", world.id()));
    }
    world
        .activate(replacement)
        .map_err(|error| error.to_string())?;
    world.stop(replacement).map_err(|error| error.to_string())?;

    let snapshot = world.snapshot();
    let metrics = snapshot.metrics.clone();
    let rejected_routes = metrics
        .rejected
        .checked_sub(metrics.publication_rejected)
        .ok_or("rejection counters violated conservation")?;
    if metrics.publication_admitted != counters.submitted.load(Ordering::Relaxed)
        || metrics.publication_rejected != counters.rejected.load(Ordering::Relaxed)
        || metrics.publication_admitted
            != metrics.publication_completed
                + metrics.publication_cancelled
                + metrics.publication_expired
                + metrics.publication_failed
        || snapshot
            .actors
            .iter()
            .find(|actor| actor.reference == slow)
            .map(|actor| actor.queue_entries)
            != Some(config.mailbox_capacity)
    {
        return Err(format!(
            "world {} ingress/terminal accounting or slow-consumer pressure failed: {snapshot:?}",
            world.id()
        ));
    }
    if rejected_routes == 0 {
        return Err(format!(
            "world {} slow consumer never rejected a route",
            world.id()
        ));
    }
    let tickets = std::mem::take(&mut *tickets.lock().unwrap());
    let mut ticket_errors = 0;
    for ticket in tickets {
        match ticket.status() {
            PublicationStatus::Terminal(report)
                if report.admitted.saturating_add(report.rejected) == report.matched => {}
            _ => ticket_errors += 1,
        }
    }
    let report = runtime.close(Duration::from_secs(2))?;
    if !report.native_done || report.timed_out {
        return Err(format!("world {} close report: {report:?}", world.id()));
    }
    let final_snapshot = world.snapshot();
    if final_snapshot.retained_payload_bytes != 0
        || final_snapshot.publications_pending != 0
        || final_snapshot.publications_retained != 0
    {
        return Err(format!(
            "world {} retained resources: {:?}",
            world.id(),
            final_snapshot
        ));
    }
    if ci && counters.submitted.load(Ordering::Relaxed) == 0 {
        return Err(format!("world {} made no submissions", world.id()));
    }
    if counters.claimed.load(Ordering::Relaxed) == 0 {
        return Err(format!("world {} consumer made no progress", world.id()));
    }
    Ok(WorkloadResult {
        world_id: world.id(),
        rounds: 1,
        submitted: counters.submitted.load(Ordering::Relaxed),
        rejected: counters.rejected.load(Ordering::Relaxed),
        claimed: counters.claimed.load(Ordering::Relaxed),
        duplicate_claims: counters.duplicate_claims.load(Ordering::Relaxed),
        ticket_errors,
        rejected_routes,
        rejected_ingress: snapshot.metrics.publication_rejected,
    })
}

fn parse_args() -> Result<(bool, bool, u64), String> {
    let mut ci = false;
    let mut soak = false;
    let mut seed = 0x51ed_5eed;
    for arg in env::args().skip(1) {
        match arg.as_str() {
            "--ci" if !ci => ci = true,
            "--soak" if !soak => soak = true,
            value if value.starts_with("--seed=") => {
                let raw = value.trim_start_matches("--seed=");
                let digits = raw
                    .strip_prefix("0x")
                    .or_else(|| raw.strip_prefix("0X"))
                    .unwrap_or(raw);
                if digits.is_empty() {
                    return Err("--seed requires a nonempty hexadecimal value".into());
                }
                seed = u64::from_str_radix(digits, 16)
                    .map_err(|_| format!("invalid hexadecimal seed: {raw}"))?;
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    if ci && soak {
        return Err("--ci and --soak are mutually exclusive".into());
    }
    Ok((ci, soak, seed))
}

fn main() {
    let (ci, soak, seed) = match parse_args() {
        Ok(parsed) => parsed,
        Err(error) => {
            eprintln!("{error}\nusage: stress_runtime [--ci | --soak] [--seed=HEX]");
            std::process::exit(2);
        }
    };
    let duration = if soak {
        Duration::from_secs(60)
    } else if ci {
        Duration::from_secs(2)
    } else {
        Duration::from_secs(5)
    };
    println!(
        "stress seed=0x{seed:x} duration={duration:?} mailbox=8 payload_bytes=512 max_publications=256 per_source=128 batch=1 snapshot_entries=4 ticket_window={TICKET_WINDOW}"
    );
    let started = Instant::now();
    let overall_deadline = started + duration;
    let done = Arc::new(AtomicBool::new(false));
    let watchdog_done = done.clone();
    let watchdog_duration = duration + Duration::from_secs(10);
    thread::spawn(move || {
        thread::sleep(watchdog_duration);
        if !watchdog_done.load(Ordering::Acquire) {
            eprintln!("stress watchdog expired after {watchdog_duration:?}");
            std::process::exit(2);
        }
    });
    let mut workers = Vec::new();
    for index in 0..2 {
        let worker_seed = seed.wrapping_add(index as u64);
        workers.push(thread::spawn(move || {
            let round_duration = if ci {
                Duration::from_millis(250)
            } else {
                Duration::from_millis(500)
            };
            let mut rounds = 0;
            let mut aggregate = WorkloadResult {
                world_id: 0,
                rounds: 0,
                submitted: 0,
                rejected: 0,
                claimed: 0,
                duplicate_claims: 0,
                ticket_errors: 0,
                rejected_routes: 0,
                rejected_ingress: 0,
            };
            while Instant::now() < overall_deadline {
                let result = run_world(worker_seed.wrapping_add(rounds), round_duration, ci)?;
                aggregate.world_id = result.world_id;
                aggregate.rounds += 1;
                aggregate.submitted += result.submitted;
                aggregate.rejected += result.rejected;
                aggregate.claimed += result.claimed;
                aggregate.duplicate_claims += result.duplicate_claims;
                aggregate.ticket_errors += result.ticket_errors;
                aggregate.rejected_routes += result.rejected_routes;
                aggregate.rejected_ingress += result.rejected_ingress;
                rounds += 1;
            }
            if aggregate.rounds == 0 {
                return Err("worker completed no lifecycle rounds".to_string());
            }
            Ok(aggregate)
        }));
    }
    let mut results = Vec::new();
    for worker in workers {
        match worker.join() {
            Ok(Ok(result)) => results.push(result),
            Ok(Err(error)) => {
                eprintln!("stress failure: {error}; seed=0x{seed:x} duration={duration:?}");
                std::process::exit(1);
            }
            Err(_) => {
                eprintln!("stress worker panicked; seed=0x{seed:x}");
                std::process::exit(1);
            }
        }
    }
    for result in &results {
        println!(
            "world={} rounds={} submitted={} rejected_ingress={} claimed={} duplicate_claims={} ticket_errors={} rejected_routes={}",
            result.world_id,
            result.rounds,
            result.submitted,
            result.rejected,
            result.claimed,
            result.duplicate_claims,
            result.ticket_errors,
            result.rejected_routes,
        );
        if result.claimed == 0 || result.duplicate_claims != 0 || result.ticket_errors != 0 {
            eprintln!("stress oracle failure: {result:?}");
            std::process::exit(1);
        }
    }
    done.store(true, Ordering::Release);
    println!(
        "stress completed in {:?} ({})",
        started.elapsed(),
        if soak {
            "soak"
        } else if ci {
            "ci"
        } else {
            "default"
        }
    );
}
