use actorplane_core::{Config, EndpointKind, Error, Payload, World};
use std::{
    thread,
    time::{Duration, Instant},
};

fn percentile(v: &[u128], p: f64) -> u128 {
    if v.is_empty() {
        return 0;
    }
    v[((v.len() as f64 * p).ceil() as usize)
        .saturating_sub(1)
        .min(v.len() - 1)]
}

fn main() {
    const N: usize = 10_000;
    const WATCHDOG: Duration = Duration::from_secs(30);
    let cfg = Config {
        mailbox_capacity: 128,
        mailbox_bytes: 64 * 1024,
        native_payload_budget: 2 * 1024 * 1024,
        ..Config::default()
    };
    let world = World::new(cfg.clone()).unwrap();
    let target = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(target).unwrap();
    let base = Instant::now();
    let deadline = base + WATCHDOG;
    let consumer_world = world.clone();
    let consumer = thread::spawn(move || {
        let mut samples = Vec::with_capacity(N);
        while samples.len() < N && Instant::now() < deadline {
            if let Ok(Some(lease)) = consumer_world.claim(target) {
                if let Payload::Pulse(stamp) = *lease.payload() {
                    samples.push(
                        base.elapsed()
                            .as_nanos()
                            .saturating_sub(stamp.max(0) as u128),
                    );
                }
                lease.finish(true);
            } else {
                thread::yield_now();
            }
        }
        samples
    });
    let started = Instant::now();
    let mut rejected = 0usize;
    for _ in 0..N {
        loop {
            if Instant::now() >= deadline {
                panic!("benchmark watchdog expired while admitting event");
            }
            let stamp = base.elapsed().as_nanos() as i64;
            match world.send(target, Payload::Pulse(stamp)) {
                Ok(_) => break,
                Err(Error::QueueFull) => {
                    rejected += 1;
                    thread::yield_now();
                }
                Err(e) => panic!("unexpected admission error: {e}"),
            }
        }
    }
    let samples = consumer.join().expect("consumer thread panicked");
    assert_eq!(samples.len(), N, "consumer watchdog expired");
    let elapsed = started.elapsed();
    let mut sorted = samples;
    sorted.sort_unstable();
    let metrics = world.snapshot().metrics;
    assert_eq!(metrics.submitted, metrics.admitted + metrics.rejected);
    assert_eq!(metrics.admitted as usize, N);
    assert_eq!(metrics.completed as usize, N);
    assert_eq!(metrics.rejected as usize, rejected);
    let stop_started = Instant::now();
    let report = world.stop(target).unwrap();
    let stop_ns = stop_started.elapsed().as_nanos();
    assert!(report.native_done);
    let closed = world.close();
    assert!(closed.native_done && closed.python_done);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    println!(
        "workload=pulse_send_claim_finish events={N} completed={} rejected_retries={rejected} throughput_eps={:.1} latency_admission_attempt_to_claim_p50_ns={} p95_ns={} p99_ns={} max_ns={} stop_ns={} mailbox_capacity={} mailbox_bytes={} native_payload_budget={} retained_bytes={}",
        sorted.len(),
        N as f64 / elapsed.as_secs_f64(),
        percentile(&sorted, 0.50),
        percentile(&sorted, 0.95),
        percentile(&sorted, 0.99),
        sorted.last().copied().unwrap_or(0),
        stop_ns,
        cfg.mailbox_capacity,
        cfg.mailbox_bytes,
        cfg.native_payload_budget,
        world.snapshot().retained_payload_bytes
    );
}
