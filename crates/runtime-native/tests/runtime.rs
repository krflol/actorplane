use actorplane_core::{Config, EndpointKind, MessageOptions, Payload, TraceContext, World};
use actorplane_native::NativeRuntime;
use std::time::{Duration, Instant};

fn world() -> World {
    World::new(Config {
        mailbox_capacity: 4,
        mailbox_bytes: 256,
        ..Config::default()
    })
    .unwrap()
}

#[test]
fn native_pipeline_makes_progress_and_completes_leases() {
    let w = world();
    let mut rt = NativeRuntime::new(w.clone(), 8).unwrap();
    let p = rt
        .start_counter(
            None,
            Duration::from_millis(2),
            Duration::from_millis(20),
            None,
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));
    let s = p.snapshot();
    assert!(s.generated > 0 && s.processed > 0 && s.sink_received > 0);
    assert!(w.snapshot().metrics.completed > 0);
    p.stop();
    assert!(rt.wait_native_idle(Duration::from_secs(1)));
    rt.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn stopping_one_pipeline_does_not_stop_another() {
    let w = world();
    let mut rt = NativeRuntime::new(w, 8).unwrap();
    let a = rt
        .start_counter(
            None,
            Duration::from_millis(3),
            Duration::from_millis(20),
            None,
        )
        .unwrap();
    let b = rt
        .start_counter(
            None,
            Duration::from_millis(3),
            Duration::from_millis(20),
            None,
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(60));
    a.stop();
    let before = b.snapshot().sink_received;
    std::thread::sleep(Duration::from_millis(60));
    assert!(b.snapshot().sink_received > before);
    rt.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn insufficient_tasks_rolls_back_allocations() {
    let w = world();
    let mut rt = NativeRuntime::new(w.clone(), 2).unwrap();
    assert!(
        rt.start_counter(
            None,
            Duration::from_millis(1),
            Duration::from_millis(5),
            None
        )
        .is_err()
    );
    assert!(
        w.snapshot()
            .actors
            .iter()
            .all(|a| matches!(a.state, actorplane_core::Lifecycle::Stopped))
    );
    rt.close(Duration::from_millis(100)).unwrap();
}

#[test]
fn repeated_stop_reclaims_actor_slots() {
    let w = world();
    let mut rt = NativeRuntime::new(w.clone(), 8).unwrap();
    for _ in 0..5 {
        let p = rt
            .start_counter(
                None,
                Duration::from_millis(2),
                Duration::from_millis(10),
                None,
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(15));
        p.stop();
        assert!(rt.wait_native_idle(Duration::from_secs(1)));
        assert!(
            w.snapshot()
                .actors
                .iter()
                .all(|a| matches!(a.state, actorplane_core::Lifecycle::Stopped))
        );
    }
    rt.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn after_waits_for_active_owner() {
    let w = world();
    let mut rt = NativeRuntime::new(w.clone(), 2).unwrap();
    let owner = w.allocate(EndpointKind::Native, None).unwrap();
    let target = w.allocate(EndpointKind::Native, None).unwrap();
    rt.after(owner, target, Duration::from_millis(5), Payload::Pulse(1))
        .unwrap();
    std::thread::sleep(Duration::from_millis(20));
    assert!(w.snapshot().metrics.admitted == 0);
    w.activate(owner).unwrap();
    w.activate(target).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while w.snapshot().metrics.admitted == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(w.snapshot().metrics.admitted > 0);
    rt.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn cancelled_timer_releases_retained_payload() {
    let w = World::new(Config {
        native_payload_budget: 8,
        ..Config::default()
    })
    .unwrap();
    let mut rt = NativeRuntime::new(w.clone(), 2).unwrap();
    let owner = w.allocate(EndpointKind::Native, None).unwrap();
    let target = w.allocate(EndpointKind::Native, None).unwrap();
    w.activate(owner).unwrap();
    w.activate(target).unwrap();
    rt.after(owner, target, Duration::from_secs(5), Payload::Pulse(1))
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while w.snapshot().retained_payload_bytes == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(w.snapshot().retained_payload_bytes, 8);
    w.stop(owner).unwrap();
    rt.close(Duration::from_millis(100)).unwrap();
    assert_eq!(w.snapshot().retained_payload_bytes, 0);
}

#[test]
fn starting_parent_fences_pipeline_progress() {
    let w = world();
    let mut rt = NativeRuntime::new(w.clone(), 8).unwrap();
    let parent = w.allocate(EndpointKind::Native, None).unwrap();
    let p = rt
        .start_counter(
            Some(parent),
            Duration::from_millis(2),
            Duration::from_millis(10),
            None,
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(40));
    assert_eq!(p.snapshot().generated, 0);
    w.activate(parent).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while p.snapshot().generated == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(p.snapshot().generated > 0);
    rt.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn long_timer_stop_is_prompt_and_does_not_send() {
    let w = world();
    let mut rt = NativeRuntime::new(w.clone(), 2).unwrap();
    let owner = w.allocate(EndpointKind::Native, None).unwrap();
    let target = w.allocate(EndpointKind::Native, None).unwrap();
    w.activate(owner).unwrap();
    w.activate(target).unwrap();
    rt.after(
        owner,
        target,
        Duration::from_secs(24 * 60 * 60),
        Payload::Pulse(1),
    )
    .unwrap();
    w.stop(owner).unwrap();
    let start = std::time::Instant::now();
    rt.close(Duration::from_millis(200)).unwrap();
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(w.snapshot().metrics.admitted, 0);
}

#[test]
fn stalled_python_queue_is_bounded_while_native_sink_progresses() {
    let w = World::new(Config {
        mailbox_capacity: 1,
        mailbox_bytes: 64,
        ..Config::default()
    })
    .unwrap();
    let python = w.allocate(EndpointKind::Python, None).unwrap();
    w.activate(python).unwrap();
    let mut rt = NativeRuntime::new(w.clone(), 8).unwrap();
    let p = rt
        .start_counter(
            None,
            Duration::from_millis(2),
            Duration::from_millis(20),
            Some(python),
        )
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while (p.snapshot().processed < 10
        || p.snapshot().sink_received < 2
        || w.snapshot().metrics.rejected == 0)
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(5));
    }
    let s = p.snapshot();
    assert!(s.processed >= 10 && s.sink_received >= 2);
    // Scheduler delays may leave only one input in a time window. Summary
    // count is not a throughput guarantee; the stalled consumer stays bounded
    // while the independent native sink receives later windows.
    let snapshot = w.snapshot();
    assert!(snapshot.metrics.rejected > 0);
    assert_eq!(
        snapshot
            .actors
            .iter()
            .find(|a| a.reference == python)
            .unwrap()
            .queue_entries,
        1
    );
    p.stop();
    rt.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn subscription_setup_failure_rolls_back_pipeline() {
    let w = World::new(Config {
        max_subscriptions: 1,
        ..Config::default()
    })
    .unwrap();
    let mut rt = NativeRuntime::new(w.clone(), 8).unwrap();
    assert!(
        rt.start_counter(
            None,
            Duration::from_millis(2),
            Duration::from_millis(10),
            None
        )
        .is_err()
    );
    assert!(
        w.snapshot()
            .actors
            .iter()
            .all(|a| matches!(a.state, actorplane_core::Lifecycle::Stopped))
    );
    assert_eq!(w.snapshot().subscriptions, 0);
    assert_eq!(rt.active_tasks(), 0);
    rt.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn envelope_timer_expires_before_delivery_and_releases_retention() {
    let w = World::new(Config {
        native_payload_budget: 8,
        ..Config::default()
    })
    .unwrap();
    let mut rt = NativeRuntime::new(w.clone(), 2).unwrap();
    let owner = w.allocate(EndpointKind::Native, None).unwrap();
    let target = w.allocate(EndpointKind::Native, None).unwrap();
    w.activate(owner).unwrap();
    w.activate(target).unwrap();
    rt.after_with(
        owner,
        target,
        Duration::from_millis(100),
        Payload::Pulse(1),
        MessageOptions {
            deadline: Some(Instant::now() + Duration::from_millis(20)),
            ..Default::default()
        },
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while w.snapshot().retained_payload_bytes != 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(w.snapshot().retained_payload_bytes, 0);
    assert_eq!(w.snapshot().metrics.admitted, 0);
    rt.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn envelope_timer_preserves_source_correlation_and_trace() {
    let w = world();
    let mut rt = NativeRuntime::new(w.clone(), 2).unwrap();
    let owner = w.allocate(EndpointKind::Native, None).unwrap();
    let target = w.allocate(EndpointKind::Native, None).unwrap();
    w.activate(owner).unwrap();
    w.activate(target).unwrap();
    let trace = TraceContext::new(
        [1; 16],
        [2; 8],
        true,
        vec![("tenant".into(), "native".into())],
    )
    .unwrap();
    rt.after_with(
        owner,
        target,
        Duration::from_millis(10),
        Payload::Pulse(1),
        MessageOptions {
            correlation_id: Some(7),
            causation_id: Some(8),
            trace: Some(trace.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    let delivery = loop {
        if let Some(delivery) = w.claim(target).unwrap() {
            break delivery;
        }
        assert!(Instant::now() < deadline, "timer did not deliver");
        std::thread::sleep(Duration::from_millis(2));
    };
    let envelope = delivery.envelope();
    assert_eq!(envelope.source, Some(owner));
    assert_eq!(envelope.correlation_id, Some(7));
    assert_eq!(envelope.causation_id, Some(8));
    assert_eq!(envelope.trace, Some(trace));
    delivery.finish(true);
    rt.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn envelope_timer_rejects_expired_deadline_and_conflicting_source() {
    let w = world();
    let mut rt = NativeRuntime::new(w.clone(), 2).unwrap();
    let owner = w.allocate(EndpointKind::Native, None).unwrap();
    let target = w.allocate(EndpointKind::Native, None).unwrap();
    let other = w.allocate(EndpointKind::Native, None).unwrap();
    w.activate(owner).unwrap();
    w.activate(target).unwrap();
    w.activate(other).unwrap();
    let expired = MessageOptions {
        deadline: Some(Instant::now() - Duration::from_millis(1)),
        ..Default::default()
    };
    assert!(
        rt.after_with(owner, target, Duration::ZERO, Payload::Pulse(1), expired)
            .is_err()
    );
    assert_eq!(rt.active_tasks(), 0);
    let conflicting = MessageOptions {
        source: Some(other),
        ..Default::default()
    };
    assert!(
        rt.after_with(
            owner,
            target,
            Duration::ZERO,
            Payload::Pulse(1),
            conflicting
        )
        .is_err()
    );
    assert_eq!(rt.active_tasks(), 0);
    rt.close(Duration::from_secs(1)).unwrap();
}
