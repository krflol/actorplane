#[cfg(feature = "test-runtime")]
use actorplane_core::PublicationStatus;
use actorplane_core::{Config, EndpointKind, Payload, World};
use actorplane_native::NativeRuntime;
use std::time::{Duration, Instant};

fn endpoints(world: &World) -> (actorplane_core::ActorRef, actorplane_core::ActorRef) {
    let source = world.allocate(EndpointKind::Native, None).unwrap();
    let target = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(source).unwrap();
    world.activate(target).unwrap();
    (source, target)
}

#[test]
fn native_router_progresses_while_caller_is_busy_and_closes_cleanly() {
    let world = World::new(Config::default()).unwrap();
    let (source, target) = endpoints(&world);
    world.subscribe(source, target).unwrap();

    let mut runtime = NativeRuntime::new(world.clone(), 8).unwrap();
    let ticket = world.publish(source, Payload::Pulse(7)).unwrap();

    // The caller does no pumping. Native routing has its own Tokio service and
    // must make progress while this thread is occupied by caller work.
    let busy_until = Instant::now() + Duration::from_millis(25);
    while Instant::now() < busy_until {
        std::hint::spin_loop();
    }

    let deadline = Instant::now() + Duration::from_secs(1);
    let lease = loop {
        if let Some(lease) = world.claim(target).unwrap() {
            break lease;
        }
        assert!(
            Instant::now() < deadline,
            "native router did not admit a delivery"
        );
        std::thread::yield_now();
    };
    assert_eq!(lease.payload(), &Payload::Pulse(7));
    lease.finish(true);
    world
        .request_drain(source, Instant::now() + Duration::from_secs(1))
        .unwrap();
    let report = runtime.close(Duration::from_secs(1)).unwrap();
    assert!(report.native_done);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    assert_eq!(ticket.report().unwrap().admitted, 1);
}

#[cfg(feature = "test-runtime")]
#[test]
fn virtual_router_fanout_is_bounded_and_snapshot_is_terminal() {
    let mut runtime = NativeRuntime::new_virtual(
        Config {
            routing_batch_size: 1,
            ..Default::default()
        },
        8,
    )
    .unwrap();
    let world = runtime.world().clone();
    let source = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(source).unwrap();
    let targets: Vec<_> = (0..65)
        .map(|_| world.allocate(EndpointKind::Native, None).unwrap())
        .collect();
    for target in &targets {
        world.activate(*target).unwrap();
        world.subscribe(source, *target).unwrap();
    }
    let ticket = world.publish(source, Payload::Pulse(11)).unwrap();

    // One routing turn admits at most one destination even though the
    // publication has a 65-target snapshot.
    let _ = runtime.pump_virtual(1).unwrap();
    assert!(ticket.report().is_none());
    let mut admitted = 0;
    for target in &targets {
        if let Some(lease) = world.claim(*target).unwrap() {
            assert_eq!(lease.payload(), &Payload::Pulse(11));
            lease.finish(true);
            admitted += 1;
        }
    }
    assert!(admitted <= 1);

    for _ in 0..256 {
        let progress = runtime.pump_virtual(32).unwrap();
        for target in &targets {
            if let Some(lease) = world.claim(*target).unwrap() {
                assert_eq!(lease.payload(), &Payload::Pulse(11));
                lease.finish(true);
                admitted += 1;
            }
        }
        assert!(progress.polls <= 32);
        if ticket.report().is_some() {
            break;
        }
    }
    assert_eq!(admitted, targets.len());
    assert!(matches!(ticket.status(), PublicationStatus::Terminal(_)));

    // A source with no subscribers still consumes one bounded routing turn;
    // it must not spin forever on a zero-destination publication.
    let idle_source = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(idle_source).unwrap();
    let empty = world.publish(idle_source, Payload::Pulse(12)).unwrap();
    for _ in 0..64 {
        let _ = runtime.pump_virtual(32).unwrap();
        if empty.report().is_some() {
            break;
        }
    }
    let empty_report = empty.report().expect("zero-fanout publication stalled");
    assert_eq!(empty_report.admitted, 0);
    let report = runtime.close(Duration::from_secs(1)).unwrap();
    assert!(report.native_done);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}
