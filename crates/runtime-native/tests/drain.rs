use actorplane_core::{Config, Lifecycle, Payload, World};
use actorplane_native::NativeRuntime;
use std::time::{Duration, Instant};

#[test]
fn drain_flushes_admitted_pulses_into_final_native_summary() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 8).unwrap();
    let pipeline = runtime
        .start_counter(
            None,
            Duration::from_secs(24 * 60 * 60),
            Duration::from_secs(24 * 60 * 60),
            None,
        )
        .unwrap();
    for _ in 0..3 {
        world.send(pipeline.counter, Payload::Pulse(1)).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(1);
    while pipeline.snapshot().processed < 3 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(pipeline.snapshot().processed, 3);
    world
        .request_drain(pipeline.owner, Instant::now() + Duration::from_secs(1))
        .unwrap();
    assert!(runtime.wait_native_idle(Duration::from_secs(1)));
    let snapshot = pipeline.snapshot();
    assert_eq!(snapshot.summaries, 1);
    assert_eq!(snapshot.sink_received, 1);
    assert_eq!(world.snapshot().metrics.cancelled, 0);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn drain_stops_source_and_retires_pipeline() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 8).unwrap();
    let pipeline = runtime
        .start_counter(None, Duration::from_millis(2), Duration::from_secs(1), None)
        .unwrap();
    std::thread::sleep(Duration::from_millis(30));
    world
        .request_drain(pipeline.owner, Instant::now() + Duration::from_secs(1))
        .unwrap();
    assert!(runtime.wait_native_idle(Duration::from_secs(1)));
    let deadline = Instant::now() + Duration::from_secs(1);
    while !matches!(world.state(pipeline.owner), Ok(Lifecycle::Stopped))
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(matches!(
        world.state(pipeline.owner),
        Ok(Lifecycle::Stopped)
    ));
    let generated = pipeline.snapshot().generated;
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(pipeline.snapshot().generated, generated);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn drain_deadline_quiesces_without_python_driver() {
    let world = World::new(Config {
        mailbox_capacity: 1,
        ..Config::default()
    })
    .unwrap();
    let python = world
        .allocate(actorplane_core::EndpointKind::Python, None)
        .unwrap();
    world.activate(python).unwrap();
    // A known admitted Python delivery guarantees outstanding drain work even
    // when OS timer resolution produces no native windows before quiescence.
    world.send(python, Payload::Pulse(1)).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 8).unwrap();
    let _pipeline = runtime
        .start_counter(
            None,
            Duration::from_millis(1),
            Duration::from_millis(5),
            Some(python),
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_millis(20);
    world.drain_all(deadline);
    assert!(runtime.wait_native_idle(Duration::from_secs(1)));
    let watchdog = Instant::now() + Duration::from_secs(1);
    while world.state(python) == Ok(Lifecycle::Quiescing) && Instant::now() < watchdog {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(world.state(python), Ok(Lifecycle::Stopping));
    assert!(world.snapshot().metrics.drain_timeouts >= 1);
    assert_eq!(world.snapshot().metrics.discarded, 1);
    assert!(!world.close().python_done);
    world.finish_python(python).unwrap();
    assert!(world.close().python_done);
    runtime.close(Duration::from_secs(1)).unwrap();
}
