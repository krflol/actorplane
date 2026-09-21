use actorplane_core::{Config, EndpointKind, World};
use actorplane_native::{NativeRuntime, components::ComponentKind};
use std::time::Duration;
fn active(w: &World) -> actorplane_core::ActorRef {
    let r = w.allocate(EndpointKind::Native, None).unwrap();
    w.activate(r).unwrap();
    r
}
#[test]
fn connected_native_components_progress_and_cleanup() {
    let w = World::new(Config::default()).unwrap();
    let mut rt = NativeRuntime::new(w.clone(), 8).unwrap();
    let parent = active(&w);
    let src = rt
        .prepare_component(
            parent,
            ComponentKind::PulseSource {
                interval: Duration::from_millis(2),
            },
        )
        .unwrap();
    let ctr = rt
        .prepare_component(
            parent,
            ComponentKind::WindowCounter {
                window: Duration::from_millis(10),
            },
        )
        .unwrap();
    let sink = rt
        .prepare_component(parent, ComponentKind::SnapshotSink)
        .unwrap();
    for c in [&src, &ctr, &sink] {
        w.activate(c.owner).unwrap()
    }
    let sp = w.port(src.owner, "pulses").unwrap();
    let ci = w.port(ctr.owner, "input").unwrap();
    let co = w.port(ctr.owner, "snapshots").unwrap();
    let si = w.port(sink.owner, "input").unwrap();
    w.link(parent, sp, ci).unwrap();
    w.link(parent, co, si).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert!(src.stats().generated > 0);
    assert!(ctr.stats().processed > 0);
    assert!(ctr.stats().summaries > 0);
    assert!(sink.stats().sink_received > 0);
    rt.close(Duration::from_secs(1)).unwrap();
}
#[test]
fn task_budget_failure_leaves_no_starting_component() {
    let w = World::new(Config::default()).unwrap();
    let mut rt = NativeRuntime::new(w.clone(), 1).unwrap();
    let parent = active(&w);
    assert!(
        rt.prepare_component(parent, ComponentKind::SnapshotSink)
            .is_ok()
    );
    assert!(
        rt.prepare_component(parent, ComponentKind::SnapshotSink)
            .is_err()
    );
    assert_eq!(rt.active_tasks(), 1);
    assert_eq!(w.snapshot().actors.len(), 2);
    rt.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn drain_flushes_partial_window_and_retires_components() {
    let config = Config {
        mailbox_capacity: 128,
        ..Default::default()
    };
    let w = World::new(config).unwrap();
    let mut rt = NativeRuntime::new(w.clone(), 8).unwrap();
    let parent = active(&w);
    let ctr = rt
        .prepare_component(
            parent,
            ComponentKind::WindowCounter {
                window: Duration::from_secs(3600),
            },
        )
        .unwrap();
    let sink = rt
        .prepare_component(parent, ComponentKind::SnapshotSink)
        .unwrap();
    for owner in [ctr.owner, sink.owner] {
        w.activate(owner).unwrap();
    }
    let ci = w.port(ctr.owner, "input").unwrap();
    let co = w.port(ctr.owner, "snapshots").unwrap();
    let si = w.port(sink.owner, "input").unwrap();
    w.link(parent, co, si).unwrap();
    for _ in 0..65 {
        w.send_port(ci, actorplane_core::Payload::Pulse(1)).unwrap();
    }
    w.request_drain(parent, std::time::Instant::now() + Duration::from_secs(1))
        .unwrap();
    assert!(rt.wait_native_idle(Duration::from_secs(1)));
    assert_eq!(ctr.stats().processed, 65);
    assert_eq!(ctr.stats().summaries, 1);
    assert_eq!(ctr.stats().last_total, 65);
    assert_eq!(sink.stats().sink_received, 1);
    let report = rt.close(Duration::from_secs(1)).unwrap();
    assert!(report.native_done);
    assert!(!report.timed_out);
    assert_eq!(w.snapshot().retained_payload_bytes, 0);
}
