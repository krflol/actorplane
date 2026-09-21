use actorplane_core::{Config, EndpointKind};
use actorplane_native::components::ComponentKind;
use actorplane_test::TestWorld;
use std::time::Duration;

fn main() -> Result<(), String> {
    let mut test = TestWorld::new(Config::default(), 16, 100_000)?;
    let parent = test
        .world()
        .allocate(EndpointKind::Native, None)
        .map_err(|e| e.to_string())?;
    test.world().activate(parent).map_err(|e| e.to_string())?;
    let source = test.runtime().prepare_component(
        parent,
        ComponentKind::PulseSource {
            interval: Duration::from_millis(10),
        },
    )?;
    let counter = test.runtime().prepare_component(
        parent,
        ComponentKind::WindowCounter {
            window: Duration::from_millis(50),
        },
    )?;
    let sink = test
        .runtime()
        .prepare_component(parent, ComponentKind::SnapshotSink)?;
    for owner in [source.owner, counter.owner, sink.owner] {
        test.world().activate(owner).map_err(|e| e.to_string())?;
    }
    let source_port = test
        .world()
        .port(source.owner, "pulses")
        .map_err(|e| e.to_string())?;
    let counter_input = test
        .world()
        .port(counter.owner, "input")
        .map_err(|e| e.to_string())?;
    let counter_output = test
        .world()
        .port(counter.owner, "snapshots")
        .map_err(|e| e.to_string())?;
    let sink_input = test
        .world()
        .port(sink.owner, "input")
        .map_err(|e| e.to_string())?;
    test.world()
        .link(parent, source_port, counter_input)
        .map_err(|e| e.to_string())?;
    test.world()
        .link(parent, counter_output, sink_input)
        .map_err(|e| e.to_string())?;
    for _ in 0..10 {
        test.advance(Duration::from_millis(10))?;
    }
    assert_eq!(source.stats().generated, 10);
    assert_eq!(counter.stats().processed, 10);
    assert!(counter.stats().summaries > 0);
    test.world()
        .request_drain(parent, test.world().now() + Duration::from_secs(1))
        .map_err(|e| e.to_string())?;
    test.run_until_idle()?;
    assert_eq!(sink.stats().sink_received, counter.stats().summaries);
    assert_eq!(test.runtime().active_tasks(), 0);
    let report = test.close(Duration::from_secs(1))?;
    assert!(report.native_done);
    assert_eq!(test.world().snapshot().retained_payload_bytes, 0);
    Ok(())
}
