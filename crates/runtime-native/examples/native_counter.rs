use actorplane_core::{Config, World};
use actorplane_native::NativeRuntime;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(Config::default())?;
    let mut runtime = NativeRuntime::new(world, 8)?;
    let pipeline = runtime.start_counter(
        None,
        Duration::from_millis(10),
        Duration::from_millis(100),
        None,
    )?;
    std::thread::sleep(Duration::from_millis(500));
    let s = pipeline.snapshot();
    let m = runtime.world().snapshot().metrics;
    println!(
        "generated_inputs={} counter_completed={} generated_summaries={} sink_completed={}",
        s.generated, s.processed, s.summaries, s.sink_received
    );
    println!(
        "world_submitted={} world_admitted={} world_completed={} world_rejected={}",
        m.submitted, m.admitted, m.completed, m.rejected
    );
    runtime.close(Duration::from_secs(1))?;
    println!(
        "active_tasks={} retained_payload_bytes={}",
        runtime.active_tasks(),
        runtime.world().snapshot().retained_payload_bytes
    );
    Ok(())
}
