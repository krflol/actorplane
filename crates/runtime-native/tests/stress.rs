use actorplane_core::{Config, World};
use actorplane_native::NativeRuntime;
use std::time::{Duration, Instant};

#[test]
fn repeated_world_pipeline_cycles_bound_tasks_and_payloads() {
    for _ in 0..40 {
        let world = World::new(Config::default()).unwrap();
        let mut runtime = NativeRuntime::new(world.clone(), 8).unwrap();
        let p = runtime
            .start_counter(
                None,
                Duration::from_millis(1),
                Duration::from_millis(8),
                None,
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_millis(200);
        while p.snapshot().generated == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(p.snapshot().generated > 0);
        p.stop();
        assert!(runtime.wait_native_idle(Duration::from_secs(1)));
        assert_eq!(runtime.active_tasks(), 0);
        assert_eq!(world.snapshot().retained_payload_bytes, 0);
        runtime.close(Duration::from_secs(1)).unwrap();
    }
}

#[test]
fn concurrent_worlds_support_drain_and_cancel_cycles() {
    for cycle in 0..20 {
        let mut runtimes = Vec::new();
        for _ in 0..2 {
            let world = World::new(Config::default()).unwrap();
            let runtime = NativeRuntime::new(world.clone(), 8).unwrap();
            let pipeline = runtime
                .start_counter(
                    None,
                    Duration::from_millis(1),
                    Duration::from_millis(5),
                    None,
                )
                .unwrap();
            let deadline = Instant::now() + Duration::from_millis(200);
            while pipeline.snapshot().generated == 0 && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            assert!(
                pipeline.snapshot().generated > 0,
                "cycle {cycle} made no progress"
            );
            if cycle % 2 == 0 {
                world
                    .request_drain(pipeline.owner, Instant::now() + Duration::from_secs(1))
                    .unwrap();
            } else {
                pipeline.stop();
            }
            runtimes.push((world, runtime));
        }
        for (world, mut runtime) in runtimes {
            assert!(runtime.wait_native_idle(Duration::from_secs(1)));
            runtime.close(Duration::from_secs(1)).unwrap();
            assert_eq!(world.snapshot().retained_payload_bytes, 0);
        }
    }
}
