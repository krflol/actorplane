use actorplane_core::{Config, EndpointKind, OperationStatus, Payload, TerminalOutcome, World};
use actorplane_native::{NativeRuntime, cpu::CpuConfig};
use std::time::{Duration, Instant};

fn main() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new_with_cpu_config(
        world.clone(),
        16,
        CpuConfig {
            workers: 2,
            max_jobs: 8,
        },
    )
    .unwrap();
    let requester = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(requester).unwrap();
    let component = runtime.prepare_sum_squares(Some(requester)).unwrap();
    world.activate(component.owner).unwrap();
    let n = 1_000_000i64;
    let deadline = Instant::now() + Duration::from_secs(3);
    let operation = world
        .request(requester, component.owner, Payload::Pulse(n), deadline)
        .unwrap();
    while matches!(
        world.operation_status(operation),
        Ok(OperationStatus::Pending(_))
    ) {
        assert!(Instant::now() < deadline, "CPU example timed out");
        std::thread::sleep(Duration::from_millis(1));
    }
    let expected = n * (n + 1) * (2 * n + 1) / 6;
    match world.take_operation(operation).unwrap().unwrap() {
        TerminalOutcome::Completed(reply) => assert_eq!(
            reply.payload(),
            &Payload::CountSnapshot {
                count: n as u64,
                total: expected
            }
        ),
        other => panic!("CPU request failed: {other:?}"),
    }
    let report = runtime.close(Duration::from_secs(1)).unwrap();
    assert!(report.native_done && !report.timed_out);
    assert_eq!(runtime.active_tasks(), 0);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    let stats = runtime.cpu_stats();
    assert_eq!((stats.queued, stats.running, stats.completed), (0, 0, 1));
    println!("sum_squares({n})={expected}; CPU completed=1; tasks=0; retained bytes=0");
}
