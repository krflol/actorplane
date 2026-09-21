use actorplane_core::{
    Config, EndpointKind, MessageOptions, OperationStatus, Payload, ServiceLease, TerminalOutcome,
    World,
};
use actorplane_native::NativeRuntime;
use std::time::{Duration, Instant};

fn calculate(world: &World, lease: ServiceLease, n: i64) {
    let deadline = Instant::now() + Duration::from_secs(3);
    let operation = world
        .service_request(
            lease,
            "requests",
            Payload::Pulse(n),
            deadline,
            MessageOptions::default(),
        )
        .unwrap();
    while matches!(
        world.operation_status(operation),
        Ok(OperationStatus::Pending(_))
    ) {
        assert!(Instant::now() < deadline, "service request timed out");
        std::thread::sleep(Duration::from_millis(1));
    }
    match world.take_operation(operation).unwrap().unwrap() {
        TerminalOutcome::Completed(value) => assert_eq!(
            value.payload(),
            &Payload::CountSnapshot {
                count: n as u64,
                total: n * (n + 1) * (2 * n + 1) / 6,
            }
        ),
        other => panic!("service request failed: {other:?}"),
    }
}

fn main() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 32).unwrap();
    let service = runtime.prepare_sum_squares(None).unwrap();
    world.activate(service.owner).unwrap();
    let contract = &service.spec().descriptor.interfaces[0];
    world
        .register_service("calculator", service.owner, contract)
        .unwrap();
    let first = world.allocate(EndpointKind::Native, None).unwrap();
    let second = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(first).unwrap();
    world.activate(second).unwrap();
    let left = world
        .acquire_service(first, "calculator", contract)
        .unwrap();
    let right = world
        .acquire_service(second, "calculator", contract)
        .unwrap();
    calculate(&world, left, 7);
    world.stop(first).unwrap();
    assert_eq!(world.services()[0].leases, 1);
    calculate(&world, right, 8);
    let report = runtime.close(Duration::from_secs(1)).unwrap();
    assert!(report.native_done && !report.timed_out);
    assert!(world.services().is_empty() && world.service_leases().is_empty());
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    println!("shared native CPU service; independent holder stop; tasks=0; retained bytes=0");
}
