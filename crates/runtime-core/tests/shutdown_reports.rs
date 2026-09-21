use actorplane_core::{
    Config, EndpointKind, FailureAction, FailureDetails, FailurePhase, Payload, World,
};
use std::time::{Duration, Instant};

fn details() -> FailureDetails {
    FailureDetails::new(
        FailurePhase::Handler,
        "test_handler",
        "TestError",
        Vec::new(),
    )
}

fn active(
    world: &World,
    kind: EndpointKind,
    parent: Option<actorplane_core::ActorRef>,
) -> actorplane_core::ActorRef {
    let reference = world.allocate(kind, parent).expect("allocation");
    world.activate(reference).expect("activation");
    reference
}

#[test]
fn scope_counts_pending_edges_once_and_separates_retained_owner_results() {
    let world = World::new(Config::default()).expect("world");
    let scope = active(&world, EndpointKind::Native, None);
    let child = active(&world, EndpointKind::Native, Some(scope));
    let external = active(&world, EndpointKind::Native, None);

    let incoming = world
        .request(
            external,
            child,
            Payload::Pulse(1),
            Instant::now() + Duration::from_secs(10),
        )
        .expect("external request");
    let outgoing = world
        .request(
            scope,
            external,
            Payload::Pulse(2),
            Instant::now() + Duration::from_secs(10),
        )
        .expect("scoped request");
    let internal = world
        .request(
            scope,
            child,
            Payload::Pulse(4),
            Instant::now() + Duration::from_secs(10),
        )
        .unwrap();
    let report = world.stop_report(scope).expect("scope report");
    assert_eq!(report.outstanding_operations, 3);
    assert_eq!(report.retained_operations, 0);

    assert!(
        world
            .complete_operation(outgoing, external, Payload::Pulse(3))
            .expect("reply")
    );
    let report = world.stop_report(scope).expect("scope report after reply");
    assert_eq!(report.outstanding_operations, 2);
    assert_eq!(report.retained_operations, 1);
    assert!(world.cancel_operation(incoming).expect("cancel incoming"));
    assert!(world.cancel_operation(internal).unwrap());
}

#[test]
fn report_separates_delivery_control_task_and_python_cleanup_counts() {
    let world = World::new(Config::default()).expect("world");
    let parent = active(&world, EndpointKind::Native, None);
    let child = active(&world, EndpointKind::Python, Some(parent));
    let task = world.track_task(parent).expect("native task");
    world
        .send(child, Payload::Pulse(1))
        .expect("business delivery");
    let delivery = world.claim(child).expect("claim").expect("delivery");
    world
        .report_failure(child, None, None, None, details(), FailureAction::Continue)
        .expect("failure notification");
    let control = world
        .claim_failure(parent)
        .expect("control claim")
        .expect("notification");

    let report = world.stop_report(parent).expect("scope report");
    assert_eq!(report.delivery_in_flight, 1);
    assert_eq!(report.control_in_flight, 1);
    assert_eq!(report.native_tasks, 1);
    assert_eq!(report.python_pending, 1);
    assert_eq!(report.in_flight, 3);

    delivery.finish(true);
    control.finish(None).expect("finish control");
    drop(task);
    world.finish_python(child).expect("python cleanup");
    let report = world.stop_report(parent).expect("clean report");
    assert_eq!(report.delivery_in_flight, 0);
    assert_eq!(report.control_in_flight, 0);
    assert_eq!(report.native_tasks, 0);
    assert_eq!(report.python_pending, 0);
}

#[test]
fn retired_child_errors_persist_without_contaminating_replacement_scope() {
    let config = Config {
        diagnostic_capacity: 0,
        ..Config::default()
    };
    let world = World::new(config).expect("world");
    let parent = active(&world, EndpointKind::Native, None);
    let child = active(&world, EndpointKind::Native, Some(parent));
    world
        .report_failure(
            child,
            Some(7),
            None,
            None,
            details(),
            FailureAction::Continue,
        )
        .expect("failure");
    let control = world
        .claim_failure(parent)
        .expect("claim")
        .expect("notification");
    control.finish(None).expect("acknowledge");
    world.stop(child).expect("retire child");
    let replacement = active(&world, EndpointKind::Native, Some(parent));

    let parent_report = world.stop_report(parent).expect("parent report");
    assert_eq!(parent_report.errors, 1);
    assert_eq!(
        parent_report
            .last_error
            .as_ref()
            .expect("last error")
            .sequence,
        1
    );
    let replacement_report = world.stop_report(replacement).expect("replacement report");
    assert_eq!(replacement_report.errors, 0);
    assert!(replacement_report.last_error.is_none());
}

#[test]
fn world_errors_survive_root_retirement_and_root_slot_reuse() {
    let world = World::new(Config::default()).expect("world");
    let root = active(&world, EndpointKind::Native, None);
    world
        .report_failure(root, None, None, None, details(), FailureAction::Continue)
        .expect("root failure");
    world.stop(root).expect("retire root");
    let replacement = active(&world, EndpointKind::Native, None);
    let report = world.shutdown_report();
    assert_eq!(report.errors, 1);
    assert_eq!(
        report.last_error.as_ref().expect("world last error").actor,
        root
    );
    let replacement_report = world.stop_report(replacement).expect("replacement report");
    assert_eq!(replacement_report.errors, 0);
    assert!(replacement_report.last_error.is_none());
}
