use actorplane_core::{
    Config, EndpointKind, FailureAction, FailureDetails, FailurePhase, Lifecycle, World,
};

fn active(world: &World, parent: Option<actorplane_core::ActorRef>) -> actorplane_core::ActorRef {
    let actor = world.allocate(EndpointKind::Native, parent).unwrap();
    world.activate(actor).unwrap();
    actor
}

fn fail(world: &World, actor: actorplane_core::ActorRef, sequence: u32) {
    world
        .report_failure(
            actor,
            Some(u64::from(sequence)),
            None,
            None,
            FailureDetails::new(FailurePhase::Handler, "test", "Failure", vec![]),
            FailureAction::Continue,
        )
        .unwrap();
}

#[test]
fn claim_and_discard_only_walk_supervisor_children() {
    let world = World::new(Config::default()).unwrap();
    let first = active(&world, None);
    let second = active(&world, None);
    let first_child = active(&world, Some(first));
    let second_child = active(&world, Some(second));
    fail(&world, first_child, 1);
    fail(&world, second_child, 2);

    let notice = world.claim_failure(first).unwrap().unwrap();
    assert_eq!(notice.failure().actor, first_child);
    notice.finish(None).unwrap();
    assert!(world.claim_failure(first).unwrap().is_none());
    let notice = world.claim_failure(second).unwrap().unwrap();
    assert_eq!(notice.failure().actor, second_child);
    notice.finish(None).unwrap();
    fail(&world, first_child, 3);
    fail(&world, second_child, 4);
    world.stop(first).unwrap();
    assert_eq!(world.snapshot().metrics.notifications_discarded, 1);
    let notice = world.claim_failure(second).unwrap().unwrap();
    assert_eq!(notice.failure().actor, second_child);
    notice.finish(None).unwrap();
}

#[test]
fn pending_notification_holds_child_until_claim_and_control_slot_until_finish() {
    let world = World::new(Config::default()).unwrap();
    let parent = active(&world, None);
    let child = active(&world, Some(parent));
    fail(&world, child, 1);
    world.stop(child).unwrap();
    assert!(matches!(world.state(child), Ok(Lifecycle::Stopping)));

    let notice = world.claim_failure(parent).unwrap().unwrap();
    assert_eq!(world.state(child), Ok(Lifecycle::Stopped));
    let report = world.stop_report(parent).unwrap();
    assert_eq!(report.control_in_flight, 1);
    notice.finish(None).unwrap();
    assert_eq!(world.stop_report(parent).unwrap().control_in_flight, 0);
    assert_eq!(world.state(child), Ok(Lifecycle::Stopped));
}

#[test]
fn stopping_parent_discards_old_notice_before_slot_reuse() {
    let world = World::new(Config::default()).unwrap();
    let parent = active(&world, None);
    let child = active(&world, Some(parent));
    fail(&world, child, 1);
    world.stop(parent).unwrap();
    let replacement = active(&world, None);
    assert_eq!(replacement.slot, parent.slot);
    assert!(replacement.generation > parent.generation);
    assert!(world.claim_failure(parent).is_err());
}
