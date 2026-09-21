use actorplane_core::{Config, EndpointKind, Payload, World};
mod common;
use common::route;

fn active(world: &World) -> actorplane_core::ActorRef {
    let actor = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(actor).unwrap();
    actor
}

#[test]
fn subscription_registration_order_controls_deterministic_fanout() {
    let world = World::new(Config::default()).unwrap();
    let source = active(&world);
    let first = active(&world);
    let second = active(&world);
    let third = active(&world);
    let a = world.subscribe(source, first).unwrap();
    let b = world.subscribe(source, second).unwrap();
    let c = world.subscribe(source, third).unwrap();
    assert!(a < b && b < c);
    for value in 0..32 {
        route(
            &world,
            world.publish(source, Payload::Pulse(value)).unwrap(),
        );
        let first_event = world.claim(first).unwrap().unwrap().event_id();
        let second_event = world.claim(second).unwrap().unwrap().event_id();
        let third_event = world.claim(third).unwrap().unwrap().event_id();
        assert!(first_event < second_event && second_event < third_event);
    }
}
