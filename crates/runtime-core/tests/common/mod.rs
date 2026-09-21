use actorplane_core::{DeliveryReport, PublicationTicket, World};

pub fn route(world: &World, ticket: PublicationTicket) -> DeliveryReport {
    for _ in 0..100_000 {
        if !world.routing_ready() {
            break;
        }
        world.route_batch();
    }
    assert!(
        !world.routing_ready(),
        "routing did not quiesce within test bound"
    );
    ticket.report().expect("publication routed")
}
