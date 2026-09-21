use actorplane_core::{
    Clock, Config, EndpointKind, Error, Lifecycle, MessageOptions, Payload,
    PublicationOutcome as Outcome, PublicationStatus as Status, World,
};
use std::{
    sync::{Arc, mpsc},
    task::{Context, Wake, Waker},
    time::{Duration, Instant},
};

fn world() -> World {
    World::new(Config {
        routing_batch_size: 1,
        ..Default::default()
    })
    .unwrap()
}
fn actor(w: &World) -> actorplane_core::ActorRef {
    let a = w.allocate(EndpointKind::Native, None).unwrap();
    w.activate(a).unwrap();
    a
}
fn route_all(w: &World) {
    for _ in 0..100 {
        if !w.routing_ready() {
            return;
        }
        w.route_batch();
    }
    panic!("routing failed to settle");
}
fn receive(w: &World, a: actorplane_core::ActorRef) -> Option<i64> {
    w.claim(a).unwrap().map(|lease| {
        let Payload::Pulse(value) = lease.payload() else {
            panic!("pulse")
        };
        let value = *value;
        lease.finish(true);
        value
    })
}

#[test]
fn ticket_separates_ingress_from_routing_snapshot_and_destination_admission() {
    let w = world();
    let s = actor(&w);
    let a = actor(&w);
    let b = actor(&w);
    let c = actor(&w);
    w.subscribe(s, a).unwrap();
    let ticket = w.publish(s, Payload::Pulse(7)).unwrap();
    assert_eq!(ticket.status(), Status::Queued);
    assert_eq!(w.snapshot().metrics.admitted, 0);
    w.subscribe(s, b).unwrap(); // Before native routing snapshot: included.
    assert_eq!(w.route_batch().destinations, 1);
    assert!(matches!(ticket.status(), Status::Routing(_)));
    w.subscribe(s, c).unwrap(); // After snapshot: excluded.
    route_all(&w);
    let report = ticket.report().unwrap();
    assert_eq!(
        (report.matched, report.admitted, report.rejected),
        (2, 2, 0)
    );
    assert_eq!(report.outcome, Outcome::Routed);
    assert_eq!(receive(&w, a), Some(7));
    assert_eq!(receive(&w, b), Some(7));
    assert_eq!(receive(&w, c), None);
    assert_eq!(w.snapshot().routing_snapshot_entries, 0);
    assert_eq!(w.snapshot().retained_payload_bytes, 0);
}

#[test]
fn bounded_turns_are_fair_between_sources_and_fifo_within_one_source() {
    let w = world();
    let s = actor(&w);
    let other = actor(&w);
    let a = actor(&w);
    let b = actor(&w);
    let independent = actor(&w);
    w.subscribe(s, a).unwrap();
    w.subscribe(s, b).unwrap();
    w.subscribe(other, independent).unwrap();
    let first = w.publish(s, Payload::Pulse(1)).unwrap();
    let second = w.publish(s, Payload::Pulse(2)).unwrap();
    let third = w.publish(other, Payload::Pulse(3)).unwrap();
    assert_eq!(w.route_batch().destinations, 1);
    assert!(first.report().is_none());
    assert_eq!(w.route_batch().destinations, 1);
    assert!(third.report().is_some());
    assert!(second.report().is_none());
    assert_eq!(receive(&w, independent), Some(3));
    route_all(&w);
    for target in [a, b] {
        assert_eq!(receive(&w, target), Some(1));
        assert_eq!(receive(&w, target), Some(2));
    }
}

#[test]
fn retained_tickets_and_pending_work_share_global_and_source_quotas() {
    let w = World::new(Config {
        max_publications: 2,
        max_publications_per_source: 1,
        ..Default::default()
    })
    .unwrap();
    let a = actor(&w);
    let b = actor(&w);
    let c = actor(&w);
    let first = w.publish(a, Payload::Pulse(1)).unwrap();
    assert_eq!(w.publish(a, Payload::Pulse(2)), Err(Error::QueueFull));
    let second = w.publish(b, Payload::Pulse(2)).unwrap();
    assert_eq!(w.publish(c, Payload::Pulse(3)), Err(Error::QueueFull));
    route_all(&w);
    assert_eq!(w.snapshot().publications_retained, 2);
    assert_eq!(w.snapshot().retained_payload_bytes, 0);
    let clone = first.clone();
    drop(first);
    assert_eq!(w.publish(a, Payload::Pulse(4)), Err(Error::QueueFull));
    drop(clone);
    let replacement = w.publish(a, Payload::Pulse(4)).unwrap();
    drop(replacement);
    drop(second);
    route_all(&w);
    assert_eq!(w.snapshot().publications_retained, 0);
    assert_eq!(w.routing_pending(), 0);
}

#[test]
fn failed_payload_reservation_rolls_back_ticket_capacity() {
    let w = World::new(Config {
        max_publications: 1,
        max_publications_per_source: 1,
        native_payload_budget: 8,
        ..Default::default()
    })
    .unwrap();
    let s = actor(&w);
    let held = w.hold(Payload::Pulse(0)).unwrap();
    assert_eq!(w.publish(s, Payload::Pulse(1)), Err(Error::BudgetExceeded));
    assert_eq!(w.snapshot().publications_retained, 0);
    assert_eq!(w.routing_pending(), 0);
    drop(held);
    let t = w.publish(s, Payload::Pulse(1)).unwrap();
    route_all(&w);
    assert_eq!(t.report().unwrap().outcome, Outcome::Routed);
}

#[test]
fn independent_target_rejection_and_unsubscribe_revalidate_each_batch_and_claim() {
    let w = World::new(Config {
        routing_batch_size: 1,
        mailbox_capacity: 1,
        ..Default::default()
    })
    .unwrap();
    let s = actor(&w);
    let a = actor(&w);
    let full = actor(&w);
    let removed = actor(&w);
    let first_route = w.subscribe(s, a).unwrap();
    w.subscribe(s, full).unwrap();
    let last_route = w.subscribe(s, removed).unwrap();
    w.send(full, Payload::Pulse(0)).unwrap();
    let t = w.publish(s, Payload::Pulse(1)).unwrap();
    w.route_batch();
    w.unsubscribe(first_route).unwrap();
    w.unsubscribe(last_route).unwrap();
    route_all(&w);
    let r = t.report().unwrap();
    assert_eq!((r.matched, r.admitted, r.rejected), (3, 1, 2));
    assert_eq!(receive(&w, a), None);
    assert_eq!(receive(&w, full), Some(0));
    assert_eq!(receive(&w, removed), None);
    assert_eq!(w.snapshot().retained_payload_bytes, 0);
}

#[test]
fn stop_cancels_unrouted_tail_and_ticket_survives_actor_slot_reuse() {
    let w = world();
    let s = actor(&w);
    let a = actor(&w);
    let b = actor(&w);
    w.subscribe(s, a).unwrap();
    w.subscribe(s, b).unwrap();
    let t = w.publish(s, Payload::Pulse(1)).unwrap();
    w.route_batch();
    assert!(!w.stop_report(s).unwrap().native_done);
    assert!(w.stop(s).unwrap().native_done);
    let r = t.report().unwrap();
    assert_eq!(r.outcome, Outcome::Cancelled);
    assert_eq!((r.admitted, r.rejected), (1, 1));
    let replacement = actor(&w);
    assert_eq!(replacement.slot, s.slot);
    assert_ne!(replacement.generation, s.generation);
    assert_eq!(t.source(), s);
    assert_eq!(receive(&w, a), None);
    assert_eq!(receive(&w, b), None);
    assert_eq!(w.snapshot().routing_snapshot_entries, 0);
    assert_eq!(w.snapshot().retained_payload_bytes, 0);
}

#[test]
fn routing_deadlines_release_snapshots_and_payloads_without_native_worker_polls() {
    let (clock, time) = Clock::manual_at(Instant::now());
    let w = World::with_clock(
        Config {
            routing_batch_size: 1,
            ..Default::default()
        },
        clock,
    )
    .unwrap();
    let s = actor(&w);
    for _ in 0..3 {
        w.subscribe(s, actor(&w)).unwrap();
    }
    let t = w
        .publish_with(
            s,
            Payload::Pulse(1),
            MessageOptions {
                deadline: Some(w.now() + Duration::from_secs(1)),
                ..Default::default()
            },
        )
        .unwrap();
    w.route_batch();
    assert_eq!(w.snapshot().routing_snapshot_entries, 3);
    time.advance(Duration::from_secs(1)).unwrap();
    w.maintain(w.now());
    let r = t.report().unwrap();
    assert_eq!((r.admitted, r.rejected), (1, 2));
    assert_eq!(r.outcome, Outcome::Expired);
    assert_eq!(w.snapshot().routing_snapshot_entries, 0);
    assert_eq!(w.snapshot().retained_payload_bytes, 0);
    assert_eq!(w.snapshot().metrics.publication_expired, 1);
    assert_eq!(w.routing_pending(), 0);
}

#[test]
fn fanout_and_aggregate_snapshot_limits_fail_explicitly_without_truncation() {
    for (fanout, snapshots, expected) in
        [(1, 8, Outcome::FanoutLimit), (8, 1, Outcome::SnapshotFull)]
    {
        let w = World::new(Config {
            max_publication_fanout: fanout,
            max_routing_snapshot_entries: snapshots,
            ..Default::default()
        })
        .unwrap();
        let s = actor(&w);
        let a = actor(&w);
        let b = actor(&w);
        w.subscribe(s, a).unwrap();
        w.subscribe(s, b).unwrap();
        let t = w.publish(s, Payload::Pulse(1)).unwrap();
        route_all(&w);
        let r = t.report().unwrap();
        assert_eq!(r.outcome, expected);
        assert_eq!(r.admitted, 0);
        assert_eq!(receive(&w, a), None);
        assert_eq!(receive(&w, b), None);
        assert_eq!(w.snapshot().retained_payload_bytes, 0);
    }
}

#[test]
fn admitted_publication_is_owned_drain_work_and_can_admit_to_quiescing_target() {
    let w = world();
    let s = actor(&w);
    let target = actor(&w);
    w.subscribe(s, target).unwrap();
    let target_work = w.track_task(target).unwrap();
    let t = w.publish(s, Payload::Pulse(5)).unwrap();
    w.request_drain(s, w.now() + Duration::from_secs(1))
        .unwrap();
    w.request_drain(target, w.now() + Duration::from_secs(1))
        .unwrap();
    assert_eq!(w.state(s), Ok(Lifecycle::Quiescing));
    assert_eq!(w.stop_report(s).unwrap().outstanding_publications, 1);
    route_all(&w);
    assert_eq!(t.report().unwrap().admitted, 1);
    assert_eq!(receive(&w, target), Some(5));
    drop(target_work);
    w.maintain(w.now());
    assert!(w.close().native_done);
}

#[test]
fn target_drain_cutoff_uses_ingress_order_even_when_clock_does_not_advance() {
    let (clock, _) = Clock::manual_at(Instant::now());
    let w = World::with_clock(Config::default(), clock).unwrap();
    let source = actor(&w);
    let target = actor(&w);
    w.subscribe(source, target).unwrap();
    let task = w.track_task(target).unwrap();
    let before = w.publish(source, Payload::Pulse(1)).unwrap();
    let frozen = w.now();
    w.request_drain(target, frozen + Duration::from_secs(1))
        .unwrap();
    let after = w.publish(source, Payload::Pulse(2)).unwrap();
    assert_eq!(w.now(), frozen);
    route_all(&w);
    assert_eq!(
        (
            before.report().unwrap().admitted,
            before.report().unwrap().rejected
        ),
        (1, 0)
    );
    assert_eq!(
        (
            after.report().unwrap().admitted,
            after.report().unwrap().rejected
        ),
        (0, 1)
    );
    assert_eq!(after.report().unwrap().outcome, Outcome::Routed);
    assert_eq!(receive(&w, target), Some(1));
    assert_eq!(receive(&w, target), None);
    drop(task);
    w.maintain(w.now());
    assert!(w.close().native_done);
}

#[test]
fn tracked_completion_can_enter_a_draining_target_after_its_cutoff() {
    let w = world();
    let source = actor(&w);
    let target = actor(&w);
    w.subscribe(source, target).unwrap();
    let source_task = w.track_task(source).unwrap();
    let target_task = w.track_task(target).unwrap();
    for owner in [source, target] {
        w.request_drain(owner, w.now() + Duration::from_secs(1))
            .unwrap();
    }
    let completion = source_task
        .publish_completion(source, Payload::Pulse(3))
        .unwrap();
    route_all(&w);
    assert_eq!(completion.report().unwrap().admitted, 1);
    assert_eq!(receive(&w, target), Some(3));
    drop(source_task);
    drop(target_task);
    w.maintain(w.now());
    assert!(w.close().native_done);
}

#[test]
fn route_owner_activation_is_revalidated_independently_of_source_and_target() {
    use actorplane_core::{ComponentDescriptor, PayloadType, PortDirection, PortSpec};
    let w = world();
    let owner = w.allocate(EndpointKind::Native, None).unwrap();
    let source = w.allocate(EndpointKind::Native, None).unwrap();
    let target = w.allocate(EndpointKind::Native, None).unwrap();
    let mut ports = Vec::new();
    for (actor, direction) in [
        (source, PortDirection::Output),
        (target, PortDirection::Input),
    ] {
        ports.push(
            w.register_component(
                actor,
                ComponentDescriptor {
                    name: format!("port-{direction:?}"),
                    version: 1,
                    ports: vec![PortSpec {
                        name: "pulse".into(),
                        direction,
                        schema: PayloadType::Pulse,
                    }],
                    interfaces: vec![],
                },
            )
            .unwrap()[0],
        );
        w.activate(actor).unwrap();
    }
    w.link(owner, ports[0], ports[1]).unwrap();
    let early = w.publish_port(ports[0], Payload::Pulse(1)).unwrap();
    route_all(&w);
    assert_eq!(
        (
            early.report().unwrap().admitted,
            early.report().unwrap().rejected
        ),
        (0, 1)
    );
    assert_eq!(receive(&w, target), None);
    w.activate(owner).unwrap();
    let ready = w.publish_port(ports[0], Payload::Pulse(2)).unwrap();
    route_all(&w);
    assert_eq!(ready.report().unwrap().admitted, 1);
    assert_eq!(receive(&w, target), Some(2));
    let fenced = w.publish_port(ports[0], Payload::Pulse(3)).unwrap();
    w.stop(owner).unwrap();
    route_all(&w);
    assert_eq!(fenced.report().unwrap().matched, 0);
    assert_eq!(receive(&w, target), None);
    assert!(w.close().native_done);
}

#[test]
fn routing_readiness_wakes_on_ingress_without_world_broadcasts() {
    struct Signal(mpsc::Sender<()>);
    impl Wake for Signal {
        fn wake(self: Arc<Self>) {
            self.0.send(()).unwrap();
        }
    }
    let w = world();
    let s = actor(&w);
    let (tx, rx) = mpsc::channel();
    let waker = Waker::from(Arc::new(Signal(tx)));
    assert!(
        w.poll_routing_ready(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let t = w.publish(s, Payload::Pulse(1)).unwrap();
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(t.report().is_none());
    route_all(&w);
    assert_eq!(t.report().unwrap().outcome, Outcome::Routed);
}

#[test]
fn aggregate_snapshot_capacity_is_released_after_partial_routing_finishes() {
    let w = World::new(Config {
        routing_batch_size: 1,
        max_routing_snapshot_entries: 2,
        ..Default::default()
    })
    .unwrap();
    let first = actor(&w);
    let second = actor(&w);
    let a = actor(&w);
    let b = actor(&w);
    for source in [first, second] {
        w.subscribe(source, a).unwrap();
        w.subscribe(source, b).unwrap();
    }
    let held = w.publish(first, Payload::Pulse(1)).unwrap();
    let rejected = w.publish(second, Payload::Pulse(2)).unwrap();
    w.route_batch();
    assert_eq!(w.snapshot().routing_snapshot_entries, 2);
    w.route_batch();
    assert_eq!(rejected.report().unwrap().outcome, Outcome::SnapshotFull);
    assert_eq!(w.snapshot().routing_snapshot_entries, 2);
    w.route_batch();
    assert_eq!(held.report().unwrap().outcome, Outcome::Routed);
    assert_eq!(w.snapshot().routing_snapshot_entries, 0);
    let retry = w.publish(second, Payload::Pulse(3)).unwrap();
    route_all(&w);
    assert_eq!(retry.report().unwrap().admitted, 2);
    for target in [a, b] {
        assert_eq!(receive(&w, target), Some(1));
        assert_eq!(receive(&w, target), Some(3));
        assert_eq!(receive(&w, target), None);
    }
}

#[test]
fn routing_waker_replacement_and_invocation_can_reenter_world() {
    struct Probe {
        world: World,
        signal: mpsc::Sender<&'static str>,
    }
    impl Wake for Probe {
        fn wake(self: Arc<Self>) {
            let _ = self.world.snapshot();
            self.signal.send("wake").unwrap();
        }
    }
    impl Drop for Probe {
        fn drop(&mut self) {
            let _ = self.world.snapshot();
            let _ = self.signal.send("drop");
        }
    }
    let w = world();
    let source = actor(&w);
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let waker = Waker::from(Arc::new(Probe {
            world: w.clone(),
            signal: tx.clone(),
        }));
        assert!(
            w.poll_routing_ready(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker);
        assert!(
            w.poll_routing_ready(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let waker = Waker::from(Arc::new(Probe {
            world: w.clone(),
            signal: tx,
        }));
        assert!(
            w.poll_routing_ready(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let _ticket = w.publish(source, Payload::Pulse(1)).unwrap();
        route_all(&w);
    });
    assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), "drop");
    assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), "wake");
    assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), "drop");
    worker.join().unwrap();
}
