#![no_main]
use actorplane_core::{
    ActorRef, Clock, Config, DeliveryLease, DeliveryReport, EndpointKind, Error, HeldPayload,
    Lifecycle, MessageOptions, Payload, PublicationOutcome, PublicationStatus, PublicationTicket,
    TaskLease, World,
};
use libfuzzer_sys::fuzz_target;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

const OWNERS: usize = 4;
const HANDLES: usize = 16;
struct Ledger {
    attempted: u64,
    accepted: u64,
    // Test-owned identity and order ledger, never read from native queues.
    payloads: HashMap<i64, ActorRef>,
    terminal: HashMap<u64, DeliveryReport>,
    delivered: HashSet<(i64, ActorRef)>,
    last: HashMap<(ActorRef, ActorRef), i64>,
}
impl Ledger {
    fn publication(
        &mut self,
        source: ActorRef,
        value: i64,
        result: Result<PublicationTicket, Error>,
    ) -> Option<PublicationTicket> {
        self.attempted += 1;
        result.ok().inspect(|ticket| {
            self.accepted += 1;
            assert_eq!(ticket.source(), source);
            assert!(self.payloads.insert(value, source).is_none());
        })
    }
    fn check(&mut self, world: &World, config: &Config, tickets: &[Option<PublicationTicket>]) {
        let snapshot = world.snapshot();
        let m = &snapshot.metrics;
        assert_eq!(m.publication_submitted, self.attempted);
        assert_eq!(m.publication_admitted, self.accepted);
        assert_eq!(m.publication_rejected, self.attempted - self.accepted);
        let terminal = m.publication_completed
            + m.publication_cancelled
            + m.publication_expired
            + m.publication_failed;
        assert_eq!(
            self.accepted,
            terminal + snapshot.publications_pending as u64
        );
        assert!(
            snapshot.publications_pending + snapshot.publications_retained
                <= config.max_publications
        );
        assert!(snapshot.retained_payload_bytes <= config.native_payload_budget);
        assert!(snapshot.routing_snapshot_entries <= config.max_routing_snapshot_entries);
        let mut retained = HashSet::new();
        let mut held_per_source = HashMap::<ActorRef, HashSet<u64>>::new();
        for ticket in tickets.iter().flatten() {
            held_per_source
                .entry(ticket.source())
                .or_default()
                .insert(ticket.id());
            if let Some(report) = ticket.report() {
                assert_ne!(report.outcome, PublicationOutcome::Pending);
                assert_eq!(report.admitted + report.rejected, report.matched);
                if let Some(previous) = self.terminal.insert(ticket.id(), report.clone()) {
                    assert_eq!(report, previous);
                }
                retained.insert(ticket.id());
            } else {
                assert!(
                    !self.terminal.contains_key(&ticket.id()),
                    "terminal ticket reverted"
                );
                if let PublicationStatus::Routing(report) = ticket.status() {
                    assert!(report.admitted + report.rejected <= report.matched);
                    assert_eq!(report.outcome, PublicationOutcome::Pending);
                }
            }
        }
        assert_eq!(snapshot.publications_retained, retained.len());
        assert!(
            held_per_source
                .values()
                .all(|ids| ids.len() <= config.max_publications_per_source)
        );
        assert!(snapshot.actors.len() <= config.max_actors);
        for actor in snapshot.actors {
            assert!(actor.queue_entries <= config.mailbox_capacity);
            assert!(actor.queue_bytes <= config.mailbox_bytes);
            assert!(actor.staged_entries <= config.mailbox_capacity);
            assert!(actor.staged_bytes <= config.mailbox_bytes);
        }
    }
}
fuzz_target!(|data: &[u8]| {
    if data.len() < 4 || data.len() > 2052 {
        return;
    }
    let config = Config {
        max_actors: 8,
        max_subscriptions: 12,
        mailbox_capacity: 1 + usize::from(data[0] % 4),
        mailbox_bytes: 32,
        native_payload_budget: 64 + 8 * usize::from(data[1] % 25),
        max_publications: 8,
        max_publications_per_source: 1 + usize::from(data[2] % 4),
        max_publication_fanout: 1 + usize::from(data[3] % 4),
        max_routing_snapshot_entries: 4 + usize::from(data[1] % 9),
        routing_batch_size: 1 + usize::from(data[0] % 3),
        diagnostic_capacity: 16,
        ..Default::default()
    };
    let (clock, control) = Clock::manual_at(Instant::now());
    let world = World::with_clock(config.clone(), clock).unwrap();
    let foreign = World::new(Config::default()).unwrap();
    let foreign_actor = foreign.allocate(EndpointKind::Native, None).unwrap();
    foreign.activate(foreign_actor).unwrap();
    let mut actors: Vec<_> = (0..OWNERS)
        .map(|_| {
            let r = world.allocate(EndpointKind::Native, None).unwrap();
            world.activate(r).unwrap();
            r
        })
        .collect();
    let mut routes = vec![
        (
            world.subscribe(actors[0], actors[1]).unwrap(),
            actors[0],
            actors[1],
        ),
        (
            world.subscribe(actors[0], actors[2]).unwrap(),
            actors[0],
            actors[2],
        ),
    ];
    let mut tickets: Vec<Option<PublicationTicket>> = (0..HANDLES).map(|_| None).collect();
    let mut claims: Vec<Option<DeliveryLease>> = (0..OWNERS).map(|_| None).collect();
    let mut tasks: Vec<Option<(ActorRef, TaskLease)>> = (0..OWNERS).map(|_| None).collect();
    let mut holds: Vec<Option<HeldPayload>> = (0..OWNERS).map(|_| None).collect();
    let mut ledger = Ledger {
        attempted: 0,
        accepted: 0,
        payloads: HashMap::new(),
        terminal: HashMap::new(),
        delivered: HashSet::new(),
        last: HashMap::new(),
    };
    for (step, command) in data[4..].chunks_exact(4).enumerate() {
        let [op, a, b, arg] = *command else {
            unreachable!()
        };
        let ai = usize::from(a) % OWNERS;
        let bi = usize::from(b) % OWNERS;
        let ti = usize::from(arg) % HANDLES;
        let source = actors[ai];
        let value = step as i64 + 1;
        match op % 21 {
            0 | 1 => {
                let options = MessageOptions {
                    deadline: (op % 21 == 1)
                        .then(|| world.now() + Duration::from_millis(u64::from(arg % 9))),
                    ..Default::default()
                };
                tickets[ti] = ledger.publication(
                    source,
                    value,
                    world.publish_with(source, Payload::Pulse(value), options),
                );
            }
            2 => {
                assert!(world.route_batch().destinations <= config.routing_batch_size);
            }
            3 => {
                if let Some(old) = claims[bi].take() {
                    old.finish(true);
                }
                if let Ok(Some(lease)) = world.claim(source) {
                    let Payload::Pulse(value) = lease.payload() else {
                        panic!("unexpected payload")
                    };
                    let publisher = ledger.payloads[value];
                    assert_eq!(lease.envelope().source, Some(publisher));
                    assert_eq!(lease.envelope().destination, source);
                    assert!(
                        routes
                            .iter()
                            .any(|(_, s, t)| *s == publisher && *t == source)
                    );
                    assert!(matches!(
                        world.state(publisher),
                        Ok(Lifecycle::Active | Lifecycle::Quiescing)
                    ));
                    assert!(
                        ledger.delivered.insert((*value, source)),
                        "duplicate invocation"
                    );
                    if let Some(previous) = ledger.last.insert((publisher, source), *value) {
                        assert!(*value > previous, "source FIFO");
                    }
                    claims[bi] = Some(lease);
                }
            }
            4 => {
                if let Some(lease) = claims[bi].take() {
                    lease.finish(arg & 1 == 0);
                }
            }
            5 => {
                if let Ok(id) = world.subscribe(source, actors[bi]) {
                    routes.push((id, source, actors[bi]));
                }
            }
            6 => {
                if !routes.is_empty() {
                    let index = usize::from(arg) % routes.len();
                    let (id, _, _) = routes.swap_remove(index);
                    world.unsubscribe(id).unwrap();
                }
            }
            7 | 8 => {
                let _ = world.stop(source);
                if op % 21 == 8 {
                    if let Ok(replacement) = world.allocate(EndpointKind::Native, None) {
                        assert_ne!(replacement, source);
                        world.activate(replacement).unwrap();
                        actors[ai] = replacement;
                    }
                    assert!(world.send(source, Payload::Pulse(-1)).is_err());
                    // A stopped generation retained by a lease may still resolve,
                    // but cannot admit a new delivery.
                    assert!(!matches!(world.claim(source), Ok(Some(_))));
                }
            }
            9 => {
                control
                    .advance(Duration::from_millis(u64::from(arg % 11)))
                    .unwrap();
                world.maintain(world.now());
            }
            10 => {
                tickets[ti] = tickets[usize::from(b) % HANDLES].clone();
            }
            11 => {
                tickets[ti] = None;
            }
            12 => {
                let invalid = match arg % 3 {
                    0 => foreign_actor,
                    1 => ActorRef {
                        generation: source.generation ^ (1 << 31),
                        ..source
                    },
                    _ => ActorRef {
                        slot: u32::MAX,
                        ..source
                    },
                };
                assert!(world.send(invalid, Payload::Pulse(-1)).is_err());
                assert!(world.claim(invalid).is_err());
                assert!(world.stop(invalid).is_err());
            }
            13 => {
                let _ = world.request_drain(
                    source,
                    world.now() + Duration::from_millis(1 + u64::from(arg % 9)),
                );
            }
            14 => {
                holds[ai] = world.hold(Payload::Pulse(-1)).ok();
            }
            15 => {
                tasks[ai] = world.track_task(source).ok().map(|lease| (source, lease));
            }
            16 => {
                if let Some((owner, lease)) = &tasks[ai] {
                    tickets[ti] = ledger.publication(
                        *owner,
                        value,
                        lease.publish_completion(*owner, Payload::Pulse(value)),
                    );
                }
            }
            17 => {
                tasks[ai] = None;
            }
            18 => {
                holds[ai] = None;
            }
            19 => {
                let before = world.snapshot().retained_payload_bytes;
                assert!(
                    world
                        .hold(Payload::Record {
                            schema: 1,
                            integers: vec![0; 1024]
                        })
                        .is_err()
                );
                assert_eq!(before, world.snapshot().retained_payload_bytes);
            }
            _ => {
                world.close();
            }
        }
        // Prune test routes only when the public lifecycle fence removed them.
        routes.retain(|(_, s, t)| {
            [s, t].iter().all(|r| {
                matches!(
                    world.state(**r),
                    Ok(Lifecycle::Active | Lifecycle::Quiescing)
                )
            })
        });
        ledger.check(&world, &config, &tickets);
    }
    world.close();
    ledger.check(&world, &config, &tickets);
    drop(claims);
    drop(tasks);
    drop(holds);
    drop(tickets);
    assert!(world.close().native_done);
    let final_state = world.snapshot();
    assert_eq!(final_state.retained_payload_bytes, 0);
    assert_eq!(final_state.publications_pending, 0);
    assert_eq!(final_state.publications_retained, 0);
    assert_eq!(final_state.routing_snapshot_entries, 0);
    assert_eq!(final_state.subscriptions, 0);
    assert!(foreign.close().native_done);
});
