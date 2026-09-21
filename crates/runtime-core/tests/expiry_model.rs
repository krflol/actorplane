use actorplane_core::{
    Clock, Config, EndpointKind, Error, Lifecycle, MessageOptions, Payload, World,
};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
struct Item {
    value: i64,
    deadline: Option<Instant>,
}

fn next(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

fn new_actor(world: &World) -> actorplane_core::ActorRef {
    let actor = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(actor).unwrap();
    actor
}

fn expire(model: &mut [VecDeque<Item>], now: Instant) -> u64 {
    let mut expired = 0;
    for queue in model {
        queue.retain(|item| {
            let keep = item.deadline.is_none_or(|at| at > now);
            expired += u64::from(!keep);
            keep
        });
    }
    expired
}

#[test]
fn seeded_public_expiry_model_matches_queue_and_budget_observations() {
    for initial_seed in [0x11_u64, 0x22, 0x33, 0x44] {
        let mut seed = initial_seed;
        let (clock, virtual_clock) = Clock::manual_at(Instant::now());
        let world = World::with_clock(
            Config {
                mailbox_capacity: 8,
                mailbox_bytes: 1024,
                max_actors: 16,
                ..Config::default()
            },
            clock,
        )
        .unwrap();
        let mut actors = Vec::new();
        let mut model: Vec<VecDeque<Item>> = Vec::new();
        for _ in 0..16 {
            actors.push(new_actor(&world));
            model.push(VecDeque::new());
        }
        let mut expected_expired = 0;
        let mut action_counts = [0; 20];
        let mut admitted = 0;
        let mut full = 0;
        let mut rejected_expired = 0;
        let mut claimed = 0;
        for step in 0..3000 {
            let choice = (next(&mut seed) % 20) as usize;
            action_counts[choice] += 1;
            let index = (next(&mut seed) as usize) % actors.len();
            let now = virtual_clock.current();
            match choice {
                0..=11 => {
                    let value = (next(&mut seed) % 10_000) as i64;
                    let deadline = match next(&mut seed) % 8 {
                        0 | 1 => None,
                        n => Some(now + Duration::from_secs(n - 2)),
                    };
                    let result = world.send_with(
                        actors[index],
                        Payload::Pulse(value),
                        MessageOptions {
                            deadline,
                            ..Default::default()
                        },
                    );
                    if deadline.is_some_and(|at| at <= now) {
                        assert_eq!(result, Err(Error::DeadlineExpired));
                        rejected_expired += 1;
                    } else if model[index].len() >= 8 {
                        assert_eq!(result, Err(Error::QueueFull));
                        full += 1;
                    } else {
                        result.unwrap();
                        admitted += 1;
                        model[index].push_back(Item { value, deadline });
                    }
                }
                12..=14 => {
                    while model[index]
                        .front()
                        .is_some_and(|item| item.deadline.is_some_and(|at| at <= now))
                    {
                        model[index].pop_front();
                        expected_expired += 1;
                    }
                    let expected = model[index].pop_front();
                    let actual = world.claim(actors[index]).unwrap();
                    match (expected, actual) {
                        (None, None) => (),
                        (Some(item), Some(lease)) => {
                            assert_eq!(lease.payload(), &Payload::Pulse(item.value));
                            claimed += 1;
                            lease.finish(true);
                        }
                        _ => panic!("claim mismatch"),
                    }
                }
                15 => {
                    virtual_clock.advance(Duration::from_secs(1)).unwrap();
                }
                16 => {
                    world.maintain(now);
                    expected_expired += expire(&mut model, now);
                }
                17 | 18 => {
                    let old = actors[index];
                    assert!(world.stop(old).unwrap().native_done);
                    model[index].clear();
                    actors[index] = new_actor(&world);
                    assert_eq!(actors[index].slot, old.slot);
                    assert_ne!(actors[index].generation, old.generation);
                    assert_eq!(
                        world.send(old, Payload::Pulse(0)),
                        Err(Error::StaleReference)
                    );
                    // Activation flushes startup output after removing due events.
                    expected_expired += expire(&mut model, now);
                }
                _ => {
                    assert_eq!(
                        world.allocate(EndpointKind::Native, None),
                        Err(Error::LimitExceeded)
                    );
                }
            }
            let snapshot = world.snapshot();
            assert_eq!(
                snapshot.metrics.expired, expected_expired,
                "seed={initial_seed} step={step}"
            );
            assert_eq!(snapshot.actors.len(), 16);
            for (actor, queue) in actors.iter().zip(&model) {
                let observed = snapshot
                    .actors
                    .iter()
                    .find(|observed| observed.reference == *actor)
                    .unwrap();
                assert_eq!(observed.state, Lifecycle::Active);
                assert_eq!(
                    observed.queue_entries,
                    queue.len(),
                    "seed={initial_seed} step={step} actor={actor:?}"
                );
                assert_eq!(observed.queue_bytes, queue.len() * 8);
            }
            let expected_entries: usize = model.iter().map(VecDeque::len).sum();
            let actual_entries: usize = snapshot
                .actors
                .iter()
                .map(|actor| actor.queue_entries)
                .sum();
            assert_eq!(actual_entries, expected_entries);
            let expected_bytes = expected_entries * 8;
            let actual_bytes: usize = snapshot.actors.iter().map(|actor| actor.queue_bytes).sum();
            assert_eq!(actual_bytes, expected_bytes);
            assert_eq!(snapshot.retained_payload_bytes, expected_bytes);
        }
        assert!(
            action_counts.iter().all(|count| *count > 50),
            "action coverage: {action_counts:?}"
        );
        assert!(
            admitted > 500 && claimed > 100 && expected_expired > 100 && rejected_expired > 100
        );
        assert!(full > 0, "seed={initial_seed} must exercise full admission");
        for actor in actors {
            world.stop(actor).unwrap();
        }
        assert_eq!(world.snapshot().retained_payload_bytes, 0);
        assert!(world.close().native_done);
    }
}
