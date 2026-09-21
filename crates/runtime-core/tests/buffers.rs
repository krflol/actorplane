use actorplane_core::{Config, Error, Payload, World};
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn buffer_reservation_shares_world_payload_budget() {
    let config = Config {
        native_payload_budget: 15,
        ..Config::default()
    };
    let world = World::new(config).unwrap();
    let budget = world.native_buffer_budget();
    let permit = budget.reserve(8).unwrap();
    assert_eq!(budget.used(), 8);
    assert!(matches!(
        world.hold(Payload::Pulse(1)),
        Err(Error::BudgetExceeded)
    ));
    drop(permit);
    let held = world.hold(Payload::Pulse(1)).unwrap();
    assert_eq!(budget.used(), 8);
    assert!(matches!(budget.reserve(8), Err(Error::BudgetExceeded)));
    drop(held);
    assert_eq!(budget.used(), 0);
}

#[test]
fn concurrent_reservations_never_exceed_limit_and_clones_share_state() {
    let config = Config {
        native_payload_budget: 64,
        ..Config::default()
    };
    let world = World::new(config).unwrap();
    let budget = Arc::new(world.native_buffer_budget());
    let barrier = Arc::new(Barrier::new(17));
    let mut threads = Vec::new();
    for _ in 0..16 {
        let budget = budget.clone();
        let barrier = barrier.clone();
        threads.push(thread::spawn(move || {
            barrier.wait();
            budget.reserve(8)
        }));
    }
    barrier.wait();
    let permits: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    let successful = permits.iter().filter(|permit| permit.is_ok()).count();
    assert_eq!(successful, 8);
    assert_eq!(budget.used(), 64);
    drop(permits);
    assert_eq!(budget.used(), 0);
}

#[test]
fn growth_failure_is_atomic_and_drop_releases_exact_bytes() {
    let config = Config {
        native_payload_budget: 10,
        ..Config::default()
    };
    let world = World::new(config).unwrap();
    let budget = world.native_buffer_budget();
    let mut permit = budget.reserve(6).unwrap();
    assert!(matches!(permit.try_grow(5), Err(Error::BudgetExceeded)));
    assert_eq!(permit.bytes(), 6);
    assert_eq!(budget.used(), 6);
    permit.try_grow(4).unwrap();
    assert_eq!(permit.bytes(), 10);
    drop(permit);
    assert_eq!(budget.used(), 0);
}
