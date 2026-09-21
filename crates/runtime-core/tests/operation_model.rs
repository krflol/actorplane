//! A bounded independent state model for indexed operation mutations.
use actorplane_core::{
    ActorRef, Error, OperationId, OperationStatus, OperationTable, TerminalOutcome,
};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status {
    Pending,
    Cancelled,
    TimedOut,
    OwnerStopped,
    TargetStopped,
}

#[derive(Clone, Copy)]
struct Record {
    id: OperationId,
    owner: ActorRef,
    target: ActorRef,
    due: u64,
    status: Status,
}

fn observed(value: OperationStatus) -> Status {
    match value {
        OperationStatus::Pending(_) => Status::Pending,
        OperationStatus::Terminal(TerminalOutcome::Cancelled) => Status::Cancelled,
        OperationStatus::Terminal(TerminalOutcome::TimedOut) => Status::TimedOut,
        OperationStatus::Terminal(TerminalOutcome::OwnerStopped) => Status::OwnerStopped,
        OperationStatus::Terminal(TerminalOutcome::TargetStopped) => Status::TargetStopped,
        other => panic!("unexpected model outcome: {other:?}"),
    }
}

fn random(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state >> 16
}

#[test]
fn indexed_operations_match_independent_model_over_bounded_seeded_mutations() {
    for seed in [1, 7, 29, 1069] {
        let mut rng = seed;
        let epoch = Instant::now();
        let refs: Vec<_> = (0..8)
            .map(|slot| ActorRef {
                world: 7,
                slot,
                generation: 1,
            })
            .collect();
        let mut table = OperationTable::new(7, 32).unwrap();
        let mut model = BTreeMap::<(u32, u64), Record>::new();
        let mut retired = None;
        for step in 0..5000u64 {
            let action = random(&mut rng) % 10;
            let owner = refs[random(&mut rng) as usize % refs.len()];
            let chosen = if model.is_empty() {
                None
            } else {
                model
                    .values()
                    .nth(random(&mut rng) as usize % model.len())
                    .copied()
            };
            match (action, chosen) {
                (0..=2, _) => {
                    let target = refs[random(&mut rng) as usize % refs.len()];
                    let due = step + random(&mut rng) % 100;
                    let result = table.reserve(owner, target, epoch + Duration::from_micros(due));
                    if model.len() == 32 {
                        assert_eq!(result, Err(Error::LimitExceeded));
                    } else {
                        let id = result.unwrap();
                        model.insert(
                            (id.slot, id.generation),
                            Record {
                                id,
                                owner,
                                target,
                                due,
                                status: Status::Pending,
                            },
                        );
                    }
                }
                (3, Some(record)) => {
                    let won = table
                        .complete(record.id, TerminalOutcome::Cancelled)
                        .unwrap();
                    assert_eq!(won, record.status == Status::Pending);
                    if won {
                        model
                            .get_mut(&(record.id.slot, record.id.generation))
                            .unwrap()
                            .status = Status::Cancelled;
                    }
                }
                (4, Some(record)) => {
                    let value = table.take(record.id).unwrap();
                    assert_eq!(value.is_some(), record.status != Status::Pending);
                    if let Some(value) = value {
                        assert_eq!(observed(OperationStatus::Terminal(value)), record.status);
                        model.remove(&(record.id.slot, record.id.generation));
                        retired = Some(record.id);
                    }
                }
                (5, Some(record)) => {
                    let released = table.release_unsubmitted(record.id).unwrap();
                    assert_eq!(released, record.status == Status::Pending);
                    if released {
                        model.remove(&(record.id.slot, record.id.generation));
                        retired = Some(record.id);
                    }
                }
                (6, _) => {
                    let mut expected = Vec::new();
                    for record in model.values_mut() {
                        if record.status == Status::Pending && record.due <= step {
                            record.status = Status::TimedOut;
                            expected.push(record.id);
                        }
                    }
                    assert_eq!(
                        table.expire_ids(epoch + Duration::from_micros(step)),
                        expected,
                        "seed={seed} step={step}"
                    );
                }
                (7, _) => {
                    let expected = model.values().filter(|r| r.owner == owner).count();
                    assert_eq!(table.retire_owner(owner), expected);
                    model.retain(|_, r| r.owner != owner);
                }
                (8, _) => {
                    let mut expected = 0;
                    for record in model.values_mut() {
                        if record.owner == owner && record.status == Status::Pending {
                            record.status = Status::OwnerStopped;
                            expected += 1;
                        }
                    }
                    assert_eq!(table.cancel_owner(owner), expected);
                }
                (9, _) => {
                    let mut expected = 0;
                    for record in model.values_mut() {
                        if record.target == owner && record.status == Status::Pending {
                            record.status = Status::TargetStopped;
                            expected += 1;
                        }
                    }
                    assert_eq!(table.stop_target(owner), expected);
                }
                _ => (),
            }
            let pending = model
                .values()
                .filter(|r| r.status == Status::Pending)
                .count();
            assert_eq!(table.len(), model.len(), "seed={seed} step={step}");
            assert_eq!(table.pending_count(), pending);
            assert_eq!(table.retained_terminal_count(), model.len() - pending);
            for reference in &refs {
                assert_eq!(
                    table.pending_for(*reference),
                    model
                        .values()
                        .filter(|r| r.owner == *reference && r.status == Status::Pending)
                        .count()
                );
                assert_eq!(
                    table.pending_for_target(*reference),
                    model
                        .values()
                        .filter(|r| r.target == *reference && r.status == Status::Pending)
                        .count()
                );
            }
            for record in model.values() {
                assert_eq!(
                    observed(table.status(record.id).unwrap()),
                    record.status,
                    "seed={seed} step={step}"
                );
            }
            if let Some(id) = retired {
                assert!(matches!(table.status(id), Err(Error::StaleReference)));
                assert_eq!(
                    table.complete(id, TerminalOutcome::Cancelled),
                    Err(Error::StaleReference)
                );
            }
        }
        for reference in refs {
            table.retire_owner(reference);
        }
        assert!(table.is_empty());
        assert_eq!(table.pending_count(), 0);
        assert!(
            table
                .expire_ids(epoch + Duration::from_secs(3600))
                .is_empty()
        );
    }
}
