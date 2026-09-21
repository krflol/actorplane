use actorplane_core::{Config, EndpointKind, Error, Payload};
use actorplane_test::TestWorld;

#[test]
fn send_batch_admits_fifo_until_mailbox_capacity() {
    let config = Config {
        mailbox_capacity: 2,
        ..Config::default()
    };
    let test = TestWorld::new(config, 4, 10_000).unwrap();
    let target = test.world().allocate(EndpointKind::Native, None).unwrap();
    test.world().activate(target).unwrap();
    let results = test.send_batch(&[
        (target, Payload::Pulse(1)),
        (target, Payload::Pulse(2)),
        (target, Payload::Pulse(3)),
    ]);
    assert!(results[0].is_ok());
    assert!(results[1].is_ok());
    assert_eq!(results[2], Err(Error::QueueFull));
    let first = test.world().claim(target).unwrap().unwrap();
    assert_eq!(first.payload(), &Payload::Pulse(1));
    let first_id = first.event_id();
    first.finish(true);
    let second = test.world().claim(target).unwrap().unwrap();
    assert!(first_id < second.event_id());
    assert_eq!(second.payload(), &Payload::Pulse(2));
    second.finish(true);
    assert_eq!(test.world().snapshot().retained_payload_bytes, 0);
    assert_eq!(test.world().snapshot().metrics.rejected, 1);
}
