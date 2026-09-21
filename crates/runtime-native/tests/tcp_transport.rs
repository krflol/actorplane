#![cfg(feature = "native-io")]

use actorplane_core::{Config, EndpointKind, Lifecycle, World};
use actorplane_native::{NativeRuntime, io::TcpConfig};
use std::{
    io::{Read, Write},
    net::TcpStream,
    time::{Duration, Instant},
};

#[track_caller]
fn wait_until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !predicate() {
        assert!(Instant::now() < deadline, "condition did not become true");
        std::thread::yield_now();
    }
}

fn frame(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + bytes.len());
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
    out
}

fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).unwrap();
    let length = u32::from_be_bytes(header) as usize;
    assert!(length <= actorplane_native::io::MAX_FRAME_BYTES);
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).unwrap();
    body
}

fn setup(
    config: TcpConfig,
) -> (
    NativeRuntime,
    actorplane_native::io::TcpListenerHandle,
    actorplane_core::ActorRef,
) {
    let world = World::new(Config::default()).unwrap();
    let runtime = NativeRuntime::new(world.clone(), 32).unwrap();
    let parent = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(parent).unwrap();
    let echo = runtime.prepare_tcp_echo(Some(parent)).unwrap();
    world.activate(echo.owner).unwrap();
    let listener = runtime.listen_tcp(parent, echo.owner, config).unwrap();
    (runtime, listener, parent)
}

fn client(address: std::net::SocketAddr) -> TcpStream {
    let stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
}

#[test]
fn fragmented_and_coalesced_frames_echo_exact_bytes() {
    let config = TcpConfig {
        max_frame_bytes: 1024,
        ..TcpConfig::default()
    };
    let (mut runtime, listener, _parent) = setup(config);
    let mut stream = client(listener.address);
    let first = frame(b"hello\0world");
    let second = frame(&[]);
    stream.write_all(&first[..2]).unwrap();
    stream.write_all(&first[2..]).unwrap();
    stream.write_all(&second).unwrap();
    assert_eq!(read_frame(&mut stream), b"hello\0world");
    assert!(read_frame(&mut stream).is_empty());
    wait_until(|| listener.stats().frames_received >= 2 && listener.stats().frames_written >= 2);
    assert_eq!(listener.stats().bytes_received, 11);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn oversized_and_truncated_frames_are_rejected_and_counted() {
    let config = TcpConfig {
        max_frame_bytes: 8,
        ..TcpConfig::default()
    };
    let (mut runtime, listener, _parent) = setup(config);
    let mut oversized = client(listener.address);
    oversized.write_all(&(9u32.to_be_bytes())).unwrap();
    wait_until(|| listener.stats().oversized_frames == 1);
    drop(oversized);
    let mut truncated = client(listener.address);
    truncated.write_all(&(4u32.to_be_bytes())).unwrap();
    truncated.write_all(b"xy").unwrap();
    drop(truncated);
    wait_until(|| listener.stats().truncated_frames == 1);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn connection_cap_and_read_timeout_are_bounded() {
    let config = TcpConfig {
        max_connections: 1,
        read_timeout: Duration::from_millis(100),
        ..TcpConfig::default()
    };
    let (mut runtime, listener, _parent) = setup(config);
    let first = client(listener.address);
    wait_until(|| listener.stats().accepted_connections == 1);
    let second = client(listener.address);
    wait_until(|| listener.stats().rejected_connections == 1);
    drop(second);
    wait_until(|| listener.stats().read_timeouts == 1);
    drop(first);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn stopping_parent_closes_listener_and_connections() {
    let (mut runtime, listener, parent) = setup(TcpConfig::default());
    let stream = client(listener.address);
    wait_until(|| !listener.connections().is_empty());
    let world = runtime.world().clone();
    world.stop(parent).unwrap();
    wait_until(|| listener.connections().is_empty());
    assert!(matches!(
        world.state(listener.owner),
        Ok(Lifecycle::Stopped | Lifecycle::Stopping)
    ));
    drop(stream);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn request_timeout_without_target_handler_closes_connection() {
    let config = TcpConfig {
        request_timeout: Duration::from_millis(100),
        ..TcpConfig::default()
    };
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 32).unwrap();
    let parent = world.allocate(EndpointKind::Native, None).unwrap();
    let target = world.allocate(EndpointKind::Native, Some(parent)).unwrap();
    world.activate(parent).unwrap();
    world.activate(target).unwrap();
    let listener = runtime.listen_tcp(parent, target, config).unwrap();
    let mut stream = client(listener.address);
    stream.write_all(&frame(b"pending")).unwrap();
    wait_until(|| listener.stats().request_timeouts == 1);
    let mut buf = [0u8; 1];
    assert!(matches!(stream.read(&mut buf), Ok(0) | Err(_)));
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn read_and_write_budgets_reject_and_release_after_connection_close() {
    let read_config = TcpConfig {
        max_frame_bytes: 8,
        read_buffer_bytes: 1,
        ..TcpConfig::default()
    };
    let (mut runtime, listener, _parent) = setup(read_config);
    let mut stream = client(listener.address);
    stream.write_all(&frame(b"four")).unwrap();
    wait_until(|| listener.stats().budget_rejections == 1);
    drop(stream);
    wait_until(|| listener.connections().is_empty());
    assert_eq!(listener.read_buffer_bytes(), 0);
    runtime.close(Duration::from_secs(1)).unwrap();

    let write_config = TcpConfig {
        max_frame_bytes: 8,
        write_buffer_bytes: 1,
        ..TcpConfig::default()
    };
    let (mut runtime, listener, _parent) = setup(write_config);
    let mut stream = client(listener.address);
    stream.write_all(&frame(b"four")).unwrap();
    wait_until(|| listener.stats().budget_rejections == 1);
    drop(stream);
    wait_until(|| listener.connections().is_empty());
    assert_eq!(listener.write_buffer_bytes(), 0);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn listener_startup_rolls_back_when_global_task_budget_is_full() {
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 1).unwrap();
    let parent = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(parent).unwrap();
    let target = runtime.prepare_tcp_echo(Some(parent)).unwrap();
    world.activate(target.owner).unwrap();
    let before = world.snapshot().actors.len();
    let result = runtime.listen_tcp(parent, target.owner, TcpConfig::default());
    assert!(matches!(
        result,
        Err(actorplane_native::io::TcpError::TaskLimit)
    ));
    assert_eq!(world.snapshot().actors.len(), before);
    runtime.close(Duration::from_secs(1)).unwrap();
}

fn controlled() -> (
    NativeRuntime,
    actorplane_native::controls::ControlledReplies,
    actorplane_native::io::TcpListenerHandle,
    u32,
) {
    let world = World::new(Config::default()).unwrap();
    let runtime = NativeRuntime::new(world.clone(), 32).unwrap();
    let parent = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(parent).unwrap();
    let schema = actorplane_native::io::register_frame(&world).unwrap();
    let control = actorplane_native::controls::ControlledReplies::new(16).unwrap();
    let target = control
        .prepare(
            &runtime,
            Some(parent),
            actorplane_core::PayloadType::Structured(schema),
        )
        .unwrap();
    world.activate(target.owner).unwrap();
    let listener = runtime
        .listen_tcp(parent, target.owner, TcpConfig::default())
        .unwrap();
    (runtime, control, listener, schema)
}

#[test]
fn drain_finishes_admitted_reply_and_closes_without_reading_another_frame() {
    let (mut runtime, control, listener, schema) = controlled();
    let world = runtime.world().clone();
    let mut stream = client(listener.address);
    stream.write_all(&frame(b"first")).unwrap();
    wait_until(|| control.pending().len() == 1);
    let operation = control.pending()[0];
    world
        .request_drain(listener.owner, world.now() + Duration::from_secs(1))
        .unwrap();
    stream.write_all(&frame(b"unadmitted")).unwrap();
    assert!(
        control
            .complete(
                &world,
                operation,
                actorplane_native::io::frame_payload(&world, schema, b"reply").unwrap()
            )
            .unwrap()
    );
    assert_eq!(read_frame(&mut stream), b"reply");
    wait_until(|| world.stop_report(listener.owner).unwrap().native_done);
    assert_eq!(listener.stats().requests_admitted, 1);
    assert_eq!(listener.stats().frames_written, 1);
    assert_eq!(listener.stats().drained_connections, 1);
    assert!(listener.connections().is_empty());
    assert_eq!(listener.read_buffer_bytes(), 0);
    assert_eq!(listener.write_buffer_bytes(), 0);
    assert_eq!(world.shutdown_report().outstanding_operations, 0);
    assert_eq!(world.shutdown_report().retained_operations, 0);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    assert!(runtime.close(Duration::from_secs(1)).unwrap().native_done);
}

#[test]
fn cancelled_connection_fences_delayed_reply_and_slot_reuse() {
    let (mut runtime, control, listener, schema) = controlled();
    let world = runtime.world().clone();
    let mut old = client(listener.address);
    old.write_all(&frame(b"old")).unwrap();
    wait_until(|| control.pending().len() == 1);
    let operation = control.pending()[0];
    let retired = listener.connections()[0];
    world.stop(retired).unwrap();
    wait_until(|| listener.connections().is_empty() && control.pending().is_empty());
    let mut replacement = client(listener.address);
    replacement.write_all(&frame(b"new")).unwrap();
    wait_until(|| control.pending().len() == 1);
    let current = listener.connections()[0];
    assert_eq!(current.slot, retired.slot);
    assert_ne!(current.generation, retired.generation);
    assert!(
        !control
            .complete(
                &world,
                operation,
                actorplane_native::io::frame_payload(&world, schema, b"stale").unwrap()
            )
            .unwrap()
    );
    let fresh = control.pending()[0];
    assert!(
        control
            .complete(
                &world,
                fresh,
                actorplane_native::io::frame_payload(&world, schema, b"fresh").unwrap()
            )
            .unwrap()
    );
    assert_eq!(read_frame(&mut replacement), b"fresh");
    drop(old);
    drop(replacement);
    assert!(runtime.close(Duration::from_secs(1)).unwrap().native_done);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    assert_eq!(world.shutdown_report().retained_operations, 0);
}

#[test]
fn cancel_drops_partially_read_frame_and_listener_socket() {
    let (mut runtime, listener, parent) = setup(TcpConfig::default());
    let mut stream = client(listener.address);
    stream.write_all(&[0, 0, 0, 8, 1, 2]).unwrap();
    wait_until(|| listener.read_buffer_bytes() == 8);
    let world = runtime.world().clone();
    world.stop(parent).unwrap();
    wait_until(|| world.stop_report(parent).unwrap().native_done);
    assert_eq!(listener.read_buffer_bytes(), 0);
    assert_eq!(listener.write_buffer_bytes(), 0);
    assert!(listener.connections().is_empty());
    assert!(TcpStream::connect_timeout(&listener.address, Duration::from_millis(100)).is_err());
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    assert!(runtime.close(Duration::from_secs(1)).unwrap().native_done);
}

#[test]
fn bind_failure_rolls_back_owner_task_and_socket() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let config = TcpConfig {
        bind: occupied.local_addr().unwrap(),
        ..TcpConfig::default()
    };
    let world = World::new(Config::default()).unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 4).unwrap();
    let parent = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(parent).unwrap();
    assert!(matches!(
        runtime.listen_tcp(parent, parent, config),
        Err(actorplane_native::io::TcpError::Bind(_))
    ));
    assert_eq!(runtime.active_tasks(), 0);
    assert!(
        world
            .snapshot()
            .actors
            .iter()
            .filter(|actor| actor.parent == Some(parent))
            .all(|actor| actor.state == Lifecycle::Stopped && actor.native_tasks == 0)
    );
    assert!(runtime.close(Duration::from_secs(1)).unwrap().native_done);
}

#[test]
fn separate_listeners_share_the_world_budget_for_partial_read_buffers() {
    let world = World::new(Config {
        native_payload_budget: 12,
        ..Config::default()
    })
    .unwrap();
    let mut runtime = NativeRuntime::new(world.clone(), 8).unwrap();
    let parent = world.allocate(EndpointKind::Native, None).unwrap();
    world.activate(parent).unwrap();
    let first = runtime
        .listen_tcp(parent, parent, TcpConfig::default())
        .unwrap();
    let second = runtime
        .listen_tcp(parent, parent, TcpConfig::default())
        .unwrap();
    let mut first_client = client(first.address);
    first_client.write_all(&[0, 0, 0, 8, 1]).unwrap();
    wait_until(|| first.read_buffer_bytes() == 8);
    assert_eq!(world.snapshot().retained_payload_bytes, 8);
    let mut second_client = client(second.address);
    second_client.write_all(&[0, 0, 0, 8]).unwrap();
    wait_until(|| second.stats().budget_rejections == 1);
    assert_eq!(second.read_buffer_bytes(), 0);
    assert_eq!(world.snapshot().retained_payload_bytes, 8);
    assert!(runtime.close(Duration::from_secs(1)).unwrap().native_done);
    assert_eq!(first.read_buffer_bytes(), 0);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
}
