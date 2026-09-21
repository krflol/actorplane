//! Small native-only TCP framing smoke test.
//!
//! The listener uses the real bounded transport and the native echo
//! component.  The client deliberately sends both binary and empty frames.

use actorplane_core::{Config, EndpointKind, Lifecycle, World};
use actorplane_native::{NativeRuntime, io::TcpConfig};
use std::{
    io::{Read, Write},
    net::TcpStream,
    time::{Duration, Instant},
};

fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> std::io::Result<()> {
    stream.write_all(&(payload.len() as u32).to_be_bytes())?;
    stream.write_all(payload)
}

fn read_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header)?;
    let length = u32::from_be_bytes(header) as usize;
    let mut payload = vec![0u8; length];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

fn wait_until(deadline: Instant, mut condition: impl FnMut() -> bool) {
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "native transport did not quiesce"
        );
        std::thread::yield_now();
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(Config::default())?;
    let mut runtime = NativeRuntime::new(world.clone(), 32)?;

    let parent = world.allocate(EndpointKind::Native, None)?;
    world.activate(parent)?;
    let echo = runtime.prepare_tcp_echo(Some(parent))?;
    world.activate(echo.owner)?;

    let config = TcpConfig {
        max_frame_bytes: 1024,
        read_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(2),
        write_timeout: Duration::from_secs(2),
        ..TcpConfig::default()
    };
    let listener = runtime.listen_tcp(parent, echo.owner, config)?;

    let mut client = TcpStream::connect(listener.address)?;
    client.set_read_timeout(Some(Duration::from_secs(2)))?;
    client.set_write_timeout(Some(Duration::from_secs(2)))?;
    let frames: [&[u8]; 3] = [b"native\0frame", b"", &[0, 255, 1, 2]];
    for frame in frames {
        write_frame(&mut client, frame)?;
        assert_eq!(read_frame(&mut client)?, frame);
    }
    drop(client);

    wait_until(Instant::now() + Duration::from_secs(2), || {
        let stats = listener.stats();
        stats.frames_received == 3 && stats.frames_written == 3
    });
    let stats = listener.stats();
    assert_eq!(stats.frames_received, 3);
    assert_eq!(stats.frames_written, 3);
    assert_eq!(stats.bytes_received, 16);

    world.request_drain(listener.owner, Instant::now() + Duration::from_secs(2))?;
    wait_until(Instant::now() + Duration::from_secs(2), || {
        matches!(world.state(listener.owner), Ok(Lifecycle::Stopped))
            && world
                .stop_report(listener.owner)
                .map(|report| report.native_done)
                .unwrap_or(false)
    });
    let listener_report = world.stop_report(listener.owner)?;
    assert!(listener_report.native_done);
    assert_eq!(listener_report.in_flight, 0);
    assert_eq!(listener.read_buffer_bytes(), 0);
    assert_eq!(listener.write_buffer_bytes(), 0);

    let report = runtime.close(Duration::from_secs(2))?;
    assert!(report.native_done);
    assert_eq!(report.in_flight, 0);
    assert_eq!(report.queued, 0);
    assert_eq!(runtime.active_tasks(), 0);
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    println!(
        "tcp loopback: frames_written={} bytes_written={} native_done={} in_flight={}",
        stats.frames_written, stats.bytes_written, report.native_done, report.in_flight
    );
    Ok(())
}
