use super::*;

#[pyclass(module = "actorplane._native")]
pub(super) struct NativeTcpListener {
    pub(super) listener: actorplane_native::io::TcpListenerHandle,
}
#[pymethods]
impl NativeTcpListener {
    #[getter]
    fn owner(&self) -> Handle {
        entry();
        handle(self.listener.owner)
    }
    #[getter]
    fn address(&self) -> (String, u16) {
        entry();
        (
            self.listener.address.ip().to_string(),
            self.listener.address.port(),
        )
    }
    fn connections(&self) -> Vec<Handle> {
        entry();
        self.listener
            .connections()
            .into_iter()
            .map(handle)
            .collect()
    }
    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        entry();
        let stats = self.listener.stats();
        let out = PyDict::new(py);
        macro_rules! fields { ($($name:ident),* $(,)?) => { $(out.set_item(stringify!($name), stats.$name)?;)* }; }
        fields!(
            accepted_connections,
            rejected_connections,
            closed_connections,
            cancelled_connections,
            drained_connections,
            frames_received,
            requests_admitted,
            frames_written,
            bytes_received,
            bytes_written,
            oversized_frames,
            truncated_frames,
            read_errors,
            write_errors,
            read_timeouts,
            request_timeouts,
            write_timeouts,
            budget_rejections,
            admission_rejections,
            failed_requests,
            invalid_replies,
            listener_errors
        );
        out.set_item("active_connections", self.listener.connections().len())?;
        out.set_item("read_buffer_bytes", self.listener.read_buffer_bytes())?;
        out.set_item("write_buffer_bytes", self.listener.write_buffer_bytes())?;
        Ok(out)
    }
}

/// Test-only evidence: the calling thread stays attached while a Rust socket
/// client completes framed exchanges and stops the listener's native subtree.
#[cfg(feature = "test-support")]
#[pyfunction]
pub(super) fn _held_gil_tcp_probe<'py>(
    py: Python<'py>,
    world: &NativeWorld,
    listener: &NativeTcpListener,
) -> PyResult<Bound<'py, PyDict>> {
    use std::{
        io::{Read, Write},
        net::TcpStream,
        sync::Barrier,
        time::Instant,
    };
    entry();
    let world = world.world.clone();
    let listener = listener.listener.clone();
    if listener.owner.world != world.id() {
        return Err(PyValueError::new_err("foreign listener"));
    }
    let boundary_start = BRIDGE_ENTRIES.load(Ordering::Relaxed);
    let hold_start = world.elapsed_ns();
    let barrier = Barrier::new(2);
    let result = std::thread::scope(|scope| {
        let observer = scope.spawn(|| -> Result<(u64, u64, u64, bool), &'static str> {
            barrier.wait();
            let mut client = TcpStream::connect_timeout(&listener.address, Duration::from_secs(1))
                .map_err(|_| "connect failed")?;
            client
                .set_read_timeout(Some(Duration::from_secs(1)))
                .map_err(|_| "read timeout setup failed")?;
            client
                .set_write_timeout(Some(Duration::from_secs(1)))
                .map_err(|_| "write timeout setup failed")?;
            let mut first = 0;
            let mut last = 0;
            for value in 0..8u8 {
                client
                    .write_all(&[0, 0, 0, 1, value])
                    .map_err(|_| "write failed")?;
                let mut reply = [0; 5];
                client.read_exact(&mut reply).map_err(|_| "read failed")?;
                if reply != [0, 0, 0, 1, value] {
                    return Err("incorrect reply");
                }
                last = world.elapsed_ns();
                if value == 0 {
                    first = last;
                }
            }
            world.stop(listener.owner).map_err(|_| "stop failed")?;
            let end = Instant::now() + Duration::from_secs(1);
            while !world
                .stop_report(listener.owner)
                .map_err(|_| "report failed")?
                .native_done
                && Instant::now() < end
            {
                std::thread::sleep(Duration::from_millis(1));
            }
            let stopped = world
                .stop_report(listener.owner)
                .map_err(|_| "report failed")?
                .native_done;
            Ok((8, first, last, stopped))
        });
        barrier.wait();
        // Do not detach: native network completion must not need the GIL.
        observer
            .join()
            .map_err(|_| runtime_error("TCP probe thread panicked"))?
            .map_err(runtime_error)
    })?;
    let hold_end = world.elapsed_ns();
    let out = PyDict::new(py);
    out.set_item("progress_in_hold", result.0)?;
    out.set_item("first_progress_ns", result.1)?;
    out.set_item("last_progress_ns", result.2)?;
    out.set_item("native_stopped_in_hold", result.3)?;
    out.set_item("hold_start_ns", hold_start)?;
    out.set_item("hold_end_ns", hold_end)?;
    out.set_item(
        "bridge_entries_during_hold",
        BRIDGE_ENTRIES.load(Ordering::Relaxed) - boundary_start,
    )?;
    out.set_item("read_buffer_bytes", listener.read_buffer_bytes())?;
    out.set_item("write_buffer_bytes", listener.write_buffer_bytes())?;
    Ok(out)
}
