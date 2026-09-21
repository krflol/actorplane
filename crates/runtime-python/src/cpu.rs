use super::*;

#[cfg(feature = "test-support")]
#[pyfunction]
#[pyo3(signature = (milliseconds=180))]
pub(super) fn _cpu_gil_probe<'py>(
    py: Python<'py>,
    milliseconds: u64,
) -> PyResult<Bound<'py, PyDict>> {
    use std::{
        sync::Barrier,
        time::{Duration, Instant},
    };
    entry();
    if !(100..=2000).contains(&milliseconds) {
        return Err(PyValueError::new_err("probe duration must be 100..2000 ms"));
    }
    let world = World::new(Config::default()).map_err(runtime_error)?;
    let runtime = NativeRuntime::new_with_cpu_config(
        world.clone(),
        32,
        actorplane_native::cpu::CpuConfig {
            workers: 2,
            max_jobs: 32,
        },
    )
    .map_err(runtime_error)?;
    let owner = world
        .allocate(EndpointKind::Native, None)
        .map_err(runtime_error)?;
    world.activate(owner).map_err(runtime_error)?;
    let target = runtime.prepare_sum_squares(None).map_err(runtime_error)?;
    world.activate(target.owner).map_err(runtime_error)?;
    let barrier = Barrier::new(2);
    let hold_start_ns = runtime.elapsed_ns();
    let boundary_start = BRIDGE_ENTRIES.load(Ordering::Relaxed);
    let (completed, first_progress_ns, last_progress_ns, native_stopped) = std::thread::scope(
        |scope| {
            let observer = scope.spawn(|| {
            barrier.wait();
            // Submit only after the measured hold begins. Observing a result
            // that completed before the hold would not prove native progress.
            let mut operations = Vec::with_capacity(8);
            for _ in 0..8 {
                operations.push(world.request(owner, target.owner, Payload::Pulse(10_000),
                    Instant::now() + Duration::from_secs(2)).expect("probe request admission"));
            }
            let deadline = Instant::now() + Duration::from_secs(1);
            let mut completed = 0usize;
            let mut first = None;
            let mut last = None;
            while Instant::now() < deadline && completed < operations.len() {
                completed = operations.iter().filter(|id| matches!(
                    world.operation_status(**id), Ok(actorplane_core::OperationStatus::Terminal(
                        actorplane_core::TerminalOutcome::Completed(_)))
                )).count();
                if completed > 0 {
                    let now = runtime.elapsed_ns();
                    first.get_or_insert(now);
                    last = Some(now);
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            let valid = operations.iter().all(|id| matches!(
                world.take_operation(*id), Ok(Some(actorplane_core::TerminalOutcome::Completed(result)))
                    if matches!(result.payload(), Payload::CountSnapshot { count: 10_000, total: 333_383_335_000 })
            ));
            let _ = world.stop(target.owner);
            let quiet_deadline = Instant::now() + Duration::from_millis(500);
            while runtime.active_tasks() > 0 && Instant::now() < quiet_deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            (if valid { completed } else { 0 }, first, last, runtime.active_tasks() == 0)
        });
            barrier.wait();
            std::thread::sleep(Duration::from_millis(milliseconds));
            observer.join().expect("CPU GIL probe observer panicked")
        },
    );
    let hold_end_ns = runtime.elapsed_ns();
    let entries = BRIDGE_ENTRIES.load(Ordering::Relaxed) - boundary_start;
    let mut runtime = runtime;
    let closed = runtime
        .close(Duration::from_secs(1))
        .map_err(runtime_error)?;
    let out = PyDict::new(py);
    out.set_item("completed", completed)?;
    out.set_item("expected", 8)?;
    out.set_item("native_stopped_in_hold", native_stopped)?;
    out.set_item("hold_start_ns", hold_start_ns)?;
    out.set_item("hold_end_ns", hold_end_ns)?;
    out.set_item("first_progress_ns", first_progress_ns)?;
    out.set_item("last_progress_ns", last_progress_ns)?;
    out.set_item("bridge_entries_during_hold", entries)?;
    out.set_item(
        "retained_payload_bytes",
        world.snapshot().retained_payload_bytes,
    )?;
    out.set_item("native_done", closed.native_done)?;
    Ok(out)
}

pub(super) fn stats<'py>(
    py: Python<'py>,
    value: actorplane_native::cpu::CpuStats,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("workers", value.workers)?;
    out.set_item("max_jobs", value.max_jobs)?;
    out.set_item("queued", value.queued)?;
    out.set_item("running", value.running)?;
    out.set_item("submitted", value.submitted)?;
    out.set_item("completed", value.completed)?;
    out.set_item("cancelled", value.cancelled)?;
    out.set_item("panicked", value.panicked)?;
    out.set_item("rejected", value.rejected)?;
    Ok(out)
}
