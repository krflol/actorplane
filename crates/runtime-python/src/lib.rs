//! Explicit Python boundary. Native crates never depend on this crate.
#![forbid(unsafe_code)]
mod components;
mod cpu;
mod envelopes;
mod schemas;
mod services;
mod tcp;
use components::{NativeComponent, PortHandle, port, port_handle};
use envelopes::{envelope_dict, parse_options};

use actorplane_core::operations::{OperationId, OperationStatus, TerminalOutcome};
use actorplane_core::{
    ActorRef, Config, DeliveryLease, EndpointKind, MessageOptions, Payload, PublicationStatus,
    PublicationTicket, PythonReadyPhase, StopReport, World,
};
use actorplane_core::{
    FailureAction, FailureDetails, FailureFrame, FailureLease, FailurePhase, FailureRecord,
};
use actorplane_native::cpu::CpuConfig;
use actorplane_native::{CounterPipeline, NativeRuntime};
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    prelude::*,
    types::{PyBool, PyBytes, PyDict, PyList, PyTuple},
};
use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

type Handle = (u64, u32, u64);
type Claimed = (u64, String, Py<PyAny>, u32, u64, Option<Handle>);
static BRIDGE_ENTRIES: AtomicU64 = AtomicU64::new(0);

fn entry() {
    BRIDGE_ENTRIES.fetch_add(1, Ordering::Relaxed);
}
fn actor(h: Handle) -> ActorRef {
    ActorRef {
        world: h.0,
        slot: h.1,
        generation: h.2,
    }
}
fn handle(r: ActorRef) -> Handle {
    (r.world, r.slot, r.generation)
}
fn runtime_error(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}
fn failure_action(value: &str) -> PyResult<FailureAction> {
    match value {
        "StopActor" | "stop_actor" => Ok(FailureAction::StopActor),
        "StopWorld" | "stop_world" => Ok(FailureAction::StopWorld),
        "Continue" | "continue" => Ok(FailureAction::Continue),
        _ => Err(PyValueError::new_err("unknown failure action")),
    }
}
fn failure_dict<'py>(py: Python<'py>, failure: &FailureRecord) -> PyResult<Bound<'py, PyDict>> {
    let output = PyDict::new(py);
    output.set_item("sequence", failure.sequence)?;
    output.set_item("elapsed_ns", failure.elapsed_ns)?;
    output.set_item("actor", handle(failure.actor))?;
    output.set_item("event_id", failure.event_id)?;
    output.set_item("schema", failure.schema)?;
    output.set_item("operation", failure.operation.map(operation_handle))?;
    output.set_item("phase", format!("{:?}", failure.details.phase()))?;
    output.set_item("handler", failure.details.handler())?;
    output.set_item("exception_type", failure.details.exception_type())?;
    output.set_item(
        "frames",
        failure
            .details
            .frames()
            .iter()
            .map(|frame| (frame.file(), frame.line(), frame.function()))
            .collect::<Vec<_>>(),
    )?;
    output.set_item("truncated", failure.details.truncated())?;
    output.set_item("action", format!("{:?}", failure.action))?;
    Ok(output)
}
fn operation(h: Handle) -> OperationId {
    OperationId {
        world: h.0,
        slot: h.1,
        generation: h.2,
    }
}
fn operation_handle(id: OperationId) -> Handle {
    (id.world, id.slot, id.generation)
}

fn ready_phase(value: &str) -> PyResult<PythonReadyPhase> {
    match value {
        "start" | "Start" => Ok(PythonReadyPhase::Start),
        "failure" | "Failure" => Ok(PythonReadyPhase::Failure),
        "delivery" | "Delivery" => Ok(PythonReadyPhase::Delivery),
        "cleanup" | "Cleanup" => Ok(PythonReadyPhase::Cleanup),
        _ => Err(PyValueError::new_err("unknown Python readiness phase")),
    }
}

fn payload_fields(py: Python<'_>, payload: &Payload) -> PyResult<(&'static str, Py<PyAny>, u32)> {
    if let Payload::Structured(record) = payload {
        return Ok((
            "structured",
            PyBytes::new(py, record.bytes()).into_any().unbind(),
            record.schema(),
        ));
    }
    let (kind, values, schema) = match payload {
        Payload::Pulse(value) => ("pulse", vec![*value], 0),
        Payload::CountSnapshot { count, total } => (
            "snapshot",
            vec![
                i64::try_from(*count).map_err(|_| {
                    PyValueError::new_err("snapshot count exceeds int64 bridge range")
                })?,
                *total,
            ],
            0,
        ),
        Payload::Record { schema, integers } => ("record", integers.clone(), *schema),
        Payload::Structured(_) => unreachable!(),
    };
    Ok((kind, PyList::new(py, values)?.into_any().unbind(), schema))
}

fn outcome_dict<'py>(
    py: Python<'py>,
    world: &World,
    outcome: TerminalOutcome,
) -> PyResult<Bound<'py, PyDict>> {
    let output = PyDict::new(py);
    match outcome {
        TerminalOutcome::Completed(payload) => {
            let (kind, values, schema) = payload_fields(py, payload.payload())?;
            output.set_item("state", "Completed")?;
            output.set_item("kind", kind)?;
            output.set_item("values", values)?;
            output.set_item("schema", schema)?;
            output.set_item(
                "envelope",
                payload
                    .envelope()
                    .map(|envelope| envelope_dict(py, world, envelope))
                    .transpose()?,
            )?;
        }
        TerminalOutcome::Failed { code, message } => {
            output.set_item("state", "Failed")?;
            output.set_item("code", format!("{code:?}"))?;
            output.set_item("message", message)?;
        }
        terminal => output.set_item("state", format!("{terminal:?}"))?,
    }
    Ok(output)
}
fn duration(ms: u64) -> PyResult<Duration> {
    if ms > 86_400_000 {
        return Err(PyValueError::new_err(
            "duration exceeds the 24-hour timer limit",
        ));
    }
    Ok(Duration::from_millis(ms))
}

fn report_dict(py: Python<'_>, report: StopReport) -> PyResult<Bound<'_, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("native_done", report.native_done)?;
    out.set_item("python_done", report.python_done)?;
    out.set_item("in_flight", report.in_flight)?;
    out.set_item("discarded", report.discarded)?;
    out.set_item("timed_out", report.timed_out)?;
    out.set_item("queued", report.queued)?;
    out.set_item("delivery_in_flight", report.delivery_in_flight)?;
    out.set_item("control_in_flight", report.control_in_flight)?;
    out.set_item("native_tasks", report.native_tasks)?;
    out.set_item("python_pending", report.python_pending)?;
    out.set_item("pending_notifications", report.pending_notifications)?;
    out.set_item("outstanding_operations", report.outstanding_operations)?;
    out.set_item("retained_operations", report.retained_operations)?;
    out.set_item("outstanding_publications", report.outstanding_publications)?;
    out.set_item("retained_publications", report.retained_publications)?;
    out.set_item("errors", report.errors)?;
    out.set_item(
        "last_error",
        report
            .last_error
            .as_ref()
            .map(|failure| failure_dict(py, failure))
            .transpose()?,
    )?;
    Ok(out)
}

fn publication_report_dict<'py>(
    py: Python<'py>,
    report: &actorplane_core::DeliveryReport,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("admitted", report.admitted)?;
    out.set_item("rejected", report.rejected)?;
    out.set_item("matched", report.matched)?;
    out.set_item("staged", report.staged)?;
    out.set_item("outcome", format!("{:?}", report.outcome))?;
    Ok(out)
}

#[pyclass(module = "actorplane._native")]
struct NativePublicationTicket {
    ticket: PublicationTicket,
}

#[pymethods]
impl NativePublicationTicket {
    fn identifier(&self) -> u64 {
        self.ticket.id()
    }

    fn source(&self) -> Handle {
        handle(self.ticket.source())
    }

    fn status(&self) -> String {
        match self.ticket.status() {
            PublicationStatus::Staged => "Staged".to_owned(),
            PublicationStatus::Queued => "Queued".to_owned(),
            PublicationStatus::Routing(_) => "Routing".to_owned(),
            PublicationStatus::Terminal(report) => format!("{:?}", report.outcome),
        }
    }

    fn report<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        self.ticket
            .report()
            .as_ref()
            .map(|report| publication_report_dict(py, report))
            .transpose()
    }
}

/// The adapter retains only bounded outstanding claims. Python callables live in
/// the Python driver; Tokio never receives a Py<T>, callback or Python payload.
#[pyclass(module = "actorplane._native")]
struct NativeWorld {
    world: World,
    runtime: Mutex<Option<NativeRuntime>>,
    cpu: actorplane_native::cpu::CpuMonitor,
    claims: Mutex<HashMap<u64, DeliveryLease>>,
    failure_claims: Mutex<HashMap<u64, FailureLease>>,
    shutdown_timed_out: AtomicBool,
    max_event_bytes: usize,
    max_schemas: usize,
    controlled: actorplane_native::controls::ControlledReplies,
}

impl NativeWorld {
    fn message_options(
        &self,
        value: Option<&Bound<'_, PyAny>>,
        claim_token: Option<u64>,
    ) -> PyResult<MessageOptions> {
        let inherited = claim_token
            .map(|token| {
                let claims = self.claims.lock().unwrap();
                let lease = claims
                    .get(&token)
                    .ok_or_else(|| PyValueError::new_err("unknown or completed claim"))?;
                Ok::<_, PyErr>(lease.envelope().child_options(lease.envelope().destination))
            })
            .transpose()?;
        parse_options(value, inherited, self.world.now())
    }
    fn payload(&self, kind: &str, values: &Bound<'_, PyAny>, schema: u32) -> PyResult<Payload> {
        if kind == "structured" {
            let bytes = values.cast::<PyBytes>().map_err(|_| {
                PyValueError::new_err("structured payload requires canonical bytes")
            })?;
            return self
                .world
                .structured(schema, bytes.as_bytes())
                .map_err(|error| PyValueError::new_err(error.to_string()));
        }
        let values = values.cast::<PyList>()?;
        if values.len() > 4096
            || values.len().saturating_mul(8).saturating_add(4) > self.max_event_bytes
        {
            return Err(PyValueError::new_err(
                "event exceeds the element or byte limit",
            ));
        }
        let integers: Vec<i64> = values
            .iter()
            .map(|v| {
                if v.is_instance_of::<PyBool>() {
                    return Err(PyValueError::new_err("bool is not an int64"));
                }
                v.extract::<i64>()
            })
            .collect::<PyResult<_>>()?;
        match (kind, integers.as_slice()) {
            ("pulse", [value]) => Ok(Payload::Pulse(*value)),
            ("snapshot", [count, total]) if *count >= 0 => Ok(Payload::CountSnapshot {
                count: *count as u64,
                total: *total,
            }),
            ("record", _) if schema > 0 => Ok(Payload::Record { schema, integers }),
            _ => Err(PyValueError::new_err(
                "invalid payload kind, schema or fields",
            )),
        }
    }
}

#[pymethods]
impl NativeWorld {
    #[new]
    #[pyo3(signature = (*, max_actors=128, mailbox_capacity=64, mailbox_bytes=65536, native_payload_budget=4194304, max_subscriptions=512, max_event_bytes=32768, max_tasks=256, max_operations=256, diagnostic_capacity=1024, max_schemas=128, max_services=64, max_service_leases=1024, max_leases_per_actor=16, max_publications=256, max_publications_per_source=32, max_publication_fanout=4096, max_routing_snapshot_entries=16384, routing_batch_size=32, virtual_time=false, cpu_workers=2, max_cpu_jobs=32))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        max_actors: usize,
        mailbox_capacity: usize,
        mailbox_bytes: usize,
        native_payload_budget: usize,
        max_subscriptions: usize,
        max_event_bytes: usize,
        max_tasks: usize,
        max_operations: usize,
        diagnostic_capacity: usize,
        max_schemas: usize,
        max_services: usize,
        max_service_leases: usize,
        max_leases_per_actor: usize,
        max_publications: usize,
        max_publications_per_source: usize,
        max_publication_fanout: usize,
        max_routing_snapshot_entries: usize,
        routing_batch_size: usize,
        virtual_time: bool,
        cpu_workers: usize,
        max_cpu_jobs: usize,
    ) -> PyResult<Self> {
        entry();
        if max_actors > 65536
            || max_subscriptions > 65536
            || max_tasks > 65536
            || max_event_bytes > 1048576
            || max_operations > 65536
            || diagnostic_capacity > 65536
            || max_schemas > 65536
            || max_services > 4096
            || max_service_leases > 65536
            || max_leases_per_actor > 1024
            || max_publications > 65536
            || max_publications_per_source > 4096
            || max_publication_fanout > 65536
            || max_routing_snapshot_entries > 1048576
            || routing_batch_size > 4096
        {
            return Err(PyValueError::new_err(
                "configuration exceeds this release's registration or event-size ceiling",
            ));
        }
        if !(1..=64).contains(&cpu_workers) || !(1..=4096).contains(&max_cpu_jobs) {
            return Err(PyValueError::new_err(
                "CPU workers must be in 1..64 and max_cpu_jobs in 1..4096",
            ));
        }
        let config = Config {
            max_actors,
            mailbox_capacity,
            mailbox_bytes,
            native_payload_budget,
            max_subscriptions,
            max_event_bytes,
            max_operations,
            diagnostic_capacity,
            max_schemas,
            max_services,
            max_service_leases,
            max_leases_per_actor,
            max_publications,
            max_publications_per_source,
            max_publication_fanout,
            max_routing_snapshot_entries,
            routing_batch_size,
        };
        let (world, runtime) = if virtual_time {
            let runtime = NativeRuntime::new_virtual(config, max_tasks).map_err(runtime_error)?;
            (runtime.world().clone(), runtime)
        } else {
            let world = World::new(config).map_err(runtime_error)?;
            let runtime = NativeRuntime::new_with_cpu_config(
                world.clone(),
                max_tasks,
                CpuConfig {
                    workers: cpu_workers,
                    max_jobs: max_cpu_jobs,
                },
            )
            .map_err(runtime_error)?;
            (world, runtime)
        };
        Ok(Self {
            world,
            cpu: runtime.cpu_monitor(),
            runtime: Mutex::new(Some(runtime)),
            claims: Mutex::new(HashMap::new()),
            failure_claims: Mutex::new(HashMap::new()),
            shutdown_timed_out: AtomicBool::new(false),
            max_event_bytes,
            max_schemas,
            controlled: actorplane_native::controls::ControlledReplies::new(max_operations)
                .map_err(runtime_error)?,
        })
    }

    #[pyo3(signature = (python=true, parent=None))]
    fn allocate(&self, python: bool, parent: Option<Handle>) -> PyResult<Handle> {
        entry();
        self.world
            .allocate(
                if python {
                    EndpointKind::Python
                } else {
                    EndpointKind::Native
                },
                parent.map(actor),
            )
            .map(handle)
            .map_err(runtime_error)
    }

    fn register_schemas(&self, descriptors: &Bound<'_, PyTuple>) -> PyResult<Vec<u32>> {
        entry();
        if descriptors.len() > self.max_schemas {
            return Err(PyValueError::new_err(
                "schema registration batch exceeds World limit",
            ));
        }
        let schemas = descriptors
            .iter()
            .map(|d| schemas::parse(&d))
            .collect::<PyResult<_>>()?;
        self.world
            .register_schemas(schemas)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    fn cpu_stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        entry();
        cpu::stats(py, self.cpu.stats())
    }

    fn cpu_sum_squares(&self, py: Python<'_>, parent: Option<Handle>) -> PyResult<Handle> {
        entry();
        py.detach(|| {
            let guard = self.runtime.lock().unwrap();
            let runtime = guard
                .as_ref()
                .ok_or_else(|| runtime_error("World is closed"))?;
            let prepared = runtime
                .prepare_sum_squares(parent.map(actor))
                .map_err(runtime_error)?;
            if let Err(error) = runtime.world().activate(prepared.owner) {
                let _ = runtime.world().stop(prepared.owner);
                return Err(runtime_error(error));
            }
            Ok(handle(prepared.owner))
        })
    }
    fn activate(&self, reference: Handle) -> PyResult<()> {
        entry();
        self.world.activate(actor(reference)).map_err(runtime_error)
    }
    fn state(&self, reference: Handle) -> PyResult<String> {
        entry();
        self.world
            .state(actor(reference))
            .map(|s| format!("{s:?}"))
            .map_err(runtime_error)
    }

    fn ready_cutoff(&self) -> PyResult<u64> {
        entry();
        Ok(self.world.python_ready_cutoff())
    }

    #[pyo3(signature = (phase, after=None, through=0))]
    fn ready_next(
        &self,
        phase: &str,
        after: Option<u64>,
        through: u64,
    ) -> PyResult<Option<(u64, Handle)>> {
        entry();
        let phase = ready_phase(phase)?;
        Ok(self
            .world
            .python_ready_next(phase, after, through)
            .map(|(order, reference)| (order, handle(reference))))
    }

    #[pyo3(signature = (reference, kind, values, schema=0, options=None, claim_token=None))]
    #[allow(clippy::too_many_arguments)]
    fn send(
        &self,
        reference: Handle,
        kind: &str,
        values: &Bound<'_, PyAny>,
        schema: u32,
        options: Option<&Bound<'_, PyAny>>,
        claim_token: Option<u64>,
    ) -> PyResult<u64> {
        entry();
        self.world
            .send_with(
                actor(reference),
                self.payload(kind, values, schema)?,
                self.message_options(options, claim_token)?,
            )
            .map_err(runtime_error)
    }
    fn subscribe(&self, source: Handle, target: Handle) -> PyResult<u64> {
        entry();
        self.world
            .subscribe(actor(source), actor(target))
            .map_err(runtime_error)
    }
    fn unsubscribe(&self, token: u64) -> PyResult<()> {
        entry();
        self.world.unsubscribe(token).map_err(runtime_error)
    }

    fn claim(&self, py: Python<'_>, reference: Handle) -> PyResult<Option<Claimed>> {
        entry();
        let Some(lease) = self.world.claim(actor(reference)).map_err(runtime_error)? else {
            return Ok(None);
        };
        let (kind, values, schema) = payload_fields(py, lease.payload())?;
        let event_id = lease.event_id();
        let operation = lease.operation().map(operation_handle);
        self.claims.lock().unwrap().insert(event_id, lease);
        Ok(Some((
            event_id,
            kind.into(),
            values,
            schema,
            event_id,
            operation,
        )))
    }

    fn claim_metadata<'py>(&self, py: Python<'py>, token: u64) -> PyResult<Bound<'py, PyDict>> {
        entry();
        let claims = self.claims.lock().unwrap();
        let lease = claims
            .get(&token)
            .ok_or_else(|| PyValueError::new_err("unknown or completed claim"))?;
        envelope_dict(py, &self.world, lease.envelope())
    }

    #[pyo3(signature = (token, success=true))]
    fn finish(&self, token: u64, success: bool) -> PyResult<()> {
        entry();
        let lease = self
            .claims
            .lock()
            .unwrap()
            .remove(&token)
            .ok_or_else(|| PyValueError::new_err("unknown or already completed claim"))?;
        lease.finish(success);
        Ok(())
    }
    #[pyo3(signature = (reference, phase, handler, exception_type, frames, action, event_id=None, schema=None, operation=None, truncated=false))]
    #[allow(clippy::too_many_arguments)]
    fn report_failure<'py>(
        &self,
        py: Python<'py>,
        reference: Handle,
        phase: &str,
        handler: &str,
        exception_type: &str,
        frames: &Bound<'py, PyList>,
        action: &str,
        event_id: Option<u64>,
        schema: Option<u32>,
        operation: Option<Handle>,
        truncated: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        entry();
        let phase = match phase {
            "Construction" => FailurePhase::Construction,
            "Configure" => FailurePhase::Configure,
            "Start" => FailurePhase::Start,
            "Handler" => FailurePhase::Handler,
            "Stop" => FailurePhase::Stop,
            "Supervisor" => FailurePhase::Supervisor,
            _ => return Err(PyValueError::new_err("unknown failure phase")),
        };
        if frames.len() > 8 {
            return Err(PyValueError::new_err(
                "failure traceback exceeds eight frames",
            ));
        }
        let mut native_frames = Vec::with_capacity(frames.len());
        for item in frames.iter() {
            let tuple = item.cast::<PyTuple>()?;
            if tuple.len() != 3 {
                return Err(PyValueError::new_err(
                    "failure frame must be (file, line, function)",
                ));
            }
            let file = tuple.get_item(0)?;
            let function = tuple.get_item(2)?;
            native_frames.push(FailureFrame::new(
                file.extract()?,
                tuple.get_item(1)?.extract()?,
                function.extract()?,
            ));
        }
        let details = FailureDetails::new(phase, handler, exception_type, native_frames)
            .with_truncated(truncated);
        let failure = self
            .world
            .report_failure(
                actor(reference),
                event_id,
                schema,
                operation.map(crate::operation),
                details,
                failure_action(action)?,
            )
            .map_err(runtime_error)?;
        failure_dict(py, &failure)
    }
    fn claim_failure<'py>(
        &self,
        py: Python<'py>,
        supervisor: Handle,
    ) -> PyResult<Option<(u64, Bound<'py, PyDict>, u64)>> {
        entry();
        let Some(lease) = self
            .world
            .claim_failure(actor(supervisor))
            .map_err(runtime_error)?
        else {
            return Ok(None);
        };
        let token = lease.failure().sequence;
        let data = failure_dict(py, lease.failure())?;
        let coalesced = lease.coalesced();
        self.failure_claims.lock().unwrap().insert(token, lease);
        Ok(Some((token, data, coalesced)))
    }
    #[pyo3(signature = (token, action=None))]
    fn finish_failure(&self, token: u64, action: Option<&str>) -> PyResult<()> {
        entry();
        let action = action.map(failure_action).transpose()?;
        let lease = self
            .failure_claims
            .lock()
            .unwrap()
            .remove(&token)
            .ok_or_else(|| PyValueError::new_err("unknown or already completed failure claim"))?;
        lease.finish(action).map_err(runtime_error)
    }
    #[pyo3(signature = (owner, target, kind, values, schema=0, timeout_ms=1000, options=None, claim_token=None))]
    #[allow(clippy::too_many_arguments)]
    fn request(
        &self,
        owner: Handle,
        target: Handle,
        kind: &str,
        values: &Bound<'_, PyAny>,
        schema: u32,
        timeout_ms: u64,
        options: Option<&Bound<'_, PyAny>>,
        claim_token: Option<u64>,
    ) -> PyResult<Handle> {
        entry();
        let deadline = self.world.now() + duration(timeout_ms)?;
        self.world
            .request_with(
                actor(owner),
                actor(target),
                self.payload(kind, values, schema)?,
                deadline,
                self.message_options(options, claim_token)?,
            )
            .map(operation_handle)
            .map_err(runtime_error)
    }
    #[pyo3(signature = (id, target, kind, values, schema=0, options=None))]
    #[allow(clippy::too_many_arguments)]
    fn reply(
        &self,
        id: Handle,
        target: Handle,
        kind: &str,
        values: &Bound<'_, PyAny>,
        schema: u32,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        entry();
        self.world
            .complete_operation_with(
                operation(id),
                actor(target),
                self.payload(kind, values, schema)?,
                Some(parse_options(options, None, self.world.now())?),
            )
            .map_err(runtime_error)
    }
    fn operation_status<'py>(&self, py: Python<'py>, id: Handle) -> PyResult<Bound<'py, PyDict>> {
        entry();
        match self
            .world
            .operation_status(operation(id))
            .map_err(runtime_error)?
        {
            OperationStatus::Pending(_) => {
                let output = PyDict::new(py);
                output.set_item("state", "Pending")?;
                Ok(output)
            }
            OperationStatus::Terminal(outcome) => outcome_dict(py, &self.world, outcome),
        }
    }
    fn take_operation<'py>(
        &self,
        py: Python<'py>,
        id: Handle,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        entry();
        self.world
            .take_operation(operation(id))
            .map_err(runtime_error)?
            .map(|outcome| outcome_dict(py, &self.world, outcome))
            .transpose()
    }
    fn cancel_operation(&self, id: Handle) -> PyResult<bool> {
        entry();
        self.world
            .cancel_operation(operation(id))
            .map_err(runtime_error)
    }
    fn stop<'py>(&self, py: Python<'py>, reference: Handle) -> PyResult<Bound<'py, PyDict>> {
        entry();
        report_dict(
            py,
            self.world.stop(actor(reference)).map_err(runtime_error)?,
        )
    }
    fn cancel_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        entry();
        report_dict(py, self.world.close())
    }
    #[pyo3(signature = (reference=None))]
    fn stop_report<'py>(
        &self,
        py: Python<'py>,
        reference: Option<Handle>,
    ) -> PyResult<Bound<'py, PyDict>> {
        entry();
        let report = if let Some(reference) = reference {
            self.world
                .stop_report(actor(reference))
                .map_err(runtime_error)?
        } else {
            let mut report = self.world.shutdown_report();
            report.timed_out |= self.shutdown_timed_out.load(Ordering::Acquire);
            report
        };
        report_dict(py, report)
    }
    fn finish_python(&self, reference: Handle) -> PyResult<()> {
        entry();
        self.world
            .finish_python(actor(reference))
            .map_err(runtime_error)
    }

    fn drain<'py>(
        &self,
        py: Python<'py>,
        reference: Handle,
        deadline_ms: u64,
    ) -> PyResult<Bound<'py, PyDict>> {
        entry();
        let deadline = self.world.now() + duration(deadline_ms)?;
        report_dict(
            py,
            self.world
                .request_drain(actor(reference), deadline)
                .map_err(runtime_error)?,
        )
    }

    fn drain_all<'py>(&self, py: Python<'py>, deadline_ms: u64) -> PyResult<Bound<'py, PyDict>> {
        entry();
        let deadline = self.world.now() + duration(deadline_ms)?;
        report_dict(py, self.world.drain_all(deadline))
    }

    fn snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        entry();
        let snapshot = self.world.snapshot();
        let active_tasks: usize = snapshot.actors.iter().map(|actor| actor.native_tasks).sum();
        let out = PyDict::new(py);
        let actors = PyList::empty(py);
        let links = PyList::empty(py);
        for link in self.world.links() {
            let item = PyDict::new(py);
            item.set_item("id", link.id)?;
            item.set_item("owner", handle(link.owner))?;
            item.set_item("source", port_handle(link.source))?;
            item.set_item("target", port_handle(link.target))?;
            links.append(item)?;
        }
        out.set_item("links", links)?;
        let services = PyList::empty(py);
        for service in self.world.services() {
            let item = PyDict::new(py);
            item.set_item("name", service.name)?;
            item.set_item("service", handle(service.service))?;
            item.set_item("contract", services::interface_dict(py, &service.contract)?)?;
            item.set_item("leases", service.leases)?;
            services.append(item)?;
        }
        out.set_item("services", services)?;
        let leases = PyList::empty(py);
        for lease in self.world.service_leases() {
            let item = PyDict::new(py);
            item.set_item("scope", handle(lease.scope))?;
            item.set_item("owner", handle(lease.owner))?;
            item.set_item("service", handle(lease.service))?;
            leases.append(item)?;
        }
        out.set_item("service_leases", leases)?;
        for a in snapshot.actors {
            let d = PyDict::new(py);
            d.set_item("handle", handle(a.reference))?;
            d.set_item("kind", format!("{:?}", a.kind))?;
            d.set_item("state", format!("{:?}", a.state))?;
            d.set_item("parent", a.parent.map(handle))?;
            d.set_item(
                "drain_remaining_ms",
                a.drain_deadline
                    .map(|d| d.saturating_duration_since(self.world.now()).as_millis() as u64),
            )?;
            d.set_item("queue_entries", a.queue_entries)?;
            d.set_item("queue_bytes", a.queue_bytes)?;
            d.set_item("staged_entries", a.staged_entries)?;
            d.set_item("staged_bytes", a.staged_bytes)?;
            d.set_item(
                "component",
                a.component
                    .as_ref()
                    .map(|value| components::descriptor_dict(py, value))
                    .transpose()?,
            )?;
            d.set_item("in_flight", a.in_flight as usize)?;
            d.set_item("control_in_flight", a.control_in_flight as usize)?;
            d.set_item("failure_pending", a.failure_pending)?;
            d.set_item("native_tasks", a.native_tasks)?;
            d.set_item("python_done", a.python_done)?;
            actors.append(d)?;
        }
        let metrics = PyDict::new(py);
        metrics.set_item("submitted", snapshot.metrics.submitted)?;
        metrics.set_item("admitted", snapshot.metrics.admitted)?;
        metrics.set_item("rejected", snapshot.metrics.rejected)?;
        metrics.set_item("started", snapshot.metrics.started)?;
        metrics.set_item("completed", snapshot.metrics.completed)?;
        metrics.set_item("failed", snapshot.metrics.failed)?;
        metrics.set_item("cancelled", snapshot.metrics.cancelled)?;
        metrics.set_item("expired", snapshot.metrics.expired)?;
        metrics.set_item("discarded", snapshot.metrics.discarded)?;
        metrics.set_item("failures", snapshot.metrics.failures)?;
        metrics.set_item(
            "notifications_coalesced",
            snapshot.metrics.notifications_coalesced,
        )?;
        metrics.set_item(
            "notifications_delivered",
            snapshot.metrics.notifications_delivered,
        )?;
        metrics.set_item(
            "notifications_discarded",
            snapshot.metrics.notifications_discarded,
        )?;
        metrics.set_item("drain_timeouts", snapshot.metrics.drain_timeouts)?;
        metrics.set_item("operation_completed", snapshot.metrics.operation_completed)?;
        metrics.set_item("operation_timed_out", snapshot.metrics.operation_timed_out)?;
        metrics.set_item("operation_cancelled", snapshot.metrics.operation_cancelled)?;
        metrics.set_item(
            "operation_late_results",
            snapshot.metrics.operation_late_results,
        )?;
        metrics.set_item(
            "publication_submitted",
            snapshot.metrics.publication_submitted,
        )?;
        metrics.set_item(
            "publication_admitted",
            snapshot.metrics.publication_admitted,
        )?;
        metrics.set_item(
            "publication_rejected",
            snapshot.metrics.publication_rejected,
        )?;
        metrics.set_item(
            "publication_completed",
            snapshot.metrics.publication_completed,
        )?;
        metrics.set_item(
            "publication_cancelled",
            snapshot.metrics.publication_cancelled,
        )?;
        metrics.set_item("publication_expired", snapshot.metrics.publication_expired)?;
        metrics.set_item("publication_failed", snapshot.metrics.publication_failed)?;
        out.set_item("id", self.world.id())?;
        out.set_item("native_schemas", self.world.schema_count())?;
        out.set_item("actors", actors)?;
        out.set_item("metrics", metrics)?;
        out.set_item("retained_payload_bytes", snapshot.retained_payload_bytes)?;
        out.set_item("publications_pending", snapshot.publications_pending)?;
        out.set_item("publications_retained", snapshot.publications_retained)?;
        out.set_item(
            "routing_snapshot_entries",
            snapshot.routing_snapshot_entries,
        )?;
        out.set_item("subscriptions", snapshot.subscriptions)?;
        out.set_item("operation_pending", snapshot.operation_pending)?;
        out.set_item("operation_retained", snapshot.operation_retained)?;
        out.set_item(
            "control_tasks",
            self.runtime
                .lock()
                .unwrap()
                .as_ref()
                .map_or(0, NativeRuntime::control_tasks),
        )?;
        out.set_item(
            "routing_tasks",
            self.runtime
                .lock()
                .unwrap()
                .as_ref()
                .map_or(0, NativeRuntime::routing_tasks),
        )?;
        out.set_item("cpu", cpu::stats(py, self.cpu.stats())?)?;
        // Core leases remain authoritative if the scheduler has been detached
        // after a shutdown timeout but a noncooperative poll is still running.
        out.set_item("active_tasks", active_tasks)?;
        Ok(out)
    }

    fn wait(&self, py: Python<'_>, milliseconds: u64) -> PyResult<()> {
        entry();
        if self.world.is_virtual() {
            return Err(runtime_error("TestWorld requires explicit virtual advance"));
        }
        if milliseconds > 1000 {
            return Err(PyValueError::new_err("driver wait cannot exceed 1000 ms"));
        }
        let world = self.world.clone();
        py.detach(|| world.wait_python_ready(Duration::from_millis(milliseconds)));
        Ok(())
    }

    #[pyo3(signature = (after_sequence=0, limit=100))]
    fn diagnostics<'py>(
        &self,
        py: Python<'py>,
        after_sequence: u64,
        limit: usize,
    ) -> PyResult<Bound<'py, PyDict>> {
        entry();
        let history = self
            .world
            .diagnostics(after_sequence, limit)
            .map_err(runtime_error)?;
        let output = PyDict::new(py);
        let entries = PyList::empty(py);
        for record in history.entries {
            let item = PyDict::new(py);
            item.set_item("sequence", record.sequence)?;
            item.set_item("elapsed_ns", record.elapsed_ns)?;
            item.set_item("code", format!("{:?}", record.code))?;
            item.set_item("actor", record.actor.map(handle))?;
            item.set_item("operation", record.operation.map(operation_handle))?;
            item.set_item("event_id", record.event_id)?;
            item.set_item("correlation_id", record.correlation_id)?;
            item.set_item("causation_id", record.causation_id)?;
            if let Some(failure) = record.failure {
                item.set_item("failure", failure_dict(py, &failure)?)?;
            } else {
                item.set_item("failure", py.None())?;
            }
            entries.append(item)?;
        }
        output.set_item("entries", entries)?;
        output.set_item("next_sequence", history.next_sequence)?;
        output.set_item("dropped", history.dropped)?;
        if let Some(gap) = history.gap {
            let value = PyDict::new(py);
            value.set_item("first_available", gap.first_available)?;
            value.set_item("requested_after", gap.requested_after)?;
            output.set_item("gap", value)?;
        } else {
            output.set_item("gap", py.None())?;
        }
        Ok(output)
    }

    fn register_component(
        &self,
        owner: Handle,
        descriptor: &Bound<'_, PyTuple>,
    ) -> PyResult<Vec<PortHandle>> {
        entry();
        self.world
            .register_component(actor(owner), components::descriptor(descriptor)?)
            .map(|ports| ports.into_iter().map(port_handle).collect())
            .map_err(runtime_error)
    }
    fn component<'py>(
        &self,
        py: Python<'py>,
        owner: Handle,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        entry();
        self.world
            .component(actor(owner))
            .map_err(runtime_error)?
            .map(|value| components::descriptor_dict(py, &value))
            .transpose()
    }
    fn require_interface(&self, owner: Handle, descriptor: &Bound<'_, PyTuple>) -> PyResult<()> {
        entry();
        let descriptor = components::descriptor(descriptor)?;
        if descriptor.interfaces.len() != 1 {
            return Err(PyValueError::new_err("one required interface expected"));
        }
        self.world
            .require_interface(actor(owner), &descriptor.interfaces[0])
            .map_err(runtime_error)
    }
    fn register_service(
        &self,
        name: &str,
        target: Handle,
        descriptor: &Bound<'_, PyTuple>,
    ) -> PyResult<()> {
        entry();
        let descriptor = services::descriptor(descriptor)?;
        if descriptor.interfaces.len() != 1 {
            return Err(PyValueError::new_err(
                "service contract requires exactly one interface",
            ));
        }
        let contract = &descriptor.interfaces[0];
        self.world
            .register_service(name, actor(target), contract)
            .map_err(runtime_error)
    }
    #[pyo3(signature = (owner, name, descriptor, expected_service=None))]
    fn acquire_service(
        &self,
        owner: Handle,
        name: &str,
        descriptor: &Bound<'_, PyTuple>,
        expected_service: Option<Handle>,
    ) -> PyResult<(Handle, Handle)> {
        entry();
        let descriptor = services::descriptor(descriptor)?;
        if descriptor.interfaces.len() != 1 {
            return Err(PyValueError::new_err(
                "service contract requires exactly one interface",
            ));
        }
        let contract = &descriptor.interfaces[0];
        let lease = match expected_service {
            Some(expected) => {
                self.world
                    .acquire_service_from(actor(owner), name, contract, actor(expected))
            }
            None => self.world.acquire_service(actor(owner), name, contract),
        }
        .map_err(runtime_error)?;
        Ok((handle(lease.scope), handle(lease.service)))
    }
    #[pyo3(signature = (scope, owner, service, port, kind, values, schema, deadline_ms, options=None, claim_token=None))]
    #[allow(clippy::too_many_arguments)]
    fn service_request(
        &self,
        scope: Handle,
        owner: Handle,
        service: Handle,
        port: &str,
        kind: &str,
        values: &Bound<'_, PyAny>,
        schema: u32,
        deadline_ms: u64,
        options: Option<&Bound<'_, PyAny>>,
        claim_token: Option<u64>,
    ) -> PyResult<Handle> {
        entry();
        let payload = self.payload(kind, values, schema)?;
        let options = self.message_options(options, claim_token)?;
        let lease = actorplane_core::ServiceLease {
            scope: actor(scope),
            owner: actor(owner),
            service: actor(service),
        };
        let id = self
            .world
            .service_request(
                lease,
                port,
                payload,
                self.world.now() + duration(deadline_ms)?,
                options,
            )
            .map_err(runtime_error)?;
        Ok(operation_handle(id))
    }
    fn service_link(
        &self,
        scope: Handle,
        owner: Handle,
        service: Handle,
        output: &str,
        target: PortHandle,
    ) -> PyResult<u64> {
        entry();
        let lease = actorplane_core::ServiceLease {
            scope: actor(scope),
            owner: actor(owner),
            service: actor(service),
        };
        self.world
            .service_link(lease, output, port(target))
            .map_err(runtime_error)
    }
    fn release_service(&self, scope: Handle, owner: Handle, service: Handle) -> PyResult<bool> {
        entry();
        let lease = actorplane_core::ServiceLease {
            scope: actor(scope),
            owner: actor(owner),
            service: actor(service),
        };
        self.world.release_service(lease).map_err(runtime_error)
    }
    fn port(&self, owner: Handle, name: &str) -> PyResult<PortHandle> {
        entry();
        self.world
            .port(actor(owner), name)
            .map(port_handle)
            .map_err(runtime_error)
    }
    fn link(&self, owner: Handle, source: PortHandle, target: PortHandle) -> PyResult<u64> {
        entry();
        self.world
            .link(actor(owner), port(source), port(target))
            .map_err(runtime_error)
    }
    #[pyo3(signature = (target, kind, values, schema, options=None, claim_token=None))]
    #[allow(clippy::too_many_arguments)]
    fn send_port(
        &self,
        target: PortHandle,
        kind: &str,
        values: &Bound<'_, PyAny>,
        schema: u32,
        options: Option<&Bound<'_, PyAny>>,
        claim_token: Option<u64>,
    ) -> PyResult<u64> {
        entry();
        self.world
            .send_port_with(
                port(target),
                self.payload(kind, values, schema)?,
                self.message_options(options, claim_token)?,
            )
            .map_err(runtime_error)
    }
    #[pyo3(signature = (source, kind, values, schema, claim_token=None, options=None))]
    #[allow(clippy::too_many_arguments)]
    fn publish_port(
        &self,
        py: Python<'_>,
        source: PortHandle,
        kind: &str,
        values: &Bound<'_, PyAny>,
        schema: u32,
        claim_token: Option<u64>,
        options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<NativePublicationTicket>> {
        entry();
        let payload = self.payload(kind, values, schema)?;
        let mut options = self.message_options(options, claim_token)?;
        options.source = Some(port(source).owner);
        let report = if let Some(token) = claim_token {
            self.claims
                .lock()
                .unwrap()
                .get(&token)
                .ok_or_else(|| runtime_error("unknown claim"))?
                .publish_port_completion_with(port(source), payload, options)
        } else {
            self.world.publish_port_with(port(source), payload, options)
        };
        let ticket = report.map_err(runtime_error)?;
        Py::new(py, NativePublicationTicket { ticket })
    }
    #[pyo3(signature = (parent, kind, period_ms=0))]
    fn prepare_component(
        &self,
        parent: Handle,
        kind: &str,
        period_ms: u64,
    ) -> PyResult<NativeComponent> {
        use actorplane_native::components::ComponentKind;
        entry();
        let kind = match kind {
            "pulse_source" => ComponentKind::PulseSource {
                interval: duration(period_ms)?,
            },
            "window_counter" => ComponentKind::WindowCounter {
                window: duration(period_ms)?,
            },
            "snapshot_sink" if period_ms == 0 => ComponentKind::SnapshotSink,
            _ => {
                return Err(PyValueError::new_err(
                    "unknown component kind or invalid period",
                ));
            }
        };
        let guard = self.runtime.lock().unwrap();
        let runtime = guard
            .as_ref()
            .ok_or_else(|| runtime_error("World is closed"))?;
        Ok(NativeComponent {
            component: runtime
                .prepare_component(actor(parent), kind)
                .map_err(runtime_error)?,
        })
    }

    #[pyo3(signature = (parent, interval_ms, window_ms, python_target=None))]
    fn counter(
        &self,
        parent: Option<Handle>,
        interval_ms: u64,
        window_ms: u64,
        python_target: Option<Handle>,
    ) -> PyResult<NativeCounter> {
        entry();
        let guard = self.runtime.lock().unwrap();
        let runtime = guard
            .as_ref()
            .ok_or_else(|| runtime_error("World is closed"))?;
        let pipeline = runtime
            .start_counter(
                parent.map(actor),
                duration(interval_ms)?,
                duration(window_ms)?,
                python_target.map(actor),
            )
            .map_err(runtime_error)?;
        Ok(NativeCounter { pipeline })
    }

    #[pyo3(signature = (owner, target, delay_ms, kind, values, schema=0, options=None, claim_token=None))]
    #[allow(clippy::too_many_arguments)]
    fn after(
        &self,
        owner: Handle,
        target: Handle,
        delay_ms: u64,
        kind: &str,
        values: &Bound<'_, PyAny>,
        schema: u32,
        options: Option<&Bound<'_, PyAny>>,
        claim_token: Option<u64>,
    ) -> PyResult<()> {
        entry();
        let payload = self.payload(kind, values, schema)?;
        let guard = self.runtime.lock().unwrap();
        let runtime = guard
            .as_ref()
            .ok_or_else(|| runtime_error("World is closed"))?;
        runtime
            .after_with(
                actor(owner),
                actor(target),
                duration(delay_ms)?,
                payload,
                self.message_options(options, claim_token)?,
            )
            .map_err(runtime_error)
    }

    fn elapsed_ns(&self) -> u64 {
        self.world.elapsed_ns()
    }

    #[allow(clippy::too_many_arguments)]
    fn tcp_listen(
        &self,
        py: Python<'_>,
        owner: Handle,
        target: Handle,
        host: &str,
        port: u16,
        max_connections: usize,
        max_frame_bytes: usize,
        read_buffer_bytes: usize,
        write_buffer_bytes: usize,
        read_timeout_ms: u64,
        request_timeout_ms: u64,
        write_timeout_ms: u64,
    ) -> PyResult<tcp::NativeTcpListener> {
        entry();
        let ip = host
            .parse::<std::net::IpAddr>()
            .map_err(|_| PyValueError::new_err("host must be a numeric IP address"))?;
        let config = actorplane_native::io::TcpConfig {
            bind: std::net::SocketAddr::new(ip, port),
            max_connections,
            max_frame_bytes,
            read_buffer_bytes,
            write_buffer_bytes,
            read_timeout: duration(read_timeout_ms)?,
            request_timeout: duration(request_timeout_ms)?,
            write_timeout: duration(write_timeout_ms)?,
        };
        let listener = py.detach(|| {
            let guard = self.runtime.lock().unwrap();
            let runtime = guard
                .as_ref()
                .ok_or_else(|| runtime_error("World is closed"))?;
            runtime
                .listen_tcp(actor(owner), actor(target), config)
                .map_err(runtime_error)
        })?;
        Ok(tcp::NativeTcpListener { listener })
    }

    #[pyo3(signature = (parent=None))]
    fn tcp_echo(&self, parent: Option<Handle>) -> PyResult<Handle> {
        entry();
        let guard = self.runtime.lock().unwrap();
        let runtime = guard
            .as_ref()
            .ok_or_else(|| runtime_error("World is closed"))?;
        let echo = runtime
            .prepare_tcp_echo(parent.map(actor))
            .map_err(runtime_error)?;
        if let Err(error) = self.world.activate(echo.owner) {
            let _ = self.world.stop(echo.owner);
            return Err(runtime_error(error));
        }
        Ok(handle(echo.owner))
    }

    fn test_pump<'py>(&self, py: Python<'py>, max_polls: u64) -> PyResult<Bound<'py, PyDict>> {
        entry();
        let guard = self.runtime.lock().unwrap();
        let runtime = guard
            .as_ref()
            .ok_or_else(|| runtime_error("World is closed"))?;
        let report = py
            .detach(|| runtime.pump_virtual(max_polls))
            .map_err(runtime_error)?;
        let out = PyDict::new(py);
        out.set_item("polls", report.polls)?;
        out.set_item("idle", report.idle)?;
        Ok(out)
    }

    fn test_advance<'py>(
        &self,
        py: Python<'py>,
        milliseconds: u64,
        max_polls: u64,
    ) -> PyResult<Bound<'py, PyDict>> {
        entry();
        let guard = self.runtime.lock().unwrap();
        let runtime = guard
            .as_ref()
            .ok_or_else(|| runtime_error("World is closed"))?;
        let amount = duration(milliseconds)?;
        let report = py
            .detach(|| runtime.advance_virtual(amount, max_polls))
            .map_err(runtime_error)?;
        let out = PyDict::new(py);
        out.set_item("polls", report.polls)?;
        out.set_item("idle", report.idle)?;
        Ok(out)
    }

    #[pyo3(signature = (kind, schema, parent=None))]
    fn test_responder(&self, kind: &str, schema: u32, parent: Option<Handle>) -> PyResult<Handle> {
        entry();
        let input = match (kind, schema) {
            ("pulse", 0) => actorplane_core::PayloadType::Pulse,
            ("snapshot", 0) => actorplane_core::PayloadType::CountSnapshot,
            ("structured", id) if id > 0 => actorplane_core::PayloadType::Structured(id),
            _ => return Err(PyValueError::new_err("invalid responder schema")),
        };
        let guard = self.runtime.lock().unwrap();
        let runtime = guard
            .as_ref()
            .ok_or_else(|| runtime_error("World is closed"))?;
        if !runtime.is_virtual() {
            return Err(runtime_error("controlled responders require TestWorld"));
        }
        let component = self
            .controlled
            .prepare(runtime, parent.map(actor), input)
            .map_err(runtime_error)?;
        if let Err(error) = self.world.activate(component.owner) {
            let _ = self.world.stop(component.owner);
            return Err(runtime_error(error));
        }
        Ok(handle(component.owner))
    }

    fn test_pending(&self) -> Vec<Handle> {
        self.controlled
            .pending()
            .into_iter()
            .map(operation_handle)
            .collect()
    }

    #[pyo3(signature = (id, kind, values, schema=0))]
    fn test_complete(
        &self,
        id: Handle,
        kind: &str,
        values: &Bound<'_, PyAny>,
        schema: u32,
    ) -> PyResult<bool> {
        entry();
        self.controlled
            .complete(
                &self.world,
                operation(id),
                self.payload(kind, values, schema)?,
            )
            .map_err(runtime_error)
    }

    #[pyo3(signature = (timeout_ms=1000))]
    fn close<'py>(&self, py: Python<'py>, timeout_ms: u64) -> PyResult<Bound<'py, PyDict>> {
        entry();
        let timeout = duration(timeout_ms)?;
        let initial = self.world.close();
        let runtime = self.runtime.lock().unwrap().take();
        if let Some(mut runtime) = runtime {
            let report = py
                .detach(move || runtime.close(timeout))
                .map_err(runtime_error)?;
            if report.timed_out {
                self.shutdown_timed_out.store(true, Ordering::Release);
            }
        }
        let mut report = self.world.close();
        report.discarded += initial.discarded;
        report.timed_out |= self.shutdown_timed_out.load(Ordering::Acquire);
        report_dict(py, report)
    }
}

#[pyclass(module = "actorplane._native")]
struct NativeCounter {
    pipeline: CounterPipeline,
}

#[pymethods]
impl NativeCounter {
    #[getter]
    fn owner(&self) -> Handle {
        entry();
        handle(self.pipeline.owner)
    }
    #[getter]
    fn source(&self) -> Handle {
        entry();
        handle(self.pipeline.source)
    }
    #[getter]
    fn counter(&self) -> Handle {
        entry();
        handle(self.pipeline.counter)
    }
    #[getter]
    fn sink(&self) -> Handle {
        entry();
        handle(self.pipeline.sink)
    }
    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        entry();
        let s = self.pipeline.snapshot();
        let d = PyDict::new(py);
        d.set_item("generated", s.generated)?;
        d.set_item("processed", s.processed)?;
        d.set_item("summaries", s.summaries)?;
        d.set_item("sink_received", s.sink_received)?;
        d.set_item("first_progress_ns", s.first_progress_ns)?;
        d.set_item("last_progress_ns", s.last_progress_ns)?;
        d.set_item("last_total", s.last_total)?;
        d.set_item("total_ticks", s.total_ticks)?;
        Ok(d)
    }
}

/// Test-only finite GIL hold. A Rust observer samples and stops the pipeline
/// while the calling thread remains attached. No Python busy-loop surrogate.
#[cfg(feature = "test-support")]
#[pyfunction]
#[pyo3(signature = (milliseconds=180))]
fn _held_gil_probe<'py>(py: Python<'py>, milliseconds: u64) -> PyResult<Bound<'py, PyDict>> {
    use std::{sync::Barrier, time::Instant};
    entry();
    if !(100..=2000).contains(&milliseconds) {
        return Err(PyValueError::new_err("probe duration must be 100..2000 ms"));
    }
    let world = World::new(Config {
        mailbox_capacity: 2,
        ..Default::default()
    })
    .map_err(runtime_error)?;
    let mut runtime = NativeRuntime::new(world.clone(), 16).map_err(runtime_error)?;
    let python_target = world
        .allocate(EndpointKind::Python, None)
        .map_err(runtime_error)?;
    world.activate(python_target).map_err(runtime_error)?;
    let pipeline = runtime
        .start_counter(
            None,
            Duration::from_millis(2),
            Duration::from_millis(8),
            Some(python_target),
        )
        .map_err(runtime_error)?;
    let barrier = Barrier::new(2);
    let hold_start_ns = runtime.elapsed_ns();
    let boundary_start = BRIDGE_ENTRIES.load(Ordering::Relaxed);
    let (progress, first, last, stopped) = std::thread::scope(|scope| {
        let observer = scope.spawn(|| {
            barrier.wait();
            let deadline = Instant::now() + Duration::from_millis(milliseconds * 2 / 3);
            let initial = pipeline.snapshot().sink_received;
            let mut first = None;
            let mut last = None;
            let mut seen = initial;
            while Instant::now() < deadline {
                let n = pipeline.snapshot().sink_received;
                if n > seen {
                    let t = runtime.elapsed_ns();
                    first.get_or_insert(t);
                    last = Some(t);
                    seen = n;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            let _ = world.stop(pipeline.owner);
            let quiet_deadline = Instant::now() + Duration::from_millis(500);
            while runtime.active_tasks() > 0 && Instant::now() < quiet_deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            (seen - initial, first, last, runtime.active_tasks() == 0)
        });
        barrier.wait();
        // Deliberately do not detach: this is the test's defining condition.
        std::thread::sleep(Duration::from_millis(milliseconds));
        observer.join().expect("native probe observer panicked")
    });
    let hold_end_ns = runtime.elapsed_ns();
    let entries = BRIDGE_ENTRIES.load(Ordering::Relaxed) - boundary_start;
    let snapshot = world.snapshot();
    let queued = snapshot
        .actors
        .iter()
        .find(|a| a.reference == python_target)
        .map_or(0, |a| a.queue_entries);
    let out = PyDict::new(py);
    out.set_item("progress_in_hold", progress)?;
    out.set_item("native_stopped_in_hold", stopped)?;
    out.set_item("hold_start_ns", hold_start_ns)?;
    out.set_item("hold_end_ns", hold_end_ns)?;
    out.set_item("first_progress_ns", first)?;
    out.set_item("last_progress_ns", last)?;
    out.set_item("bridge_entries_during_hold", entries)?;
    out.set_item("python_queued", queued)?;
    out.set_item("rejected", snapshot.metrics.rejected)?;
    // The source scope's cancellation retires its subscriptions; claims to its
    // stale queued publications must fail rather than invoke Python afterward.
    world.close();
    world.finish_python(python_target).map_err(runtime_error)?;
    py.detach(|| runtime.close(Duration::from_secs(1)))
        .map_err(runtime_error)?;
    Ok(out)
}

#[pymodule]
#[pyo3(gil_used = true)]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeWorld>()?;
    m.add_class::<NativePublicationTicket>()?;
    m.add_class::<NativeCounter>()?;
    m.add_class::<tcp::NativeTcpListener>()?;
    #[cfg(feature = "test-support")]
    m.add_function(wrap_pyfunction!(_held_gil_probe, m)?)?;
    #[cfg(feature = "test-support")]
    m.add_function(wrap_pyfunction!(_hold_gil, m)?)?;
    #[cfg(feature = "test-support")]
    m.add_function(wrap_pyfunction!(cpu::_cpu_gil_probe, m)?)?;
    #[cfg(feature = "test-support")]
    m.add_function(wrap_pyfunction!(tcp::_held_gil_tcp_probe, m)?)?;
    Ok(())
}

#[cfg(feature = "test-support")]
#[pyfunction]
fn _hold_gil(milliseconds: u64) -> PyResult<()> {
    entry();
    if !(1..=2000).contains(&milliseconds) {
        return Err(PyValueError::new_err("test GIL hold must be 1..2000 ms"));
    }
    std::thread::sleep(Duration::from_millis(milliseconds));
    Ok(())
}
