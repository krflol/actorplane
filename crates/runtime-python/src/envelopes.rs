use super::{actor, components, handle};
use actorplane_core::{Envelope, MessageOptions, SchemaKind, TraceContext, World};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyBool, PyBytes, PyDict, PyInt, PyString, PyTuple},
};
use std::time::{Duration, Instant};

const MAX_TIMEOUT_MS: u64 = 86_400_000;
const MAX_BAGGAGE: usize = 8;
const MAX_KEY_BYTES: usize = 64;
const MAX_VALUE_BYTES: usize = 256;

fn invalid(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(message.into())
}

fn strict_u64(value: &Bound<'_, PyAny>, name: &str) -> PyResult<u64> {
    if !value.is_exact_instance_of::<PyInt>() {
        return Err(invalid(format!("{name} must be an integer")));
    }
    value
        .extract::<u64>()
        .map_err(|_| invalid(format!("{name} must be a non-negative integer")))
}

fn strict_u32(value: &Bound<'_, PyAny>, name: &str) -> PyResult<u32> {
    if !value.is_exact_instance_of::<PyInt>() {
        return Err(invalid(format!("{name} must be an integer")));
    }
    value
        .extract::<u32>()
        .map_err(|_| invalid(format!("{name} must be a uint32")))
}

fn optional_item<'py>(value: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyAny>>> {
    if value.is_none() {
        Ok(None)
    } else {
        Ok(Some(value.clone()))
    }
}

fn parse_source(value: &Bound<'_, PyAny>) -> PyResult<Option<actorplane_core::ActorRef>> {
    let Some(value) = optional_item(value)? else {
        return Ok(None);
    };
    if !value.is_exact_instance_of::<PyTuple>() {
        return Err(invalid("source must be a 3-tuple or None"));
    }
    let tuple = value
        .cast::<PyTuple>()
        .map_err(|_| invalid("source must be a 3-tuple or None"))?;
    if tuple.len() != 3 {
        return Err(invalid("source must be a 3-tuple or None"));
    }
    let world = strict_u64(&tuple.get_item(0)?, "source.world")?;
    let slot = strict_u32(&tuple.get_item(1)?, "source.slot")?;
    let generation = strict_u64(&tuple.get_item(2)?, "source.generation")?;
    if world == 0 || generation == 0 {
        return Err(invalid("source world and generation must be nonzero"));
    }
    Ok(Some(actor((world, slot, generation))))
}

fn parse_trace(value: &Bound<'_, PyAny>) -> PyResult<Option<TraceContext>> {
    let Some(value) = optional_item(value)? else {
        return Ok(None);
    };
    if !value.is_exact_instance_of::<PyTuple>() {
        return Err(invalid("trace must be a 4-tuple or None"));
    }
    let tuple = value
        .cast::<PyTuple>()
        .map_err(|_| invalid("trace must be a 4-tuple or None"))?;
    if tuple.len() != 4 {
        return Err(invalid("trace must be a 4-tuple or None"));
    }
    let trace_value = tuple.get_item(0)?;
    let span_value = tuple.get_item(1)?;
    if !trace_value.is_exact_instance_of::<PyBytes>()
        || !span_value.is_exact_instance_of::<PyBytes>()
    {
        return Err(invalid("trace_id and span_id must be bytes"));
    }
    let trace_bytes = trace_value.cast::<PyBytes>()?;
    let span_bytes = span_value.cast::<PyBytes>()?;
    if trace_bytes.as_bytes().len() != 16 || span_bytes.as_bytes().len() != 8 {
        return Err(invalid("trace_id must be 16 bytes and span_id 8 bytes"));
    }
    let sampled = tuple.get_item(2)?;
    if !sampled.is_exact_instance_of::<PyBool>() {
        return Err(invalid("sampled must be bool"));
    }
    let sampled = sampled.extract::<bool>()?;
    let baggage_value = tuple.get_item(3)?;
    if !baggage_value.is_exact_instance_of::<PyTuple>() {
        return Err(invalid("baggage must be a tuple"));
    }
    let baggage = baggage_value.cast::<PyTuple>()?;
    if baggage.len() > MAX_BAGGAGE {
        return Err(invalid("baggage exceeds eight entries"));
    }
    let mut trace_id = [0; 16];
    trace_id.copy_from_slice(trace_bytes.as_bytes());
    let mut span_id = [0; 8];
    span_id.copy_from_slice(span_bytes.as_bytes());
    let mut entries = Vec::with_capacity(baggage.len());
    for item in baggage.iter() {
        if !item.is_exact_instance_of::<PyTuple>() {
            return Err(invalid("baggage entry must be a pair"));
        }
        let pair = item.cast::<PyTuple>()?;
        if pair.len() != 2 {
            return Err(invalid("baggage entry must be a pair"));
        }
        let key_obj = pair.get_item(0)?;
        let val_obj = pair.get_item(1)?;
        if !key_obj.is_exact_instance_of::<PyString>()
            || !val_obj.is_exact_instance_of::<PyString>()
        {
            return Err(invalid("baggage key and value must be text"));
        }
        let key_obj = key_obj.cast::<PyString>()?;
        let val_obj = val_obj.cast::<PyString>()?;
        if key_obj.len()? > MAX_KEY_BYTES || val_obj.len()? > MAX_VALUE_BYTES {
            return Err(invalid("baggage key or value exceeds its byte limit"));
        }
        let key = key_obj.to_str()?;
        let val = val_obj.to_str()?;
        if key.is_empty() || key.len() > MAX_KEY_BYTES || val.len() > MAX_VALUE_BYTES {
            return Err(invalid("baggage key or value exceeds its byte limit"));
        }
        entries.push((key.to_owned(), val.to_owned()));
    }
    TraceContext::new(trace_id, span_id, sampled, entries)
        .map(Some)
        .map_err(|error| invalid(error.to_string()))
}

pub(crate) fn parse_options(
    value: Option<&Bound<'_, PyAny>>,
    inherited: Option<MessageOptions>,
    now: Instant,
) -> PyResult<MessageOptions> {
    let mut options = inherited.unwrap_or_default();
    let Some(value) = value else {
        return Ok(options);
    };
    if value.is_none() {
        return Ok(options);
    }
    if !value.is_exact_instance_of::<PyTuple>() {
        return Err(invalid("options must be a 5-tuple or None"));
    }
    let tuple = value
        .cast::<PyTuple>()
        .map_err(|_| invalid("options must be a 5-tuple or None"))?;
    if tuple.len() != 5 {
        return Err(invalid(
            "options must contain source, timeout, correlation, causation, and trace",
        ));
    }
    if let Some(source) = parse_source(&tuple.get_item(0)?)? {
        options.source = Some(source);
    }
    if let Some(timeout) = optional_item(&tuple.get_item(1)?)? {
        let timeout = strict_u64(&timeout, "timeout_ms")?;
        if timeout > MAX_TIMEOUT_MS {
            return Err(invalid("timeout exceeds 24 hours"));
        }
        let deadline = now
            .checked_add(Duration::from_millis(timeout))
            .ok_or_else(|| invalid("timeout overflow"))?;
        options.deadline = Some(options.deadline.map_or(deadline, |old| old.min(deadline)));
    }
    if let Some(correlation) = optional_item(&tuple.get_item(2)?)? {
        let id = strict_u64(&correlation, "correlation_id")?;
        if id == 0 {
            return Err(invalid("correlation_id must be nonzero"));
        }
        options.correlation_id = Some(id);
    }
    if let Some(causation) = optional_item(&tuple.get_item(3)?)? {
        let id = strict_u64(&causation, "causation_id")?;
        if id == 0 {
            return Err(invalid("causation_id must be nonzero"));
        }
        options.causation_id = Some(id);
    }
    if let Some(trace) = parse_trace(&tuple.get_item(4)?)? {
        options.trace = Some(trace);
    }
    options
        .validate()
        .map_err(|error| invalid(error.to_string()))?;
    Ok(options)
}

fn port_value<'py>(
    py: Python<'py>,
    port: Option<actorplane_core::PortRef>,
) -> PyResult<Bound<'py, PyAny>> {
    Ok(match port {
        Some(port) => components::port_handle(port).into_pyobject(py)?.into_any(),
        None => py.None().into_bound(py),
    })
}

pub(crate) fn envelope_dict<'py>(
    py: Python<'py>,
    world: &World,
    envelope: &Envelope,
) -> PyResult<Bound<'py, PyDict>> {
    let output = PyDict::new(py);
    output.set_item("event_id", envelope.event_id)?;
    output.set_item(
        "schema_kind",
        match envelope.schema.kind {
            SchemaKind::Pulse => "pulse",
            SchemaKind::CountSnapshot => "snapshot",
            SchemaKind::Record => "record",
            SchemaKind::Structured => "structured",
        },
    )?;
    output.set_item("schema_id", envelope.schema.id)?;
    output.set_item("schema_version", envelope.schema.version)?;
    output.set_item("source", envelope.source.map(handle))?;
    output.set_item("destination", handle(envelope.destination))?;
    output.set_item("owner", handle(envelope.owner))?;
    output.set_item("dispatcher", port_value(py, envelope.dispatcher)?)?;
    output.set_item(
        "destination_port",
        port_value(py, envelope.destination_port)?,
    )?;
    output.set_item("enqueued_at_ns", world.instant_ns(envelope.enqueued_at))?;
    output.set_item(
        "deadline_ns",
        envelope.deadline.map(|value| world.instant_ns(value)),
    )?;
    output.set_item("correlation_id", envelope.correlation_id)?;
    output.set_item("causation_id", envelope.causation_id)?;
    if let Some(trace) = &envelope.trace {
        let data = PyDict::new(py);
        data.set_item("trace_id", PyBytes::new(py, trace.trace_id()))?;
        data.set_item("span_id", PyBytes::new(py, trace.span_id()))?;
        data.set_item("sampled", trace.sampled())?;
        let baggage = PyTuple::new(py, trace.baggage().iter().map(|(key, value)| (key, value)))?;
        data.set_item("baggage", baggage)?;
        output.set_item("trace", data)?;
    } else {
        output.set_item("trace", py.None())?;
    }
    Ok(output)
}
