use super::*;

pub(super) fn descriptor(
    value: &Bound<'_, PyTuple>,
) -> PyResult<actorplane_core::ComponentDescriptor> {
    components::descriptor(value)
}

pub(super) fn interface_dict<'py>(
    py: Python<'py>,
    value: &actorplane_core::InterfaceSpec,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("name", &value.name)?;
    out.set_item("version", value.version)?;
    let ports = PyList::empty(py);
    for port in &value.ports {
        let (kind, schema) = match port.schema {
            actorplane_core::PayloadType::Pulse => ("pulse", 0),
            actorplane_core::PayloadType::CountSnapshot => ("snapshot", 0),
            actorplane_core::PayloadType::Structured(id) => ("structured", id),
        };
        ports.append((
            port.name.as_str(),
            format!("{:?}", port.direction).to_lowercase(),
            kind,
            schema,
        ))?;
    }
    out.set_item("ports", ports)?;
    Ok(out)
}
