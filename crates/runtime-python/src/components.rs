use super::*;
use actorplane_core::{
    ComponentDescriptor, InterfaceSpec, PayloadType, PortDirection, PortRef, PortSpec,
};
use pyo3::types::PyString;

pub(super) type PortHandle = (u64, u32, u64, u16);
pub(super) fn port(h: PortHandle) -> PortRef {
    PortRef {
        owner: actor((h.0, h.1, h.2)),
        index: h.3,
    }
}
pub(super) fn port_handle(p: PortRef) -> PortHandle {
    (p.owner.world, p.owner.slot, p.owner.generation, p.index)
}
fn name(value: &Bound<'_, PyAny>) -> PyResult<String> {
    let text = value.cast::<PyString>()?.to_str()?;
    if text.is_empty() || text.len() > 128 {
        return Err(PyValueError::new_err(
            "contract name must be 1..128 UTF-8 bytes",
        ));
    }
    Ok(text.to_owned())
}
fn ports(value: &Bound<'_, PyAny>) -> PyResult<Vec<PortSpec>> {
    let items = value.cast::<PyTuple>()?;
    if items.len() > 64 {
        return Err(PyValueError::new_err("at most 64 ports"));
    }
    items
        .iter()
        .map(|item| {
            let item = item.cast::<PyTuple>()?;
            if item.len() != 4 {
                return Err(PyValueError::new_err(
                    "port requires name, direction, kind, schema",
                ));
            }
            let direction = match item.get_item(1)?.extract::<&str>()? {
                "input" => PortDirection::Input,
                "output" => PortDirection::Output,
                _ => return Err(PyValueError::new_err("invalid port direction")),
            };
            let schema_id = item.get_item(3)?;
            if schema_id.is_instance_of::<PyBool>() {
                return Err(PyValueError::new_err("schema ID is not a bool"));
            }
            let id = schema_id.extract::<u32>()?;
            let schema = match (item.get_item(2)?.extract::<&str>()?, id) {
                ("pulse", 0) => PayloadType::Pulse,
                ("snapshot", 0) => PayloadType::CountSnapshot,
                ("structured", id) if id > 0 => PayloadType::Structured(id),
                _ => return Err(PyValueError::new_err("invalid port schema")),
            };
            Ok(PortSpec {
                name: name(&item.get_item(0)?)?,
                direction,
                schema,
            })
        })
        .collect()
}
pub(super) fn descriptor(value: &Bound<'_, PyTuple>) -> PyResult<ComponentDescriptor> {
    if value.len() != 4 {
        return Err(PyValueError::new_err(
            "component requires name, version, ports, interfaces",
        ));
    }
    let version = value.get_item(1)?;
    if version.is_instance_of::<PyBool>() {
        return Err(PyValueError::new_err("version is not a bool"));
    }
    let items = value.get_item(3)?;
    let items = items.cast::<PyTuple>()?;
    if items.len() > 16 {
        return Err(PyValueError::new_err("at most 16 interfaces"));
    }
    let interfaces = items
        .iter()
        .map(|item| {
            let item = item.cast::<PyTuple>()?;
            if item.len() != 3 {
                return Err(PyValueError::new_err(
                    "interface requires name, version, ports",
                ));
            }
            let version = item.get_item(1)?;
            if version.is_instance_of::<PyBool>() {
                return Err(PyValueError::new_err("version is not a bool"));
            }
            Ok(InterfaceSpec {
                name: name(&item.get_item(0)?)?,
                version: version.extract()?,
                ports: ports(&item.get_item(2)?)?,
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    Ok(ComponentDescriptor {
        name: name(&value.get_item(0)?)?,
        version: version.extract()?,
        ports: ports(&value.get_item(2)?)?,
        interfaces,
    })
}

pub(super) fn descriptor_dict<'py>(
    py: Python<'py>,
    descriptor: &ComponentDescriptor,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("name", &descriptor.name)?;
    out.set_item("version", descriptor.version)?;
    let ports = PyList::empty(py);
    for port in &descriptor.ports {
        let item = PyDict::new(py);
        item.set_item("name", &port.name)?;
        item.set_item(
            "direction",
            if port.direction == PortDirection::Input {
                "input"
            } else {
                "output"
            },
        )?;
        let (kind, id) = match port.schema {
            PayloadType::Pulse => ("pulse", 0),
            PayloadType::CountSnapshot => ("snapshot", 0),
            PayloadType::Structured(id) => ("structured", id),
        };
        item.set_item("kind", kind)?;
        item.set_item("schema", id)?;
        ports.append(item)?;
    }
    out.set_item("ports", ports)?;
    out.set_item(
        "interfaces",
        descriptor
            .interfaces
            .iter()
            .map(|i| (i.name.clone(), i.version))
            .collect::<Vec<_>>(),
    )?;
    out.set_item("ordering", "fifo_admission")?;
    out.set_item("overload", "reject")?;
    out.set_item("cancellation", "cancel_unclaimed_on_owner_stop")?;
    Ok(out)
}

#[pyclass(module = "actorplane._native")]
pub(super) struct NativeComponent {
    pub(super) component: actorplane_native::components::NativeComponent,
}
#[pymethods]
impl NativeComponent {
    #[getter]
    fn owner(&self) -> Handle {
        entry();
        handle(self.component.owner)
    }
    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        entry();
        let stats = self.component.stats();
        let output = PyDict::new(py);
        output.set_item("generated", stats.generated)?;
        output.set_item("processed", stats.processed)?;
        output.set_item("summaries", stats.summaries)?;
        output.set_item("sink_received", stats.sink_received)?;
        output.set_item("last_total", stats.last_total)?;
        Ok(output)
    }
}
