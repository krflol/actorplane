//! Registration-only descriptor conversion. No Python metadata reaches workers.
use actorplane_core::schema::{Field, FieldType, Schema};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyBool, PyString, PyTuple},
};

fn invalid(message: &str) -> PyErr {
    PyValueError::new_err(message.to_owned())
}

fn text(value: &Bound<'_, PyAny>, limit: usize) -> PyResult<String> {
    let text = value.cast::<PyString>()?.to_str()?;
    if text.is_empty() || text.len() > limit {
        return Err(invalid("schema name exceeds its UTF-8 byte limit"));
    }
    Ok(text.to_owned())
}

struct Parser {
    nodes: usize,
}
impl Parser {
    fn node(&mut self, value: &Bound<'_, PyAny>, depth: usize) -> PyResult<FieldType> {
        self.nodes += 1;
        if self.nodes > 1024 || depth > 16 {
            return Err(invalid("schema exceeds node or depth limit"));
        }
        let tuple = value.cast::<PyTuple>()?;
        if tuple.is_empty() {
            return Err(invalid("schema node requires a type tag"));
        }
        let tag = tuple.get_item(0)?;
        let tag = tag.extract::<&str>()?;
        let expected = match tag {
            "bool" => 1,
            "float" | "string" | "bytes" | "optional" => 2,
            "int" | "uint" | "enum" | "sequence" => 3,
            "record" => 4,
            _ => return Err(invalid("unsupported native schema type")),
        };
        if tuple.len() != expected {
            return Err(invalid("schema descriptor has wrong arity"));
        }
        // Reject bool-as-int metadata before numeric extraction.
        let number = |index| -> PyResult<Bound<'_, PyAny>> {
            let item = tuple.get_item(index)?;
            if item.is_instance_of::<PyBool>() {
                return Err(invalid("schema bound must be an integer, not bool"));
            }
            Ok(item)
        };
        Ok(match tag {
            "int" => FieldType::Int {
                min: number(1)?.extract()?,
                max: number(2)?.extract()?,
            },
            "uint" => FieldType::UInt {
                min: number(1)?.extract()?,
                max: number(2)?.extract()?,
            },
            "bool" => FieldType::Bool,
            "float" => {
                let value = tuple.get_item(1)?;
                if !value.is_instance_of::<PyBool>() {
                    return Err(invalid("float policy requires bool"));
                }
                FieldType::Float {
                    allow_non_finite: value.extract()?,
                }
            }
            "string" => FieldType::String {
                max_bytes: number(1)?.extract()?,
            },
            "bytes" => FieldType::Bytes {
                max_bytes: number(1)?.extract()?,
            },
            "enum" => {
                let raw = tuple.get_item(2)?;
                let variants = raw.cast::<PyTuple>()?;
                if variants.len() > 256 {
                    return Err(invalid("enum exceeds 256 symbols"));
                }
                FieldType::Enum {
                    name: text(&tuple.get_item(1)?, 256)?,
                    variants: variants
                        .iter()
                        .map(|v| text(&v, 256))
                        .collect::<PyResult<_>>()?,
                }
            }
            "optional" => FieldType::Optional(Box::new(self.node(&tuple.get_item(1)?, depth + 1)?)),
            "sequence" => FieldType::Sequence {
                item: Box::new(self.node(&tuple.get_item(1)?, depth + 1)?),
                max_len: number(2)?.extract()?,
            },
            "record" => {
                let raw = tuple.get_item(3)?;
                let raw = raw.cast::<PyTuple>()?;
                if raw.len() > 256 {
                    return Err(invalid("record exceeds 256 fields"));
                }
                let mut fields = Vec::with_capacity(raw.len());
                for value in raw.iter() {
                    let field = value.cast::<PyTuple>()?;
                    if field.len() != 2 {
                        return Err(invalid("field requires name and type"));
                    }
                    fields.push(Field {
                        name: text(&field.get_item(0)?, 128)?,
                        ty: self.node(&field.get_item(1)?, depth + 1)?,
                    });
                }
                FieldType::Record(Box::new(Schema {
                    name: text(&tuple.get_item(1)?, 256)?,
                    version: number(2)?.extract()?,
                    fields,
                }))
            }
            _ => unreachable!(),
        })
    }
}

pub fn parse(value: &Bound<'_, PyAny>) -> PyResult<Schema> {
    let node = Parser { nodes: 0 }.node(value, 0)?;
    let FieldType::Record(schema) = node else {
        return Err(invalid("event schema must be a record"));
    };
    schema.validate().map_err(|e| invalid(&e.to_string()))?;
    Ok(*schema)
}
