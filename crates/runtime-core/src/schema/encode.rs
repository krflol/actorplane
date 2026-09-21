use super::{FieldType, Schema, SchemaError, Value};
fn e(s: &str) -> SchemaError {
    SchemaError::new(s)
}
pub(super) fn encode(
    schema: &Schema,
    value: &Value,
    max_bytes: usize,
) -> Result<Vec<u8>, SchemaError> {
    schema.validate()?;
    if max_bytes > 1 << 20 {
        return Err(e("event byte limit"));
    }
    let mut w = W {
        out: Vec::new(),
        max: max_bytes,
        nodes: 0,
    };
    w.record(schema, value, 0).map_err(|e| e.at(&schema.name))?;
    Ok(w.out)
}
struct W {
    out: Vec<u8>,
    max: usize,
    nodes: usize,
}
impl W {
    fn add(&mut self, b: &[u8]) -> Result<(), SchemaError> {
        if self
            .out
            .len()
            .checked_add(b.len())
            .ok_or_else(|| e("event size"))?
            > self.max
        {
            return Err(e("event size"));
        }
        self.out.extend_from_slice(b);
        Ok(())
    }
    fn node(&mut self, d: usize) -> Result<(), SchemaError> {
        self.nodes += 1;
        if self.nodes > 4096 || d > 16 {
            Err(e("value node/depth limit"))
        } else {
            Ok(())
        }
    }
    fn record(&mut self, s: &Schema, v: &Value, d: usize) -> Result<(), SchemaError> {
        self.node(d)?;
        let Value::Record(vals) = v else {
            return Err(e("expected record"));
        };
        if vals.len() != s.fields.len() {
            return Err(e("record field count"));
        }
        for (f, x) in s.fields.iter().zip(vals) {
            self.val(&f.ty, x, d + 1).map_err(|e| e.at(&f.name))?
        }
        Ok(())
    }
    fn val(&mut self, t: &FieldType, v: &Value, d: usize) -> Result<(), SchemaError> {
        if let FieldType::Record(schema) = t {
            return self.record(schema, v, d);
        }
        self.node(d)?;
        match (t, v) {
            (FieldType::Int { min, max }, Value::Int(x)) if x >= min && x <= max => {
                self.add(&x.to_le_bytes())
            }
            (FieldType::UInt { min, max }, Value::UInt(x)) if x >= min && x <= max => {
                self.add(&x.to_le_bytes())
            }
            (FieldType::Float { allow_non_finite }, Value::Float(x)) => {
                if !*allow_non_finite && !x.is_finite() {
                    return Err(e("invalid float"));
                }
                let bits = if x.is_nan() {
                    0x7ff8000000000000
                } else {
                    x.to_bits()
                };
                self.add(&bits.to_le_bytes())
            }
            (FieldType::Bool, Value::Bool(x)) => self.add(&[*x as u8]),
            (FieldType::String { max_bytes }, Value::String(x)) => {
                let b = x.as_bytes();
                if b.len() > *max_bytes {
                    return Err(e("string limit"));
                }
                let n = (b.len() as u32).to_le_bytes();
                self.add(&n)?;
                self.add(b)
            }
            (FieldType::Bytes { max_bytes }, Value::Bytes(b)) => {
                if b.len() > *max_bytes {
                    return Err(e("bytes limit"));
                }
                let n = (b.len() as u32).to_le_bytes();
                self.add(&n)?;
                self.add(b)
            }
            (FieldType::Enum { variants, .. }, Value::Enum(x))
                if (*x as usize) < variants.len() =>
            {
                self.add(&x.to_le_bytes())
            }
            (FieldType::Optional(x), Value::Optional(v)) => match v {
                None => self.add(&[0]),
                Some(v) => {
                    self.add(&[1])?;
                    self.val(x, v, d + 1)
                }
            },
            (FieldType::Sequence { item, max_len }, Value::Sequence(v)) if v.len() <= *max_len => {
                if v.len() > 4096 - self.nodes {
                    return Err(e("remaining value node limit"));
                }
                self.add(&(v.len() as u32).to_le_bytes())?;
                for (index, x) in v.iter().enumerate() {
                    self.val(item, x, d + 1)
                        .map_err(|e| e.at(&format!("[{index}]")))?
                }
                Ok(())
            }
            (FieldType::Record(_), _) => unreachable!(),
            _ => Err(e("value does not match schema")),
        }
    }
}
