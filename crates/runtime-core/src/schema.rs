use std::{collections::HashMap, fmt, sync::Arc};
mod encode;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    pub name: String,
    pub version: u32,
    pub fields: Vec<Field>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub ty: FieldType,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldType {
    Int {
        min: i64,
        max: i64,
    },
    UInt {
        min: u64,
        max: u64,
    },
    Float {
        allow_non_finite: bool,
    },
    Bool,
    String {
        max_bytes: usize,
    },
    Bytes {
        max_bytes: usize,
    },
    Enum {
        name: String,
        variants: Vec<String>,
    },
    Optional(Box<FieldType>),
    Sequence {
        item: Box<FieldType>,
        max_len: usize,
    },
    Record(Box<Schema>),
}
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Int(i64),
    UInt(u64),
    Float(f64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Enum(u32),
    Optional(Option<Box<Value>>),
    Sequence(Vec<Value>),
    Record(Vec<Value>),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaError(pub String);
impl SchemaError {
    pub fn new(message: impl Into<String>) -> Self {
        err(&message.into())
    }
    fn at(self, field: &str) -> Self {
        err(&format!("{field}.{}", self.0))
    }
}
impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for SchemaError {}
fn err(s: &str) -> SchemaError {
    SchemaError(s.chars().take(256).collect())
}
impl Schema {
    /// Encode typed native values using the same canonical contract as Python.
    pub fn encode(&self, value: &Value, max_bytes: usize) -> Result<Vec<u8>, SchemaError> {
        encode::encode(self, value, max_bytes)
    }
    pub fn normalized_clone(&self) -> Self {
        fn ty(t: &FieldType) -> FieldType {
            match t {
                FieldType::Optional(x) => FieldType::Optional(Box::new(ty(x))),
                FieldType::Sequence { item, max_len } => FieldType::Sequence {
                    item: Box::new(ty(item)),
                    max_len: *max_len,
                },
                FieldType::Record(s) => FieldType::Record(Box::new(s.normalized_clone())),
                FieldType::Enum { name, variants } => FieldType::Enum {
                    name: name.clone(),
                    variants: variants.clone(),
                },
                x => x.clone(),
            }
        }
        Schema {
            name: self.name.clone(),
            version: self.version,
            fields: self
                .fields
                .iter()
                .map(|f| Field {
                    name: f.name.clone(),
                    ty: ty(&f.ty),
                })
                .collect(),
        }
    }
    pub fn validate(&self) -> Result<(), SchemaError> {
        let mut nodes = 0;
        validate_schema(self, 0, &mut nodes)
    }
    pub fn validate_bytes(&self, b: &[u8]) -> Result<(), SchemaError> {
        self.validate()?;
        self.validate_registered_bytes(b)
    }
    /// The World calls this only on immutable, registered descriptors. No
    /// values or path strings are allocated on a successful validation.
    pub(crate) fn validate_registered_bytes(&self, b: &[u8]) -> Result<(), SchemaError> {
        self.read::<false>(b).map(|_| ())
    }
    pub fn decode(&self, b: &[u8]) -> Result<Value, SchemaError> {
        self.validate()?;
        self.read::<true>(b)
    }
    fn read<const BUILD: bool>(&self, b: &[u8]) -> Result<Value, SchemaError> {
        if b.len() > (1 << 20) {
            return Err(err("event exceeds 1048576 bytes"));
        }
        let mut r = Reader::<BUILD> { b, p: 0, nodes: 0 };
        let out = r.record(self, 0).map_err(|e| e.at(&self.name))?;
        if r.p != b.len() {
            return Err(err("trailing bytes"));
        }
        Ok(out)
    }
}
fn validate_schema(s: &Schema, d: usize, nodes: &mut usize) -> Result<(), SchemaError> {
    if d > 16 {
        return Err(err("schema depth limit"));
    }
    *nodes += 1;
    if *nodes > 1024 {
        return Err(err("schema node limit"));
    }
    if s.name.is_empty() || s.name.len() > 256 || s.version == 0 || s.fields.len() > 256 {
        return Err(err("invalid schema metadata"));
    }
    let mut names = std::collections::HashSet::new();
    for f in &s.fields {
        if f.name.is_empty() || f.name.len() > 128 {
            return Err(err("invalid field metadata"));
        }
        if !names.insert(&f.name) {
            return Err(err("duplicate field name"));
        }
        valid_ty(&f.ty, d + 1, nodes).map_err(|e| e.at(&f.name))?
    }
    Ok(())
}
fn valid_ty(t: &FieldType, d: usize, nodes: &mut usize) -> Result<(), SchemaError> {
    if let FieldType::Record(schema) = t {
        return validate_schema(schema, d, nodes);
    }
    *nodes += 1;
    if *nodes > 1024 {
        return Err(err("schema node limit 1024"));
    }
    if d > 16 {
        return Err(err("schema depth limit"));
    }
    match t {
        FieldType::Int { min, max } => {
            if min > max {
                Err(err("invalid integer range"))
            } else {
                Ok(())
            }
        }
        FieldType::UInt { min, max } => {
            if min > max {
                Err(err("invalid unsigned range"))
            } else {
                Ok(())
            }
        }
        FieldType::String { max_bytes } | FieldType::Bytes { max_bytes } => {
            if *max_bytes > 1 << 20 {
                Err(err("byte limit"))
            } else {
                Ok(())
            }
        }
        FieldType::Enum { name, variants } => {
            if name.is_empty()
                || name.len() > 256
                || variants.is_empty()
                || variants.len() > 256
                || variants.iter().any(|v| v.is_empty() || v.len() > 256)
                || variants.windows(2).any(|w| w[0] >= w[1])
            {
                Err(err("enum limit"))
            } else {
                Ok(())
            }
        }
        FieldType::Optional(x) => valid_ty(x, d + 1, nodes),
        FieldType::Sequence { item, max_len } => {
            if *max_len > 4096 {
                Err(err("sequence limit"))
            } else {
                valid_ty(item, d + 1, nodes)
            }
        }
        FieldType::Record(_) => unreachable!(),
        _ => Ok(()),
    }
}
struct Reader<'a, const BUILD: bool> {
    b: &'a [u8],
    p: usize,
    nodes: usize,
}
impl<'a, const BUILD: bool> Reader<'a, BUILD> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], SchemaError> {
        if n > self.b.len() - self.p {
            return Err(err("truncated value"));
        }
        let x = &self.b[self.p..self.p + n];
        self.p += n;
        Ok(x)
    }
    fn u32(&mut self) -> Result<u32, SchemaError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn val(&mut self, t: &FieldType, d: usize) -> Result<Value, SchemaError> {
        if let FieldType::Record(schema) = t {
            return self.record(schema, d);
        }
        self.nodes += 1;
        if self.nodes > 4096 {
            return Err(err("value node limit"));
        }
        if d > 16 {
            return Err(err("value depth limit"));
        }
        match t {
            FieldType::Int { min, max } => {
                let v = i64::from_le_bytes(self.take(8)?.try_into().unwrap());
                if v < *min || v > *max {
                    Err(err("integer range"))
                } else {
                    Ok(Value::Int(v))
                }
            }
            FieldType::UInt { min, max } => {
                let v = u64::from_le_bytes(self.take(8)?.try_into().unwrap());
                if v < *min || v > *max {
                    Err(err("unsigned range"))
                } else {
                    Ok(Value::UInt(v))
                }
            }
            FieldType::Float { allow_non_finite } => {
                let x = u64::from_le_bytes(self.take(8)?.try_into().unwrap());
                if !*allow_non_finite && !f64::from_bits(x).is_finite() {
                    return Err(err("non-finite float"));
                }
                if *allow_non_finite && f64::from_bits(x).is_nan() && x != 0x7ff8_0000_0000_0000 {
                    return Err(err("non-canonical NaN"));
                }
                Ok(Value::Float(f64::from_bits(x)))
            }
            FieldType::Bool => match self.take(1)?[0] {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                _ => Err(err("invalid bool")),
            },
            FieldType::String { max_bytes } => {
                let n = self.u32()? as usize;
                if n > *max_bytes {
                    return Err(err("string limit"));
                }
                let text = std::str::from_utf8(self.take(n)?)
                    .map_err(|_| err("expected valid UTF-8 string"))?;
                Ok(Value::String(if BUILD {
                    text.to_owned()
                } else {
                    String::new()
                }))
            }
            FieldType::Bytes { max_bytes } => {
                let n = self.u32()? as usize;
                if n > *max_bytes {
                    return Err(err("bytes limit"));
                }
                let bytes = self.take(n)?;
                Ok(Value::Bytes(if BUILD {
                    bytes.to_vec()
                } else {
                    Vec::new()
                }))
            }
            FieldType::Enum { variants, .. } => {
                let n = self.u32()?;
                if n as usize >= variants.len() {
                    Err(err("enum index"))
                } else {
                    Ok(Value::Enum(n))
                }
            }
            FieldType::Optional(x) => {
                let p = self.take(1)?[0];
                match p {
                    0 => Ok(Value::Optional(None)),
                    1 => {
                        let value = self.val(x, d + 1)?;
                        Ok(Value::Optional(if BUILD {
                            Some(Box::new(value))
                        } else {
                            None
                        }))
                    }
                    _ => Err(err("invalid option")),
                }
            }
            FieldType::Sequence { item, max_len } => {
                let n = self.u32()? as usize;
                if n > *max_len || n > 4096 - self.nodes {
                    return Err(err("sequence length"));
                }
                let mut v = if BUILD {
                    Vec::with_capacity(n)
                } else {
                    Vec::new()
                };
                for index in 0..n {
                    let value = self
                        .val(item, d + 1)
                        .map_err(|e| e.at(&format!("[{index}]")))?;
                    if BUILD {
                        v.push(value);
                    }
                }
                Ok(Value::Sequence(v))
            }
            FieldType::Record(_) => unreachable!(),
        }
    }
    fn record(&mut self, s: &Schema, d: usize) -> Result<Value, SchemaError> {
        self.nodes += 1;
        if self.nodes > 4096 || d > 16 {
            return Err(err("value node limit"));
        }
        let mut out = Vec::new();
        for f in &s.fields {
            let value = self.val(&f.ty, d + 1).map_err(|e| e.at(&f.name))?;
            if BUILD {
                out.push(value);
            }
        }
        Ok(Value::Record(out))
    }
}
pub struct SchemaRegistry {
    max: usize,
    next: u32,
    items: HashMap<(String, u32), (u32, Arc<Schema>)>,
}
impl SchemaRegistry {
    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    pub fn new(max: usize) -> Result<Self, SchemaError> {
        if max == 0 || max > 65536 {
            return Err(err("schema registry limit"));
        }
        Ok(Self {
            max: max.min(65536),
            next: 1,
            items: HashMap::new(),
        })
    }
    pub fn register(&mut self, s: Schema) -> Result<u32, SchemaError> {
        Ok(self.register_batch(vec![s])?.remove(0))
    }
    pub fn register_batch(&mut self, schemas: Vec<Schema>) -> Result<Vec<u32>, SchemaError> {
        if schemas.len() > self.max {
            return Err(err("schema registration batch exceeds limit"));
        }
        for s in &schemas {
            s.validate()?;
        }
        fn collect(
            s: &Schema,
            seen: &mut HashMap<(String, u32), Schema>,
        ) -> Result<(), SchemaError> {
            let k = (s.name.clone(), s.version);
            if let Some(old) = seen.get(&k) {
                if old != s {
                    return Err(err("nested schema identity collision"));
                }
            } else {
                seen.insert(k, s.clone());
            }
            for f in &s.fields {
                fn walk(
                    t: &FieldType,
                    seen: &mut HashMap<(String, u32), Schema>,
                ) -> Result<(), SchemaError> {
                    match t {
                        FieldType::Optional(x) | FieldType::Sequence { item: x, .. } => {
                            walk(x, seen)
                        }
                        FieldType::Record(s) => collect(s, seen),
                        _ => Ok(()),
                    }
                }
                walk(&f.ty, seen)?;
            }
            Ok(())
        }
        let mut nested = HashMap::new();
        for s in &schemas {
            collect(s, &mut nested)?;
        }
        for (_, s) in self.items.values() {
            collect(s, &mut nested)?;
        }
        let mut staged = self.items.clone();
        let mut next = self.next;
        let mut ids = Vec::new();
        for s in schemas {
            let k = (s.name.clone(), s.version);
            if let Some((id, old)) = staged.get(&k) {
                if **old == s {
                    ids.push(*id);
                    continue;
                }
                return Err(err("schema identity collision"));
            }
            if staged.len() >= self.max {
                return Err(err("schema registry limit"));
            }
            let id = next;
            next = next.checked_add(1).ok_or_else(|| err("schema id limit"))?;
            staged.insert(k, (id, Arc::new(s.normalized_clone())));
            ids.push(id);
        }
        self.items = staged;
        self.next = next;
        Ok(ids)
    }
    pub fn schema(&self, id: u32) -> Result<Arc<Schema>, SchemaError> {
        self.items
            .values()
            .find(|(i, _)| *i == id)
            .map(|(_, s)| s.clone())
            .ok_or_else(|| err("unknown schema"))
    }
}
