#![no_main]
use actorplane_core::schema::{Field, FieldType, Schema};
use libfuzzer_sys::fuzz_target;

fn field(name: &str, ty: FieldType) -> Field {
    Field {
        name: name.into(),
        ty,
    }
}
fn schema(ty: FieldType) -> Schema {
    Schema {
        name: "fuzz.Wire".into(),
        version: 1,
        fields: vec![field("value", ty)],
    }
}
fn exercise(schema: &Schema, bytes: &[u8]) {
    let valid = schema.validate_bytes(bytes);
    let decoded = schema.decode(bytes);
    // Validation does not build values; decode uses a different BUILD path.
    assert_eq!(valid.is_ok(), decoded.is_ok());
    if let Ok(value) = decoded {
        let canonical = schema
            .encode(&value, 1 << 20)
            .expect("decoded value encodes");
        assert!(schema.validate_bytes(&canonical).is_ok());
        let again = schema.decode(&canonical).expect("canonical bytes decode");
        // Byte equality also handles NaNs: canonicalization must be idempotent.
        assert_eq!(canonical, schema.encode(&again, 1 << 20).unwrap());
        if !canonical.is_empty() {
            assert!(schema.encode(&again, canonical.len() - 1).is_err());
        }
        let mut trailing = canonical.clone();
        trailing.push(0);
        assert!(schema.decode(&trailing).is_err());
    }
}
fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 65536 {
        return;
    }
    let mut bytes = &data[1..];
    let selected = match data[0] % 12 {
        0 => schema(FieldType::Int {
            min: -1024,
            max: 1024,
        }),
        1 => schema(FieldType::UInt {
            min: 0,
            max: u64::MAX,
        }),
        2 | 3 => schema(FieldType::Float {
            allow_non_finite: data[0] % 12 == 3,
        }),
        4 => schema(FieldType::Bool),
        5 => schema(FieldType::String { max_bytes: 128 }),
        6 => schema(FieldType::Bytes { max_bytes: 128 }),
        7 => schema(FieldType::Enum {
            name: "fuzz.Choice".into(),
            variants: vec!["A".into(), "B".into(), "C".into()],
        }),
        8 => schema(FieldType::Optional(Box::new(FieldType::Bool))),
        9 => schema(FieldType::Sequence {
            item: Box::new(FieldType::Int { min: -8, max: 8 }),
            max_len: 16,
        }),
        10 => Schema {
            name: "fuzz.Nested".into(),
            version: 1,
            fields: vec![
                field(
                    "inner",
                    FieldType::Record(Box::new(schema(FieldType::Bool))),
                ),
                field(
                    "tail",
                    FieldType::Sequence {
                        item: Box::new(FieldType::Bytes { max_bytes: 32 }),
                        max_len: 8,
                    },
                ),
            ],
        },
        _ => {
            if data.len() < 3 {
                return;
            }
            let flags = data[1];
            let depth = usize::from(data[2] % 20);
            let mut ty = FieldType::Int {
                min: 0,
                max: if flags & 8 == 0 { i64::MAX } else { -1 },
            };
            for _ in 0..depth {
                ty = FieldType::Optional(Box::new(ty));
            }
            let mut descriptor = schema(ty);
            if flags & 1 != 0 {
                descriptor.name.clear();
            }
            if flags & 2 != 0 {
                descriptor.version = 0;
            }
            if flags & 4 != 0 {
                descriptor.fields.push(descriptor.fields[0].clone());
            }
            assert_eq!(
                descriptor.validate().is_ok(),
                flags & 15 == 0 && depth <= 15
            );
            bytes = &data[3..];
            descriptor
        }
    };
    exercise(&selected, bytes);
});
