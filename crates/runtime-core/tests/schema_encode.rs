use actorplane_core::schema::{Field, FieldType, Schema, Value};

fn schema() -> Schema {
    Schema {
        name: "enc".into(),
        version: 1,
        fields: vec![
            Field {
                name: "i".into(),
                ty: FieldType::Int { min: -2, max: 2 },
            },
            Field {
                name: "u".into(),
                ty: FieldType::UInt {
                    min: 0,
                    max: u64::MAX,
                },
            },
            Field {
                name: "f".into(),
                ty: FieldType::Float {
                    allow_non_finite: true,
                },
            },
            Field {
                name: "s".into(),
                ty: FieldType::Sequence {
                    item: Box::new(FieldType::Bool),
                    max_len: 4,
                },
            },
        ],
    }
}
#[test]
fn encode_mixed_native_values() {
    let s = schema();
    let v = Value::Record(vec![
        Value::Int(-2),
        Value::UInt(u64::MAX),
        Value::Float(-0.0),
        Value::Sequence(vec![Value::Bool(true), Value::Bool(false)]),
    ]);
    let b = s.encode(&v, 1024).unwrap();
    assert_eq!(b.len(), 8 + 8 + 8 + 4 + 2);
    assert_eq!(s.decode(&b).unwrap(), v);
    assert_eq!(&b[16..24], &(-0.0f64).to_le_bytes());
}
#[test]
fn reject_shape_range_and_exact_budget() {
    let s = schema();
    let v = Value::Record(vec![
        Value::Int(3),
        Value::UInt(0),
        Value::Float(0.0),
        Value::Sequence(vec![]),
    ]);
    assert!(s.encode(&v, 1024).is_err());
    let ok = Value::Record(vec![
        Value::Int(1),
        Value::UInt(0),
        Value::Float(0.0),
        Value::Sequence(vec![]),
    ]);
    let b = s.encode(&ok, 28).unwrap();
    assert!(s.encode(&ok, b.len() - 1).is_err());
    assert!(s.encode(&Value::Record(vec![]), 128).is_err());
}

#[test]
fn native_encoder_canonicalizes_nan_and_nested_records_preserve_the_tail() {
    let inner = schema();
    let nested = Schema {
        name: "outer".into(),
        version: 1,
        fields: vec![
            Field {
                name: "child".into(),
                ty: FieldType::Record(Box::new(inner)),
            },
            Field {
                name: "tail".into(),
                ty: FieldType::Bool,
            },
        ],
    };
    let value = Value::Record(vec![
        Value::Record(vec![
            Value::Int(0),
            Value::UInt(u64::MAX),
            Value::Float(f64::from_bits(0x7ff0000000000001)),
            Value::Sequence(vec![]),
        ]),
        Value::Bool(true),
    ]);
    let encoded = nested.encode(&value, 100).unwrap();
    assert_eq!(&encoded[16..24], &0x7ff8000000000000u64.to_le_bytes());
    assert_eq!(encoded.last(), Some(&1));
    assert!(nested.validate_bytes(&encoded).is_ok());
    assert_eq!(
        nested
            .encode(&nested.decode(&encoded).unwrap(), 100)
            .unwrap(),
        encoded
    );
}

#[test]
fn native_encoder_and_reader_share_exact_value_node_limit() {
    let schema = Schema {
        name: "many".into(),
        version: 1,
        fields: vec![Field {
            name: "values".into(),
            ty: FieldType::Sequence {
                item: Box::new(FieldType::Bool),
                max_len: 4096,
            },
        }],
    };
    let exact = Value::Record(vec![Value::Sequence(vec![Value::Bool(true); 4094])]);
    assert!(
        schema
            .validate_bytes(&schema.encode(&exact, 5000).unwrap())
            .is_ok()
    );
    let over = Value::Record(vec![Value::Sequence(vec![Value::Bool(true); 4095])]);
    assert!(schema.encode(&over, 5000).is_err());
}
