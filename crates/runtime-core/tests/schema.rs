use actorplane_core::schema::{Field, FieldType, Schema, SchemaError, SchemaRegistry, Value};

fn s(t: FieldType) -> Schema {
    Schema {
        name: "x".into(),
        version: 1,
        fields: vec![Field {
            name: "v".into(),
            ty: t,
        }],
    }
}
#[test]
fn decode_int_string_and_sequence() {
    let x = s(FieldType::Int { min: -2, max: 4 });
    assert_eq!(
        x.decode(&3i64.to_le_bytes()),
        Ok(Value::Record(vec![Value::Int(3)]))
    );
    let x = s(FieldType::String { max_bytes: 4 });
    assert!(x.decode(&[2, 0, 0, 0, b'o', b'k']).is_ok());
}
#[test]
fn malformed_and_bounds_rejected() {
    let x = s(FieldType::Bool);
    assert!(x.decode(&[2]).is_err());
    let x = s(FieldType::Sequence {
        item: Box::new(FieldType::UInt { min: 0, max: 9 }),
        max_len: 2,
    });
    assert!(x.decode(&[3, 0, 0, 0]).is_err());
    assert!(
        Schema {
            name: "x".into(),
            version: 1,
            fields: vec![Field {
                name: "x".into(),
                ty: FieldType::String {
                    max_bytes: 1 << 20 | 1
                }
            }]
        }
        .validate()
        .is_err()
    );
}
#[test]
fn float_nan_policy_and_depth() {
    let x = s(FieldType::Float {
        allow_non_finite: false,
    });
    assert!(x.decode(&0x7ff0_0000_0000_0000u64.to_le_bytes()).is_err());
    let x = s(FieldType::Float {
        allow_non_finite: true,
    });
    assert!(x.decode(&0x7ff8_0000_0000_0000u64.to_le_bytes()).is_ok());
    assert!(x.decode(&0x7ff8_0000_0000_0001u64.to_le_bytes()).is_err());
}
#[test]
fn registry_identity_collision_and_limit() {
    let mut r = SchemaRegistry::new(1).unwrap();
    let x = s(FieldType::Bool);
    assert_eq!(r.register(x.clone()), Ok(1));
    assert_eq!(r.register(x), Ok(1));
    assert!(
        r.register(Schema {
            name: "x".into(),
            version: 1,
            fields: vec![Field {
                name: "v".into(),
                ty: FieldType::Int { min: 0, max: 1 }
            }]
        })
        .is_err()
    );
    assert_eq!(r.schema(1).unwrap().name, "x");
    let _: SchemaError = SchemaError("x".into());
}

#[test]
fn nested_identity_conflict_is_rejected_atomically() {
    let mut r = SchemaRegistry::new(4).unwrap();
    let nested_a = Schema {
        name: "nested".into(),
        version: 1,
        fields: vec![Field {
            name: "x".into(),
            ty: FieldType::Bool,
        }],
    };
    let nested_b = Schema {
        name: "nested".into(),
        version: 1,
        fields: vec![Field {
            name: "x".into(),
            ty: FieldType::UInt { min: 0, max: 1 },
        }],
    };
    let root = |name: &str, n: Schema| Schema {
        name: name.into(),
        version: 1,
        fields: vec![Field {
            name: "n".into(),
            ty: FieldType::Record(Box::new(n)),
        }],
    };
    assert!(r.register(root("a", nested_a)).is_ok());
    assert!(r.register_batch(vec![root("b", nested_b)]).is_err());
    assert!(r.schema(2).is_err());
}

#[test]
fn identical_nested_identity_is_allowed_and_batch_is_atomic() {
    let mut r = SchemaRegistry::new(4).unwrap();
    let nested = Schema {
        name: "nested".into(),
        version: 1,
        fields: vec![Field {
            name: "x".into(),
            ty: FieldType::Bool,
        }],
    };
    let root = |name: &str| Schema {
        name: name.into(),
        version: 1,
        fields: vec![Field {
            name: "n".into(),
            ty: FieldType::Record(Box::new(nested.clone())),
        }],
    };
    assert_eq!(
        r.register_batch(vec![root("a"), root("b")]).unwrap(),
        vec![1, 2]
    );
    let bad = Schema {
        name: "bad".into(),
        version: 1,
        fields: vec![Field {
            name: "x".into(),
            ty: FieldType::String {
                max_bytes: 1 << 20 | 1,
            },
        }],
    };
    assert!(r.register_batch(vec![root("c"), bad]).is_err());
    assert!(r.schema(3).is_err());
}

#[test]
fn nested_record_is_decoded_in_shared_reader() {
    let nested = Schema {
        name: "nested2".into(),
        version: 1,
        fields: vec![Field {
            name: "x".into(),
            ty: FieldType::Bool,
        }],
    };
    let root = Schema {
        name: "root2".into(),
        version: 1,
        fields: vec![
            Field {
                name: "a".into(),
                ty: FieldType::Record(Box::new(nested.clone())),
            },
            Field {
                name: "b".into(),
                ty: FieldType::Bool,
            },
        ],
    };
    assert_eq!(
        root.decode(&[1, 0]),
        Ok(Value::Record(vec![
            Value::Record(vec![Value::Bool(true)]),
            Value::Bool(false)
        ]))
    );
}
