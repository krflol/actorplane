use actorplane_core::{
    Config, EndpointKind, Error, Payload, World,
    schema::{Field, FieldType, Schema},
};
use std::time::{Duration, Instant};

fn int_schema(name: &str, version: u32) -> Schema {
    Schema {
        name: name.into(),
        version,
        fields: vec![Field {
            name: "value".into(),
            ty: FieldType::Int {
                min: i64::MIN,
                max: i64::MAX,
            },
        }],
    }
}

fn bytes_for_i64(value: i64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

#[test]
fn schema_depth_and_node_boundaries_are_enforced() {
    fn nested(depth: usize) -> Schema {
        let mut schema = int_schema("leaf", 1);
        for i in 0..depth {
            schema = Schema {
                name: format!("record_{i}"),
                version: 1,
                fields: vec![Field {
                    name: "nested".into(),
                    ty: FieldType::Record(Box::new(schema)),
                }],
            };
        }
        schema
    }
    assert!(nested(15).validate().is_ok());
    assert!(nested(16).validate().is_err());
    assert!(nested(17).validate().is_err());

    fn wide(counts: &[usize]) -> Schema {
        Schema {
            name: "wide".into(),
            version: 1,
            fields: counts
                .iter()
                .enumerate()
                .map(|(branch, count)| Field {
                    name: format!("branch_{branch}"),
                    ty: FieldType::Record(Box::new(Schema {
                        name: format!("branch_{branch}"),
                        version: 1,
                        fields: (0..*count)
                            .map(|i| Field {
                                name: format!("leaf_{branch}_{i}"),
                                ty: FieldType::Bool,
                            })
                            .collect(),
                    })),
                })
                .collect(),
        }
    }
    let exact = wide(&[255, 255, 255, 254]);
    assert!(exact.validate().is_ok());
    let over = Schema {
        fields: wide(&[255, 255, 255, 255]).fields,
        ..exact.clone()
    };
    assert!(over.validate().is_err());
}

#[test]
fn schema_metadata_rejects_duplicate_fields_zero_versions_and_empty_enums() {
    let duplicate = Schema {
        name: "duplicate".into(),
        version: 1,
        fields: vec![
            Field {
                name: "x".into(),
                ty: FieldType::Bool,
            },
            Field {
                name: "x".into(),
                ty: FieldType::Bool,
            },
        ],
    };
    assert!(duplicate.validate().is_err());
    assert!(int_schema("zero", 0).validate().is_err());
    let empty_enum = Schema {
        name: "enum".into(),
        version: 1,
        fields: vec![Field {
            name: "kind".into(),
            ty: FieldType::Enum {
                name: "Kind".into(),
                variants: Vec::new(),
            },
        }],
    };
    assert!(empty_enum.validate().is_err());
}

#[test]
fn malformed_structured_values_are_rejected_before_admission() {
    let bool_schema = Schema {
        name: "bool".into(),
        version: 1,
        fields: vec![Field {
            name: "v".into(),
            ty: FieldType::Bool,
        }],
    };
    assert!(bool_schema.decode(&[2]).is_err());

    let option_schema = Schema {
        name: "option".into(),
        version: 1,
        fields: vec![Field {
            name: "v".into(),
            ty: FieldType::Optional(Box::new(FieldType::Bool)),
        }],
    };
    assert!(option_schema.decode(&[2]).is_err());

    let string_schema = Schema {
        name: "string".into(),
        version: 1,
        fields: vec![Field {
            name: "v".into(),
            ty: FieldType::String { max_bytes: 8 },
        }],
    };
    assert!(string_schema.decode(&[2, 0, 0, 0, 0xff, 0xff]).is_err());
    assert!(string_schema.decode(&[0, 0, 0, 0, 1]).is_err());

    let float_schema = Schema {
        name: "float".into(),
        version: 1,
        fields: vec![Field {
            name: "v".into(),
            ty: FieldType::Float {
                allow_non_finite: true,
            },
        }],
    };
    assert!(
        float_schema
            .decode(&0x7ff8_0000_0000_0001u64.to_le_bytes())
            .is_err()
    );
    let finite_schema = Schema {
        fields: vec![Field {
            name: "v".into(),
            ty: FieldType::Float {
                allow_non_finite: false,
            },
        }],
        ..float_schema.clone()
    };
    assert!(
        finite_schema
            .decode(&f64::INFINITY.to_bits().to_le_bytes())
            .is_err()
    );
    assert!(finite_schema.decode(&[1, 2, 3, 4]).is_err());

    let sequence_schema = Schema {
        name: "sequence".into(),
        version: 1,
        fields: vec![Field {
            name: "values".into(),
            ty: FieldType::Sequence {
                item: Box::new(FieldType::Bool),
                max_len: 4096,
            },
        }],
    };
    let mut many = (4094u32).to_le_bytes().to_vec();
    many.extend(std::iter::repeat_n(1u8, 4094));
    assert!(sequence_schema.decode(&many).is_ok());
    let mut too_many_nodes = (4096u32).to_le_bytes().to_vec();
    too_many_nodes.extend(std::iter::repeat_n(1u8, 4096));
    assert!(sequence_schema.decode(&too_many_nodes).is_err());
}

#[test]
fn registry_batch_is_atomic_on_nested_identity_collision() {
    let mut registry = actorplane_core::schema::SchemaRegistry::new(4).expect("registry");
    registry.register(int_schema("existing", 1)).expect("first");
    let conflicting = Schema {
        name: "outer".into(),
        version: 1,
        fields: vec![Field {
            name: "nested".into(),
            ty: FieldType::Record(Box::new(Schema {
                name: "existing".into(),
                version: 1,
                fields: vec![Field {
                    name: "different".into(),
                    ty: FieldType::Bool,
                }],
            })),
        }],
    };
    assert!(
        registry
            .register_batch(vec![int_schema("new", 1), conflicting])
            .is_err()
    );
    assert_eq!(registry.len(), 1);
    assert!(registry.schema(2).is_err());
}

#[test]
fn structured_payload_world_fence_and_retained_bytes_are_bounded() {
    let schema = Schema {
        name: "record".into(),
        version: 1,
        fields: vec![Field {
            name: "v".into(),
            ty: FieldType::Bytes { max_bytes: 64 },
        }],
    };
    let w1 = World::new(Config {
        native_payload_budget: 64,
        ..Config::default()
    })
    .expect("world one");
    let w2 = World::new(Config::default()).expect("world two");
    let id = w1
        .register_schemas(vec![schema])
        .expect("schema registration")[0];
    let payload = w1
        .structured(id, &[3, 0, 0, 0, 1, 2, 3])
        .expect("structured payload");
    let actor = w2.allocate(EndpointKind::Native, None).expect("actor");
    w2.activate(actor).expect("activate");
    let held = w1.hold(payload.clone()).expect("hold");
    assert_eq!(w2.send(actor, payload.clone()), Err(Error::CrossWorld));
    assert_eq!(w2.send_held(actor, held.clone()), Err(Error::CrossWorld));
    assert!(w1.snapshot().retained_payload_bytes > 0);
    drop(payload);
    assert!(w1.snapshot().retained_payload_bytes > 0);
    drop(held);
    assert_eq!(w1.snapshot().retained_payload_bytes, 0);
}

#[test]
fn structured_request_result_releases_budget_and_close_fences_registration() {
    let schema = Schema {
        name: "result".into(),
        version: 1,
        fields: vec![Field {
            name: "v".into(),
            ty: FieldType::Int {
                min: i64::MIN,
                max: i64::MAX,
            },
        }],
    };
    let world = World::new(Config {
        native_payload_budget: 32,
        ..Config::default()
    })
    .expect("world");
    let id = world.register_schemas(vec![schema]).expect("schema")[0];
    let owner = world.allocate(EndpointKind::Native, None).expect("owner");
    let target = world.allocate(EndpointKind::Native, None).expect("target");
    world.activate(owner).expect("owner active");
    world.activate(target).expect("target active");
    let request = world
        .structured(id, &bytes_for_i64(4))
        .expect("request payload");
    let operation = world
        .request(
            owner,
            target,
            request,
            Instant::now() + Duration::from_secs(10),
        )
        .expect("request");
    let lease = world
        .claim(target)
        .expect("claim")
        .expect("request delivery");
    assert!(
        world
            .complete_operation(
                operation,
                target,
                Payload::Structured(match lease.payload() {
                    Payload::Structured(record) => record.clone(),
                    _ => panic!("expected structured request"),
                },)
            )
            .expect("complete")
    );
    lease.finish(true);
    assert!(world.snapshot().retained_payload_bytes > 0);
    assert!(world.register_schemas(vec![int_schema("late", 1)]).is_ok());
    world.close();
    assert_eq!(world.snapshot().retained_payload_bytes, 0);
    assert!(
        world
            .register_schemas(vec![int_schema("closed", 1)])
            .is_err()
    );
}
