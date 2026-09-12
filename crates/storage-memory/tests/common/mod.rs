//! Fixtures shared by the behavioral test suite. Every test drives the
//! store through the `TelemetryStore` trait — the same access the query
//! engine, the correlation engine and the composition root have — never
//! through the concrete type.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use runtime_trail_storage::{EvictionHook, TelemetryStore};
use runtime_trail_storage_memory::{InMemoryStore, MemoryConfig};
use runtime_trail_telemetry_model::{AdmissionTime, Admitted, EntityId};
use runtime_trail_telemetry_model::{
    Attributes, EmitterDroppedCounts, InstrumentationScope, LogRecord, MetricNumber, MetricPoint,
    NumberPoint, Resource, Span, SpanId, SpanKind, SpanStatus, SpanStatusCode, StreamIdentity,
    StreamKind, TraceContext, TraceFlags, TraceId, TraceState, Value,
};

/// The wall-clock reading a fixture uses, in nanoseconds.
#[must_use]
pub fn at(nano: u64) -> AdmissionTime {
    AdmissionTime::from_unix_nano(nano)
}

/// An assigned entity id built from a raw session serial, the way the
/// ledger would hand one out.
#[must_use]
pub fn assigned(serial: u64) -> EntityId {
    use runtime_trail_telemetry_model::AssignedId;
    use std::num::NonZeroU64;

    EntityId::Assigned(AssignedId::from_serial(
        NonZeroU64::new(serial).expect("fixture serials are nonzero"),
    ))
}

/// A span with valid trace and span ids (so its natural identity is the
/// entity id) and a body whose accounted size is the given payload length.
#[must_use]
pub fn span(trace: [u8; 16], span_id: [u8; 8], name: &str) -> Span {
    Span {
        context: TraceContext {
            trace_id: TraceId::from_bytes(trace),
            span_id: SpanId::from_bytes(span_id),
            flags: TraceFlags::new(1),
            tracestate: TraceState::default(),
        },
        parent_span_id: None,
        name: name.to_owned(),
        kind: SpanKind::Server,
        start_time_unix_nano: 10,
        end_time_unix_nano: Some(20),
        resource: Arc::new(Resource {
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }),
        scope: Arc::new(InstrumentationScope {
            name: String::new(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }),
        attributes: Attributes::default(),
        emitter_dropped: EmitterDroppedCounts::default(),
        events: Vec::new(),
        links: Vec::new(),
        status: SpanStatus {
            code: SpanStatusCode::Unset,
            message: String::new(),
        },
    }
}

/// A log record with a string body. Log records have no natural identity,
/// so callers pass fresh assigned ids.
#[must_use]
pub fn log_record(body: &str) -> LogRecord {
    LogRecord {
        timestamp_unix_nano: Some(1),
        observed_timestamp_unix_nano: Some(2),
        severity_number: None,
        severity_text: None,
        body: Some(Value::String(body.to_owned())),
        resource: Arc::new(Resource {
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }),
        scope: Arc::new(InstrumentationScope {
            name: String::new(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        }),
        attributes: Attributes::default(),
        dropped_attribute_count: 0,
        trace_id: None,
        span_id: None,
        trace_flags: None,
        event_name: None,
    }
}

/// A gauge point at `time` carrying an int value.
#[must_use]
pub fn gauge_point(time: u64, value: i64) -> MetricPoint {
    MetricPoint::Number(NumberPoint::measurement(
        time,
        MetricNumber::int(value),
        Attributes::default(),
        Vec::new(),
    ))
}

/// One stream identity, shared by every point a test keeps.
#[must_use]
pub fn stream() -> Arc<StreamIdentity> {
    Arc::new(StreamIdentity {
        resource: Resource {
            attributes: Attributes::from_pairs(vec![(
                "service.name".to_owned(),
                Value::String("checkout".to_owned()),
            )])
            .expect("unique keys"),
            schema_url: None,
            dropped_attributes_count: 0,
        },
        scope: InstrumentationScope {
            name: "scope".to_owned(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        },
        name: "requests".to_owned(),
        kind: StreamKind::Gauge,
        temporality: None,
    })
}

/// A stream identity named `name`, empty otherwise — the smallest legal
/// content, for tests that count streams rather than their bytes.
#[must_use]
pub fn bare_stream(name: &str) -> Arc<StreamIdentity> {
    Arc::new(StreamIdentity {
        resource: Resource {
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        },
        scope: InstrumentationScope {
            name: String::new(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        },
        name: name.to_owned(),
        kind: StreamKind::Gauge,
        temporality: None,
    })
}

/// A stream identity whose **resource** carries `attributes` attributes of
/// `value_bytes` payload bytes each — the reviewer's probe shape: many
/// distinct streams, each pinning large legal identity content that a
/// per-point formula never sees.
#[must_use]
pub fn heavy_stream(name: &str, attributes: usize, value_bytes: usize) -> Arc<StreamIdentity> {
    let pairs: Vec<(String, Value)> = (0..attributes)
        .map(|index| (format!("k{index}"), Value::String("x".repeat(value_bytes))))
        .collect();
    Arc::new(StreamIdentity {
        resource: Resource {
            attributes: Attributes::from_pairs(pairs).expect("unique keys"),
            schema_url: None,
            dropped_attributes_count: 0,
        },
        scope: InstrumentationScope {
            name: String::new(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        },
        name: name.to_owned(),
        kind: StreamKind::Gauge,
        temporality: None,
    })
}

/// An admitted record: entity, admission time, shared payload — the exact
/// hand-off ingestion makes.
#[must_use]
pub fn admitted<R>(entity: EntityId, nano: u64, record: R) -> Admitted<Arc<R>> {
    Admitted {
        entity,
        admitted_at: at(nano),
        record: Arc::new(record),
    }
}

/// A store over the trait object, the shape the composition root holds.
#[must_use]
pub fn boxed(config: MemoryConfig, hook: Option<Box<dyn EvictionHook>>) -> Box<dyn TelemetryStore> {
    Box::new(InMemoryStore::new(config, hook))
}

/// A hook that records every removal report, in the order the store
/// delivered it: evicted records and, separately, the streams whose last
/// resident point went with them.
#[derive(Debug, Default)]
pub struct RecordingHook {
    pub evicted: Vec<EntityId>,
    pub streams_released: Vec<Arc<StreamIdentity>>,
}

impl EvictionHook for RecordingHook {
    fn evicted(&mut self, entity: EntityId) {
        self.evicted.push(entity);
    }

    fn stream_released(&mut self, stream: &Arc<StreamIdentity>) {
        self.streams_released.push(Arc::clone(stream));
    }
}

/// A hook whose recording is shared with the test through a `Send + Sync`
/// handle: the test reads what the store delivered while the store, and
/// the boxed hook, still live.
#[derive(Debug, Default, Clone)]
pub struct SharedRecordingHook(pub Arc<Mutex<RecordingHook>>);

impl EvictionHook for SharedRecordingHook {
    fn evicted(&mut self, entity: EntityId) {
        self.0
            .lock()
            .expect("hook lock poisoned")
            .evicted
            .push(entity);
    }

    fn stream_released(&mut self, stream: &Arc<StreamIdentity>) {
        self.0
            .lock()
            .expect("hook lock poisoned")
            .streams_released
            .push(Arc::clone(stream));
    }
}
