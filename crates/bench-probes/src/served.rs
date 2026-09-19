//! The served-runtime probe machinery: what the three Phase 6 cases
//! measure, measured from *outside* the server process.
//!
//! The phase-one probes compose the real layers in-process (`probes.rs`)
//! and report the accounted margin beside their own RSS. The
//! served-runtime probes flip that: they launch no composition, they talk
//! to a real running server binary over its real HTTP surfaces — the
//! OTLP/HTTP receiver (`/v1/traces`, `/v1/logs`, `/v1/metrics`) and the
//! investigation surface (`/v1/investigations/traces`) — and they read
//! the *server process's* RSS from `/proc/<pid>/status`, the same kernel
//! accounting the phase-one probes read for themselves
//! (`crate::rss::sample_pid`). This is deliberately process-level: the
//! probes carry no HTTP stack (std `TcpStream` + hand-rolled HTTP/1.1,
//! ~150 lines, like the wire encoder) and no dependency on
//! `runtime-trail-server` or any tokio/axum user (AGENTS.md dependency
//! law holds: `crates/bench-probes` composes only what the boundary table
//! allows).
//!
//! The served runtime is contract-lawful: budgets live in
//! `docs/architecture/runtime-constraints.md` (startup < 1 s, typical
//! workload RSS < 100 MiB, query memory bounded — refuse, don't grow).
//! The cases (in `scripts/bench/cases/`) assert those budgets; these
//! probes produce the numbers and the truthful answers, and fail loudly
//! when the served surface answers something a probe cannot interpret.
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use crate::json::{JsonRecord, JsonValue};
use crate::probes::ProbeError;
use crate::shapes::{
    INVESTIGATED_ROOT_SPAN_ID, INVESTIGATED_TRACE_ID, related_log_shape, trace_shape,
    workload_gauge_shape,
};
use crate::wire::{TraceContext, encode_gauge_export, encode_log_export, encode_trace_export};

/// The OTLP/HTTP receiver's content type — the only one the served
/// surface accepts (JSON is refused: the receiver is protobuf-only).
pub const OTLP_CONTENT_TYPE: &str = "application/x-protobuf";

/// The investigation surface's request content type.
pub const INVESTIGATION_CONTENT_TYPE: &str = "application/json";

/// One HTTP answer the served surface gave.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpAnswer {
    /// The status code.
    pub status: u16,
    /// The raw response body.
    pub body: Vec<u8>,
}

/// Posts one request over raw TCP: HTTP/1.1 with `Content-Length` and
/// `Connection: close`, exactly as the served receiver reads it.
///
/// # Errors
///
/// When the connection cannot be made, the request cannot be written, the
/// answer is not valid HTTP, or the response body exceeds the cap.
pub fn post(
    port: u16,
    path: &str,
    content_type: &str,
    body: &[u8],
) -> Result<HttpAnswer, ProbeError> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).map_err(|error| {
        ProbeError::new(format!(
            "cannot connect to the served runtime on 127.0.0.1:{port}: {error}"
        ))
    })?;
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .map_err(|error| ProbeError::new(format!("cannot set the read timeout: {error}")))?;
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: {content_type}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .and_then(|()| stream.write_all(body))
        .and_then(|()| stream.flush())
        .map_err(|error| ProbeError::new(format!("cannot write the request: {error}")))?;

    let mut reader = BufReader::new(&mut stream);
    let mut status_line = String::new();
    reader
        .read_line(&mut status_line)
        .map_err(|error| ProbeError::new(format!("cannot read the status line: {error}")))?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| {
            ProbeError::new(format!(
                "the served runtime answered a non-HTTP status line: {status_line:?}"
            ))
        })?;

    let mut content_length = None;
    loop {
        let mut line = String::new();
        let read = reader
            .read_line(&mut line)
            .map_err(|error| ProbeError::new(format!("cannot read the headers: {error}")))?;
        if read == 0 || line == "\r\n" {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if let Some(value) = lower.strip_prefix("content-length:") {
            content_length = value.trim().parse::<usize>().ok();
        }
    }

    // The served surface states Content-Length; when absent, read to EOF —
    // the read timeout bounds a server that never closes.
    let mut body = Vec::new();
    if let Some(length) = content_length {
        body.reserve(length);
        let mut remaining = length;
        let mut chunk = [0u8; 8192];
        while remaining > 0 {
            let want = remaining.min(chunk.len());
            let read = reader.read(&mut chunk[..want]).map_err(|error| {
                ProbeError::new(format!("cannot read the response body: {error}"))
            })?;
            if read == 0 {
                return Err(ProbeError::new(format!(
                    "the served runtime closed the body early: {length} bytes promised, {read} read"
                )));
            }
            body.extend_from_slice(&chunk[..read]);
            remaining -= read;
        }
    } else {
        reader
            .read_to_end(&mut body)
            .map_err(|error| ProbeError::new(format!("cannot read the response body: {error}")))?;
    }
    Ok(HttpAnswer { status, body })
}

/// Whether a served envelope truthfully names the budget pressure the
/// query-budget case is built to provoke: degraded runs inside the spans
/// group and the chain stopped on a named limit (`total_entities` — the
/// 10,000-entity chain cap, see `docs/architecture/query-model.md`).
/// Verified against the real server's compact serde JSON.
fn envelope_names_budget_pressure(body: &[u8]) -> bool {
    let text = String::from_utf8_lossy(body);
    text.contains("\"kind\":\"degraded\"") && text.contains("\"dimension\":\"results\"")
}

/// Whether a served envelope names a truthful budget refusal: a refused
/// outcome carrying the dimension, the limit and the observed spend
/// (query-model.md invariant 6 — "A refused query names the dimension,
/// the limit and the observed spend"). A refusal that names any fewer is
/// not a refusal the contract recognizes.
fn envelope_names_refusal(body: &[u8]) -> bool {
    let text = String::from_utf8_lossy(body);
    text.contains("\"kind\":\"refused\"")
        && text.contains("\"dimension\":")
        && text.contains("\"limit\":")
        && text.contains("\"observed\":")
}

/// How the query-budget storm classified one investigation answer, under
/// the refuse-or-degrade contract (query-model.md § refuse-or-degrade):
/// every storm answer must be a truthful refusal or a truthful degraded
/// envelope; a 200 that names neither fabricates completeness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StormAnswer {
    /// Truthful under refuse-or-degrade: a degraded envelope naming the
    /// budget pressure, or a refusal naming dimension/limit/spend.
    Truthful,
    /// HTTP 200 whose envelope names neither the degradation nor a
    /// refusal — a truncated answer presented as complete.
    FabricatedComplete,
    /// An HTTP status outside the surface contract, with no truthful
    /// refusal envelope to read.
    Unexpected(u16),
}

fn classify_storm_answer(answer: &HttpAnswer) -> StormAnswer {
    if envelope_names_budget_pressure(&answer.body) || envelope_names_refusal(&answer.body) {
        StormAnswer::Truthful
    } else if answer.status == 200 {
        StormAnswer::FabricatedComplete
    } else {
        StormAnswer::Unexpected(answer.status)
    }
}

/// The one investigation subject the cases use: the served trace's root
/// span, as the investigation surface's request body requires it.
#[must_use]
pub fn root_subject_json() -> String {
    format!(
        "{{\"root_span\":{{\"span\":{{\"trace_id\":\"{}\",\"span_id\":\"{}\"}}}}}}",
        hex(&INVESTIGATED_TRACE_ID),
        hex(&INVESTIGATED_ROOT_SPAN_ID)
    )
}

/// A hex-encoded byte string (lowercase, the surface's accepted form).
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

/// The workload probe's configuration: a typical investigation session —
/// one trace of a few hundred spans, its related logs, and a handful of
/// points inside the trace's window.
#[derive(Clone, Debug)]
pub struct WorkloadConfig {
    /// Spans per trace export.
    pub span_count: usize,
    /// Log records carrying the trace's context.
    pub log_count: usize,
    /// Gauge points inside the trace's window.
    pub point_count: usize,
    /// How long to wait for residency after posting (polls of 20 ms).
    pub residency_polls: u32,
    /// How long to settle before the settled RSS reading.
    pub settle: Duration,
}

impl Default for WorkloadConfig {
    /// The published contract workload: the workload-rss case measures
    /// this configuration, stated in the record next to the numbers.
    fn default() -> Self {
        Self {
            span_count: 250,
            log_count: 50,
            point_count: 16,
            residency_polls: 250,
            settle: Duration::from_millis(500),
        }
    }
}

/// What one workload run measured.
#[derive(Clone, Debug)]
pub struct WorkloadReport {
    /// Spans posted.
    pub spans_posted: usize,
    /// Log records posted.
    pub logs_posted: usize,
    /// Gauge points posted.
    pub points_posted: usize,
    /// The OTLP payload bytes in total.
    pub otlp_payload_bytes: usize,
    /// Investigation responses: resident subject 200s.
    pub investigations_ok: u32,
    /// Investigation responses: well-formed non-resident subjects (404s).
    pub investigations_unresolved: u32,
    /// The server's RSS during the pump.
    pub server_rss_pump_kib: u64,
    /// The server's RSS after settle.
    pub server_rss_settled_kib: u64,
    /// The server's RSS high-water mark.
    pub server_rss_hwm_kib: u64,
    /// The wall time of the run.
    pub wall_seconds: f64,
}

/// Runs the workload probe against the served runtime: posts the trace,
/// its related logs and the points, waits for residency, investigates the
/// root (and a non-resident subject for the alternate answer), and reads
/// the server's RSS during the pump and after settle.
///
/// # Errors
///
/// When the served surface answers something unexpected — a refused legal
/// export, an investigation that never resolves, an unreadable server
/// RSS — the probe fails loudly with the surface's own words.
pub fn run_workload(
    pid: u32,
    port: u16,
    config: &WorkloadConfig,
) -> Result<WorkloadReport, ProbeError> {
    let started = Instant::now();
    let context = TraceContext {
        trace_id: INVESTIGATED_TRACE_ID,
        span_id: INVESTIGATED_ROOT_SPAN_ID,
    };
    let trace = trace_shape(config.span_count);
    let logs = related_log_shape(context, config.log_count);
    let points = workload_gauge_shape(config.point_count, crate::shapes::BASE_TIME_UNIX_NANO + 100);

    let trace_payload = encode_trace_export(&trace, 0, crate::shapes::trace_span_attributes);
    let logs_payload = encode_log_export(&logs, 0, crate::shapes::log_record_attributes);
    let points_payload = encode_gauge_export(&points, 0, crate::shapes::gauge_point_attributes);

    for (path, payload) in [
        ("/v1/traces", trace_payload.as_slice()),
        ("/v1/logs", logs_payload.as_slice()),
        ("/v1/metrics", points_payload.as_slice()),
    ] {
        let answer = post(port, path, OTLP_CONTENT_TYPE, payload)?;
        if answer.status != 200 {
            return Err(ProbeError::new(format!(
                "the served runtime refused a legal {path} export with HTTP {}",
                answer.status
            )));
        }
    }

    // Residency is async (queue → pump → store): poll the root subject
    // until the investigation answers 200 — the pump drained — then keep
    // the unresolved-alternative answer for the surface contract.
    let mut investigations_ok = 0;
    let mut investigations_unresolved = 0;
    for _ in 0..config.residency_polls {
        let answer = post(
            port,
            "/v1/investigations/traces",
            INVESTIGATION_CONTENT_TYPE,
            root_subject_json().as_bytes(),
        )?;
        match answer.status {
            200 => {
                investigations_ok += 1;
                break;
            }
            404 => {
                investigations_unresolved += 1;
                std::thread::sleep(Duration::from_millis(20));
            }
            other => {
                return Err(ProbeError::new(format!(
                    "the investigation surface answered HTTP {other} for the resident root"
                )));
            }
        }
    }
    if investigations_ok == 0 {
        return Err(ProbeError::new(
            "the served runtime never resolved the workload's root span within the poll budget \
         (residency did not drain)"
                .to_owned(),
        ));
    }

    // A well-formed subject the server cannot have: the alternate answer.
    let foreign = format!(
        "{{\"root_span\":{{\"span\":{{\"trace_id\":\"{}\",\"span_id\":\"{}\"}}}}}}",
        hex(&[0xDE; 16]),
        hex(&[0xAD; 8])
    );
    let answer = post(
        port,
        "/v1/investigations/traces",
        INVESTIGATION_CONTENT_TYPE,
        foreign.as_bytes(),
    )?;
    if answer.status != 404 {
        return Err(ProbeError::new(format!(
            "a well-formed non-resident subject should answer 404, got HTTP {}",
            answer.status
        )));
    }
    investigations_unresolved += 1;

    let pump_sample = crate::rss::sample_pid(pid)?;
    std::thread::sleep(config.settle);
    let settled_sample = crate::rss::sample_pid(pid)?;

    Ok(WorkloadReport {
        spans_posted: config.span_count,
        logs_posted: config.log_count,
        points_posted: config.point_count,
        otlp_payload_bytes: trace_payload.len() + logs_payload.len() + points_payload.len(),
        investigations_ok,
        investigations_unresolved,
        server_rss_pump_kib: pump_sample.vm_rss_kib,
        server_rss_settled_kib: settled_sample.vm_rss_kib,
        server_rss_hwm_kib: settled_sample.vm_hwm_kib.max(pump_sample.vm_hwm_kib),
        wall_seconds: started.elapsed().as_secs_f64(),
    })
}

/// The query-budget probe's configuration: a chain forced past its
/// per-page budgets, then a storm of investigations that must answer
/// truthfully while the server's RSS stays bound.
#[derive(Clone, Debug)]
pub struct QueryBudgetConfig {
    /// Spans resident (one trace).
    pub span_count: usize,
    /// Spans per export (legal, never over the per-export cap).
    pub spans_per_export: usize,
    /// Warm-up investigations run before the storm: enough to reach the
    /// refusal steady state. The first investigations fault in one-time
    /// chain state (page boundaries, span index); the storm must measure
    /// the steady-state refusal cost — refuse, don't grow — so the
    /// warm-up absorbs the one-time expansion and settles.
    pub warm_investigations: u32,
    /// How many investigations to run in the refusal storm.
    pub storm_investigations: u32,
    /// The settle after each RSS-reading phase (after the warm-up, after
    /// the storm).
    pub settle: Duration,
}

impl Default for QueryBudgetConfig {
    /// The published contract workload: 12,000 spans — chain
    /// `total_entities` is 10,000, so the flows degrade truthfully.
    fn default() -> Self {
        Self {
            span_count: 12_000,
            spans_per_export: 1_000,
            warm_investigations: 30,
            storm_investigations: 20,
            settle: Duration::from_millis(300),
        }
    }
}

/// What one query-budget run measured.
#[derive(Clone, Debug)]
pub struct QueryBudgetReport {
    /// Exports posted.
    pub exports_ingested: u64,
    /// Spans resident.
    pub spans_ingested: usize,
    /// The OTLP payload bytes in total.
    pub otlp_payload_bytes: usize,
    /// Warm-up investigations run before the storm, all of which named
    /// the budget pressure truthfully (the steady state the storm is
    /// measured from).
    pub warm_investigations: u32,
    /// How many storm investigations answered truthfully under
    /// refuse-or-degrade: a degraded envelope naming the budget
    /// pressure, or a refusal naming dimension/limit/spend.
    pub truthful_answers: u32,
    /// How many storm investigations answered HTTP 200 with an envelope
    /// naming neither the degradation nor a refusal — a truncated
    /// answer presented as complete. Any above zero fails the case.
    pub fabricated_complete: u32,
    /// The server's RSS before the storm.
    pub server_rss_before_kib: u64,
    /// The server's RSS after the storm.
    pub server_rss_after_kib: u64,
    /// The wall time of the run.
    pub wall_seconds: f64,
}

/// Runs the query-budget probe: ingests one trace large enough to blow
/// the investigation's chain budget, warms the surface to the refusal
/// steady state (the first investigations fault in one-time chain state;
/// the storm must measure the steady-state cost), then storms it with
/// the same investigation and counts the truthful answers — degraded
/// envelopes or refusals naming dimension, limit and spend — beside any
/// fabricated-complete 200 (an answer that is neither fails the probe). The
/// refuse-don't-grow half: the storm must not grow it.
///
/// # Errors
///
/// When the served surface answers something unexpected — a refused legal
/// export, an envelope that cannot be parsed, an unreadable server RSS —
/// the probe fails loudly.
pub fn run_query_budget(
    pid: u32,
    port: u16,
    config: &QueryBudgetConfig,
) -> Result<QueryBudgetReport, ProbeError> {
    let started = Instant::now();
    let shape = trace_shape(config.spans_per_export);
    let exports = config.span_count.div_ceil(config.spans_per_export);
    let mut payload_bytes = 0;
    for export in 0..exports {
        let payload =
            encode_trace_export(&shape, export as u64, crate::shapes::trace_span_attributes);
        payload_bytes += payload.len();
        let answer = post(port, "/v1/traces", OTLP_CONTENT_TYPE, payload.as_slice())?;
        if answer.status != 200 {
            return Err(ProbeError::new(format!(
                "the served runtime refused a legal trace export ({export}/{exports}) with HTTP {}",
                answer.status
            )));
        }
    }

    // Warm the surface to the refusal steady state. The first
    // investigations fault in one-time chain state (page boundaries,
    // span index) and settle; every warm-up answer must name the budget
    // pressure truthfully, or this probe is not measuring what it
    // claims.
    for warm in 0..config.warm_investigations {
        let warm_answer = post(
            port,
            "/v1/investigations/traces",
            INVESTIGATION_CONTENT_TYPE,
            root_subject_json().as_bytes(),
        )?;
        if warm_answer.status != 200 {
            return Err(ProbeError::new(format!(
                "warm-up investigation {warm}/{} answered HTTP {}: the budget-constrained \
                 surface must keep answering",
                config.warm_investigations, warm_answer.status
            )));
        }
        if !envelope_names_budget_pressure(&warm_answer.body) {
            return Err(ProbeError::new(format!(
                "warm-up investigation {warm}/{} did not name the budget pressure truthfully \
                 (HTTP {}): the chain is not being driven past its per-page budgets, so this \
                 probe is not measuring what it claims",
                config.warm_investigations, warm_answer.status
            )));
        }
    }
    std::thread::sleep(config.settle);
    let before = crate::rss::sample_pid(pid)?;

    let mut truthful_answers = 0;
    let mut fabricated_complete = 0;
    for storm in 0..config.storm_investigations {
        let answer = post(
            port,
            "/v1/investigations/traces",
            INVESTIGATION_CONTENT_TYPE,
            root_subject_json().as_bytes(),
        )?;
        match classify_storm_answer(&answer) {
            StormAnswer::Truthful => truthful_answers += 1,
            StormAnswer::FabricatedComplete => {
                fabricated_complete += 1;
                eprintln!(
                    "✗ storm answer {storm}: HTTP 200 with an envelope naming neither the \
                     budget degradation nor a refusal — a truncated answer presented as \
                     complete (query-model.md § refuse-or-degrade)"
                );
            }
            StormAnswer::Unexpected(status) => {
                return Err(ProbeError::new(format!(
                    "storm answer {storm}: HTTP {status} with an envelope naming neither the \
                     budget degradation nor a refusal; the budget-constrained surface must \
                     answer truthfully — degrade or refuse (query-model.md § refuse-or-degrade)"
                )));
            }
        }
    }
    std::thread::sleep(config.settle);
    let after = crate::rss::sample_pid(pid)?;

    Ok(QueryBudgetReport {
        exports_ingested: exports as u64,
        spans_ingested: config.span_count,
        otlp_payload_bytes: payload_bytes,
        warm_investigations: config.warm_investigations,
        truthful_answers,
        fabricated_complete,
        server_rss_before_kib: before.vm_rss_kib,
        server_rss_after_kib: after.vm_rss_kib,
        wall_seconds: started.elapsed().as_secs_f64(),
    })
}

/// The workload record.
#[must_use]
pub fn workload_record(report: &WorkloadReport) -> JsonRecord {
    let mut record = JsonRecord::new();
    record
        .field("probe", JsonValue::Text("probe-workload".to_owned()))
        .field(
            "signal",
            JsonValue::Text(
                "served runtime: OTLP trace+logs+metrics, investigation surface".to_owned(),
            ),
        )
        .field("spans_posted", JsonValue::U64(report.spans_posted as u64))
        .field("logs_posted", JsonValue::U64(report.logs_posted as u64))
        .field("points_posted", JsonValue::U64(report.points_posted as u64))
        .field(
            "otlp_payload_bytes",
            JsonValue::U64(report.otlp_payload_bytes as u64),
        )
        .field(
            "investigations_ok",
            JsonValue::U64(u64::from(report.investigations_ok)),
        )
        .field(
            "investigations_unresolved",
            JsonValue::U64(u64::from(report.investigations_unresolved)),
        )
        .field(
            "server_rss_pump_kib",
            JsonValue::U64(report.server_rss_pump_kib),
        )
        .field(
            "server_rss_settled_kib",
            JsonValue::U64(report.server_rss_settled_kib),
        )
        .field(
            "server_rss_hwm_kib",
            JsonValue::U64(report.server_rss_hwm_kib),
        )
        .field("wall_seconds", JsonValue::F64(report.wall_seconds));
    record
}

/// The query-budget record.
#[must_use]
pub fn query_budget_record(report: &QueryBudgetReport) -> JsonRecord {
    let mut record = JsonRecord::new();
    record
        .field("probe", JsonValue::Text("probe-query-budget".to_owned()))
        .field(
            "signal",
            JsonValue::Text(
                "served runtime: investigation surface under chain-budget saturation".to_owned(),
            ),
        )
        .field("exports_ingested", JsonValue::U64(report.exports_ingested))
        .field(
            "spans_ingested",
            JsonValue::U64(report.spans_ingested as u64),
        )
        .field(
            "otlp_payload_bytes",
            JsonValue::U64(report.otlp_payload_bytes as u64),
        )
        .field(
            "warm_investigations",
            JsonValue::U64(u64::from(report.warm_investigations)),
        )
        .field(
            "truthful_answers",
            JsonValue::U64(u64::from(report.truthful_answers)),
        )
        .field(
            "fabricated_complete",
            JsonValue::U64(u64::from(report.fabricated_complete)),
        )
        .field(
            "server_rss_before_kib",
            JsonValue::U64(report.server_rss_before_kib),
        )
        .field(
            "server_rss_after_kib",
            JsonValue::U64(report.server_rss_after_kib),
        )
        .field("wall_seconds", JsonValue::F64(report.wall_seconds));
    record
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The served caller against a canned answer: a tiny std `TcpListener`
    /// parses the request framing and answers status + Content-Length —
    /// the client must read exactly the body, and the request must carry
    /// the declared framing.
    #[test]
    fn post_reads_the_full_http_answer() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("a port to test on");
        let port = listener.local_addr().expect("the bound address").port();
        let expected = b"{\"status\":\"ok\"}".to_vec();
        let canned = expected.clone();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("a connection");
            let mut reader = BufReader::new(&mut stream);
            let mut head = String::new();
            reader.read_line(&mut head).expect("the request line");
            assert!(
                head.starts_with("POST /v1/traces HTTP/1.1\r\n"),
                "framed POST: {head:?}"
            );
            let mut length = None;
            for _ in 0..10 {
                let mut line = String::new();
                reader.read_line(&mut line).expect("a header line");
                if line == "\r\n" {
                    break;
                }
                if line.to_ascii_lowercase().starts_with("content-length:") {
                    length = line
                        .split(':')
                        .nth(1)
                        .and_then(|v| v.trim().parse::<usize>().ok());
                }
            }
            let mut body = vec![0u8; length.expect("a content length")];
            reader.read_exact(&mut body).expect("the body");
            assert_eq!(body, b"payload", "the body arrives as framed");
            let answer = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\nContent-Length: {}\r\n\r\n",
                canned.len()
            );
            stream
                .write_all(answer.as_bytes())
                .and_then(|()| stream.write_all(&canned))
                .expect("the canned answer");
        });

        let answer = post(port, "/v1/traces", OTLP_CONTENT_TYPE, b"payload").expect("the answer");
        handle.join().expect("the server thread");
        assert_eq!(answer.status, 200);
        assert_eq!(answer.body, expected);
    }

    #[test]
    fn the_root_subject_names_the_fixed_trace() {
        let subject = root_subject_json();
        assert!(subject.contains(&hex(&INVESTIGATED_TRACE_ID)), "{subject}");
        assert!(
            subject.contains(&hex(&INVESTIGATED_ROOT_SPAN_ID)),
            "{subject}"
        );
        assert!(subject.starts_with("{\"root_span\":"), "{subject}");
    }

    #[test]
    fn the_envelope_check_recognizes_the_truthful_answer() {
        let degraded = b"{\"run_groups\":[{\"part\":\"spans\",\"runs\":[{\"kind\":\"degraded\",\"dimension\":\"results\"}]}]}";
        assert!(envelope_names_budget_pressure(degraded));
        let healthy = b"{\"run_groups\":[{\"part\":\"spans\",\"runs\":[{\"kind\":\"complete\"}]}]}";
        assert!(!envelope_names_budget_pressure(healthy));
    }

    #[test]
    fn the_refusal_check_requires_dimension_limit_and_spend() {
        let refusal = b"{\"run_groups\":[{\"part\":\"spans\",\"runs\":[{\"outcome\":{\"kind\":\"refused\",\"refusal\":{\"dimension\":\"results\",\"limit\":{\"units\":10},\"observed\":{\"units\":12}}}}]}]}";
        assert!(envelope_names_refusal(refusal));
        let partial = b"{\"run_groups\":[{\"part\":\"spans\",\"runs\":[{\"outcome\":{\"kind\":\"refused\",\"refusal\":{\"dimension\":\"results\",\"limit\":{\"units\":10}}}}]}]}";
        assert!(
            !envelope_names_refusal(partial),
            "a refusal without the observed spend names no spend"
        );
        let complete = b"{\"run_groups\":[{\"part\":\"spans\",\"runs\":[{\"outcome\":{\"kind\":\"complete\"}}]}]}";
        assert!(!envelope_names_refusal(complete));
    }

    #[test]
    fn the_storm_classifies_every_answer_as_truthful_fabricated_or_unexpected() {
        let degraded = HttpAnswer {
            status: 200,
            body: b"{\"run_groups\":[{\"part\":\"spans\",\"runs\":[{\"kind\":\"degraded\",\"dimension\":\"results\"}]}]}"
                .to_vec(),
        };
        let refused = HttpAnswer {
            status: 507,
            body: b"{\"run_groups\":[{\"part\":\"spans\",\"runs\":[{\"outcome\":{\"kind\":\"refused\",\"refusal\":{\"dimension\":\"results\",\"limit\":{\"units\":10},\"observed\":{\"units\":12}}}}]}]}"
                .to_vec(),
        };
        let fabricated = HttpAnswer {
            status: 200,
            body: b"{\"run_groups\":[{\"part\":\"spans\",\"runs\":[{\"outcome\":{\"kind\":\"complete\"}}]}]}"
                .to_vec(),
        };
        let unexpected = HttpAnswer {
            status: 500,
            body: b"{\"error\":\"internal\"}".to_vec(),
        };
        assert_eq!(classify_storm_answer(&degraded), StormAnswer::Truthful);
        assert_eq!(classify_storm_answer(&refused), StormAnswer::Truthful);
        assert_eq!(
            classify_storm_answer(&fabricated),
            StormAnswer::FabricatedComplete
        );
        assert_eq!(
            classify_storm_answer(&unexpected),
            StormAnswer::Unexpected(500)
        );
    }
}
