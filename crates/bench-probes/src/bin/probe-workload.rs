//! `probe-workload` — the served runtime under a typical investigation
//! session.
//!
//! The case script builds and launches the real server binary
//! (`runtime-trail-server`) and hands this probe the server's PID and the
//! port it listens on. The probe drives the served runtime the way a
//! person would: one trace of a few hundred spans, its related logs, and
//! a handful of surrounding metric points, all over the OTLP/HTTP
//! receiver; then the investigation surface for the trace's root (200)
//! and for a well-formed foreign subject (404). The *server's* RSS is
//! read from `/proc/<pid>/status` during the pump and after settle — the
//! measurement is process-level: measured from outside, never composed
//! in (AGENTS.md dependency law).
//!
//! Prints one JSON record on stdout (schema in
//! `runtime_trail_bench_probes::served::workload_record`); exits non-zero
//! if the served surface answers something unexpected, or the server
//! RSS cannot be read.
//!
//! Usage: `probe-workload <server-pid> <port>`

fn main() {
    let mut args = std::env::args().skip(1);
    let Some(pid) = args.next().and_then(|value| value.parse().ok()) else {
        eprintln!("✗ probe-workload needs <server-pid> <port>");
        std::process::exit(2);
    };
    let Some(port) = args.next().and_then(|value| value.parse().ok()) else {
        eprintln!("✗ probe-workload needs <server-pid> <port>");
        std::process::exit(2);
    };
    if let Err(error) = run(pid, port) {
        eprintln!("✗ probe-workload could not measure: {error}");
        std::process::exit(1);
    }
}

fn run(pid: u32, port: u16) -> Result<(), runtime_trail_bench_probes::probes::ProbeError> {
    let report = runtime_trail_bench_probes::served::run_workload(
        pid,
        port,
        &runtime_trail_bench_probes::served::WorkloadConfig::default(),
    )?;
    println!(
        "{}",
        runtime_trail_bench_probes::served::workload_record(&report).render()
    );
    Ok(())
}
