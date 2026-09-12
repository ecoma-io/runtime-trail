//! `probe-retention-bound` — the retention law holding the line.
//!
//! Fills the in-memory store to its retention ceilings (a small, stated
//! configuration — the contract numbers are defaults, and config exists
//! so a probe reaches its ceilings in seconds) with log-record exports
//! through the real pipeline, then keeps the same load on and samples
//! RSS across the steady state. The plateau samples, the eviction
//! counters and the residency facts go in the record; whether the
//! plateau is bounded enough is read from the record by a person, never
//! gated by the probe.
//!
//! Log records are the load on purpose — see the library docs for what
//! that means and why.
//!
//! Prints one JSON record on stdout (schema in
//! `runtime_trail_bench_probes::probes::retention_record`); exits
//! non-zero if it could not measure.

fn main() {
    if let Err(error) = run() {
        eprintln!("✗ probe-retention-bound could not measure: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), runtime_trail_bench_probes::probes::ProbeError> {
    let report = runtime_trail_bench_probes::probes::run_retention(
        &runtime_trail_bench_probes::probes::RetentionConfig::default(),
    )?;
    println!(
        "{}",
        runtime_trail_bench_probes::probes::retention_record(&report).render()
    );
    Ok(())
}
