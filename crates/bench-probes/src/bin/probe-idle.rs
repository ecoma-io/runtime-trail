//! `probe-idle` — the composition, constructed and left alone.
//!
//! This is the number the "Idle RSS < 50 MiB" target is *about*: the
//! queue, the admission pipeline and the in-memory store built the way
//! the composition root builds them, no traffic, resident set settled.
//! Phase 1 measures it; no probe asserts it against the target.
//!
//! Prints one JSON record on stdout (schema in
//! `runtime_trail_bench_probes::probes::idle_record`); exits non-zero if
//! it could not measure.

fn main() {
    if let Err(error) = run() {
        eprintln!("✗ probe-idle could not measure: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), runtime_trail_bench_probes::probes::ProbeError> {
    let report = runtime_trail_bench_probes::probes::run_idle()?;
    println!(
        "{}",
        runtime_trail_bench_probes::probes::idle_record(&report).render()
    );
    Ok(())
}
