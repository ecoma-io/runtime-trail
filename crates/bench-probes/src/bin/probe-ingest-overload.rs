//! `probe-ingest-overload` — the real memory path at saturation.
//!
//! Repeated max-size legal OTLP exports (at-cap point counts,
//! attribute-heavy resources, under the 4 MiB wire ceiling) through the
//! real pipeline into the real in-memory store at the contract ceilings
//! (64 MiB queue, 256 MiB accounted retention): first pumped until the
//! store's ceilings hold under sustained load, then unpumped until
//! `QueueSaturated` — the one retryable admission signal — persists. RSS
//! is sampled along the way; the record carries the accounted totals
//! beside the kernel's readings so the real-to-accounted margin is a
//! subtraction away.
//!
//! Prints one JSON record on stdout (schema in
//! `runtime_trail_bench_probes::probes::overload_record`); exits non-zero
//! if it could not measure.

fn main() {
    if let Err(error) = run() {
        eprintln!("✗ probe-ingest-overload could not measure: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), runtime_trail_bench_probes::probes::ProbeError> {
    let report = runtime_trail_bench_probes::probes::run_overload(
        &runtime_trail_bench_probes::probes::OverloadConfig::default(),
    )?;
    println!(
        "{}",
        runtime_trail_bench_probes::probes::overload_record(&report).render()
    );
    Ok(())
}
