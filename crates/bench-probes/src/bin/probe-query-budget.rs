//! `probe-query-budget` — a query forced past its per-page budgets,
//! answered truthfully, served memory holding the line.
//!
//! The case script builds and launches the real server binary
//! (`runtime-trail-server`) and hands this probe the server's PID and the
//! port it listens on. The probe ingests one trace large enough that the
//! investigation chain must blow its per-page budgets (chain
//! `total_entities` is 10,000; the probe residents 12,000 spans), warms
//! the investigation surface, reads the server's settled RSS, then storms
//! the surface with the same investigation and counts the truthful
//! envelope answers (200 + the envelope naming the budget pressure:
//! degraded run, `dimension: results`). The server's RSS is read settled
//! before the storm and settled after — refuse-don't-grow — always from
//! `/proc/<pid>/status`.
//!
//! Prints one JSON record on stdout (schema in
//! `runtime_trail_bench_probes::served::query_budget_record`); exits
//! non-zero if the served surface answers something unexpected, or the
//! server RSS cannot be read.
//!
//! Usage: `probe-query-budget <server-pid> <port>`

fn main() {
    let mut args = std::env::args().skip(1);
    let Some(pid) = args.next().and_then(|value| value.parse().ok()) else {
        eprintln!("✗ probe-query-budget needs <server-pid> <port>");
        std::process::exit(2);
    };
    let Some(port) = args.next().and_then(|value| value.parse().ok()) else {
        eprintln!("✗ probe-query-budget needs <server-pid> <port>");
        std::process::exit(2);
    };
    if let Err(error) = run(pid, port) {
        eprintln!("✗ probe-query-budget could not measure: {error}");
        std::process::exit(1);
    }
}

fn run(pid: u32, port: u16) -> Result<(), runtime_trail_bench_probes::probes::ProbeError> {
    let report = runtime_trail_bench_probes::served::run_query_budget(
        pid,
        port,
        &runtime_trail_bench_probes::served::QueryBudgetConfig::default(),
    )?;
    println!(
        "{}",
        runtime_trail_bench_probes::served::query_budget_record(&report).render()
    );
    Ok(())
}
