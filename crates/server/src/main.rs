//! CLI entry point for the `runtime-trail` native core. Argument parsing
//! lives in the library (`parse_args`) so the CLI contract is testable; this
//! binary only wires the parsed configuration to [`runtime_trail_server::serve`].

use std::process::ExitCode;

use runtime_trail_server::{Cli, PRODUCT, USAGE, VERSION, parse_args, serve};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    match parse_args(std::env::args().skip(1)) {
        Ok(Cli::Run(config)) => match serve(config).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                tracing::error!(%error, "runtime-trail core failed");
                ExitCode::FAILURE
            }
        },
        Ok(Cli::Help) => {
            print!("{USAGE}");
            ExitCode::SUCCESS
        }
        Ok(Cli::Version) => {
            println!("{PRODUCT} {VERSION}");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("{message}\n\n{USAGE}");
            ExitCode::from(2)
        }
    }
}
