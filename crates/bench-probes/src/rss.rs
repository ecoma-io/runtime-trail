//! Process memory readings, taken from the operating system.
//!
//! Every RSS number a probe reports is read from `/proc/self/status`
//! (`VmRSS`, the resident set now; `VmHWM`, the resident-set high-water
//! mark). Linux is the platform (`docs/decisions/0001-runtime-language.md`
//! names it); a probe that cannot read the kernel's own accounting
//! **fails loudly and exits non-zero** — it never prints a guessed or
//! zero number, because a fabricated measurement is worse than no
//! measurement (AGENTS.md, "Prohibited shortcuts").
//!
//! The numbers are the kernel's page-accounted view: resident pages of the
//! whole process, in KiB exactly as `/proc` states them. That view includes
//! everything — the binary's text, stacks, the allocator's arenas — which
//! is precisely what "real memory" means next to the model's *accounted*
//! bytes: the gap between the two is the margin the benchmarks contract
//! exists to measure, never a number a probe may fudge in either
//! direction.

use std::fmt;

/// Why a probe could not measure the process it runs in.
#[derive(Debug)]
pub struct RssError(String);

impl fmt::Display for RssError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for RssError {}

/// One reading of the process's resident-set state, in KiB as `/proc`
/// reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RssSample {
    /// `VmRSS`: the resident set right now.
    pub vm_rss_kib: u64,
    /// `VmHWM`: the peak resident set this process has ever reached.
    pub vm_hwm_kib: u64,
}

/// Whether this probe can measure RSS where it is running.
#[must_use]
pub const fn platform_supported() -> bool {
    cfg!(target_os = "linux")
}

/// Reads the process's current resident-set state.
///
/// # Errors
///
/// [`RssError`] off Linux, when `/proc/self/status` cannot be read, or
/// when the kernel's answer does not carry the two fields every probe
/// needs — a probe that cannot measure says so and stops, never reports a
/// made-up number.
pub fn sample() -> Result<RssSample, RssError> {
    if !platform_supported() {
        return Err(RssError(
            "RSS probes read /proc/self/status, which exists on Linux only; \
             this platform cannot be measured here, so the probe refuses to \
             print numbers (AGENTS.md: a check that cannot run says so loudly)"
                .to_owned(),
        ));
    }
    let status = std::fs::read_to_string("/proc/self/status")
        .map_err(|error| RssError(format!("cannot read /proc/self/status: {error}")))?;
    let vm_rss_kib = field_kib(&status, "VmRSS:").ok_or_else(|| {
        RssError("/proc/self/status carried no VmRSS line — refusing to report".to_owned())
    })?;
    let vm_hwm_kib = field_kib(&status, "VmHWM:").ok_or_else(|| {
        RssError("/proc/self/status carried no VmHWM line — refusing to report".to_owned())
    })?;
    Ok(RssSample {
        vm_rss_kib,
        vm_hwm_kib,
    })
}

/// Extracts one `"<name>:\t <value> kB"` field's value from a
/// `/proc/self/status` body.
fn field_kib(status: &str, name: &str) -> Option<u64> {
    status
        .lines()
        .find(|line| line.starts_with(name))?
        .strip_prefix(name)?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A trimmed-down `/proc/self/status` body in the kernel's exact
    /// format — tab-separated, `kB` unit, the fields probes must skip
    /// present and shifted so a wrong parse shows.
    const STATUS: &str = "Name:\tprobe-ingest-overload\n\
                          Umask:\t0022\n\
                          State:\tR (running)\n\
                          VmPeak:\t 9999999 kB\n\
                          VmRSS:\t      1234 kB\n\
                          RssAnon:\t     800 kB\n\
                          VmHWM:\t      5678 kB\n\
                          Threads:\t1\n";

    #[test]
    fn parses_the_kernel_field_format() {
        assert_eq!(field_kib(STATUS, "VmRSS:"), Some(1234));
        assert_eq!(field_kib(STATUS, "VmHWM:"), Some(5678));
    }

    #[test]
    fn skips_lookalike_fields_and_absent_ones() {
        // `RssAnon` and `VmPeak` must not answer for the fields asked for,
        // and a field the kernel did not print is absence, not zero.
        assert_eq!(field_kib(STATUS, "RssAnon:"), Some(800));
        assert_eq!(field_kib(STATUS, "VmSwap:"), None);
        assert_eq!(
            field_kib("VmRSS:\n", "VmRSS:"),
            None,
            "no value is no value"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reads_this_process_on_linux() {
        let reading = sample().expect("this test runs on Linux, where /proc exists");
        assert!(
            reading.vm_rss_kib > 0,
            "a running test process has a non-empty resident set"
        );
        assert!(
            reading.vm_hwm_kib >= reading.vm_rss_kib,
            "the high-water mark is never below the current set"
        );
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn refuses_to_report_off_linux() {
        assert!(sample().is_err(), "no /proc, no numbers");
    }
}
