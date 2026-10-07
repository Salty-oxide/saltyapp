//! What librdkafka was *actually* compiled with, asked of librdkafka itself.
//!
//! Every optional codec (zstd, Snappy, gzip) and TLS support is a
//! *compile-time* `#if` inside the vendored librdkafka C sources. When one is
//! missing, nothing fails at build time and nothing fails at connect time —
//! the app only breaks much later, when a consumer meets a batch it has no
//! code path to decompress, and reports the generic
//! `NotImplemented (Local: Not Implemented)`.
//!
//! That silence is what made this class of bug recur five times: every check
//! we had was a *proxy* for the real state (a Cargo feature being resolved, a
//! vcpkg install succeeding, an env var being non-empty, a generated
//! `config.h` holding a define). Each proxy can be true while the shipped
//! binary is still missing the codec — most sharply on Windows, where
//! librdkafka's C sources include `win32_config.h` and never read the
//! generated `config.h` at all (`librdkafka/src/rd.h`), so inspecting that
//! file proved nothing whatsoever about the build it was supposed to verify.
//!
//! `builtin.features` is not a proxy. It is librdkafka's own report, derived
//! from the very same `WITH_*` macros that gate the decompression switch in
//! `rdkafka_msgset_reader.c`, read out of the binary that is about to ship.
//! If it says `zstd`, the zstd code path is compiled in.

use rdkafka::bindings as rdsys;
use std::ffi::{CStr, CString};

/// Features this app is broken without, and which have each been silently
/// missing from a shipped build at least once.
///
/// `snappy`/`zstd`/`gzip`: a topic compressed with a missing codec fails every
/// poll with `Local: Not Implemented`. `lz4` is bundled unconditionally, so it
/// doubles as a control — if *it* is ever absent, the probe itself is wrong
/// rather than the build. `ssl` is what every managed Kafka (Confluent Cloud,
/// MSK) requires, and is equally silent when absent.
pub const REQUIRED_FEATURES: &[&str] = &["gzip", "snappy", "lz4", "zstd", "ssl"];

/// librdkafka's own comma-separated list of the optional features compiled
/// into this binary, e.g. `gzip,snappy,ssl,sasl,regex,lz4,...,zstd`.
///
/// Returns an empty string only if librdkafka itself refuses to report, which
/// would mean a far more basic problem than a missing codec.
pub fn builtin_features() -> String {
    let name = CString::new("builtin.features").expect("literal contains no interior NUL");

    // SAFETY: `conf` is owned here and destroyed on every return path.
    // `rd_kafka_conf_get` only reads through `conf`, and writes at most
    // `size` bytes into `dest` — and `size` is the length librdkafka itself
    // asked for on the first (null-`dest`) call, which is how the C API is
    // documented to be used.
    unsafe {
        let conf = rdsys::rd_kafka_conf_new();

        // A null destination asks only "how big a buffer do you need?"
        // (including the trailing NUL); nothing is copied.
        let mut size: usize = 0;
        if rdsys::rd_kafka_conf_get(conf, name.as_ptr(), std::ptr::null_mut(), &mut size)
            != rdsys::rd_kafka_conf_res_t::RD_KAFKA_CONF_OK
        {
            rdsys::rd_kafka_conf_destroy(conf);
            return String::new();
        }

        let mut buf = vec![0u8; size];
        let res = rdsys::rd_kafka_conf_get(conf, name.as_ptr(), buf.as_mut_ptr().cast(), &mut size);
        rdsys::rd_kafka_conf_destroy(conf);

        if res != rdsys::rd_kafka_conf_res_t::RD_KAFKA_CONF_OK {
            return String::new();
        }

        CStr::from_bytes_until_nul(&buf)
            .map(|value| value.to_string_lossy().into_owned())
            .unwrap_or_default()
    }
}

/// Which of [`REQUIRED_FEATURES`] this binary was built without — empty when
/// the build is sound.
pub fn missing_required_features() -> Vec<&'static str> {
    missing_from(&builtin_features())
}

/// Split out from the FFI so the "is this list complete?" logic can be tested
/// against a build that *is* missing a codec — which is the case that matters
/// and the one no machine here can produce for real.
fn missing_from(features: &str) -> Vec<&'static str> {
    REQUIRED_FEATURES
        .iter()
        .copied()
        .filter(|required| !features.split(',').any(|present| present == *required))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The check that would have caught this bug on the first release instead
    /// of the sixth. It runs on every platform, so the Windows CMake build is
    /// held to the same standard as the macOS/Linux `configure` build rather
    /// than being verified by a Windows-only shell script inspecting a file
    /// the Windows build does not read.
    #[test]
    fn librdkafka_has_every_required_feature_compiled_in() {
        // Printed (with `--nocapture` in CI) so every build leaves a record of
        // what it actually compiled in, not just a pass/fail.
        println!("librdkafka builtin.features = {}", builtin_features());

        let missing = missing_required_features();
        assert!(
            missing.is_empty(),
            "librdkafka was built without {missing:?}. Topics using a missing \
             codec fail every poll with \"Local: Not Implemented\". \
             librdkafka reported builtin.features = {:?}",
            builtin_features(),
        );
    }

    /// Guards the probe itself: an empty or unparseable report would make the
    /// test above vacuous in exactly the way the old `config.h` check was.
    #[test]
    fn builtin_features_is_actually_reported() {
        let features = builtin_features();
        assert!(
            features.contains(','),
            "builtin.features came back as {features:?}, which is not a feature \
             list — the probe is broken, not the build"
        );
    }

    /// The negative control. Windows is the platform that ships the broken
    /// build, so on every machine that can run this suite the real check above
    /// passes trivially; this pins down that it would have *failed* on the
    /// build we have been shipping, rather than passing for lack of teeth.
    #[test]
    fn a_build_missing_codecs_is_reported_as_missing() {
        // What the Windows CMake build actually produces: no `snappy` (its
        // `WITH_SNAPPY` CMake variable is never set by anything, so the
        // `WITHOUT_WIN32_CONFIG` branch compiles in `WITH_SNAPPY=0`) and no
        // `zstd` (its Cargo feature was missing until v0.28.0).
        let windows_build = "gzip,ssl,sasl,regex,lz4,sasl_plain,sasl_scram,plugins";
        assert_eq!(missing_from(windows_build), vec!["snappy", "zstd"]);
    }

    /// A sound build reports nothing missing — including the exact string a
    /// working Linux/macOS build produces today.
    #[test]
    fn a_sound_build_is_reported_as_complete() {
        let good_build =
            "gzip,snappy,ssl,sasl,regex,lz4,sasl_plain,sasl_scram,plugins,zstd,sasl_oauthbearer";
        assert!(missing_from(good_build).is_empty());
    }
}
