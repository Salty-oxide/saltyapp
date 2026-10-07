//! A compile-check for the startup glue in `src-tauri/src/main.rs`.
//!
//! `src-tauri` needs GTK and pkg-config and cannot be built in every
//! environment this workspace is developed in, so the block that calls
//! [`salty_core::adopt_legacy_app_data`] is mirrored here verbatim — same
//! match arms, same field accesses, same format strings — against a stub
//! logger. It cannot catch a wrong `AppHandle`, but it does catch the things
//! a rename actually breaks: a field that moved, a variant that changed
//! shape, a `Result` that is no longer a `Result`.
//!
//! Keep this in step with `main.rs` by hand. The sibling
//! `tauri_error_shape.rs` exists for the same reason.

use salty_core::{DB_FILE, adopt_legacy_app_data};

/// Stands in for `logging::emit_log`, whose real signature is
/// `(&AppHandle, &str, impl Into<String>)`.
fn emit_log(sink: &mut Vec<(String, String)>, level: &str, message: impl Into<String>) {
    sink.push((level.to_string(), message.into()));
}

#[test]
fn the_startup_block_still_typechecks() {
    let mut log: Vec<(String, String)> = Vec::new();

    let data_dir = std::env::temp_dir().join("salty-startup-shape-test");
    std::fs::create_dir_all(&data_dir).unwrap();

    // --- mirrored from main.rs ---
    let adopted = adopt_legacy_app_data(&data_dir);

    let db_path = data_dir.join(DB_FILE);
    let database_url = format!("sqlite://{}?mode=rwc", db_path.display());

    match adopted {
        Ok(Some(adopted)) => emit_log(
            &mut log,
            "info",
            format!(
                "Adopted data from the previous version ({}): {}",
                adopted.from.display(),
                adopted.files.join(", "),
            ),
        ),
        Ok(None) => {}
        Err(error) => emit_log(
            &mut log,
            "warn",
            format!(
                "Could not copy data from the previous version: {error}. Saved \
                 connections from before the rename to Salty will not appear.",
            ),
        ),
    }
    // --- end mirror ---

    assert!(
        database_url.contains(DB_FILE),
        "the app must open the current database file"
    );
    std::fs::remove_dir_all(&data_dir).ok();
}

/// The database filename and the legacy one must differ, or the migration
/// would be copying a file onto itself.
#[test]
fn the_current_and_legacy_database_names_are_distinct() {
    assert_ne!(DB_FILE, salty_core::LEGACY_DB_FILE);
    assert_eq!(salty_core::LEGACY_IDENTIFIER, "dev.kafkaoxide.app");
}
