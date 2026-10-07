//! Carrying a user's data across the rename from Offset Explorer Oxide to
//! Salty.
//!
//! The app's data directory is derived from the Tauri bundle identifier, so
//! changing the identifier points the app at an empty folder: every saved
//! connection, tab and schema is still on disk, just under the old name.
//! Without the copy below, the first launch after upgrading looks exactly
//! like a fresh install.
//!
//! This lives in `salty-core` rather than in `src-tauri` so it can be tested.
//! `src-tauri` needs GTK and pkg-config to compile, which is not available
//! everywhere this workspace is developed, and a one-shot migration that only
//! ever runs on a user's machine is the last thing that should go untested.

use std::path::{Path, PathBuf};

/// The bundle identifier the app shipped under before the rename.
///
/// Frozen on purpose: it names a directory that already exists on users'
/// machines, so it can never track the current identifier.
pub const LEGACY_IDENTIFIER: &str = "dev.kafkaoxide.app";

/// The database filename inside that directory.
pub const LEGACY_DB_FILE: &str = "kafkaoxide.sqlite";

/// The database filename this version reads and writes.
pub const DB_FILE: &str = "salty.sqlite";

/// SQLite's sidecar files. The database runs in WAL mode (see
/// `salty_db::init_pool`), which means committed transactions can still be
/// sitting in `-wal` rather than in the main file — copying the database on
/// its own would silently lose the user's most recent work.
const SIDECAR_SUFFIXES: [&str; 2] = ["-wal", "-shm"];

/// What a migration moved, for the caller to log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdoptedData {
    /// The directory the data came from.
    pub from: PathBuf,
    /// The files copied, in the order they were written.
    pub files: Vec<String>,
}

/// Copies the previous version's database into `data_dir`, if there is one to
/// copy and nothing here already.
///
/// Returns `Ok(None)` when there is nothing to do, which is the common case:
/// every launch after the first, and every genuinely new install.
///
/// The old directory is left untouched. A copy rather than a move means a
/// user who downgrades still finds their data where the old version looks for
/// it, and it means a half-finished migration can never destroy the original.
pub fn adopt_legacy_app_data(data_dir: &Path) -> std::io::Result<Option<AdoptedData>> {
    // Never overwrite. If this app has a database here, it owns it — whatever
    // is in the old directory is older, and importing it would throw away
    // whatever the user has done since.
    if data_dir.join(DB_FILE).exists() {
        return Ok(None);
    }

    let Some(legacy_dir) = legacy_dir_for(data_dir) else {
        return Ok(None);
    };
    let legacy_db = legacy_dir.join(LEGACY_DB_FILE);
    if !legacy_db.is_file() {
        return Ok(None);
    }

    std::fs::create_dir_all(data_dir)?;

    let mut files = Vec::new();

    // Sidecars first, database last. The database's presence is what marks
    // the migration as done (the guard above), so writing it last means an
    // interrupted copy leaves nothing to find and the next launch simply
    // tries again — rather than leaving a database whose WAL never arrived.
    for suffix in SIDECAR_SUFFIXES {
        let from = with_suffix(&legacy_db, suffix);
        if from.is_file() {
            let to = with_suffix(&data_dir.join(DB_FILE), suffix);
            std::fs::copy(&from, &to)?;
            files.push(file_name_of(&to));
        }
    }

    // Via a temporary name, because `fs::copy` is not atomic: a crash partway
    // through would otherwise leave a truncated `salty.sqlite` that the guard
    // above would accept forever as this app's own database.
    let destination = data_dir.join(DB_FILE);
    let partial = data_dir.join(format!("{DB_FILE}.partial"));
    std::fs::copy(&legacy_db, &partial)?;
    std::fs::rename(&partial, &destination)?;
    files.push(file_name_of(&destination));

    Ok(Some(AdoptedData {
        from: legacy_dir,
        files,
    }))
}

/// The old identifier's directory, as a sibling of the current one.
///
/// Derived by swapping the last path component rather than by recomputing the
/// platform's data directory, which is Tauri's job and differs on all three
/// platforms. The one assumption — that the app data directory is named after
/// the bundle identifier — is what Tauri guarantees, and if it ever stopped
/// being true the check below would simply find nothing to migrate.
fn legacy_dir_for(data_dir: &Path) -> Option<PathBuf> {
    if data_dir.file_name()?.to_str()? == LEGACY_IDENTIFIER {
        return None;
    }
    Some(data_dir.parent()?.join(LEGACY_IDENTIFIER))
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}

fn file_name_of(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A unique scratch directory. `tempfile` is not a dependency of this
    /// crate and one test module does not justify adding it.
    fn scratch(label: &str) -> PathBuf {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("salty-migration-{label}-{unique}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Lays out `<root>/dev.salty.app` and `<root>/dev.kafkaoxide.app`, the
    /// way the two identifiers sit on a real machine.
    fn identifiers(label: &str) -> (PathBuf, PathBuf, PathBuf) {
        let root = scratch(label);
        let current = root.join("dev.salty.app");
        let legacy = root.join(LEGACY_IDENTIFIER);
        (root, current, legacy)
    }

    fn write(path: &Path, contents: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, contents).unwrap();
    }

    #[test]
    fn adopts_the_database_from_the_old_identifier() {
        let (root, current, legacy) = identifiers("adopts");
        write(&legacy.join(LEGACY_DB_FILE), "the user's connections");

        let adopted = adopt_legacy_app_data(&current)
            .unwrap()
            .expect("should have migrated");

        assert_eq!(adopted.from, legacy);
        assert_eq!(adopted.files, vec![DB_FILE.to_string()]);
        assert_eq!(
            std::fs::read_to_string(current.join(DB_FILE)).unwrap(),
            "the user's connections"
        );
        std::fs::remove_dir_all(root).ok();
    }

    /// WAL mode keeps committed transactions outside the main file until a
    /// checkpoint, so a migration that took only `salty.sqlite` would lose
    /// whatever the user did in their last session.
    #[test]
    fn brings_the_write_ahead_log_along_with_the_database() {
        let (root, current, legacy) = identifiers("wal");
        write(&legacy.join(LEGACY_DB_FILE), "main");
        write(
            &legacy.join(format!("{LEGACY_DB_FILE}-wal")),
            "recent commits",
        );
        write(
            &legacy.join(format!("{LEGACY_DB_FILE}-shm")),
            "shared memory",
        );

        let adopted = adopt_legacy_app_data(&current)
            .unwrap()
            .expect("should have migrated");

        assert_eq!(
            adopted.files,
            vec![
                format!("{DB_FILE}-wal"),
                format!("{DB_FILE}-shm"),
                DB_FILE.to_string()
            ],
            "the database must be written last, so an interrupted copy is retried"
        );
        assert_eq!(
            std::fs::read_to_string(current.join(format!("{DB_FILE}-wal"))).unwrap(),
            "recent commits"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn does_nothing_when_this_version_already_has_a_database() {
        let (root, current, legacy) = identifiers("existing");
        write(&legacy.join(LEGACY_DB_FILE), "old");
        write(&current.join(DB_FILE), "current");

        assert_eq!(adopt_legacy_app_data(&current).unwrap(), None);
        assert_eq!(
            std::fs::read_to_string(current.join(DB_FILE)).unwrap(),
            "current",
            "an existing database must never be overwritten"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn does_nothing_on_a_fresh_install() {
        let (root, current, _legacy) = identifiers("fresh");

        assert_eq!(adopt_legacy_app_data(&current).unwrap(), None);
        assert!(!current.join(DB_FILE).exists());
        std::fs::remove_dir_all(root).ok();
    }

    /// The old directory can exist without a database in it — the app was
    /// installed and never opened, or the file was deleted by hand.
    #[test]
    fn does_nothing_when_the_old_directory_holds_no_database() {
        let (root, current, legacy) = identifiers("empty-legacy");
        std::fs::create_dir_all(&legacy).unwrap();

        assert_eq!(adopt_legacy_app_data(&current).unwrap(), None);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn leaves_the_old_directory_untouched() {
        let (root, current, legacy) = identifiers("nondestructive");
        write(&legacy.join(LEGACY_DB_FILE), "the user's connections");

        adopt_legacy_app_data(&current).unwrap().unwrap();

        assert_eq!(
            std::fs::read_to_string(legacy.join(LEGACY_DB_FILE)).unwrap(),
            "the user's connections"
        );
        std::fs::remove_dir_all(root).ok();
    }

    /// Guards against the identifier ever being reverted: pointed at the old
    /// directory itself, the migration would otherwise try to copy a file
    /// onto itself.
    #[test]
    fn does_nothing_when_the_current_directory_is_the_old_one() {
        let (root, _current, legacy) = identifiers("same");
        write(&legacy.join(LEGACY_DB_FILE), "old");

        assert_eq!(adopt_legacy_app_data(&legacy).unwrap(), None);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn leaves_no_partial_file_behind_on_success() {
        let (root, current, legacy) = identifiers("partial");
        write(&legacy.join(LEGACY_DB_FILE), "data");

        adopt_legacy_app_data(&current).unwrap().unwrap();

        assert!(!current.join(format!("{DB_FILE}.partial")).exists());
        std::fs::remove_dir_all(root).ok();
    }
}
