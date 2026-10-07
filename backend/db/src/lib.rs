pub mod connections;
pub mod tabs;
pub mod topic_schemas;

use error_stack::ResultExt;
use salty_core::AppError;
use salty_core::Result;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous,
};
use std::str::FromStr;

/// Opens the app's SQLite database, applying the two settings that decide
/// what a write costs, and runs any outstanding migrations.
///
/// Both pragmas have to be set explicitly. sqlx does *not* default to either:
/// it deliberately leaves `journal_mode` alone ("Don't set `journal_mode`
/// unless the user requested it" — sqlx-sqlite's own comment, because
/// switching modes needs an exclusive lock), and `synchronous` is SQLite's
/// own `FULL` default. Left as they were, this database ran in rollback-
/// journal mode at `synchronous=FULL`:
///
/// * **`journal_mode = WAL`** — in rollback mode a writer blocks every
///   reader, and this app reads the database on *every* broker command
///   (`connection_for_request`) plus the per-connection status poll. So
///   renaming a tab could stall an unrelated cluster request behind it, up
///   to sqlx's 5s `busy_timeout`. Under WAL, readers and one writer proceed
///   together.
/// * **`synchronous = NORMAL`** — `FULL` fsyncs on every single commit.
///   Every tab rename, every drag-to-reorder, every connection save paid
///   that, which on Windows is milliseconds of stall for a few dozen bytes.
///   `NORMAL` under WAL is the setting SQLite itself recommends: a commit is
///   still durable across an application crash, and only a power loss or OS
///   crash can lose the most recent transactions. For a desktop Kafka
///   browser's local UI state, that is the right trade; the data of record
///   lives on the cluster.
///
/// WAL is a permanent property of the database file, so an existing
/// rollback-mode database is converted on the first open after this change —
/// once, at startup, while this is the only connection.
pub async fn init_pool(database_url: &str) -> Result<SqlitePool, AppError> {
    let options = SqliteConnectOptions::from_str(database_url)
        .change_context(AppError::Db)
        .attach_with(|| format!("failed to parse sqlite url {database_url}"))?
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal);

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .change_context(AppError::Db)
        .attach_with(|| format!("failed to connect to sqlite at {database_url}"))?;

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .change_context(AppError::Db)
        .attach("failed to run migrations")?;

    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `init_pool` is the app's only entry point into storage: it opens the
    /// database *and* brings its schema up to date, and every command that
    /// follows assumes both happened. The migrations are the part worth
    /// pinning — a pool that connects but skips them fails later, at the
    /// first query, in whichever feature happened to run first.
    #[tokio::test]
    async fn init_pool_opens_a_database_and_runs_every_migration() {
        let pool = init_pool("sqlite::memory:")
            .await
            .expect("init_pool failed");

        // The tables the migrations create, one per feature area that stores
        // anything — queried rather than merely listed, so a table that
        // exists with the wrong shape fails here too.
        for table in ["connections", "tabs", "topic_schemas"] {
            // sqlx 0.9 requires dynamic SQL to be marked as audited.
            // `table` comes from the hard-coded list above, never from input.
            sqlx::query(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(&pool)
                .await
                .unwrap_or_else(|err| panic!("migrations left `{table}` unusable: {err}"));
        }
    }

    /// The two pragmas `init_pool` exists to set. Asserted against a real
    /// file (an in-memory database reports `journal_mode = memory` whatever
    /// is asked for, so it cannot show this), and read back from a *pooled*
    /// connection rather than the one that opened the file — the settings are
    /// per connection, so what matters is that every connection the app
    /// borrows has them.
    #[tokio::test]
    async fn init_pool_opens_the_database_in_wal_mode_with_synchronous_normal() {
        let dir = std::env::temp_dir().join(format!("salty-db-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("failed to create temp dir");
        let url = format!("sqlite://{}?mode=rwc", dir.join("app.db").display());

        let pool = init_pool(&url).await.expect("init_pool failed");

        let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&pool)
            .await
            .expect("failed to read journal_mode");
        assert_eq!(
            journal_mode.to_lowercase(),
            "wal",
            "a writer would otherwise block every reader"
        );

        // 1 is NORMAL; 2 (the default this replaces) is FULL.
        let synchronous: i64 = sqlx::query_scalar("PRAGMA synchronous")
            .fetch_one(&pool)
            .await
            .expect("failed to read synchronous");
        assert_eq!(synchronous, 1, "every commit would otherwise fsync");

        pool.close().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// WAL is a permanent property of the file, so the app has to be able to
    /// open a database that an older build created in rollback-journal mode —
    /// converting it, rather than failing or silently staying in rollback.
    #[tokio::test]
    async fn init_pool_converts_an_existing_rollback_mode_database_to_wal() {
        let dir = std::env::temp_dir().join(format!("salty-db-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("failed to create temp dir");
        let path = dir.join("app.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());

        // What every install before this change looks like on disk.
        let legacy = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::from_str(&url)
                    .expect("failed to parse url")
                    .journal_mode(SqliteJournalMode::Delete),
            )
            .await
            .expect("failed to create the legacy database");
        sqlx::query("CREATE TABLE probe (id INTEGER PRIMARY KEY)")
            .execute(&legacy)
            .await
            .expect("failed to write to the legacy database");
        legacy.close().await;

        let pool = init_pool(&url)
            .await
            .expect("init_pool failed on a rollback-mode database");

        let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&pool)
            .await
            .expect("failed to read journal_mode");
        assert_eq!(journal_mode.to_lowercase(), "wal");
        // The pre-existing contents survived the conversion.
        sqlx::query("SELECT COUNT(*) FROM probe")
            .fetch_one(&pool)
            .await
            .expect("the legacy table did not survive the conversion to WAL");

        pool.close().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Running against an already-migrated database is the ordinary case —
    /// every launch after the first — so it must be a no-op rather than an
    /// error about migrations that have already been applied.
    #[tokio::test]
    async fn init_pool_is_idempotent_against_an_existing_database() {
        let dir = std::env::temp_dir().join(format!("salty-db-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("failed to create temp dir");
        let url = format!("sqlite://{}?mode=rwc", dir.join("app.db").display());

        init_pool(&url).await.expect("first init_pool failed");
        init_pool(&url)
            .await
            .expect("second init_pool failed on an already-migrated database");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A path that cannot be opened has to surface as an `AppError::Db`
    /// rather than a panic — this is what the app reports when its data
    /// directory is missing or unwritable.
    #[tokio::test]
    async fn init_pool_reports_a_db_error_for_an_unopenable_path() {
        let error = init_pool("sqlite:///nonexistent-directory/salty/app.db")
            .await
            .expect_err("opening a database under a missing directory must fail");

        assert!(
            matches!(error.current_context(), AppError::Db),
            "got {error:?}"
        );
    }
}
