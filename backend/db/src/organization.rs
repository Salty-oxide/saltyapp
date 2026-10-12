//! Pins and folders for the sidebar tree: which topics are pinned, and which
//! named folder each topic or consumer group has been filed under.
//!
//! Everything here is *organisation*, not connection configuration, so none
//! of it is part of `PortableConnection` / the connections export.
//!
//! Names that no longer exist on the cluster (a deleted topic) are kept: the
//! rows are cheap, and the pin or folder is there again if the topic returns.

use chrono::Utc;
use error_stack::{Report, ResultExt};
use salty_core::AppError;
use salty_core::Result;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use sqlx::sqlite::SqlitePool;
use uuid::Uuid;

/// The longest folder name accepted, in characters.
pub const MAX_FOLDER_NAME_LEN: usize = 80;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Folder {
    pub id: String,
    pub kind: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Assignment {
    pub kind: String,
    pub resource_name: String,
    pub folder_id: String,
}

/// Everything the tree needs for one connection, in one round trip.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Organization {
    pub folders: Vec<Folder>,
    pub assignments: Vec<Assignment>,
    pub pinned_topics: Vec<String>,
}

fn validate_kind(kind: &str) -> Result<(), AppError> {
    if kind == "topic" || kind == "consumer" {
        Ok(())
    } else {
        Err(Report::new(AppError::Validation))
            .attach_with(|| format!("unknown folder kind '{kind}'"))
    }
}

fn clean_name(name: &str) -> Result<String, AppError> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(Report::new(AppError::Validation)).attach("a folder needs a name");
    }
    if trimmed.chars().count() > MAX_FOLDER_NAME_LEN {
        return Err(Report::new(AppError::Validation)).attach_with(|| {
            format!("a folder name can be at most {MAX_FOLDER_NAME_LEN} characters")
        });
    }
    Ok(trimmed.to_string())
}

async fn name_taken(
    pool: &SqlitePool,
    connection_id: &str,
    kind: &str,
    name: &str,
    except_id: Option<&str>,
) -> Result<bool, AppError> {
    let existing: Option<String> = sqlx::query_scalar(
        "SELECT id FROM resource_folders
         WHERE connection_id = ?1 AND kind = ?2 AND name = ?3 COLLATE NOCASE",
    )
    .bind(connection_id)
    .bind(kind)
    .bind(name)
    .fetch_optional(pool)
    .await
    .change_context(AppError::Db)
    .attach("failed to check folder name")?;

    Ok(match (existing, except_id) {
        (Some(id), Some(except)) => id != except,
        (Some(_), None) => true,
        (None, _) => false,
    })
}

pub async fn get(pool: &SqlitePool, connection_id: &str) -> Result<Organization, AppError> {
    let folders = sqlx::query_as::<_, Folder>(
        "SELECT id, kind, name FROM resource_folders
         WHERE connection_id = ?1 ORDER BY name COLLATE NOCASE ASC",
    )
    .bind(connection_id)
    .fetch_all(pool)
    .await
    .change_context(AppError::Db)
    .attach_with(|| format!("failed to list folders for {connection_id}"))?;

    let assignments = sqlx::query_as::<_, Assignment>(
        "SELECT kind, resource_name, folder_id FROM resource_folder_items
         WHERE connection_id = ?1",
    )
    .bind(connection_id)
    .fetch_all(pool)
    .await
    .change_context(AppError::Db)
    .attach_with(|| format!("failed to list folder items for {connection_id}"))?;

    let pinned_topics = sqlx::query_scalar::<_, String>(
        "SELECT topic_name FROM pinned_topics
         WHERE connection_id = ?1 ORDER BY pinned_at ASC, topic_name ASC",
    )
    .bind(connection_id)
    .fetch_all(pool)
    .await
    .change_context(AppError::Db)
    .attach_with(|| format!("failed to list pinned topics for {connection_id}"))?;

    Ok(Organization {
        folders,
        assignments,
        pinned_topics,
    })
}

pub async fn create_folder(
    pool: &SqlitePool,
    connection_id: &str,
    kind: &str,
    name: &str,
) -> Result<Folder, AppError> {
    validate_kind(kind)?;
    let name = clean_name(name)?;
    if name_taken(pool, connection_id, kind, &name, None).await? {
        return Err(Report::new(AppError::Validation))
            .attach_with(|| format!("a folder named '{name}' already exists"));
    }

    let id = Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO resource_folders (id, connection_id, kind, name, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
    )
    .bind(&id)
    .bind(connection_id)
    .bind(kind)
    .bind(&name)
    .bind(Utc::now().to_rfc3339())
    .execute(pool)
    .await
    .change_context(AppError::Db)
    .attach_with(|| format!("failed to create folder '{name}'"))?;

    Ok(Folder {
        id,
        kind: kind.to_string(),
        name,
    })
}

pub async fn rename_folder(pool: &SqlitePool, id: &str, name: &str) -> Result<(), AppError> {
    let name = clean_name(name)?;
    let row: Option<(String, String)> =
        sqlx::query_as("SELECT connection_id, kind FROM resource_folders WHERE id = ?1")
            .bind(id)
            .fetch_optional(pool)
            .await
            .change_context(AppError::Db)
            .attach_with(|| format!("failed to load folder {id}"))?;
    let Some((connection_id, kind)) = row else {
        return Err(Report::new(AppError::NotFound))
            .attach_with(|| format!("folder {id} not found"));
    };

    if name_taken(pool, &connection_id, &kind, &name, Some(id)).await? {
        return Err(Report::new(AppError::Validation))
            .attach_with(|| format!("a folder named '{name}' already exists"));
    }

    sqlx::query("UPDATE resource_folders SET name = ?2 WHERE id = ?1")
        .bind(id)
        .bind(&name)
        .execute(pool)
        .await
        .change_context(AppError::Db)
        .attach_with(|| format!("failed to rename folder {id}"))?;

    Ok(())
}

/// Deletes a folder; the items that were filed in it return to the list's
/// root (their assignment rows go, the topics and groups themselves are not
/// touched).
pub async fn delete_folder(pool: &SqlitePool, id: &str) -> Result<(), AppError> {
    let mut tx = pool
        .begin()
        .await
        .change_context(AppError::Db)
        .attach("failed to start transaction for folder deletion")?;

    sqlx::query("DELETE FROM resource_folder_items WHERE folder_id = ?1")
        .bind(id)
        .execute(&mut *tx)
        .await
        .change_context(AppError::Db)
        .attach_with(|| format!("failed to unfile the items of folder {id}"))?;

    let result = sqlx::query("DELETE FROM resource_folders WHERE id = ?1")
        .bind(id)
        .execute(&mut *tx)
        .await
        .change_context(AppError::Db)
        .attach_with(|| format!("failed to delete folder {id}"))?;

    if result.rows_affected() == 0 {
        return Err(Report::new(AppError::NotFound))
            .attach_with(|| format!("folder {id} not found"));
    }

    tx.commit()
        .await
        .change_context(AppError::Db)
        .attach("failed to commit folder deletion")?;
    Ok(())
}

/// Files `resource_name` in `folder_id`, or returns it to the root when
/// `folder_id` is `None`. Moving between folders replaces the old assignment.
/// The folder must belong to the same connection and kind.
pub async fn move_resource(
    pool: &SqlitePool,
    connection_id: &str,
    kind: &str,
    resource_name: &str,
    folder_id: Option<&str>,
) -> Result<(), AppError> {
    validate_kind(kind)?;

    let Some(folder_id) = folder_id else {
        sqlx::query(
            "DELETE FROM resource_folder_items
             WHERE connection_id = ?1 AND kind = ?2 AND resource_name = ?3",
        )
        .bind(connection_id)
        .bind(kind)
        .bind(resource_name)
        .execute(pool)
        .await
        .change_context(AppError::Db)
        .attach_with(|| format!("failed to unfile {resource_name}"))?;
        return Ok(());
    };

    let exists: Option<String> = sqlx::query_scalar(
        "SELECT id FROM resource_folders WHERE id = ?1 AND connection_id = ?2 AND kind = ?3",
    )
    .bind(folder_id)
    .bind(connection_id)
    .bind(kind)
    .fetch_optional(pool)
    .await
    .change_context(AppError::Db)
    .attach_with(|| format!("failed to load folder {folder_id}"))?;
    if exists.is_none() {
        return Err(Report::new(AppError::NotFound))
            .attach_with(|| format!("folder {folder_id} not found for this connection"));
    }

    sqlx::query(
        "INSERT INTO resource_folder_items (connection_id, kind, resource_name, folder_id)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT (connection_id, kind, resource_name)
         DO UPDATE SET folder_id = excluded.folder_id",
    )
    .bind(connection_id)
    .bind(kind)
    .bind(resource_name)
    .bind(folder_id)
    .execute(pool)
    .await
    .change_context(AppError::Db)
    .attach_with(|| format!("failed to file {resource_name}"))?;

    Ok(())
}

/// Idempotent in both directions: pinning a pinned topic, or unpinning one
/// that is not pinned, succeeds and changes nothing.
pub async fn set_pinned(
    pool: &SqlitePool,
    connection_id: &str,
    topic_name: &str,
    pinned: bool,
) -> Result<(), AppError> {
    let query = if pinned {
        sqlx::query(
            "INSERT OR IGNORE INTO pinned_topics (connection_id, topic_name, pinned_at)
             VALUES (?1, ?2, ?3)",
        )
        .bind(connection_id)
        .bind(topic_name)
        .bind(Utc::now().to_rfc3339())
    } else {
        sqlx::query("DELETE FROM pinned_topics WHERE connection_id = ?1 AND topic_name = ?2")
            .bind(connection_id)
            .bind(topic_name)
    };

    query
        .execute(pool)
        .await
        .change_context(AppError::Db)
        .attach_with(|| format!("failed to update pin for {topic_name}"))?;
    Ok(())
}

/// Called from `connection_delete`: the tables have no foreign key (see the
/// migration), so a deleted connection's pins and folders go explicitly.
pub async fn delete_all_for_connection(
    pool: &SqlitePool,
    connection_id: &str,
) -> Result<(), AppError> {
    for statement in [
        "DELETE FROM resource_folder_items WHERE connection_id = ?1",
        "DELETE FROM resource_folders WHERE connection_id = ?1",
        "DELETE FROM pinned_topics WHERE connection_id = ?1",
    ] {
        sqlx::query(statement)
            .bind(connection_id)
            .execute(pool)
            .await
            .change_context(AppError::Db)
            .attach_with(|| format!("failed to delete organization for {connection_id}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn test_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    fn is(error: &Report<AppError>, expected: AppError) -> bool {
        std::mem::discriminant(error.current_context()) == std::mem::discriminant(&expected)
    }

    #[tokio::test]
    async fn a_connection_with_nothing_set_is_empty() {
        let pool = test_pool().await;
        assert_eq!(get(&pool, "c1").await.unwrap(), Organization::default());
    }

    #[tokio::test]
    async fn creates_a_folder_and_lists_it() {
        let pool = test_pool().await;
        let folder = create_folder(&pool, "c1", "topic", "  Billing ")
            .await
            .unwrap();

        assert_eq!(folder.name, "Billing");
        assert_eq!(get(&pool, "c1").await.unwrap().folders, vec![folder]);
    }

    #[tokio::test]
    async fn folders_are_listed_alphabetically_ignoring_case() {
        let pool = test_pool().await;
        create_folder(&pool, "c1", "topic", "zeta").await.unwrap();
        create_folder(&pool, "c1", "topic", "Alpha").await.unwrap();
        create_folder(&pool, "c1", "topic", "beta").await.unwrap();

        let names: Vec<String> = get(&pool, "c1")
            .await
            .unwrap()
            .folders
            .into_iter()
            .map(|f| f.name)
            .collect();
        assert_eq!(names, ["Alpha", "beta", "zeta"]);
    }

    #[tokio::test]
    async fn rejects_blank_overlong_and_unknown_kind() {
        let pool = test_pool().await;
        let blank = create_folder(&pool, "c1", "topic", "   ")
            .await
            .unwrap_err();
        let long = create_folder(&pool, "c1", "topic", &"x".repeat(MAX_FOLDER_NAME_LEN + 1))
            .await
            .unwrap_err();
        let kind = create_folder(&pool, "c1", "broker", "ok")
            .await
            .unwrap_err();

        assert!(is(&blank, AppError::Validation));
        assert!(is(&long, AppError::Validation));
        assert!(is(&kind, AppError::Validation));
        assert!(
            create_folder(&pool, "c1", "topic", &"x".repeat(MAX_FOLDER_NAME_LEN))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn rejects_a_duplicate_name_ignoring_case_within_one_connection_and_kind() {
        let pool = test_pool().await;
        create_folder(&pool, "c1", "topic", "Billing")
            .await
            .unwrap();

        let duplicate = create_folder(&pool, "c1", "topic", "billing")
            .await
            .unwrap_err();
        assert!(is(&duplicate, AppError::Validation));

        // Same name is fine for the other kind and for another connection.
        assert!(
            create_folder(&pool, "c1", "consumer", "Billing")
                .await
                .is_ok()
        );
        assert!(create_folder(&pool, "c2", "topic", "Billing").await.is_ok());
    }

    #[tokio::test]
    async fn renames_a_folder_and_refuses_a_taken_name() {
        let pool = test_pool().await;
        let a = create_folder(&pool, "c1", "topic", "A").await.unwrap();
        create_folder(&pool, "c1", "topic", "B").await.unwrap();

        rename_folder(&pool, &a.id, "Renamed").await.unwrap();
        assert_eq!(get(&pool, "c1").await.unwrap().folders[0].name, "B");
        assert!(
            get(&pool, "c1")
                .await
                .unwrap()
                .folders
                .iter()
                .any(|f| f.name == "Renamed")
        );

        let taken = rename_folder(&pool, &a.id, "b").await.unwrap_err();
        assert!(is(&taken, AppError::Validation));
        // Renaming to its own name (only the case differs) is not a clash.
        assert!(rename_folder(&pool, &a.id, "RENAMED").await.is_ok());
        let missing = rename_folder(&pool, "nope", "X").await.unwrap_err();
        assert!(is(&missing, AppError::NotFound));
    }

    #[tokio::test]
    async fn files_a_topic_and_moves_it_between_folders() {
        let pool = test_pool().await;
        let a = create_folder(&pool, "c1", "topic", "A").await.unwrap();
        let b = create_folder(&pool, "c1", "topic", "B").await.unwrap();

        move_resource(&pool, "c1", "topic", "orders", Some(&a.id))
            .await
            .unwrap();
        move_resource(&pool, "c1", "topic", "orders", Some(&b.id))
            .await
            .unwrap();

        let assignments = get(&pool, "c1").await.unwrap().assignments;
        assert_eq!(assignments.len(), 1, "an item is in at most one folder");
        assert_eq!(assignments[0].folder_id, b.id);
        assert_eq!(assignments[0].resource_name, "orders");
    }

    #[tokio::test]
    async fn moving_to_none_returns_the_item_to_the_root() {
        let pool = test_pool().await;
        let a = create_folder(&pool, "c1", "topic", "A").await.unwrap();
        move_resource(&pool, "c1", "topic", "orders", Some(&a.id))
            .await
            .unwrap();

        move_resource(&pool, "c1", "topic", "orders", None)
            .await
            .unwrap();
        // Already at the root: still fine.
        move_resource(&pool, "c1", "topic", "orders", None)
            .await
            .unwrap();

        assert!(get(&pool, "c1").await.unwrap().assignments.is_empty());
    }

    #[tokio::test]
    async fn refuses_a_folder_of_another_kind_or_connection() {
        let pool = test_pool().await;
        let consumer_folder = create_folder(&pool, "c1", "consumer", "G").await.unwrap();
        let other = create_folder(&pool, "c2", "topic", "T").await.unwrap();

        let wrong_kind = move_resource(&pool, "c1", "topic", "orders", Some(&consumer_folder.id))
            .await
            .unwrap_err();
        let wrong_connection = move_resource(&pool, "c1", "topic", "orders", Some(&other.id))
            .await
            .unwrap_err();
        let missing = move_resource(&pool, "c1", "topic", "orders", Some("nope"))
            .await
            .unwrap_err();

        assert!(is(&wrong_kind, AppError::NotFound));
        assert!(is(&wrong_connection, AppError::NotFound));
        assert!(is(&missing, AppError::NotFound));
        assert!(get(&pool, "c1").await.unwrap().assignments.is_empty());
    }

    #[tokio::test]
    async fn a_topic_and_a_consumer_group_with_the_same_name_are_independent() {
        let pool = test_pool().await;
        let t = create_folder(&pool, "c1", "topic", "T").await.unwrap();
        let g = create_folder(&pool, "c1", "consumer", "G").await.unwrap();

        move_resource(&pool, "c1", "topic", "orders", Some(&t.id))
            .await
            .unwrap();
        move_resource(&pool, "c1", "consumer", "orders", Some(&g.id))
            .await
            .unwrap();

        assert_eq!(get(&pool, "c1").await.unwrap().assignments.len(), 2);
    }

    #[tokio::test]
    async fn deleting_a_folder_returns_its_items_to_the_root_and_leaves_others() {
        let pool = test_pool().await;
        let a = create_folder(&pool, "c1", "topic", "A").await.unwrap();
        let b = create_folder(&pool, "c1", "topic", "B").await.unwrap();
        move_resource(&pool, "c1", "topic", "orders", Some(&a.id))
            .await
            .unwrap();
        move_resource(&pool, "c1", "topic", "payments", Some(&b.id))
            .await
            .unwrap();

        delete_folder(&pool, &a.id).await.unwrap();

        let org = get(&pool, "c1").await.unwrap();
        assert_eq!(org.folders, vec![b.clone()]);
        assert_eq!(org.assignments.len(), 1);
        assert_eq!(org.assignments[0].resource_name, "payments");
        let missing = delete_folder(&pool, &a.id).await.unwrap_err();
        assert!(is(&missing, AppError::NotFound));
    }

    #[tokio::test]
    async fn pinning_is_idempotent_in_both_directions() {
        let pool = test_pool().await;
        set_pinned(&pool, "c1", "orders", true).await.unwrap();
        set_pinned(&pool, "c1", "orders", true).await.unwrap();
        assert_eq!(get(&pool, "c1").await.unwrap().pinned_topics, ["orders"]);

        set_pinned(&pool, "c1", "orders", false).await.unwrap();
        set_pinned(&pool, "c1", "orders", false).await.unwrap();
        assert!(get(&pool, "c1").await.unwrap().pinned_topics.is_empty());
    }

    #[tokio::test]
    async fn pins_are_per_connection() {
        let pool = test_pool().await;
        set_pinned(&pool, "c1", "orders", true).await.unwrap();

        assert!(get(&pool, "c2").await.unwrap().pinned_topics.is_empty());
    }

    #[tokio::test]
    async fn deleting_a_connection_removes_only_its_organization() {
        let pool = test_pool().await;
        let a = create_folder(&pool, "c1", "topic", "A").await.unwrap();
        move_resource(&pool, "c1", "topic", "orders", Some(&a.id))
            .await
            .unwrap();
        set_pinned(&pool, "c1", "orders", true).await.unwrap();
        let keep = create_folder(&pool, "c2", "topic", "Keep").await.unwrap();
        set_pinned(&pool, "c2", "audit", true).await.unwrap();

        delete_all_for_connection(&pool, "c1").await.unwrap();

        assert_eq!(get(&pool, "c1").await.unwrap(), Organization::default());
        let kept = get(&pool, "c2").await.unwrap();
        assert_eq!(kept.folders, vec![keep]);
        assert_eq!(kept.pinned_topics, ["audit"]);
    }
}
