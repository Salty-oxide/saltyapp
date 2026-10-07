use error_stack::ResultExt;
use salty_core::AppError;
use salty_core::Result;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use sqlx::sqlite::SqlitePool;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Tab {
    pub id: String,
    pub name: String,
    pub position: i64,
}

pub async fn list(pool: &SqlitePool) -> Result<Vec<Tab>, AppError> {
    sqlx::query_as::<_, Tab>("SELECT id, name, position FROM tabs ORDER BY position ASC")
        .fetch_all(pool)
        .await
        .change_context(AppError::Db)
        .attach("failed to list tabs")
}

pub async fn create(pool: &SqlitePool, name: &str) -> Result<Tab, AppError> {
    let id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();

    let mut tx = pool
        .begin()
        .await
        .change_context(AppError::Db)
        .attach("failed to start transaction for tab creation")?;

    let next_position: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(position), -1) + 1 FROM tabs")
        .fetch_one(&mut *tx)
        .await
        .change_context(AppError::Db)
        .attach("failed to compute next tab position")?;

    sqlx::query("INSERT INTO tabs (id, name, position, created_at) VALUES (?1, ?2, ?3, ?4)")
        .bind(&id)
        .bind(name)
        .bind(next_position)
        .bind(&now)
        .execute(&mut *tx)
        .await
        .change_context(AppError::Db)
        .attach("failed to insert tab")?;

    tx.commit()
        .await
        .change_context(AppError::Db)
        .attach("failed to commit tab creation transaction")?;

    Ok(Tab {
        id,
        name: name.to_string(),
        position: next_position,
    })
}

pub async fn rename(pool: &SqlitePool, id: &str, name: &str) -> Result<(), AppError> {
    let result = sqlx::query("UPDATE tabs SET name = ?1 WHERE id = ?2")
        .bind(name)
        .bind(id)
        .execute(pool)
        .await
        .change_context(AppError::Db)
        .attach_with(|| format!("failed to rename tab {id}"))?;

    if result.rows_affected() == 0 {
        return Err(error_stack::Report::new(AppError::NotFound))
            .attach_with(|| format!("tab {id} not found"));
    }

    Ok(())
}

/// Persists a new tab order — `ids` is the full, front-to-back list of tab
/// ids after a drag-to-reorder. Each id's position becomes its index.
pub async fn reorder(pool: &SqlitePool, ids: &[String]) -> Result<(), AppError> {
    let mut tx = pool
        .begin()
        .await
        .change_context(AppError::Db)
        .attach("failed to start transaction for tab reorder")?;

    for (position, id) in ids.iter().enumerate() {
        sqlx::query("UPDATE tabs SET position = ?1 WHERE id = ?2")
            .bind(position as i64)
            .bind(id)
            .execute(&mut *tx)
            .await
            .change_context(AppError::Db)
            .attach_with(|| format!("failed to reposition tab {id}"))?;
    }

    tx.commit()
        .await
        .change_context(AppError::Db)
        .attach("failed to commit tab reorder transaction")?;

    Ok(())
}

pub async fn delete(pool: &SqlitePool, id: &str) -> Result<(), AppError> {
    let result = sqlx::query("DELETE FROM tabs WHERE id = ?1")
        .bind(id)
        .execute(pool)
        .await
        .change_context(AppError::Db)
        .attach_with(|| format!("failed to delete tab {id}"))?;

    if result.rows_affected() == 0 {
        return Err(error_stack::Report::new(AppError::NotFound))
            .attach_with(|| format!("tab {id} not found"));
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

    #[tokio::test]
    async fn creates_tabs_with_increasing_position() {
        let pool = test_pool().await;
        let first = create(&pool, "First").await.unwrap();
        let second = create(&pool, "Second").await.unwrap();

        assert_eq!(first.position, 0);
        assert_eq!(second.position, 1);
    }

    #[tokio::test]
    async fn lists_tabs_ordered_by_position() {
        let pool = test_pool().await;
        create(&pool, "First").await.unwrap();
        create(&pool, "Second").await.unwrap();

        let tabs = list(&pool).await.unwrap();
        assert_eq!(
            tabs.iter().map(|t| t.name.clone()).collect::<Vec<_>>(),
            vec!["First", "Second"]
        );
    }

    #[tokio::test]
    async fn renames_a_tab() {
        let pool = test_pool().await;
        let tab = create(&pool, "First").await.unwrap();

        rename(&pool, &tab.id, "Renamed").await.unwrap();

        let tabs = list(&pool).await.unwrap();
        assert_eq!(tabs[0].name, "Renamed");
    }

    #[tokio::test]
    async fn rename_of_missing_tab_returns_not_found() {
        let pool = test_pool().await;
        let result = rename(&pool, "missing-id", "X").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn reorders_tabs_by_the_given_id_order() {
        let pool = test_pool().await;
        let first = create(&pool, "First").await.unwrap();
        let second = create(&pool, "Second").await.unwrap();
        let third = create(&pool, "Third").await.unwrap();

        reorder(
            &pool,
            &[third.id.clone(), first.id.clone(), second.id.clone()],
        )
        .await
        .unwrap();

        let tabs = list(&pool).await.unwrap();
        assert_eq!(
            tabs.iter().map(|t| t.name.clone()).collect::<Vec<_>>(),
            vec!["Third", "First", "Second"]
        );
    }

    #[tokio::test]
    async fn deletes_a_tab() {
        let pool = test_pool().await;
        let tab = create(&pool, "First").await.unwrap();

        delete(&pool, &tab.id).await.unwrap();

        let tabs = list(&pool).await.unwrap();
        assert!(tabs.is_empty());
    }

    #[tokio::test]
    async fn delete_of_missing_tab_returns_not_found() {
        let pool = test_pool().await;
        let result = delete(&pool, "missing-id").await;
        assert!(result.is_err());
    }
}
