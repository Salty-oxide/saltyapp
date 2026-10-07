use chrono::Utc;
use error_stack::ResultExt;
use salty_core::AppError;
use salty_core::Result;
use sqlx::sqlite::SqlitePool;

pub async fn get(
    pool: &SqlitePool,
    connection_id: &str,
    topic: &str,
    format: &str,
) -> Result<Option<String>, AppError> {
    sqlx::query_scalar::<_, String>(
        "SELECT schema_text FROM topic_schemas WHERE connection_id = ?1 AND topic = ?2 AND format = ?3",
    )
    .bind(connection_id)
    .bind(topic)
    .bind(format)
    .fetch_optional(pool)
    .await
    .change_context(AppError::Db)
    .attach_with(|| format!("failed to load schema for {connection_id}/{topic}/{format}"))
}

pub async fn set(
    pool: &SqlitePool,
    connection_id: &str,
    topic: &str,
    format: &str,
    schema_text: &str,
) -> Result<(), AppError> {
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO topic_schemas (connection_id, topic, format, schema_text, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?5)
         ON CONFLICT (connection_id, topic, format)
         DO UPDATE SET schema_text = excluded.schema_text, updated_at = excluded.updated_at",
    )
    .bind(connection_id)
    .bind(topic)
    .bind(format)
    .bind(schema_text)
    .bind(&now)
    .execute(pool)
    .await
    .change_context(AppError::Db)
    .attach_with(|| format!("failed to save schema for {connection_id}/{topic}/{format}"))?;

    Ok(())
}

/// Backs the Schema tab's Clear button — idempotent, no error when nothing
/// was set.
pub async fn delete(
    pool: &SqlitePool,
    connection_id: &str,
    topic: &str,
    format: &str,
) -> Result<(), AppError> {
    sqlx::query(
        "DELETE FROM topic_schemas WHERE connection_id = ?1 AND topic = ?2 AND format = ?3",
    )
    .bind(connection_id)
    .bind(topic)
    .bind(format)
    .execute(pool)
    .await
    .change_context(AppError::Db)
    .attach_with(|| format!("failed to delete schema for {connection_id}/{topic}/{format}"))?;

    Ok(())
}

/// Will be called when a connection is deleted (Task 5 wires this into
/// `connection_delete`) — explicit cleanup since `topic_schemas` has no
/// enforced foreign key (see the migration file's comment).
pub async fn delete_all_for_connection(
    pool: &SqlitePool,
    connection_id: &str,
) -> Result<(), AppError> {
    sqlx::query("DELETE FROM topic_schemas WHERE connection_id = ?1")
        .bind(connection_id)
        .execute(pool)
        .await
        .change_context(AppError::Db)
        .attach_with(|| format!("failed to delete topic schemas for connection {connection_id}"))?;

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
    async fn returns_none_when_no_schema_is_set() {
        let pool = test_pool().await;
        let result = get(&pool, "conn-1", "orders", "avro").await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn round_trips_a_schema() {
        let pool = test_pool().await;
        set(&pool, "conn-1", "orders", "avro", "{\"type\":\"string\"}")
            .await
            .unwrap();

        let result = get(&pool, "conn-1", "orders", "avro").await.unwrap();

        assert_eq!(result, Some("{\"type\":\"string\"}".to_string()));
    }

    #[tokio::test]
    async fn set_upserts_an_existing_schema() {
        let pool = test_pool().await;
        set(&pool, "conn-1", "orders", "avro", "first")
            .await
            .unwrap();
        set(&pool, "conn-1", "orders", "avro", "second")
            .await
            .unwrap();

        let result = get(&pool, "conn-1", "orders", "avro").await.unwrap();

        assert_eq!(result, Some("second".to_string()));
    }

    #[tokio::test]
    async fn keeps_schemas_independent_per_topic_and_format() {
        let pool = test_pool().await;
        set(&pool, "conn-1", "orders", "avro", "orders-schema")
            .await
            .unwrap();
        set(&pool, "conn-1", "payments", "avro", "payments-schema")
            .await
            .unwrap();
        set(&pool, "conn-1", "orders", "protobuf", "orders-proto-schema")
            .await
            .unwrap();

        assert_eq!(
            get(&pool, "conn-1", "orders", "avro").await.unwrap(),
            Some("orders-schema".to_string())
        );
        assert_eq!(
            get(&pool, "conn-1", "payments", "avro").await.unwrap(),
            Some("payments-schema".to_string())
        );
        assert_eq!(
            get(&pool, "conn-1", "orders", "protobuf").await.unwrap(),
            Some("orders-proto-schema".to_string())
        );
    }

    #[tokio::test]
    async fn deletes_a_schema() {
        let pool = test_pool().await;
        set(&pool, "conn-1", "orders", "avro", "schema")
            .await
            .unwrap();

        delete(&pool, "conn-1", "orders", "avro").await.unwrap();

        assert_eq!(get(&pool, "conn-1", "orders", "avro").await.unwrap(), None);
    }

    #[tokio::test]
    async fn delete_of_a_missing_schema_is_a_no_op() {
        let pool = test_pool().await;
        delete(&pool, "conn-1", "orders", "avro").await.unwrap();
    }

    #[tokio::test]
    async fn delete_all_for_connection_only_removes_that_connections_schemas() {
        let pool = test_pool().await;
        set(&pool, "conn-1", "orders", "avro", "schema-1")
            .await
            .unwrap();
        set(&pool, "conn-2", "orders", "avro", "schema-2")
            .await
            .unwrap();

        delete_all_for_connection(&pool, "conn-1").await.unwrap();

        assert_eq!(get(&pool, "conn-1", "orders", "avro").await.unwrap(), None);
        assert_eq!(
            get(&pool, "conn-2", "orders", "avro").await.unwrap(),
            Some("schema-2".to_string())
        );
    }
}
