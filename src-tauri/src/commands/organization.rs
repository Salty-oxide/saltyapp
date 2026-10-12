use crate::commands::connections::CommandError;
use crate::state::AppState;
use salty_db::organization::{Folder, Organization};
use tauri::State;

#[tauri::command]
pub async fn organization_get(
    state: State<'_, AppState>,
    connection_id: String,
) -> Result<Organization, CommandError> {
    Ok(salty_db::organization::get(&state.pool, &connection_id).await?)
}

#[tauri::command]
pub async fn organization_folder_create(
    state: State<'_, AppState>,
    connection_id: String,
    kind: String,
    name: String,
) -> Result<Folder, CommandError> {
    Ok(salty_db::organization::create_folder(&state.pool, &connection_id, &kind, &name).await?)
}

#[tauri::command]
pub async fn organization_folder_rename(
    state: State<'_, AppState>,
    id: String,
    name: String,
) -> Result<(), CommandError> {
    Ok(salty_db::organization::rename_folder(&state.pool, &id, &name).await?)
}

#[tauri::command]
pub async fn organization_folder_delete(
    state: State<'_, AppState>,
    id: String,
) -> Result<(), CommandError> {
    Ok(salty_db::organization::delete_folder(&state.pool, &id).await?)
}

#[tauri::command]
pub async fn organization_move_resource(
    state: State<'_, AppState>,
    connection_id: String,
    kind: String,
    resource_name: String,
    folder_id: Option<String>,
) -> Result<(), CommandError> {
    Ok(salty_db::organization::move_resource(
        &state.pool,
        &connection_id,
        &kind,
        &resource_name,
        folder_id.as_deref(),
    )
    .await?)
}

#[tauri::command]
pub async fn organization_set_topic_pinned(
    state: State<'_, AppState>,
    connection_id: String,
    topic_name: String,
    pinned: bool,
) -> Result<(), CommandError> {
    Ok(
        salty_db::organization::set_pinned(&state.pool, &connection_id, &topic_name, pinned)
            .await?,
    )
}
