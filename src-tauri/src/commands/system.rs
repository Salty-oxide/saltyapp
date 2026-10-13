use crate::commands::connections::CommandError;
use crate::state::AppState;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use error_stack::ResultExt;
use salty_core::AppError;
use tauri::State;

/// Asks the OS to trim this process's working set — the number Windows'
/// Task Manager shows as "Memory". Clearing the frontend's cached rows/
/// selection (a JS-side action) and dropping Rust's own per-fetch native
/// allocations (Kafka clients, decoded payload buffers, JSON IPC framing)
/// both free that memory correctly, but neither the JS engine's heap nor
/// Rust's default allocator eagerly hands freed pages back to the OS —
/// both keep them reserved for the next allocation, which is normal
/// runtime behavior, not a leak (this is unrelated to the earlier,
/// genuinely-unbounded Windows growth issue, already root-caused and fixed
/// separately — that was continuous native-client creation on a timer;
/// this is memory retained-for-reuse after real, one-time work). On
/// Windows specifically, `SetProcessWorkingSetSize` with (-1, -1) is the
/// documented way to force an immediate trim of the visible working set
/// regardless — it doesn't change what the app can still allocate later,
/// it just evicts currently-unused pages right now so the number in Task
/// Manager actually reflects that. No-op everywhere else — macOS/Linux
/// don't report memory this way, and the growth this addresses was
/// reported as Windows-only to begin with.
#[tauri::command]
pub fn trim_process_memory() {
    #[cfg(windows)]
    {
        use windows::Win32::System::Threading::{GetCurrentProcess, SetProcessWorkingSetSize};
        unsafe {
            let _ = SetProcessWorkingSetSize(GetCurrentProcess(), usize::MAX, usize::MAX);
        }
    }
}

/// Backs the bottom panel's "Clear memory": drops every payload the backend
/// has cached (`salty_cache`), for every connection and tab, and returns how
/// many there were.
///
/// The cache is app-wide, so this is not scoped to the tab whose button was
/// pressed — what another tab loses is only speed: its next row open goes to
/// the broker, as it did before the cache existed. Called before
/// `trim_process_memory`, so there are freed pages to hand back.
#[tauri::command]
pub fn clear_payload_cache(state: State<'_, AppState>) -> u64 {
    state.kafka.clear_payload_cache()
}

/// Backs the Settings panel's "Payload cache size": sets the backend cache's
/// byte cap, `0` turning it off, and returns whether that was a change.
///
/// The frontend states its setting at every start, so an unchanged value must
/// do nothing — a change builds a new, empty cache (moka cannot resize in
/// place), which is why this is a separate call from `clear_payload_cache`
/// rather than a side effect of every fetch.
#[tauri::command]
pub fn set_payload_cache_limit(state: State<'_, AppState>, max_bytes: u64) -> bool {
    state.kafka.set_payload_cache_limit(max_bytes)
}

/// Backs the payload viewer's Save button. `path` is resolved by the
/// frontend beforehand via the native save dialog — this command only
/// decodes and writes the bytes, exactly like `connections_export`.
///
/// Takes base64 rather than a `Vec<u8>`: Tauri's IPC is JSON, so a byte
/// vector crosses it as a decimal array — roughly four characters per byte
/// against base64's four per three, i.e. three times the transfer for the
/// multi-megabyte payloads this button exists for. It is also already the
/// shape the payload is held in on the frontend, so nothing has to be
/// re-encoded to call this.
///
/// It stays a *bytes* write rather than a string one even though Save is now
/// the only caller: the frontend base64-encodes the rendered text itself
/// (`textToBase64`), so a UTF-8 string would only move that encoding across
/// the IPC boundary at three times the size for nothing.
#[tauri::command]
pub fn payload_save(path: String, contents_base64: String) -> Result<(), CommandError> {
    let bytes = BASE64
        .decode(&contents_base64)
        .change_context(AppError::Validation)
        .attach("contents aren't valid base64")?;
    std::fs::write(&path, bytes)
        .change_context(AppError::Validation)
        .attach("failed to write the file")?;
    Ok(())
}
