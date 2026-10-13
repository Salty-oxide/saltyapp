import type { UnlistenFn } from "@tauri-apps/api/event";
import { getCurrentWindow } from "@tauri-apps/api/window";

/**
 * Closes the app's only window — on desktop, this quits the app.
 *
 * Uses `destroy()`, not `close()`: `close()` raises a close request, which
 * `onCloseRequested` below turns into the "Close application?" prompt — so a
 * caller that has already asked the user would ask a second time. Thin wrapper
 * so callers can mock it in tests.
 */
export function closeAppWindow(): Promise<void> {
  return getCurrentWindow().destroy();
}

/**
 * Subscribes to the user asking to close the window (the title-bar button,
 * Alt+F4, Cmd+W). The close is always held back — the handler decides
 * whether to go through with it via `closeAppWindow`. Thin wrapper so callers
 * can mock it in tests.
 */
export function onCloseRequested(handler: () => void): Promise<UnlistenFn> {
  return getCurrentWindow().onCloseRequested((event) => {
    event.preventDefault();
    handler();
  });
}

/**
 * Subscribes to the OS window gaining/losing focus (minimized, alt-tabbed
 * away, etc.) — the real signal, unlike the DOM's `visibilitychange`/
 * `window.blur`, which WebView2 (Tauri's Windows webview) doesn't reliably
 * fire when the window loses *OS* focus while still technically "visible".
 * Thin wrapper so callers can mock it in tests.
 */
export function onWindowFocusChanged(handler: (focused: boolean) => void): Promise<UnlistenFn> {
  return getCurrentWindow().onFocusChanged((event) => handler(event.payload));
}
