import { useEffect, useState } from "react";
import { closeAppWindow, onCloseRequested } from "../../lib/appWindow";

/**
 * Asks "Close application?" whenever the user closes the window, and only
 * closes it once they confirm. The window's own close is held back by
 * `onCloseRequested`; `closeAppWindow` is the way through.
 */
export function CloseConfirmation() {
  const [asking, setAsking] = useState(false);

  useEffect(() => {
    const unlisten = onCloseRequested(() => setAsking(true));
    return () => {
      void unlisten.then((stop) => stop());
    };
  }, []);

  useEffect(() => {
    if (!asking) return;
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") setAsking(false);
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [asking]);

  if (!asking) return null;

  return (
    <div className="connection-modal-overlay" onClick={() => setAsking(false)}>
      <div
        className="connection-modal connection-modal--confirm"
        role="dialog"
        aria-modal="true"
        aria-label="Close application"
        onClick={(e) => e.stopPropagation()}
      >
        <header className="connection-modal-header">
          <h2>Close application?</h2>
        </header>
        <p>Are you sure you want to close Salty?</p>
        <footer className="connection-modal-footer">
          <button type="button" autoFocus onClick={() => void closeAppWindow()}>
            Close application
          </button>
          <button type="button" onClick={() => setAsking(false)}>
            Cancel
          </button>
        </footer>
      </div>
    </div>
  );
}
