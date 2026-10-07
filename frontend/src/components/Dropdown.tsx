import { KeyboardEvent as ReactKeyboardEvent, useEffect, useRef, useState } from "react";

export interface DropdownOption {
  id: string;
  label: string;
}

export interface DropdownProps {
  /** The field label shown above the dropdown. */
  label: string;
  /** aria-label on the listbox itself. */
  ariaLabel: string;
  options: DropdownOption[];
  /** id whose label shows on the toggle button — differs from appliedId while previewing. */
  displayedId: string;
  /** id that gets the checkmark and is treated as the keyboard-nav starting point. */
  appliedId: string;
  onCommit: (id: string) => void;
  /** Called with the hovered option's id, or null on mouse-leave/close — omit to disable hover preview. */
  onPreview?: (id: string | null) => void;
  /** Keeps the label for assistive tech but doesn't show it — for dropdowns whose toggle already says what they are. */
  hideLabel?: boolean;
}

/**
 * The app's shared custom dropdown — a button + listbox (not a native
 * `<select>`), used everywhere a dropdown appears (Settings' Theme/Font
 * style/Font size, the New Connection modal's Type/SASL mechanism/Kafka
 * version) so they all look and behave identically: checkmark on the
 * applied option, optional live hover-preview, keyboard nav, and — unlike a
 * native `<select>`, whose native popup can open upward when there's no
 * room below — the listbox is always positioned directly under the toggle
 * button via `position: absolute; top: 100%`, so it always opens downward.
 */
export function Dropdown({ label, ariaLabel, options, displayedId, appliedId, onCommit, onPreview, hideLabel }: DropdownProps) {
  const [open, setOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const toggleButtonRef = useRef<HTMLButtonElement>(null);
  const optionRefs = useRef<Array<HTMLLIElement | null>>([]);

  const displayedOption = options.find((o) => o.id === displayedId) ?? options[0];

  function close() {
    setOpen(false);
    onPreview?.(null);
  }

  function commit(id: string) {
    onCommit(id);
    close();
    toggleButtonRef.current?.focus();
  }

  // Close the listbox on outside click and on Escape (from anywhere while it's open).
  useEffect(() => {
    if (!open) return;

    function handlePointerDown(e: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        close();
      }
    }
    function handleKeyDown(e: globalThis.KeyboardEvent) {
      if (e.key === "Escape") {
        close();
        toggleButtonRef.current?.focus();
      }
    }

    document.addEventListener("mousedown", handlePointerDown);
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
      document.removeEventListener("keydown", handleKeyDown);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  // Move focus onto the currently-applied option (or the first option) when the menu opens.
  useEffect(() => {
    if (!open) return;
    const activeIndex = options.findIndex((o) => o.id === appliedId);
    const index = activeIndex >= 0 ? activeIndex : 0;
    optionRefs.current[index]?.focus();
    // Only run when the menu transitions open; re-focusing on every appliedId
    // change would fight the user's own arrow-key navigation.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  function handleOptionKeyDown(e: ReactKeyboardEvent<HTMLLIElement>, index: number, id: string) {
    switch (e.key) {
      case "ArrowDown": {
        e.preventDefault();
        const next = Math.min(index + 1, options.length - 1);
        optionRefs.current[next]?.focus();
        break;
      }
      case "ArrowUp": {
        e.preventDefault();
        const prev = Math.max(index - 1, 0);
        optionRefs.current[prev]?.focus();
        break;
      }
      case "Enter":
      case " ":
        e.preventDefault();
        commit(id);
        break;
      default:
        break;
    }
  }

  return (
    <div className="dropdown-field">
      <span className={hideLabel ? "visually-hidden" : undefined}>{label}</span>
      <div className="dropdown" ref={dropdownRef}>
        <button
          type="button"
          ref={toggleButtonRef}
          aria-haspopup="listbox"
          aria-expanded={open}
          onClick={() => setOpen((v) => !v)}
        >
          {displayedOption?.label} ▾
        </button>
        {open && (
          <ul role="listbox" aria-label={ariaLabel} onMouseLeave={() => onPreview?.(null)}>
            {options.map((option, index) => (
              <li
                key={option.id}
                role="option"
                tabIndex={-1}
                ref={(el) => {
                  optionRefs.current[index] = el;
                }}
                aria-selected={option.id === appliedId}
                onMouseEnter={() => onPreview?.(option.id)}
                onClick={() => commit(option.id)}
                onKeyDown={(e) => handleOptionKeyDown(e, index, option.id)}
              >
                {option.id === appliedId ? `✓ ${option.label}` : option.label}
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
