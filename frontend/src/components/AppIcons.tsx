/**
 * The app's chrome glyphs — the ones that label a *place* or an *action*
 * rather than a payload format (those live in `FormatIcons.tsx`, one per
 * `ValueMode`).
 *
 * Drawn here rather than pulled from an icon set for the reason every other
 * icon in this app is: it ships no icon dependency, and these are a handful
 * of path commands each.
 *
 * All of them are stroked in `currentColor` at 1.2–1.5, which is what lets
 * the same glyph sit on a muted tab, an accent-coloured active tab and a
 * filled primary button without a second copy in a second colour.
 */

const ICON_PROPS = {
  width: 14,
  height: 14,
  viewBox: "0 0 16 16",
  fill: "none",
  "aria-hidden": true,
} as const;

const STROKE = {
  stroke: "currentColor",
  strokeWidth: 1.3,
  strokeLinecap: "round",
  strokeLinejoin: "round",
} as const;

/**
 * A file tree — the workspace tabs' glyph, and the one Zed puts on its
 * project panel: a full-width root row, a trunk dropping from it with an
 * elbow, and the indented children it reaches.
 *
 * A plain folder was the obvious alternative and says the wrong thing: a
 * workspace tab holds a *tree* of clusters, topics and partitions, and the
 * sidebar it opens is drawn exactly like this glyph.
 */
export function FileTreeIcon() {
  return (
    <svg {...ICON_PROPS}>
      <path d="M2.4 3.3h11.2" {...STROKE} strokeWidth={1.5} />
      <path d="M4.6 3.3v8.5a1.4 1.4 0 0 0 1.4 1.4h1.1" {...STROKE} />
      <path d="M4.6 8.2h2.5" {...STROKE} />
      <path d="M9 8.2h4.6" {...STROKE} strokeWidth={1.5} />
      <path d="M9 13.2h4.6" {...STROKE} strokeWidth={1.5} />
    </svg>
  );
}

/**
 * A cog — the Settings tab.
 *
 * The teeth start *on* the ring rather than out at the edge of the box. Long
 * thin rays with a gap between them and a small circle is the anatomy of a
 * sun, not a gear, and that is exactly what the first attempt read as at
 * 14px.
 */
export function GearIcon() {
  return (
    <svg {...ICON_PROPS}>
      <circle cx="8" cy="8" r="3.3" {...STROKE} />
      <path
        d="M8 4.7V2.5M8 11.3v2.2M11.3 8h2.2M4.7 8H2.5M10.35 5.65l1.55-1.55M5.65 10.35 4.1 11.9M10.35 10.35l1.55 1.55M5.65 5.65 4.1 4.1"
        {...STROKE}
        strokeWidth={1.7}
        strokeLinecap="butt"
      />
    </svg>
  );
}

/** Lines of output in a panel — the status strip's logs toggle. */
export function LogsIcon() {
  return (
    <svg {...ICON_PROPS}>
      <rect x="2" y="3" width="12" height="10" rx="2" {...STROKE} />
      <path d="M4.6 6.4h4.2M4.6 8.6h6.8M4.6 10.8h3.2" {...STROKE} strokeWidth={1.2} />
    </svg>
  );
}

/** A plus — Add Cluster. */
export function PlusIcon() {
  return (
    <svg {...ICON_PROPS}>
      <path d="M8 3.3v9.4M3.3 8h9.4" {...STROKE} strokeWidth={1.5} />
    </svg>
  );
}

/** An arrow leaving a tray — Export All. */
export function ExportIcon() {
  return (
    <svg {...ICON_PROPS}>
      <path d="M8 10.2V2.6M5.3 5.3 8 2.6l2.7 2.7" {...STROKE} />
      <path d="M2.9 9.9v2.3a1.2 1.2 0 0 0 1.2 1.2h7.8a1.2 1.2 0 0 0 1.2-1.2V9.9" {...STROKE} />
    </svg>
  );
}

/** An arrow entering a tray — Import. */
export function ImportIcon() {
  return (
    <svg {...ICON_PROPS}>
      <path d="M8 2.6v7.6M5.3 7.5 8 10.2l2.7-2.7" {...STROKE} />
      <path d="M2.9 9.9v2.3a1.2 1.2 0 0 0 1.2 1.2h7.8a1.2 1.2 0 0 0 1.2-1.2V9.9" {...STROKE} />
    </svg>
  );
}

export function CopyIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <rect x="5.5" y="5.5" width="9" height="9" rx="1.5" stroke="currentColor" />
      <path d="M3.5 10.5h-1a1 1 0 0 1-1-1v-7a1 1 0 0 1 1-1h7a1 1 0 0 1 1 1v1" stroke="currentColor" />
    </svg>
  );
}

export function CheckIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <path d="M3 8.5l3 3 7-7" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

/** A floppy disk — Save, which writes what is on screen in the chosen format. */
export function SaveIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <path d="M2.5 3.5a1 1 0 0 1 1-1h7.6l2.4 2.4v7.6a1 1 0 0 1-1 1h-9a1 1 0 0 1-1-1v-9Z" stroke="currentColor" />
      <path d="M5 2.5v4h6v-4M5 13.5v-4h6v4" stroke="currentColor" strokeLinejoin="round" />
    </svg>
  );
}


/** A closed folder — a named group in the Topics / Consumers lists. */
export function FolderIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <path
        d="M1.5 4.5a1 1 0 0 1 1-1h3.2l1.5 1.7h6.3a1 1 0 0 1 1 1v5.8a1 1 0 0 1-1 1h-11a1 1 0 0 1-1-1V4.5Z"
        stroke="currentColor"
        strokeWidth="1.3"
        strokeLinejoin="round"
      />
    </svg>
  );
}

/** A map pin — marks a topic pinned to the top of its list. */
export function PinIcon() {
  return (
    <svg width="12" height="12" viewBox="0 0 16 16" fill="none" aria-hidden="true">
      <path
        d="M9.8 1.8l4.4 4.4-1.4.5-2.2 2.2.2 3-1.2 1.2-2.6-2.6-3.9 3.9-.7-.7 3.9-3.9-2.6-2.6 1.2-1.2 3-.2 2.2-2.2.5-1.4Z"
        stroke="currentColor"
        strokeWidth="1.2"
        strokeLinejoin="round"
      />
    </svg>
  );
}
