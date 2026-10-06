/**
 * Which searchable view Ctrl+F belongs to.
 *
 * More than one can be on screen: a JSON viewer tab fills the middle pane
 * while the payload viewer fills the right one, and `App.tsx` gates the right
 * pane on a selected message rather than on which tab is active — so both are
 * mounted together routinely. Each used to bind its own `document` listener,
 * so one keypress opened a find bar in every one of them.
 *
 * This owns a single pair of listeners for the whole app and hands the
 * keypress to exactly one view.
 */

export interface FindTarget {
  id: number;
  /** The view's root element, or null once it has unmounted. */
  element: () => HTMLElement | null;
  open: () => void;
  close: () => void;
}

/**
 * The view a Ctrl+F should go to.
 *
 * Whichever one the reader was last inside, because that is the one they are
 * looking at — a rule that holds however the panes are arranged. With nothing
 * to go on (nothing clicked yet this session) the most recently registered
 * view wins: it is the one that just appeared.
 *
 * Pure, so the rule can be tested without mounting anything.
 */
export function chooseFindTarget(targets: readonly FindTarget[], lastInteraction: Node | null): FindTarget | null {
  const live = targets.filter((target) => target.element() !== null);
  if (live.length === 0) return null;

  if (lastInteraction) {
    // Last-registered first, so a view nested inside another still wins.
    for (let i = live.length - 1; i >= 0; i--) {
      const element = live[i].element();
      if (element && (element === lastInteraction || element.contains(lastInteraction))) return live[i];
    }
  }
  return live[live.length - 1];
}

const targets: FindTarget[] = [];
let lastInteraction: Node | null = null;
let nextId = 1;
let listening = false;

function onPointerOrFocus(event: Event) {
  const target = event.target;
  lastInteraction = target instanceof Node ? target : null;
}

function onKeyDown(event: KeyboardEvent) {
  if (!(event.ctrlKey || event.metaKey) || event.key.toLowerCase() !== "f") return;
  const chosen = chooseFindTarget(targets, lastInteraction);
  if (!chosen) return;
  event.preventDefault();
  // Closing the rest is what guarantees a single bar even if two were somehow
  // opened — by a focus change between keypresses, say.
  for (const target of targets) {
    if (target.id !== chosen.id) target.close();
  }
  chosen.open();
}

function startListening() {
  if (listening) return;
  document.addEventListener("keydown", onKeyDown);
  // `pointerdown` rather than `click`: it fires before focus moves, and it
  // catches a drag-scroll inside a pane, which is interaction too.
  document.addEventListener("pointerdown", onPointerOrFocus, true);
  document.addEventListener("focusin", onPointerOrFocus, true);
  listening = true;
}

function stopListening() {
  if (!listening) return;
  document.removeEventListener("keydown", onKeyDown);
  document.removeEventListener("pointerdown", onPointerOrFocus, true);
  document.removeEventListener("focusin", onPointerOrFocus, true);
  listening = false;
  lastInteraction = null;
}

/**
 * Registers a searchable view for the lifetime of the returned disposer.
 *
 * The listeners exist only while at least one view is registered, so a screen
 * with nothing searchable on it carries no global key handler.
 */
export function registerFindTarget(target: Omit<FindTarget, "id">): () => void {
  const entry: FindTarget = { id: nextId++, ...target };
  targets.push(entry);
  startListening();

  return () => {
    const index = targets.findIndex((candidate) => candidate.id === entry.id);
    if (index >= 0) targets.splice(index, 1);
    if (targets.length === 0) stopListening();
  };
}

/** Test seam: forgets every registration and detaches the listeners. */
export function resetFindRegistry(): void {
  targets.length = 0;
  stopListening();
}
