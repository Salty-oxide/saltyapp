/// <reference types="node" />
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

// Read off disk, not imported: see the note in typeScale.test.ts.
const css = readFileSync(resolve(process.cwd(), "src/styles/global.css"), "utf8");

/**
 * Guards "the Kafka version dropdown is filled with the accent colour".
 *
 * `.connection-modal-input-row button` is a descendant selector, so it also
 * matched the shared `Dropdown`'s toggle (`.dropdown-field > .dropdown >
 * button`) whenever a dropdown shared a row with an action button. It ties
 * `.dropdown > button` on specificity and comes later, so the accent fill won.
 * The row's action buttons (Ping) are direct children.
 */
describe("global.css connection-modal-input-row buttons", () => {
  it("styles only the row's direct-child buttons, never a nested dropdown toggle", () => {
    expect(css.length).toBeGreaterThan(1000);

    expect(css).not.toMatch(/\.connection-modal-input-row\s+button/);
    expect(css).toMatch(/\.connection-modal-input-row\s*>\s*button\s*\{/);
    expect(css).toMatch(/\.connection-modal-input-row\s*>\s*button:disabled\s*\{/);
  });
});

/** The version dropdown only ever holds a short string like "3.9" or "4.1". */
describe("global.css Kafka version field", () => {
  it("caps the version dropdown's width instead of letting it fill the row", () => {
    expect(css).toMatch(/\.connection-modal-version-field\s*\{[^}]*max-width:\s*\d+(px|ch|rem)/);
  });

  it("keeps the field label on one line despite the narrow width", () => {
    expect(css).toMatch(/\.connection-modal-version-field\s+\.dropdown-field\s*>\s*span\s*\{[^}]*white-space:\s*nowrap/);
  });
});
