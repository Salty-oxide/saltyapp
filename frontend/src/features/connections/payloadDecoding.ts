import { TopicMessage } from "../../lib/tauri";

/** Decodes a base64 string (as sent by the backend) into raw bytes. */
export function base64ToBytes(base64: string): Uint8Array {
  const binary = atob(base64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

/** Decodes bytes as UTF-8 text. */
export function bytesToText(bytes: Uint8Array): string {
  return new TextDecoder("utf-8").decode(bytes);
}

/**
 * Decodes a base64 message key or header value (`TopicMessage.keyBase64`,
 * `MessageHeader.valueBase64`) into text for display. These fields are
 * arbitrary Kafka byte strings, not guaranteed UTF-8, so this is
 * display-only and lossy (invalid sequences become "�") — the base64 field
 * itself is what preserves the real bytes exactly.
 */
export function base64ToDisplayText(base64: string | null): string | null {
  if (base64 === null) return null;
  return bytesToText(base64ToBytes(base64));
}

/** Parses a JSON string into a value for JsonTreeView, or returns undefined if it isn't valid JSON. */
export function tryParseJson(text: string): unknown {
  if (text.trim().length === 0) return undefined;
  try {
    return JSON.parse(text);
  } catch {
    return undefined;
  }
}

export interface XmlElementNode {
  tag: string;
  attributes: [string, string][];
  children: XmlElementNode[];
  /** Trimmed text content — only set for a leaf element (no child elements). */
  text: string | null;
}

function elementToNode(el: Element): XmlElementNode {
  const attributes: [string, string][] = Array.from(el.attributes).map((attr) => [attr.name, attr.value]);
  const childElements = Array.from(el.children);
  const children = childElements.map(elementToNode);
  const text = childElements.length === 0 ? (el.textContent ?? "").trim() || null : null;
  return { tag: el.tagName, attributes, children, text };
}

/** Parses an XML string into a tree for XmlTreeView, or returns undefined if it isn't well-formed XML. */
export function tryParseXml(text: string): XmlElementNode | undefined {
  if (text.trim().length === 0) return undefined;
  let doc: Document;
  try {
    doc = new DOMParser().parseFromString(text, "application/xml");
  } catch {
    return undefined;
  }
  if (doc.querySelector("parsererror") || !doc.documentElement) return undefined;
  return elementToNode(doc.documentElement);
}

/** Pretty-prints an XmlElementNode back to indented XML text — used by XmlTreeView's Copy button. */
export function formatXmlNode(node: XmlElementNode, depth = 0): string {
  const indent = "  ".repeat(depth);
  const attrs = node.attributes.map(([key, value]) => ` ${key}="${value}"`).join("");
  if (node.children.length === 0) {
    return node.text
      ? `${indent}<${node.tag}${attrs}>${node.text}</${node.tag}>`
      : `${indent}<${node.tag}${attrs}/>`;
  }
  const children = node.children.map((child) => formatXmlNode(child, depth + 1)).join("\n");
  return `${indent}<${node.tag}${attrs}>\n${children}\n${indent}</${node.tag}>`;
}

export interface ConfluentAvroInfo {
  schemaId: number;
}

/**
 * Detects the Confluent Schema Registry wire format: a leading magic byte
 * (0x00) followed by a 4-byte big-endian schema id, then the Avro-encoded
 * body. Full Avro decoding requires fetching the schema from the registry
 * and isn't implemented — this only surfaces the schema id so the UI can
 * label the payload rather than silently mis-rendering it as text/JSON.
 */
export function detectConfluentAvro(bytes: Uint8Array): ConfluentAvroInfo | null {
  if (bytes.length < 5 || bytes[0] !== 0x00) return null;
  const schemaId = (bytes[1] << 24) | (bytes[2] << 16) | (bytes[3] << 8) | bytes[4];
  return { schemaId: schemaId >>> 0 };
}

/**
 * How much of a payload the Data tab's Value column decodes per row.
 *
 * A grid cell shows a single line, so decoding a whole message to render one
 * is wasted work — and on a topic of multi-megabyte JSON it is the work that
 * makes the grid unusable. Measured over 300 rows of 2 MB payloads: decoding
 * every row in full costs ~1.35s, against ~1ms for a bounded preview.
 *
 * 4 KB is far more than any cell can display, and generous enough that the
 * identifying fields at the top of a typical JSON document are in it.
 */
export const VALUE_PREVIEW_BYTES = 4096;

/**
 * Decodes the first [`VALUE_PREVIEW_BYTES`] of a payload for display in a
 * grid cell. Returns "" when the payload wasn't loaded.
 *
 * base64 packs 3 bytes into every 4 characters, so cutting on a multiple of
 * 4 leaves a prefix that still decodes on its own. A multi-byte character
 * straddling the cut becomes a replacement character — acceptable in a
 * one-line preview, and never seen by the payload viewer, which decodes the
 * real bytes.
 */
export function decodeValuePreview(payloadBase64: string | null): string {
  if (!payloadBase64) return "";

  const prefix = payloadBase64.slice(0, Math.ceil(VALUE_PREVIEW_BYTES / 3) * 4);
  const bytes = base64ToBytes(prefix);
  const avro = detectConfluentAvro(bytes);
  if (avro) return `Avro (schema id: ${avro.schemaId})`;
  return bytesToText(bytes);
}

/**
 * How many bytes a base64 string decodes to, without decoding it: every 4
 * characters carry 3 bytes, less one byte per '=' of padding.
 */
export function base64DecodedLength(base64: string): number {
  if (base64.length === 0) return 0;
  const padding = base64.endsWith("==") ? 2 : base64.endsWith("=") ? 1 : 0;
  return (base64.length / 4) * 3 - padding;
}

/**
 * The payload bytes a set of loaded rows is holding in the webview.
 *
 * Measures what is **retained**, not what was read off the broker. Those are
 * very different numbers and only the first one can crash anything: a fetch
 * reads whole messages but keeps at most `maxPayloadPreviewBytes` of each
 * (see `inlinePayloadBytesFor`), so browsing a topic of 4 MB records can read
 * gigabytes while holding tens of megabytes. Budgeting against bytes read
 * therefore both over-counts a large-message fetch and under-counts a
 * long-lived tab that has accumulated many small whole payloads — which is
 * the case that actually fills memory.
 *
 * Counts decoded payload bytes. The base64 string each row really holds is
 * about 4/3 of that in characters, so this is a consistent underestimate of
 * the true JS footprint by a constant factor — fine for a budget the user
 * sets in payload terms, and noted here so nobody reads it as an exact
 * heap figure.
 */
export function retainedPayloadBytes(messages: Pick<TopicMessage, "payloadBase64">[]): number {
  return messages.reduce((total, m) => total + (m.payloadBase64 === null ? 0 : base64DecodedLength(m.payloadBase64)), 0);
}

/**
 * What one cached row costs beyond its own bytes: the object itself, its
 * numeric fields, and the per-field bookkeeping the engine keeps for them.
 *
 * A round figure, not a measurement — there is no way to ask the engine what
 * an object weighs, and the number exists so that a tab holding a hundred
 * thousand metadata-only rows doesn't report 0.00 MB.
 */
export const ROW_OVERHEAD_BYTES = 128;

/**
 * What a set of cached rows is holding, payloads included — the bottom
 * panel's "Tab memory".
 *
 * Deliberately arithmetic rather than `JSON.stringify(messages).length`,
 * which this replaced, for two reasons:
 *
 * 1. **It agrees with the ceiling.** The payload part of this is exactly what
 *    [`retainedPayloadBytes`] charges against Max Total Fetch Size, so
 *    "Tab memory" and "Payloads (all tabs)" describe the same rows in the
 *    same units. Serialized, a payload counted as its base64 *characters* —
 *    about 4/3 of the bytes they carry — so the two figures disagreed by a
 *    third on any tab fetched with "Fetch message payload" on, and agreed
 *    only when there were no payloads to disagree about.
 * 2. **Measuring memory shouldn't allocate a copy of it.** Serializing built
 *    a string as large as everything being measured, on every render of the
 *    bottom panel — which re-renders on every store change, including each
 *    of the ten row flushes a second a streaming fetch produces. On the
 *    large-payload fetches this figure exists to warn about, the measurement
 *    was itself a bigger allocation than most of what it was measuring.
 */
export function retainedRowBytes(messages: TopicMessage[]): number {
  return messages.reduce(
    (total, message) =>
      total +
      ROW_OVERHEAD_BYTES +
      (message.payloadBase64 === null ? 0 : base64DecodedLength(message.payloadBase64)) +
      (message.keyBase64 === null ? 0 : base64DecodedLength(message.keyBase64)) +
      message.headers.reduce(
        (headerTotal, header) =>
          headerTotal + header.key.length + (header.valueBase64 === null ? 0 : base64DecodedLength(header.valueBase64)),
        0,
      ),
    0,
  );
}

/**
 * A payload's size, in the unit that makes it readable — bytes under a
 * kilobyte, then KB, then MB.
 *
 * Scaled rather than always in bytes (which is what `formatBytes` in
 * `publishMessages.ts` does, deliberately, because a *draft* being published
 * is usually a few hundred bytes and the exact count is the interesting
 * number). A fetched message is as likely to be four megabytes, and
 * "4,194,304 bytes" in a toolbar is a number to be counted rather than read.
 *
 * One decimal for KB and two for MB, so the figure stays the same width as
 * it grows and a 1.05 MB payload doesn't render as "1 MB" — the difference
 * between one and four megabytes is exactly what this is for.
 */
export function formatPayloadSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

/**
 * Whether `payloadBase64` holds only part of the message it came from.
 *
 * The Data tab's fetch asks the backend for a bounded slice of each payload,
 * so a row's `payloadBase64` is usually a preview — enough for the grid's
 * one-line cell, never enough to display or decode as the message. Anything
 * that shows the payload itself has to check this and fetch the real bytes
 * for that single message first.
 */
export function isPayloadTruncated(payloadBase64: string | null, payloadSizeBytes: number | null): boolean {
  if (payloadBase64 === null || payloadSizeBytes === null) return false;
  return base64DecodedLength(payloadBase64) < payloadSizeBytes;
}

/**
 * The most of any one payload a grid fetch carries back.
 *
 * Deliberately not [`VALUE_PREVIEW_BYTES`], which the two used to share.
 * That constant is sized for what a *grid cell* decodes — 4 KB, ample for
 * one line — and using it as the transport bound too meant every row of any
 * ordinary JSON or Avro topic arrived truncated. The
 * payload viewer then had nothing whole to show, so opening a message went
 * back to the broker for its real bytes: a fresh consumer, a TLS and SASL
 * handshake, metadata and watermarks, on every single click. Carrying more
 * per row costs memory once, at fetch time, and makes opening anything that
 * fits instant again.
 *
 * 256 KB covers ordinary records many times over while still cutting the
 * multi-megabyte payloads this bound exists to cut.
 */
export const MAX_INLINE_PAYLOAD_BYTES = 256 * 1024;

/**
 * Total payload bytes one fetch's rows may hold in the webview at once.
 *
 * This is what keeps [`MAX_INLINE_PAYLOAD_BYTES`] from being the memory bug
 * again. A bound applied per row says nothing about what a fetch costs in
 * aggregate — 1,000 rows of multi-megabyte records is what killed the
 * webview — so the per-row bound is priced out of this budget rather than
 * fixed: the more rows a fetch asks for, the less of each payload it
 * carries.
 */
export const PAYLOAD_RETENTION_BUDGET_BYTES = 64 * 1024 * 1024;

/**
 * How much of each payload a fetch of `estimatedRows` rows may carry.
 *
 * Floored at [`VALUE_PREVIEW_BYTES`] and capped at
 * [`MAX_INLINE_PAYLOAD_BYTES`], so the bytes one fetch retains never exceed
 * `max(PAYLOAD_RETENTION_BUDGET_BYTES, rows * VALUE_PREVIEW_BYTES)` — and
 * that second term is exactly what a flat `VALUE_PREVIEW_BYTES` bound
 * retained. A fetch can therefore never hold more than the code this
 * replaced, whatever the user types into the filter form.
 *
 * `null` rows means the count isn't knowable from the filter alone (no
 * overall budget and no explicit partition list, so it is the topic's
 * partition count that decides), which takes the conservative floor.
 */
export function inlinePayloadBytesFor(estimatedRows: number | null): number {
  if (estimatedRows === null || estimatedRows <= 0) return VALUE_PREVIEW_BYTES;
  const perRow = Math.floor(PAYLOAD_RETENTION_BUDGET_BYTES / estimatedRows);
  return Math.min(MAX_INLINE_PAYLOAD_BYTES, Math.max(VALUE_PREVIEW_BYTES, perRow));
}

/**
 * Renders bytes as a classic hex dump — offset, 16 bytes in hex, then those
 * same bytes as printable ASCII — one line per 16 bytes.
 *
 * A flat run of hex pairs would be shorter to produce and useless to read: a
 * payload viewed as hex is being viewed because something about the *bytes*
 * matters (a magic byte, a framing header, an encoding that isn't UTF-8), and
 * answering "which byte is that?" needs the offset column. The ASCII column
 * is what makes the structured parts of a mostly-binary payload legible
 * without switching back to Raw.
 *
 * Bounded by the caller: this allocates roughly four characters per input
 * byte, so it is only ever handed a slice (see `TEXT_PREVIEW_CHARS`).
 */
export function bytesToHexDump(bytes: Uint8Array): string {
  const lines: string[] = [];
  for (let start = 0; start < bytes.length; start += 16) {
    const row = bytes.subarray(start, start + 16);
    const hex: string[] = [];
    let ascii = "";
    for (let i = 0; i < 16; i++) {
      if (i < row.length) {
        hex.push(row[i].toString(16).padStart(2, "0"));
        // Printable ASCII only. Anything else becomes "." rather than the
        // character it would decode to: control codes would move the cursor
        // and wreck the column alignment this view exists for.
        ascii += row[i] >= 0x20 && row[i] <= 0x7e ? String.fromCharCode(row[i]) : ".";
      } else {
        hex.push("  ");
      }
    }
    // Split 8 + 8 the way every hex dump does — sixteen unbroken pairs are
    // very hard to count along.
    const left = hex.slice(0, 8).join(" ");
    const right = hex.slice(8).join(" ");
    lines.push(`${start.toString(16).padStart(8, "0")}  ${left}  ${right}  |${ascii}|`);
  }
  return lines.join("\n");
}

/**
 * Re-wraps a base64 string into fixed-width lines for display.
 *
 * The backend sends one unbroken string, which in a viewer that doesn't wrap
 * (the payload views don't — wrapping would desynchronise the line-number
 * gutter from what is on screen) is a single line megabytes wide. 76
 * characters is the MIME line length, so the result looks like base64 as it
 * appears anywhere else it is written down.
 */
export function wrapBase64(base64: string, width = 76): string {
  const lines: string[] = [];
  for (let i = 0; i < base64.length; i += width) {
    lines.push(base64.slice(i, i + width));
  }
  return lines.join("\n");
}

/**
 * UTF-8 encodes text and base64s it — what the Save button hands the backend,
 * which writes bytes rather than a string (see `api.savePayloadFile`).
 *
 * Encoded in chunks because `String.fromCharCode(...bytes)` spreads one
 * argument per byte, and a megabyte-sized payload is a megabyte-long argument
 * list: every engine throws `RangeError: Maximum call stack size exceeded`
 * somewhere in the low hundreds of thousands. 32 KB at a time is comfortably
 * under every limit and costs one extra string concat per chunk.
 */
export function textToBase64(text: string): string {
  return bytesToBase64(new TextEncoder().encode(text));
}

/** Base64-encodes bytes. See `textToBase64` for why this is chunked. */
export function bytesToBase64(bytes: Uint8Array): string {
  const CHUNK = 0x8000;
  let binary = "";
  for (let i = 0; i < bytes.length; i += CHUNK) {
    binary += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
  }
  return btoa(binary);
}
