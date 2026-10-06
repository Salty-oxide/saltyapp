import { describe, expect, it } from "vitest";
import { TopicMessage } from "../../lib/tauri";
import {
  ROW_OVERHEAD_BYTES,
  VALUE_PREVIEW_BYTES,
  base64DecodedLength,
  base64ToBytes,
  base64ToDisplayText,
  bytesToBase64,
  bytesToHexDump,
  bytesToText,
  decodeValuePreview,
  detectConfluentAvro,
  formatXmlNode,
  formatPayloadSize,
  isPayloadTruncated,
  retainedPayloadBytes,
  retainedRowBytes,
  textToBase64,
  tryParseJson,
  tryParseXml,
  wrapBase64,
} from "./payloadDecoding";

function toBase64(bytes: number[]): string {
  return btoa(String.fromCharCode(...bytes));
}

describe("base64ToBytes / bytesToText", () => {
  it("round-trips plain text through base64", () => {
    const original = "hello world";
    const b64 = btoa(original);
    expect(bytesToText(base64ToBytes(b64))).toBe(original);
  });

  it("decodes UTF-8 multi-byte characters correctly", () => {
    const original = "héllo wörld 日本語";
    const bytes = Array.from(new TextEncoder().encode(original));
    expect(bytesToText(base64ToBytes(toBase64(bytes)))).toBe(original);
  });
});

describe("base64ToDisplayText", () => {
  it("decodes a base64 key/header value for display", () => {
    expect(base64ToDisplayText(btoa("order-1"))).toBe("order-1");
  });

  it("returns null unchanged, for a message/header with no key or value at all", () => {
    expect(base64ToDisplayText(null)).toBeNull();
  });

  it("does not throw on binary (non-UTF-8) bytes — shows the replacement character instead", () => {
    // A lone continuation byte (0x80) is invalid as the start of a UTF-8
    // sequence. This must degrade gracefully for display, since a real
    // binary key would fail identically — the underlying base64 field
    // itself (not this display helper) is what preserves the exact bytes.
    const binary = toBase64([0x80, 0x81]);
    expect(() => base64ToDisplayText(binary)).not.toThrow();
    expect(base64ToDisplayText(binary)).toContain("�");
  });
});

describe("tryParseJson", () => {
  it("parses valid JSON into a value", () => {
    expect(tryParseJson('{"a":1}')).toEqual({ a: 1 });
  });

  it("returns undefined for invalid JSON", () => {
    expect(tryParseJson("not json")).toBeUndefined();
  });

  it("returns undefined for an empty string", () => {
    expect(tryParseJson("")).toBeUndefined();
  });
});

describe("tryParseXml", () => {
  it("parses a well-formed XML document into a tree", () => {
    expect(tryParseXml('<root a="1"><child>hi</child></root>')).toEqual({
      tag: "root",
      attributes: [["a", "1"]],
      children: [{ tag: "child", attributes: [], children: [], text: "hi" }],
      text: null,
    });
  });

  it("returns undefined for malformed XML", () => {
    expect(tryParseXml("<root><unclosed></root>")).toBeUndefined();
  });

  it("returns undefined for JSON text", () => {
    expect(tryParseXml('{"a":1}')).toBeUndefined();
  });

  it("returns undefined for an empty string", () => {
    expect(tryParseXml("")).toBeUndefined();
  });
});

describe("formatXmlNode", () => {
  it("pretty-prints a leaf element with text content", () => {
    expect(formatXmlNode({ tag: "a", attributes: [], children: [], text: "1" })).toBe("<a>1</a>");
  });

  it("pretty-prints a self-closing leaf element with no text", () => {
    expect(formatXmlNode({ tag: "a", attributes: [], children: [], text: null })).toBe("<a/>");
  });

  it("pretty-prints attributes inline with the opening tag", () => {
    expect(formatXmlNode({ tag: "user", attributes: [["id", "1"]], children: [], text: null })).toBe(
      '<user id="1"/>',
    );
  });

  it("pretty-prints nested children with indentation", () => {
    const tree = {
      tag: "root",
      attributes: [],
      children: [{ tag: "child", attributes: [], children: [], text: "hi" }],
      text: null,
    };
    expect(formatXmlNode(tree)).toBe("<root>\n  <child>hi</child>\n</root>");
  });
});

describe("detectConfluentAvro", () => {
  it("detects the Confluent wire format (magic byte 0 + 4-byte big-endian schema id)", () => {
    // magic byte 0x00, schema id 42 as 4-byte big-endian, then arbitrary avro body
    const bytes = new Uint8Array([0x00, 0x00, 0x00, 0x00, 0x2a, 0xde, 0xad, 0xbe, 0xef]);
    expect(detectConfluentAvro(bytes)).toEqual({ schemaId: 42 });
  });

  it("returns null when the magic byte is not 0", () => {
    const bytes = new Uint8Array([0x01, 0x00, 0x00, 0x00, 0x2a]);
    expect(detectConfluentAvro(bytes)).toBeNull();
  });

  it("returns null for payloads too short to contain a schema id", () => {
    expect(detectConfluentAvro(new Uint8Array([0x00, 0x01]))).toBeNull();
  });

  it("returns null for an empty payload", () => {
    expect(detectConfluentAvro(new Uint8Array([]))).toBeNull();
  });
});

describe("decodeValuePreview", () => {
  const encodeText = (text: string) => btoa(String.fromCharCode(...new TextEncoder().encode(text)));

  it("is blank when the payload wasn't loaded", () => {
    expect(decodeValuePreview(null)).toBe("");
  });

  it("decodes a payload smaller than the preview limit in full", () => {
    expect(decodeValuePreview(encodeText('{"id":42}'))).toBe('{"id":42}');
  });

  /**
   * The regression this exists for: the Value column used to decode whole
   * payloads, so a grid of multi-megabyte messages spent seconds decoding
   * text no cell could show — and AG Grid then cached a lowercased copy of
   * every one of them for the quick filter.
   */
  it("stops at the preview limit instead of decoding a huge payload in full", () => {
    const payload = `${"a".repeat(VALUE_PREVIEW_BYTES * 4)}TAIL`;

    const preview = decodeValuePreview(encodeText(payload));

    expect(preview.length).toBeLessThan(payload.length);
    expect(preview).not.toContain("TAIL");
    expect(preview.startsWith("aaa")).toBe(true);
  });

  it("still recognises a Confluent Avro payload from its prefix", () => {
    // magic byte 0x00, schema id 7 as 4-byte big-endian, then an avro body.
    expect(decodeValuePreview(toBase64([0, 0, 0, 0, 7, 1, 2, 3]))).toBe("Avro (schema id: 7)");
  });
});

describe("base64DecodedLength", () => {
  const encodeBytes = (n: number) => btoa("a".repeat(n));

  it("counts the bytes a base64 string decodes to", () => {
    expect(base64DecodedLength(encodeBytes(3))).toBe(3);
    expect(base64DecodedLength(encodeBytes(4))).toBe(4);
    expect(base64DecodedLength(encodeBytes(5))).toBe(5);
    expect(base64DecodedLength(encodeBytes(6))).toBe(6);
  });

  it("is zero for an empty string", () => {
    expect(base64DecodedLength("")).toBe(0);
  });
});

describe("formatPayloadSize", () => {
  it("shows exact bytes below a kilobyte", () => {
    expect(formatPayloadSize(0)).toBe("0 B");
    expect(formatPayloadSize(842)).toBe("842 B");
    expect(formatPayloadSize(1023)).toBe("1023 B");
  });

  it("switches to KB at a kilobyte, with one decimal", () => {
    expect(formatPayloadSize(1024)).toBe("1.0 KB");
    expect(formatPayloadSize(4096)).toBe("4.0 KB");
    expect(formatPayloadSize(1024 * 1024 - 1)).toBe("1024.0 KB");
  });

  it("switches to MB at a megabyte, with two decimals", () => {
    expect(formatPayloadSize(1024 * 1024)).toBe("1.00 MB");
    expect(formatPayloadSize(4 * 1024 * 1024)).toBe("4.00 MB");
  });

  // The point of the second decimal: a payload just over the line must not
  // render as a flat "1 MB" beside one four times its size.
  it("keeps a megabyte and a bit distinguishable from a flat megabyte", () => {
    expect(formatPayloadSize(1024 * 1024)).not.toBe(formatPayloadSize(1.05 * 1024 * 1024));
  });
});

describe("isPayloadTruncated", () => {
  const encodeBytes = (n: number) => btoa("a".repeat(n));

  it("is false when no payload was loaded at all", () => {
    expect(isPayloadTruncated(null, 5000)).toBe(false);
  });

  it("is false when the size is unknown", () => {
    expect(isPayloadTruncated(encodeBytes(10), null)).toBe(false);
  });

  it("is false when the base64 carries the whole payload", () => {
    expect(isPayloadTruncated(encodeBytes(100), 100)).toBe(false);
  });

  it("is true when the base64 carries less than the payload's real size", () => {
    expect(isPayloadTruncated(encodeBytes(4096), 3_145_728)).toBe(true);
  });
});

describe("retainedRowBytes", () => {
  const encodeBytes = (n: number) => btoa("a".repeat(n));
  const row = (over: Partial<TopicMessage> = {}): TopicMessage => ({
    partition: 0,
    offset: 1,
    timestampMs: null,
    keyBase64: null,
    payloadBase64: null,
    payloadSizeBytes: null,
    headers: [],
    ...over,
  });

  it("is a fixed per-row overhead for metadata-only rows", () => {
    expect(retainedRowBytes([row(), row(), row()])).toBe(3 * ROW_OVERHEAD_BYTES);
  });

  // The whole point of measuring rows this way: "Tab memory" and "Payloads
  // (all tabs)" describe the same cached rows, so the payload part of the
  // first has to be the same number the second charges against the ceiling.
  // Sized by JSON.stringify, it was ~4/3 of it (base64 characters, not the
  // bytes they carry) and the two figures disagreed by a third.
  it("counts a payload as exactly what retainedPayloadBytes charges for it", () => {
    const rows = [row({ payloadBase64: encodeBytes(4096) }), row({ payloadBase64: encodeBytes(2048) })];

    expect(retainedRowBytes(rows) - 2 * ROW_OVERHEAD_BYTES).toBe(retainedPayloadBytes(rows));
  });

  it("counts a row's key and headers on top of its payload", () => {
    const rows = [
      row({
        payloadBase64: encodeBytes(300),
        keyBase64: encodeBytes(20),
        headers: [{ key: "trace-id", valueBase64: encodeBytes(16) }],
      }),
    ];

    expect(retainedRowBytes(rows)).toBe(ROW_OVERHEAD_BYTES + 300 + 20 + "trace-id".length + 16);
  });
});

describe("bytesToHexDump", () => {
  it("lays out one line per 16 bytes: offset, hex, then printable ASCII", () => {
    const dump = bytesToHexDump(new TextEncoder().encode("Hello, hex dump!"));

    expect(dump).toBe(
      "00000000  48 65 6c 6c 6f 2c 20 68  65 78 20 64 75 6d 70 21  |Hello, hex dump!|",
    );
  });

  it("numbers each line with the offset of its first byte", () => {
    const dump = bytesToHexDump(new Uint8Array(33));

    expect(dump.split("\n").map((line) => line.slice(0, 8))).toEqual(["00000000", "00000010", "00000020"]);
  });

  // The hex column is fixed-width whatever the last line holds, so the ASCII
  // column stays where it is instead of sliding left on the final row.
  it("pads a short final line so the columns stay aligned", () => {
    const lines = bytesToHexDump(new TextEncoder().encode("abcdefghijklmnopqr")).split("\n");

    expect(lines[1].indexOf("|")).toBe(lines[0].indexOf("|"));
  });

  // Control codes would move the cursor and wreck the alignment the view
  // exists for.
  it("renders unprintable bytes as dots in the ASCII column", () => {
    const dump = bytesToHexDump(new Uint8Array([0x00, 0x41, 0x1f, 0x7f, 0xff]));

    expect(dump).toContain("|.A...|");
  });

  it("renders no lines for no bytes", () => {
    expect(bytesToHexDump(new Uint8Array())).toBe("");
  });
});

describe("wrapBase64", () => {
  it("breaks the string into MIME-width lines", () => {
    const wrapped = wrapBase64("a".repeat(200));

    expect(wrapped.split("\n").map((line) => line.length)).toEqual([76, 76, 48]);
  });

  it("leaves a string shorter than one line alone", () => {
    expect(wrapBase64("abcd")).toBe("abcd");
  });

  it("wraps at a caller-chosen width", () => {
    expect(wrapBase64("abcdef", 2)).toBe("ab\ncd\nef");
  });
});

describe("textToBase64", () => {
  it("round-trips text through base64", () => {
    expect(bytesToText(base64ToBytes(textToBase64("hello world")))).toBe("hello world");
  });

  it("encodes as UTF-8 rather than one byte per character", () => {
    // "é" is two bytes in UTF-8; `btoa` on the raw string would throw.
    expect(base64ToBytes(textToBase64("é"))).toEqual(new Uint8Array([0xc3, 0xa9]));
  });

  // `String.fromCharCode(...bytes)` spreads one argument per byte and blows
  // the call stack somewhere in the low hundreds of thousands — which is well
  // inside the range of payloads this app opens.
  it("encodes a payload far larger than the argument-spread limit", () => {
    const large = "x".repeat(1_000_000);

    expect(bytesToText(base64ToBytes(textToBase64(large)))).toBe(large);
  });
});

describe("bytesToBase64", () => {
  it("round-trips bytes", () => {
    const bytes = new Uint8Array([0, 1, 2, 250, 251, 255]);

    expect(base64ToBytes(bytesToBase64(bytes))).toEqual(bytes);
  });

  it("encodes no bytes as an empty string", () => {
    expect(bytesToBase64(new Uint8Array())).toBe("");
  });
});
