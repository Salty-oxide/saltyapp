import { describe, expect, it } from "vitest";
import { TopicMessage } from "../../lib/tauri";
import { textToBase64 } from "./payloadDecoding";
import {
  activeHeaderCriteria,
  collectHeaderKeys,
  emptyHeaderRow,
  filterByHeaders,
  HeaderFilterRow,
} from "./headerFilters";

function message(offset: number, headers: Array<[string, string | null]>): TopicMessage {
  return {
    partition: 0,
    offset,
    timestampMs: null,
    keyBase64: null,
    payloadBase64: null,
    payloadSizeBytes: null,
    headers: headers.map(([key, value]) => ({ key, valueBase64: value === null ? null : textToBase64(value) })),
  };
}

function row(key: string, value: string): HeaderFilterRow {
  return { ...emptyHeaderRow(), key, value };
}

describe("emptyHeaderRow", () => {
  it("is blank and gets a fresh id each time", () => {
    const a = emptyHeaderRow();
    const b = emptyHeaderRow();
    expect(a.key).toBe("");
    expect(a.value).toBe("");
    expect(a.id).not.toBe(b.id);
  });
});

describe("activeHeaderCriteria", () => {
  it("keeps rows with both a key and a value, trimming the value's leading and trailing spaces", () => {
    expect(activeHeaderCriteria([row("source", "  billing \t")])).toEqual([{ key: "source", value: "billing" }]);
  });

  it("drops rows missing a key, or whose value is blank after trimming", () => {
    expect(activeHeaderCriteria([row("", "x"), row("source", "   "), row("source", "")])).toEqual([]);
  });

  it("keeps spaces inside a value", () => {
    expect(activeHeaderCriteria([row("k", " a b ")])).toEqual([{ key: "k", value: "a b" }]);
  });
});

describe("collectHeaderKeys", () => {
  it("returns each distinct key once, sorted", () => {
    const messages = [message(0, [["b", "1"], ["a", "1"]]), message(1, [["b", "2"]]), message(2, [])];
    expect(collectHeaderKeys(messages)).toEqual(["a", "b"]);
  });

  it("tolerates a row that carries no headers field at all", () => {
    const bare = { ...message(0, []), headers: undefined } as unknown as TopicMessage;
    expect(collectHeaderKeys([bare, message(1, [["a", "1"]])])).toEqual(["a"]);
  });

  it("returns nothing when no message has headers", () => {
    expect(collectHeaderKeys([message(0, [])])).toEqual([]);
  });
});

describe("filterByHeaders", () => {
  const messages = [
    message(0, [["source", "billing"], ["env", "prod"]]),
    message(1, [["source", "billing"], ["env", "dev"]]),
    message(2, [["source", "orders"]]),
    message(3, []),
  ];

  it("returns every message, untouched, when there are no criteria", () => {
    expect(filterByHeaders(messages, [])).toBe(messages);
  });

  it("keeps messages whose header has the key with exactly that value", () => {
    expect(filterByHeaders(messages, [{ key: "source", value: "billing" }]).map((m) => m.offset)).toEqual([0, 1]);
  });

  it("requires every criterion to match (AND)", () => {
    const result = filterByHeaders(messages, [
      { key: "source", value: "billing" },
      { key: "env", value: "prod" },
    ]);
    expect(result.map((m) => m.offset)).toEqual([0]);
  });

  it("matches the whole value, case-sensitively", () => {
    expect(filterByHeaders(messages, [{ key: "source", value: "bill" }])).toEqual([]);
    expect(filterByHeaders(messages, [{ key: "source", value: "Billing" }])).toEqual([]);
  });

  it("matches when any header with a repeated key has the value", () => {
    const repeated = [message(0, [["tag", "a"], ["tag", "b"]])];
    expect(filterByHeaders(repeated, [{ key: "tag", value: "b" }])).toHaveLength(1);
  });

  it("never matches a header with no value, or a message lacking the key", () => {
    const nullValued = [message(0, [["source", null]])];
    expect(filterByHeaders(nullValued, [{ key: "source", value: "x" }])).toEqual([]);
    expect(filterByHeaders(messages, [{ key: "missing", value: "x" }])).toEqual([]);
  });
});
