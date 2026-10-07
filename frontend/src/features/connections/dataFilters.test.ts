import { describe, expect, it } from "vitest";
import { emptyFilterForm, toMessageFilter, validateDateRange, validateMaxMessagesPerPartition } from "./dataFilters";
import { MAX_INLINE_PAYLOAD_BYTES, PAYLOAD_RETENTION_BUDGET_BYTES, VALUE_PREVIEW_BYTES } from "./payloadDecoding";
import { formatLocalTimestamp } from "../../lib/time";

describe("emptyFilterForm", () => {
  it("pre-fills both count caps, and leaves every other field blank with includePayload checked", () => {
    const form = emptyFilterForm();
    expect(form.maxMessagesPerPartition).toBe("100");
    // Prefilled rather than blank: blank means "no overall budget", which
    // costs the per-partition cap times the topic's partition count.
    expect(form.maxTotalMessages).toBe("100");
    expect(form.partitions).toBe("");
    expect(form.fromDate).toBe("");
    expect(form.toDate).toBe("");
    expect(form.offset).toBe("");
    expect(form.includePayload).toBe(true);
  });
});

describe("toMessageFilter", () => {
  it("converts the default form to a filter capped at 100 overall and 100 per partition", () => {
    expect(toMessageFilter(emptyFilterForm())).toEqual({
      partitions: null,
      maxMessagesPerPartition: 100,
      maxTotalMessages: 100,
      fromTimestampMs: null,
      toTimestampMs: null,
      offset: null,
      includePayload: true,
      maxPayloadPreviewBytes: MAX_INLINE_PAYLOAD_BYTES,
    });
  });

  it("falls back to the backend's own default cap (null) when maxMessagesPerPartition is cleared back to blank", () => {
    const form = { ...emptyFilterForm(), maxMessagesPerPartition: "" };
    expect(toMessageFilter(form).maxMessagesPerPartition).toBeNull();
  });

  // Asking the backend for whole payloads shipped gigabytes of base64 across
  // the IPC boundary to render a few hundred KB of text — twice over, once
  // streamed and once in the fetch result — and the webview was killed
  // holding it. A bound has to travel with every fetch this form produces,
  // checkbox on or off.
  it("always bounds the payload each row carries", () => {
    expect(toMessageFilter(emptyFilterForm()).maxPayloadPreviewBytes).not.toBeNull();
    expect(
      toMessageFilter({ ...emptyFilterForm(), includePayload: true }).maxPayloadPreviewBytes,
    ).not.toBeNull();
  });

  // Regression test for opening a message taking seconds. The bound above
  // used to be VALUE_PREVIEW_BYTES — 4 KB, what a *grid cell* decodes — so
  // every row of any ordinary JSON or Avro topic arrived truncated, and
  // opening one had to go back to the broker for the real bytes: a fresh
  // consumer, a TLS and SASL handshake, metadata and watermarks, on every
  // click. A default fetch is 100 rows, which fits far more than that per
  // row inside the retention budget.
  it("gives each row enough payload for the viewer to open it without a second fetch", () => {
    expect(toMessageFilter(emptyFilterForm()).maxPayloadPreviewBytes).toBe(MAX_INLINE_PAYLOAD_BYTES);
  });

  // The budget is what keeps the line above from being the memory bug again:
  // the more rows a fetch asks for, the less of each payload it carries.
  it("shrinks the per-row bound as the row budget grows, so the total stays inside the retention budget", () => {
    const rows = 512;
    const filter = toMessageFilter({ ...emptyFilterForm(), maxTotalMessages: String(rows) });
    expect(filter.maxPayloadPreviewBytes).toBe(PAYLOAD_RETENTION_BUDGET_BYTES / rows);
    expect(filter.maxPayloadPreviewBytes! * rows).toBeLessThanOrEqual(PAYLOAD_RETENTION_BUDGET_BYTES);
  });

  // The floor, and the reason this can never retain more than the code it
  // replaced: a fetch big enough to price each row below the grid's own
  // preview gets exactly the old bound back.
  it("never drops below what the grid cell decodes, however many rows are asked for", () => {
    expect(
      toMessageFilter({ ...emptyFilterForm(), maxTotalMessages: "1000000" }).maxPayloadPreviewBytes,
    ).toBe(VALUE_PREVIEW_BYTES);
  });

  // With no overall budget and no explicit partition list the row count is
  // whatever the topic's partition count makes it, which this form cannot
  // see — so it stays at the old conservative bound rather than guessing.
  it("stays at the grid's bound when the fetch has no knowable row count", () => {
    expect(
      toMessageFilter({ ...emptyFilterForm(), maxTotalMessages: "" }).maxPayloadPreviewBytes,
    ).toBe(VALUE_PREVIEW_BYTES);
  });

  // An explicit partition list does make it knowable, without an overall budget.
  it("derives the row count from an explicit partition list when there is no overall budget", () => {
    const filter = toMessageFilter({
      ...emptyFilterForm(),
      maxTotalMessages: "",
      maxMessagesPerPartition: "100",
      partitions: "0, 1, 2, 3",
    });
    // 100 per partition x 4 partitions = 400 rows, priced out of the budget.
    expect(filter.maxPayloadPreviewBytes).toBe(Math.floor(PAYLOAD_RETENTION_BUDGET_BYTES / 400));
    expect(filter.maxPayloadPreviewBytes).toBeGreaterThan(VALUE_PREVIEW_BYTES);
  });

  it("carries includePayload through when checked", () => {
    const form = { ...emptyFilterForm(), includePayload: true };
    expect(toMessageFilter(form).includePayload).toBe(true);
  });

  it("parses maxMessagesPerPartition and maxTotalMessages as numbers", () => {
    const form = { ...emptyFilterForm(), maxMessagesPerPartition: "50", maxTotalMessages: "500" };
    const filter = toMessageFilter(form);
    expect(filter.maxMessagesPerPartition).toBe(50);
    expect(filter.maxTotalMessages).toBe(500);
  });

  it("parses a comma-separated partitions list into numbers, ignoring extra whitespace", () => {
    const form = { ...emptyFilterForm(), partitions: " 0, 2 ,5" };
    expect(toMessageFilter(form).partitions).toEqual([0, 2, 5]);
  });

  /**
   * Asserted against an explicitly-local `Date` rather than against
   * `new Date("2026-01-01T00:00")` — which is how this read before, and which
   * proved nothing: both sides ran the same parse, so the test passed whether
   * that parse was local or UTC. Built from local Y/M/D/h/m instead, it fails
   * if the boundary is ever read as UTC, in any timezone the suite runs in.
   */
  it("converts fromDate/toDate datetime-local values to epoch milliseconds in the system's timezone", () => {
    const form = { ...emptyFilterForm(), fromDate: "2026-01-01T09:00", toDate: "2026-01-02T17:30" };
    const filter = toMessageFilter(form);
    expect(filter.fromTimestampMs).toBe(new Date(2026, 0, 1, 9, 0, 0, 0).getTime());
    expect(filter.toTimestampMs).toBe(new Date(2026, 0, 2, 17, 30, 0, 0).getTime());
  });

  /**
   * The zone the filter is read in has to be the zone the Timestamp column is
   * rendered in, or the user filters from 09:00 and gets rows labelled 03:30.
   * This pins the two together across the whole round trip: form value ->
   * epoch ms -> the string the grid would show for that instant.
   */
  it("agrees with how the grid renders the same instant, so a typed boundary matches the column", () => {
    const form = { ...emptyFilterForm(), fromDate: "2026-01-01T09:00" };
    const fromMs = toMessageFilter(form).fromTimestampMs;
    expect(formatLocalTimestamp(fromMs)).toBe("2026-01-01 09:00:00.000");
  });

  /**
   * A date with no time is the one form ECMAScript reads as UTC — see
   * `parseDate`. Left alone it would shift the boundary by the machine's
   * offset.
   */
  it("reads a date-only value as local midnight rather than UTC midnight", () => {
    const form = { ...emptyFilterForm(), fromDate: "2026-01-01" };
    expect(toMessageFilter(form).fromTimestampMs).toBe(new Date(2026, 0, 1, 0, 0, 0, 0).getTime());
  });

  it("ignores a date value it cannot parse at all rather than sending NaN to the backend", () => {
    const form = { ...emptyFilterForm(), fromDate: "not a date", toDate: "   " };
    const filter = toMessageFilter(form);
    expect(filter.fromTimestampMs).toBeNull();
    expect(filter.toTimestampMs).toBeNull();
  });

  it("ignores an empty partitions string rather than producing [NaN]", () => {
    const form = { ...emptyFilterForm(), partitions: "   " };
    expect(toMessageFilter(form).partitions).toBeNull();
  });

  it("parses offset as a number", () => {
    const form = { ...emptyFilterForm(), offset: "100" };
    expect(toMessageFilter(form).offset).toBe(100);
  });

  it("applies the same default maxMessagesPerPartition cap regardless of whether offset is set", () => {
    const form = { ...emptyFilterForm(), offset: "100" };
    expect(toMessageFilter(form).maxMessagesPerPartition).toBe(100);
  });

  it("keeps an explicit maxMessagesPerPartition value when offset is also set", () => {
    const form = { ...emptyFilterForm(), offset: "100", maxMessagesPerPartition: "20" };
    expect(toMessageFilter(form).maxMessagesPerPartition).toBe(20);
  });
});

describe("validateDateRange", () => {
  it("passes when both dates are blank", () => {
    expect(validateDateRange(emptyFilterForm())).toBeNull();
  });

  it("passes when only From is set", () => {
    const form = { ...emptyFilterForm(), fromDate: "2026-01-01T00:00" };
    expect(validateDateRange(form)).toBeNull();
  });

  it("passes when only To is set", () => {
    const form = { ...emptyFilterForm(), toDate: "2026-01-01T00:00" };
    expect(validateDateRange(form)).toBeNull();
  });

  it("passes when To is after From", () => {
    const form = { ...emptyFilterForm(), fromDate: "2026-01-01T00:00", toDate: "2026-01-02T00:00" };
    expect(validateDateRange(form)).toBeNull();
  });

  it("fails when To equals From", () => {
    const form = { ...emptyFilterForm(), fromDate: "2026-01-01T00:00", toDate: "2026-01-01T00:00" };
    expect(validateDateRange(form)).toBe('"To" date must be after "From" date');
  });

  it("fails when To is before From", () => {
    const form = { ...emptyFilterForm(), fromDate: "2026-01-02T00:00", toDate: "2026-01-01T00:00" };
    expect(validateDateRange(form)).toBe('"To" date must be after "From" date');
  });
});

describe("validateMaxMessagesPerPartition", () => {
  it("passes with the default pre-filled value", () => {
    expect(validateMaxMessagesPerPartition(emptyFilterForm())).toBeNull();
  });

  it("passes with an explicit value", () => {
    const form = { ...emptyFilterForm(), maxMessagesPerPartition: "500" };
    expect(validateMaxMessagesPerPartition(form)).toBeNull();
  });

  it("fails when cleared to blank", () => {
    const form = { ...emptyFilterForm(), maxMessagesPerPartition: "" };
    expect(validateMaxMessagesPerPartition(form)).toBe('"Max messages per partition" is required');
  });

  it("fails when only whitespace", () => {
    const form = { ...emptyFilterForm(), maxMessagesPerPartition: "   " };
    expect(validateMaxMessagesPerPartition(form)).toBe('"Max messages per partition" is required');
  });
});
