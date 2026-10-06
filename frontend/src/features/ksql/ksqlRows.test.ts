import { describe, expect, it } from "vitest";
import {
  appendRows,
  columnDefsFor,
  createStreamStatement,
  defaultTopicQuery,
  MAX_RESULT_ROWS,
  resultSummary,
  suggestedStreamName,
} from "./ksqlRows";

describe("columnDefsFor", () => {
  it("builds one column per header entry, in order", () => {
    const defs = columnDefsFor([
      { name: "ID", kind: "STRING" },
      { name: "AMOUNT", kind: "BIGINT" },
    ]);

    expect(defs.map((def) => def.headerName)).toEqual(["ID", "AMOUNT"]);
  });

  // A numeric column with a text filter offers "contains", which is useless
  // for a number, and sorts 10 before 9.
  it("gives numeric ksqlDB types a numeric filter", () => {
    const [id, amount] = columnDefsFor([
      { name: "ID", kind: "STRING" },
      { name: "AMOUNT", kind: "BIGINT" },
    ]);

    expect(id.filter).toBe("agTextColumnFilter");
    expect(amount.filter).toBe("agNumberColumnFilter");
    expect(amount.type).toBe("numericColumn");
  });

  it("recognises a parameterised numeric type", () => {
    const [price] = columnDefsFor([{ name: "PRICE", kind: "DECIMAL(10,2)" }]);

    expect(price.filter).toBe("agNumberColumnFilter");
  });

  it("treats an unfamiliar type as text rather than failing", () => {
    const [blob] = columnDefsFor([{ name: "PAYLOAD", kind: "STRUCT<A INT>" }]);

    expect(blob.filter).toBe("agTextColumnFilter");
  });

  function valueOf(def: ReturnType<typeof columnDefsFor>[number], values: unknown[]) {
    const getter = def.valueGetter as (params: { data?: { values: unknown[] } }) => unknown;
    return getter({ data: { index: 0, values } as never });
  }

  it("reads each column's value by its position in the row", () => {
    const defs = columnDefsFor([
      { name: "A", kind: "STRING" },
      { name: "B", kind: "STRING" },
    ]);

    expect(valueOf(defs[0], ["first", "second"])).toBe("first");
    expect(valueOf(defs[1], ["first", "second"])).toBe("second");
  });

  // `null` is a value in a ksql result. Rendering it as a blank cell would make
  // it indistinguishable from an empty string, which is a different thing.
  it("renders a null value as null rather than as an empty cell", () => {
    const [def] = columnDefsFor([{ name: "A", kind: "STRING" }]);

    expect(valueOf(def, [null])).toBe("null");
    expect(valueOf(def, [""])).toBe("");
  });

  // ksqlDB's LIMIT is best-effort and its frames are hand-parsed, so a row
  // arriving narrower than the header is a real possibility. An empty cell is
  // the honest rendering; indexing past the end must not throw.
  it("renders a missing trailing value as an empty cell rather than throwing", () => {
    const defs = columnDefsFor([
      { name: "A", kind: "STRING" },
      { name: "B", kind: "STRING" },
    ]);

    expect(valueOf(defs[1], ["only-one-value"])).toBe("");
  });

  it("renders a structured value as JSON rather than [object Object]", () => {
    const [def] = columnDefsFor([{ name: "A", kind: "STRUCT" }]);

    expect(valueOf(def, [{ nested: 1 }])).toBe('{"nested":1}');
  });

  it("carries the ksqlDB type in the header tooltip, which is otherwise invisible", () => {
    const [def] = columnDefsFor([{ name: "AMOUNT", kind: "BIGINT" }]);

    expect(def.headerTooltip).toContain("BIGINT");
  });
});

describe("appendRows", () => {
  it("appends and numbers rows by arrival", () => {
    const first = appendRows([], [["a"], ["b"]], 0);

    expect(first.rows.map((row) => row.index)).toEqual([0, 1]);
    expect(first.nextIndex).toBe(2);
  });

  it("keeps numbering across appends", () => {
    const first = appendRows([], [["a"]], 0);
    const second = appendRows(first.rows, [["b"]], first.nextIndex);

    expect(second.rows.map((row) => row.index)).toEqual([0, 1]);
  });

  it("leaves the array untouched when nothing arrived", () => {
    const existing = appendRows([], [["a"]], 0).rows;

    const result = appendRows(existing, [], 1);

    expect(result.rows).toBe(existing);
    expect(result.nextIndex).toBe(1);
  });

  // The cap is what stops a live tail being a memory leak with a Run button.
  it("keeps only the newest rows once the cap is reached", () => {
    const many = Array.from({ length: MAX_RESULT_ROWS + 500 }, (_, i) => [`row-${i}`]);

    const result = appendRows([], many, 0);

    expect(result.rows).toHaveLength(MAX_RESULT_ROWS);
    // The oldest 500 were dropped, so the first kept row is #500.
    expect(result.rows[0].values[0]).toBe("row-500");
    expect(result.rows[result.rows.length - 1].values[0]).toBe(`row-${MAX_RESULT_ROWS + 499}`);
  });

  // Identity must not shift because older rows were dropped ahead of it, or
  // the grid re-keys every visible row on each batch.
  it("keeps a surviving row's index stable when older rows are evicted", () => {
    const first = appendRows([], Array.from({ length: MAX_RESULT_ROWS }, (_, i) => [i]), 0);
    const marked = first.rows[first.rows.length - 1];

    const second = appendRows(first.rows, [["new"]], first.nextIndex);
    const stillThere = second.rows.find((row) => row.index === marked.index);

    expect(stillThere).toBeDefined();
    expect(stillThere?.values).toEqual(marked.values);
  });
});

describe("resultSummary", () => {
  it("says nothing before a query has run", () => {
    expect(resultSummary(0, 0, "idle")).toBe("");
  });

  it("marks a running query as live", () => {
    expect(resultSummary(12, 12, "running")).toBe("12 rows · live");
  });

  it("distinguishes stopped from finished", () => {
    expect(resultSummary(5, 5, "stopped")).toContain("stopped");
    expect(resultSummary(5, 5, "done")).toContain("finished");
  });

  it("uses the singular for one row", () => {
    expect(resultSummary(1, 1, "done")).toBe("1 row · finished");
  });

  // Saying "10,000 rows" when 143,902 arrived would be a quiet lie about what
  // the query did.
  it("says how many were received once the cap starts dropping rows", () => {
    expect(resultSummary(10_000, 143_902, "running")).toBe(
      "showing last 10,000 of 143,902 received · live",
    );
  });
});

describe("defaultTopicQuery", () => {
  // ksqlDB cannot express "the most recent N rows" — a push query runs forward
  // from an offset — so the topic tab tails instead of pretending otherwise.
  it("opens on a live tail of the stream", () => {
    expect(defaultTopicQuery("ORDERS_STREAM")).toBe("SELECT * FROM ORDERS_STREAM EMIT CHANGES;");
  });
});

describe("createStreamStatement", () => {
  it("registers a stream over a topic with the given value format", () => {
    expect(createStreamStatement("orders", "ORDERS", "AVRO")).toBe(
      "CREATE STREAM ORDERS WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='AVRO');",
    );
  });
});

describe("suggestedStreamName", () => {
  // Topic names routinely contain hyphens; ksqlDB identifiers cannot.
  it("upper-cases and replaces characters an identifier cannot hold", () => {
    expect(suggestedStreamName("order-events.v2")).toBe("ORDER_EVENTS_V2");
  });

  it("leaves an already-valid name alone but for case", () => {
    expect(suggestedStreamName("orders")).toBe("ORDERS");
  });

  it("prefixes a name that would start with a digit", () => {
    expect(suggestedStreamName("2024-orders")).toBe("S_2024_ORDERS");
  });
});
