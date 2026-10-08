import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { barsPerRow, niceCeil, PartitionBar, PartitionBarRows } from "./PartitionBarRows";

vi.mock("recharts", async (importOriginal) => {
  const actual = await importOriginal<typeof import("recharts")>();
  return {
    ...actual,
    ResponsiveContainer: ({ children }: { children: React.ReactElement }) => (
      <div data-testid="row">{children}</div>
    ),
  };
});

const bars = (n: number): PartitionBar[] =>
  Array.from({ length: n }, (_, i) => ({ key: String(i), label: `P${i}`, title: `Partition ${i}`, value: i + 1 }));

const render_ = (n: number) =>
  render(<PartitionBarRows bars={bars(n)} seriesName="Messages" colorOf={() => "red"} ariaLabel="chart" />);

describe("niceCeil", () => {
  it("rounds up to a figure a person would pick", () => {
    expect(niceCeil(0)).toBe(1);
    expect(niceCeil(184_000)).toBe(200_000);
    expect(niceCeil(46_000)).toBe(50_000);
    expect(niceCeil(21_000)).toBe(25_000);
    expect(niceCeil(100)).toBe(100);
    expect(niceCeil(101)).toBe(200);
  });
});

describe("barsPerRow", () => {
  it("falls back to a default where there is no layout", () => {
    expect(barsPerRow(0)).toBe(12);
  });

  it("fits more bars into a wider panel but never fewer than four", () => {
    expect(barsPerRow(1000)).toBeGreaterThan(barsPerRow(700));
    expect(barsPerRow(100)).toBe(4);
  });
});

describe("PartitionBarRows", () => {
  it("keeps one row when everything fits", () => {
    render_(8);
    expect(screen.getAllByTestId("row")).toHaveLength(1);
  });

  it("wraps onto more rows instead of shrinking the bars", () => {
    render_(30);
    // 12 per row without a measured width: 12 + 12 + 6.
    expect(screen.getAllByTestId("row")).toHaveLength(3);
  });

  it("is a single labelled image however many rows it has", () => {
    render_(30);
    expect(screen.getByRole("img", { name: "chart" })).toBeInTheDocument();
  });

  it("draws nothing for no partitions", () => {
    render_(0);
    expect(screen.queryAllByTestId("row")).toHaveLength(0);
  });
});
