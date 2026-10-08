import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { ConsumerGroupLag } from "../../lib/tauri";
import { LagMetricsTab } from "./metricsTabs";

vi.mock("recharts", async (importOriginal) => {
  const actual = await importOriginal<typeof import("recharts")>();
  return {
    ...actual,
    ResponsiveContainer: ({ children }: { children: React.ReactElement }) => (
      <div data-testid="chart">{children}</div>
    ),
  };
});

const data: ConsumerGroupLag = {
  state: "Stable",
  partitions: [
    { topic: "orders", partition: 0, currentOffset: 1, logEndOffset: 5, lag: 4, clientId: null, clientHost: null },
    { topic: "orders", partition: 1, currentOffset: null, logEndOffset: 5, lag: null, clientId: null, clientHost: null },
  ],
};

describe("LagMetricsTab", () => {
  it("asks for a Refresh before there is anything to chart", async () => {
    render(<LagMetricsTab data={undefined} samples={[]} />);
    expect(await screen.findByText(/Press Refresh on the Lag tab/)).toBeInTheDocument();
  });

  it("says so when the group has no assignment", async () => {
    render(<LagMetricsTab data={{ state: "Empty", partitions: [] }} samples={[]} />);
    expect(await screen.findByText(/no active partition assignment/)).toBeInTheDocument();
  });

  it("draws both charts and hints that one sample is not a trend", async () => {
    render(<LagMetricsTab data={data} samples={[{ at: 1, totalLag: 4 }]} />);

    expect(await screen.findByRole("img", { name: "Lag by partition" })).toBeInTheDocument();
    expect(screen.getByRole("img", { name: "Total lag over time" })).toBeInTheDocument();
    expect(screen.getByText(/Refresh again to see the trend/)).toBeInTheDocument();
  });

  it("drops the hint once there are two samples", async () => {
    render(
      <LagMetricsTab
        data={data}
        samples={[
          { at: 1, totalLag: 4 },
          { at: 2, totalLag: 6 },
        ]}
      />,
    );
    await screen.findByRole("img", { name: "Lag by partition" });
    expect(screen.queryByText(/Refresh again to see the trend/)).not.toBeInTheDocument();
  });

  it("lists the most lagging partitions on a wide group, skipping ones with no lag", async () => {
    const wide: ConsumerGroupLag = {
      state: "Stable",
      partitions: Array.from({ length: 40 }, (_, i) => ({
        topic: "orders",
        partition: i,
        currentOffset: 1,
        logEndOffset: 5,
        lag: i === 3 ? 900 : i < 10 ? 50 : 0,
        clientId: null,
        clientHost: null,
      })),
    };
    render(<LagMetricsTab data={wide} samples={[]} />);

    expect(await screen.findByText("Most lagging partitions")).toBeInTheDocument();
    const items = screen.getAllByRole("listitem");
    expect(items[0]).toHaveTextContent("orders-3");
    expect(items[0]).toHaveTextContent("900 behind");
    expect(items).toHaveLength(8);
  });

  it("omits that list on a narrow group", async () => {
    render(<LagMetricsTab data={data} samples={[]} />);
    await screen.findByRole("img", { name: "Lag by partition" });
    expect(screen.queryByText("Most lagging partitions")).not.toBeInTheDocument();
  });
});
