import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { PartitionsTab } from "./PartitionsTab";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

function renderWithClient(ui: React.ReactElement) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("PartitionsTab", () => {
  it("shows a loading state before the partitions arrive", () => {
    setInvokeHandlers({ connection_list_partitions: () => [] });
    renderWithClient(<PartitionsTab connectionId="1" topicName="orders" />);
    expect(screen.getByText(/loading/i)).toBeInTheDocument();
  });

  it("lists each partition's id, leader, replicas, ISR, and offsets", async () => {
    setInvokeHandlers({
      connection_list_partitions: () => [
        { id: 0, leader: 1, replicas: [1, 2], isr: [1, 2], lowOffset: 0, highOffset: 150 },
      ],
    });
    renderWithClient(<PartitionsTab connectionId="1" topicName="orders" />);

    const row = (await screen.findAllByRole("row"))[1];
    const cells = within(row)
      .getAllByRole("cell")
      .map((cell) => cell.textContent);
    expect(cells).toEqual(["0", "1", "1, 2", "1, 2", "0", "150"]);
  });

  it("shows an empty state when the topic has no partitions", async () => {
    setInvokeHandlers({ connection_list_partitions: () => [] });
    renderWithClient(<PartitionsTab connectionId="1" topicName="orders" />);
    expect(await screen.findByText(/no partitions/i)).toBeInTheDocument();
  });

  it("shows when the data was read and asks the backend to refresh only on Refresh", async () => {
    const read = vi.fn(() => ({ value: [{ id: 0, leader: 1, replicas: [1], isr: [1], lowOffset: 0, highOffset: 5 }], fetchedAtMs: new Date(2024, 0, 2, 3, 4, 5).getTime() }));
    setInvokeHandlers({ connection_cached_partitions: read });
    renderWithClient(<PartitionsTab connectionId="1" topicName="orders" />);
    await screen.findAllByRole("row");
    expect(screen.getByText(/Loaded at/)).toBeInTheDocument();
    expect(read).toHaveBeenLastCalledWith(expect.objectContaining({ refresh: false }));

    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => expect(read).toHaveBeenLastCalledWith(expect.objectContaining({ refresh: true })));
  });
});
