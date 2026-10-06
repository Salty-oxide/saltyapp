import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { TopicDetailPanel } from "./TopicDetailPanel";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));
vi.mock("@tauri-apps/api/event", () => ({ listen: vi.fn(() => Promise.resolve(() => {})) }));

function renderWithClient(ui: React.ReactElement) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

describe("TopicDetailPanel", () => {
  it("opens on the Data tab by default", () => {
    renderWithClient(<TopicDetailPanel connectionId="1" topicName="orders" />);

    expect(screen.getByRole("tab", { name: "Data" })).toHaveAttribute("aria-selected", "true");
    expect(screen.queryByLabelText("Topic name")).not.toBeInTheDocument();
  });

  /**
   * Order is asserted, not just membership: Data leads because it is both the
   * first tab and the default one, Meta Data sits next to it, and the two
   * read-only tabs (Config, Access) are last.
   */
  it("renders Data, Meta Data, Partitions, Schema, Config, Access and Query, in that order", () => {
    renderWithClient(<TopicDetailPanel connectionId="1" topicName="orders" />);

    expect(screen.getAllByRole("tab").map((tab) => tab.textContent)).toEqual([
      "Data",
      "Meta Data",
      "Partitions",
      "Schema",
      "Config",
      // The read-only tabs sit together at the end: Config is the broker's
      // view of the topic's settings, Access its view of who may touch it,
      // and Query reads from a different server entirely. None of the three
      // can change anything from here.
      "Access",
      "Query",
    ]);
  });

  it("switches to the Config tab when clicked, listing the broker's settings for the topic", async () => {
    setInvokeHandlers({
      connection_describe_topic_config: () => [{ name: "retention.ms", value: "604800000" }],
    });
    const user = userEvent.setup();
    renderWithClient(<TopicDetailPanel connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("tab", { name: "Config" }));

    expect(await screen.findByText("retention.ms")).toBeInTheDocument();
    expect(screen.getByText("604800000")).toBeInTheDocument();
  });

  it("makes the default tab the first tab", () => {
    renderWithClient(<TopicDetailPanel connectionId="1" topicName="orders" />);

    const [first] = screen.getAllByRole("tab");
    expect(first).toHaveAttribute("aria-selected", "true");
    expect(first).toHaveTextContent("Data");
  });

  it("switches to the Schema tab when clicked", async () => {
    setInvokeHandlers({ topic_schema_get: () => null });
    const user = userEvent.setup();
    renderWithClient(<TopicDetailPanel connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("tab", { name: "Schema" }));

    expect(await screen.findByLabelText("Avro schema")).toBeInTheDocument();
  });

  it("switches to the Meta Data tab when clicked, showing the topic name", async () => {
    const user = userEvent.setup();
    renderWithClient(<TopicDetailPanel connectionId="1" topicName="orders" />);

    await user.click(screen.getByRole("tab", { name: "Meta Data" }));

    expect(screen.getByRole("tab", { name: "Meta Data" })).toHaveAttribute("aria-selected", "true");
    expect(screen.getByLabelText("Topic name")).toHaveValue("orders");
  });
});
