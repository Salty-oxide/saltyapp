import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { TopicKsqlTab } from "./TopicKsqlTab";
import { useKsqlStore } from "./useKsqlStore";
import { useTabsStore } from "../tabs/useTabsStore";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));
vi.mock("@tauri-apps/api/event", () => ({
  listen: () => Promise.resolve(() => {}),
}));

function renderWithClient(ui: React.ReactElement) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

const CONFIGURED = {
  id: "1",
  name: "Local",
  ksqldbEndpoint: "http://localhost:8088",
  ksqldbBasicAuthCredentials: null,
};

beforeEach(() => {
  vi.clearAllMocks();
  useKsqlStore.setState({ byKey: {} });
  useTabsStore.setState({ tabs: [], activeTabId: "tab-1", error: null });
});

describe("TopicKsqlTab", () => {
  // Most clusters run no ksqlDB server, so this is a state to explain rather
  // than a failure to report.
  it("explains that the connection has no ksqlDB server, without calling one", async () => {
    const calls: string[] = [];
    setInvokeHandlers({
      connection_list: () => [{ ...CONFIGURED, ksqldbEndpoint: null }],
      ksql_stream_for_topic: () => {
        calls.push("ksql_stream_for_topic");
        return null;
      },
    });
    renderWithClient(<TopicKsqlTab connectionId="1" topicName="orders" />);

    expect(await screen.findByTestId("ksql-not-configured")).toHaveTextContent(
      /no ksqlDB server/i,
    );
    expect(calls).toEqual([]);
  });

  it("treats a whitespace-only endpoint as no endpoint", async () => {
    setInvokeHandlers({
      connection_list: () => [{ ...CONFIGURED, ksqldbEndpoint: "   " }],
    });
    renderWithClient(<TopicKsqlTab connectionId="1" topicName="orders" />);

    expect(await screen.findByTestId("ksql-not-configured")).toBeInTheDocument();
  });

  // ksqlDB cannot SELECT from a raw topic, which is the single most confusing
  // thing about it. Stated plainly, with the statement that fixes it.
  it("says no stream is registered, and shows the CREATE STREAM that would fix it", async () => {
    setInvokeHandlers({
      connection_list: () => [CONFIGURED],
      ksql_stream_for_topic: () => null,
    });
    renderWithClient(<TopicKsqlTab connectionId="1" topicName="order-events" />);

    const notice = await screen.findByTestId("ksql-no-stream");
    expect(notice).toHaveTextContent(/cannot\s+query a raw topic/i);
    // The suggested identifier is derived from the topic: hyphens are not
    // legal in a ksqlDB identifier.
    expect(notice).toHaveTextContent("ORDER_EVENTS");
    expect(notice).toHaveTextContent("KAFKA_TOPIC='order-events'");
  });

  it("opens on a live tail of the stream registered over the topic", async () => {
    setInvokeHandlers({
      connection_list: () => [CONFIGURED],
      ksql_stream_for_topic: () => "ORDERS_STREAM",
    });
    renderWithClient(<TopicKsqlTab connectionId="1" topicName="orders" />);

    expect(await screen.findByLabelText("ksqlDB query")).toHaveValue(
      "SELECT * FROM ORDERS_STREAM EMIT CHANGES;",
    );
  });

  it("says which stream it is tailing, and that Stop ends it", async () => {
    setInvokeHandlers({
      connection_list: () => [CONFIGURED],
      ksql_stream_for_topic: () => "ORDERS_STREAM",
    });
    renderWithClient(<TopicKsqlTab connectionId="1" topicName="orders" />);

    expect(await screen.findByText(/Tailing/)).toHaveTextContent("ORDERS_STREAM");
    expect(screen.getByText(/press Stop to end it/)).toBeInTheDocument();
  });

  it("reports an unreachable ksqlDB as an error rather than as no stream", async () => {
    setInvokeHandlers({
      connection_list: () => [CONFIGURED],
      ksql_stream_for_topic: () => {
        throw new Error("could not reach ksqlDB");
      },
    });
    renderWithClient(<TopicKsqlTab connectionId="1" topicName="orders" />);

    expect(await screen.findByRole("alert")).toHaveTextContent("could not reach ksqlDB");
    expect(screen.queryByTestId("ksql-no-stream")).not.toBeInTheDocument();
  });
});
