import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { Connection, PublishOutcome } from "../../lib/tauri";
import { useTabsStore } from "../tabs/useTabsStore";
import { PublishTab } from "./PublishTab";
import { usePublishDraftStore } from "./usePublishDraftStore";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

function renderWithClient(ui: React.ReactElement) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

function connection(overrides: Partial<Connection> = {}): Connection {
  return {
    id: "conn-1",
    name: "Local Kafka",
    bootstrapServers: "localhost:9092",
    kafkaVersion: "3.7",
    zookeeperEnabled: false,
    zookeeperHost: null,
    zookeeperPort: null,
    zookeeperChrootPath: null,
    securityProtocol: "PLAINTEXT",
    saslMechanism: null,
    saslUsername: null,
    saslPassword: null,
    saslOauthUrl: null,
    schemaRegistryEndpoint: null,
    schemaRegistryBasicAuthCredentials: null,
    ksqldbEndpoint: null,
    ksqldbBasicAuthCredentials: null,
    schemaRegistryTrustStoreLocation: null,
    schemaRegistryTrustStorePassword: null,
    schemaRegistryKeystoreLocation: null,
    schemaRegistryKeystorePassword: null,
    schemaRegistryKeystoreKeyPassword: null,
    sslTruststoreLocation: null,
    sslTruststorePassword: null,
    sslKeystoreLocation: null,
    sslKeystorePassword: null,
    sslKeystoreKeyPassword: null,
    allowPublishing: true,
    createdAt: "now",
    updatedAt: "now",
    ...overrides,
  };
}

const OK_OUTCOME: PublishOutcome = {
  delivered: [{ index: 0, partition: 2, offset: 41 }],
  failure: null,
  notAttempted: 0,
};

/**
 * The happy-path backend: one publish-enabled connection, connected, with no
 * cached denial. Individual tests override whichever handler they are about.
 */
function handlers(overrides: Record<string, (args: any) => unknown> = {}) {
  return {
    connection_list: () => [connection()],
    connection_is_connected: () => true,
    connection_write_denied_reason: () => null,
    connection_publish_messages: () => OK_OUTCOME,
    ...overrides,
  };
}

function renderTab() {
  return renderWithClient(<PublishTab connectionId="conn-1" topicName="orders" partitionId={2} />);
}

/** Waits until the gates have resolved and the editor is on screen. */
async function editor() {
  return screen.findByTestId("publish-message-1");
}

beforeEach(() => {
  vi.clearAllMocks();
  usePublishDraftStore.setState({ messagesByTab: {} });
  useTabsStore.setState({ activeTabId: "tab-1" });
});

describe("PublishTab gates", () => {
  it("shows no form and publishes nothing when the cluster is not connected", async () => {
    const publish = vi.fn(() => OK_OUTCOME);
    setInvokeHandlers(
      handlers({ connection_is_connected: () => false, connection_publish_messages: publish }),
    );
    renderTab();

    expect(await screen.findByTestId("publish-gate")).toHaveTextContent("not connected");
    expect(screen.queryByTestId("publish-message-1")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Publish" })).not.toBeInTheDocument();
    expect(publish).not.toHaveBeenCalled();
  });

  it("shows no form when the connection does not allow publishing", async () => {
    setInvokeHandlers(handlers({ connection_list: () => [connection({ allowPublishing: false })] }));
    renderTab();

    const gate = await screen.findByTestId("publish-gate");
    expect(gate).toHaveTextContent("Publishing is disabled for this connection");
    // Says where to turn it on, because a gate with no remedy is just a wall.
    expect(gate).toHaveTextContent("Allow publishing messages to this cluster");
    expect(screen.queryByTestId("publish-message-1")).not.toBeInTheDocument();
  });

  it("shows no form when the broker has already refused a publish to this topic", async () => {
    setInvokeHandlers(
      handlers({
        connection_write_denied_reason: () => "you do not have write access to \"orders\".",
      }),
    );
    renderTab();

    const gate = await screen.findByTestId("publish-gate");
    expect(gate).toHaveTextContent("You do not have write access to orders");
    expect(gate).toHaveTextContent("blocked until you reconnect");
    expect(screen.queryByTestId("publish-message-1")).not.toBeInTheDocument();
  });

  it("reports being disconnected ahead of every other reason, matching the backend order", async () => {
    setInvokeHandlers(
      handlers({
        connection_is_connected: () => false,
        connection_list: () => [connection({ allowPublishing: false })],
        connection_write_denied_reason: () => "denied",
      }),
    );
    renderTab();

    expect(await screen.findByTestId("publish-gate")).toHaveTextContent("not connected");
  });

  it("shows the editor once every gate is clear", async () => {
    setInvokeHandlers(handlers());
    renderTab();

    expect(await editor()).toBeInTheDocument();
    expect(screen.getByTestId("publish-target")).toHaveTextContent("Local Kafka");
    expect(screen.getByTestId("publish-target")).toHaveTextContent("orders");
    expect(screen.getByTestId("publish-target")).toHaveTextContent("2");
  });
});

describe("PublishTab editor", () => {
  it("starts with a single message", async () => {
    setInvokeHandlers(handlers());
    renderTab();

    expect(await editor()).toBeInTheDocument();
    expect(screen.queryByTestId("publish-message-2")).not.toBeInTheDocument();
    expect(screen.getByTestId("publish-summary")).toHaveTextContent("1 message");
  });

  it("adds, duplicates and removes messages", async () => {
    setInvokeHandlers(handlers());
    const user = userEvent.setup();
    renderTab();
    await editor();

    await user.click(screen.getByRole("button", { name: "Add message" }));
    expect(await screen.findByTestId("publish-message-2")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Duplicate message 1" }));
    expect(await screen.findByTestId("publish-message-3")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Remove message 3" }));
    await waitFor(() => expect(screen.queryByTestId("publish-message-3")).not.toBeInTheDocument());
  });

  it("will not let the last message be removed", async () => {
    setInvokeHandlers(handlers());
    renderTab();
    await editor();

    expect(screen.getByRole("button", { name: "Remove message 1" })).toBeDisabled();
  });

  it("duplicating copies what was typed", async () => {
    setInvokeHandlers(handlers());
    const user = userEvent.setup();
    renderTab();
    await editor();

    await user.type(screen.getByLabelText("Value on message 1"), "hello");
    await user.click(screen.getByRole("button", { name: "Duplicate message 1" }));

    expect(await screen.findByLabelText("Value on message 2")).toHaveValue("hello");
  });

  it("keeps what was typed across an unmount, so a topic switch does not lose it", async () => {
    setInvokeHandlers(handlers());
    const user = userEvent.setup();
    const { unmount } = renderTab();
    await editor();

    await user.type(screen.getByLabelText("Value on message 1"), "keep me");
    unmount();
    renderTab();

    expect(await screen.findByLabelText("Value on message 1")).toHaveValue("keep me");
  });

  it("shows a running byte total", async () => {
    setInvokeHandlers(handlers());
    const user = userEvent.setup();
    renderTab();
    await editor();

    await user.type(screen.getByLabelText("Value on message 1"), "12345");
    await waitFor(() =>
      expect(screen.getByTestId("publish-summary")).toHaveTextContent("5 bytes"),
    );
  });

  it("adds and removes headers", async () => {
    setInvokeHandlers(handlers());
    const user = userEvent.setup();
    renderTab();
    await editor();

    await user.click(screen.getByRole("button", { name: "Add header to message 1" }));
    const name = await screen.findByLabelText("Header 1 name on message 1");
    await user.type(name, "content-type");
    expect(name).toHaveValue("content-type");

    await user.click(screen.getByRole("button", { name: "Remove header 1 from message 1" }));
    await waitFor(() =>
      expect(screen.queryByLabelText("Header 1 name on message 1")).not.toBeInTheDocument(),
    );
  });

  it("edits a header's value, including switching its encoding", async () => {
    setInvokeHandlers(handlers());
    const user = userEvent.setup();
    renderTab();
    await editor();

    await user.click(screen.getByRole("button", { name: "Add header to message 1" }));
    await user.type(await screen.findByLabelText("Header 1 name on message 1"), "trace-id");
    await user.type(screen.getByLabelText("Header 1 value on message 1"), "AAEC");

    const message = screen.getByTestId("publish-message-1");
    // Two "Text ▾" toggles now: the value field's and the header value's. The
    // header's is the last one.
    const textToggles = within(message).getAllByRole("button", { name: /Text/ });
    await user.click(textToggles[textToggles.length - 1]);
    await user.click(within(message).getByRole("option", { name: /Base64/ }));

    // 4 base64 characters decode to 3 bytes, plus the 8-byte header name.
    await waitFor(() =>
      expect(screen.getByTestId("publish-summary")).toHaveTextContent("11 bytes"),
    );
  });

  it("does not offer a form for a connection that no longer exists", async () => {
    // A connection deleted while its Publish tab was open: better a plain
    // statement than a form whose target is gone.
    setInvokeHandlers(handlers({ connection_list: () => [] }));
    renderTab();

    expect(await screen.findByText("Connection not found.")).toBeInTheDocument();
    expect(screen.queryByTestId("publish-message-1")).not.toBeInTheDocument();
  });

  it("hides the text box for a null field, which has nothing to type into", async () => {
    setInvokeHandlers(handlers());
    renderTab();
    await editor();

    // The key starts as null, the value as text.
    expect(screen.queryByLabelText("Key on message 1")).not.toBeInTheDocument();
    expect(screen.getByLabelText("Value on message 1")).toBeInTheDocument();
  });

  it("disables Publish and explains why when a message is invalid", async () => {
    setInvokeHandlers(handlers());
    const user = userEvent.setup();
    renderTab();
    await editor();

    // Switch the value to JSON and type something that is not JSON. The shared
    // Dropdown names its toggle button after the current option, so the value
    // field's is the one reading "Text" (the key field's reads "Null").
    const message = screen.getByTestId("publish-message-1");
    await user.click(within(message).getByRole("button", { name: /Text/ }));
    await user.click(within(message).getByRole("option", { name: /JSON/ }));
    // `{{` is how userEvent types a literal brace — a bare `{` is a key descriptor.
    await user.type(screen.getByLabelText("Value on message 1"), "{{oops");

    await waitFor(() => expect(screen.getByRole("button", { name: "Publish" })).toBeDisabled());
    expect(screen.getByTestId("publish-message-1")).toHaveTextContent("Value is not valid JSON");
  });
});

describe("PublishTab confirmation", () => {
  it("does not publish until the confirmation is accepted", async () => {
    const publish = vi.fn(() => OK_OUTCOME);
    setInvokeHandlers(handlers({ connection_publish_messages: publish }));
    const user = userEvent.setup();
    renderTab();
    await editor();

    await user.type(screen.getByLabelText("Value on message 1"), "hi");
    await user.click(screen.getByRole("button", { name: "Publish" }));

    const confirm = await screen.findByTestId("publish-confirm");
    expect(publish).not.toHaveBeenCalled();
    // States the destination in full: a wrong publish is usually a wrong target.
    expect(confirm).toHaveTextContent("Local Kafka");
    expect(confirm).toHaveTextContent("orders");
    expect(confirm).toHaveTextContent("acks=all");
    expect(confirm).toHaveTextContent("cannot be undone");
  });

  it("cancelling publishes nothing and returns to the editor", async () => {
    const publish = vi.fn(() => OK_OUTCOME);
    setInvokeHandlers(handlers({ connection_publish_messages: publish }));
    const user = userEvent.setup();
    renderTab();
    await editor();

    await user.click(screen.getByRole("button", { name: "Publish" }));
    await screen.findByTestId("publish-confirm");
    await user.click(screen.getByRole("button", { name: "Cancel" }));

    await waitFor(() => expect(screen.queryByTestId("publish-confirm")).not.toBeInTheDocument());
    expect(publish).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Publish" })).toBeInTheDocument();
  });

  it("confirming publishes exactly once, with the entered messages", async () => {
    const publish = vi.fn(() => OK_OUTCOME);
    setInvokeHandlers(handlers({ connection_publish_messages: publish }));
    const user = userEvent.setup();
    renderTab();
    await editor();

    await user.type(screen.getByLabelText("Value on message 1"), "hi");
    await user.click(screen.getByRole("button", { name: "Publish" }));
    await user.click(await screen.findByRole("button", { name: "Publish to orders[2]" }));

    await waitFor(() => expect(publish).toHaveBeenCalledTimes(1));
    expect(publish).toHaveBeenCalledWith(
      expect.objectContaining({
        id: "conn-1",
        topic: "orders",
        partition: 2,
        messages: [
          {
            key: { encoding: "null", text: "" },
            value: { encoding: "text", text: "hi" },
            headers: [],
          },
        ],
      }),
    );
  });

  it("a second click while in flight does not publish twice", async () => {
    // The one failure mode a confirmation step can itself introduce.
    let release: ((outcome: PublishOutcome) => void) | undefined;
    const publish = vi.fn(() => new Promise<PublishOutcome>((resolve) => (release = resolve)));
    setInvokeHandlers(handlers({ connection_publish_messages: publish }));
    const user = userEvent.setup();
    renderTab();
    await editor();

    await user.click(screen.getByRole("button", { name: "Publish" }));
    const confirmButton = await screen.findByRole("button", { name: "Publish to orders[2]" });
    await user.click(confirmButton);

    await waitFor(() => expect(screen.getByRole("button", { name: "Publishing…" })).toBeDisabled());
    await user.click(screen.getByRole("button", { name: "Publishing…" }));
    expect(publish).toHaveBeenCalledTimes(1);

    release?.(OK_OUTCOME);
    await waitFor(() => expect(screen.getByTestId("publish-result")).toBeInTheDocument());
  });
});

describe("PublishTab result", () => {
  it("reports what landed and clears the rows after a clean publish", async () => {
    setInvokeHandlers(handlers());
    const user = userEvent.setup();
    renderTab();
    await editor();

    await user.type(screen.getByLabelText("Value on message 1"), "hi");
    await user.click(screen.getByRole("button", { name: "Publish" }));
    await user.click(await screen.findByRole("button", { name: "Publish to orders[2]" }));

    const result = await screen.findByTestId("publish-result");
    expect(result).toHaveTextContent("Published 1 message");
    expect(result).toHaveTextContent("41");

    // Rows reset, so the same batch cannot be fired again by reflex — while the
    // result stays on screen.
    await waitFor(() => expect(screen.getByLabelText("Value on message 1")).toHaveValue(""));
    expect(screen.getByTestId("publish-result")).toBeInTheDocument();
  });

  it("reports a partial publish: what landed, what failed, and what was skipped", async () => {
    const outcome: PublishOutcome = {
      delivered: [{ index: 0, partition: 2, offset: 7 }],
      failure: { index: 1, kind: "validation", reason: "Broker: Message size too large" },
      notAttempted: 1,
    };
    setInvokeHandlers(handlers({ connection_publish_messages: () => outcome }));
    const user = userEvent.setup();
    renderTab();
    await editor();

    await user.type(screen.getByLabelText("Value on message 1"), "a");
    await user.click(screen.getByRole("button", { name: "Add message" }));
    await user.type(await screen.findByLabelText("Value on message 2"), "b");
    await user.click(screen.getByRole("button", { name: "Add message" }));
    await user.type(await screen.findByLabelText("Value on message 3"), "c");

    await user.click(screen.getByRole("button", { name: "Publish" }));
    await user.click(await screen.findByRole("button", { name: "Publish to orders[2]" }));

    const result = await screen.findByTestId("publish-result");
    expect(result).toHaveTextContent("Publish failed after 1 message");
    expect(result).toHaveTextContent("Message 2 failed: Broker: Message size too large");
    expect(result).toHaveTextContent("1 later message was not attempted");

    // The rows survive a partial publish, so the failed message can be fixed
    // rather than retyped — and the one that landed is marked.
    expect(screen.getByLabelText("Value on message 2")).toHaveValue("b");
    expect(screen.getByTestId("publish-message-1")).toHaveTextContent("published");
  });

  it("shows the backend's own message when the publish is refused outright", async () => {
    setInvokeHandlers(
      handlers({
        connection_publish_messages: () => {
          throw new Error("authorization error: you do not have write access to \"orders\"");
        },
      }),
    );
    const user = userEvent.setup();
    renderTab();
    await editor();

    await user.click(screen.getByRole("button", { name: "Publish" }));
    await user.click(await screen.findByRole("button", { name: "Publish to orders[2]" }));

    expect(await screen.findByTestId("publish-error")).toHaveTextContent(
      "you do not have write access",
    );
    // And the confirmation is dismissed rather than left armed over an error.
    expect(screen.queryByTestId("publish-confirm")).not.toBeInTheDocument();
  });
});
