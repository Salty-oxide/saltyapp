import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { App, queryClient } from "./App";
import { useJsonViewerTabsStore } from "./features/tabs/useJsonViewerTabsStore";
import { useTabsStore } from "./features/tabs/useTabsStore";
import { useWorkspaceSelectionStore } from "./features/workspace/useWorkspaceSelectionStore";
import { useMessageViewerStore } from "./features/workspace/useMessageViewerStore";
import { dataTabCacheKey, useTabDataStore } from "./features/workspace/useTabDataStore";
import { useTreeUiStore } from "./features/connections/useTreeUiStore";
import { sampleNewConnection } from "./features/connections/connectionTestFixtures";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));
vi.mock("@tauri-apps/api/event", () => ({ listen: vi.fn(() => Promise.resolve(() => {})) }));
const save = vi.fn();
const open = vi.fn();
vi.mock("@tauri-apps/plugin-dialog", () => ({
  save: (...args: unknown[]) => save(...args),
  open: (...args: unknown[]) => open(...args),
}));
let capturedFocusHandler: ((focused: boolean) => void) | null = null;
vi.mock("./lib/appWindow", () => ({
  closeAppWindow: vi.fn(),
  onCloseRequested: vi.fn(() => Promise.resolve(() => {})),
  onWindowFocusChanged: vi.fn((handler: (focused: boolean) => void) => {
    capturedFocusHandler = handler;
    return Promise.resolve(() => {});
  }),
}));

beforeEach(() => {
  // The app's QueryClient is module-level, so cluster listings cached by one
  // case would otherwise be served to the next within their stale window.
  queryClient.clear();
  useTabsStore.setState({ tabs: [], activeTabId: null, error: null });
  useJsonViewerTabsStore.setState({ tabs: [] });
  useWorkspaceSelectionStore.setState({ selection: null, activeTabId: null, byTab: {} });
  useMessageViewerStore.setState({ message: null, connectionId: null, topic: null, partitionId: undefined, activeTabId: null, byTab: {} });
  useTabDataStore.setState({ messagesByTab: {}, totalMatchingByTab: {} });
  useTreeUiStore.setState({ expanded: {}, searchText: {}, hideEmptyConsumerGroups: {} });
  capturedFocusHandler = null;
});

describe("App", () => {
  it("renders the shell with tab bar, sidebar, and bottom panel", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "connection_list") return Promise.resolve([]);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });

    render(<App />);

    expect(await screen.findByText("No connections yet. Add one to get started.")).toBeInTheDocument();
    expect(screen.getByText("Select a cluster, broker, or topic.")).toBeInTheDocument();
    expect(screen.getByLabelText("New tab")).toBeInTheDocument();
  });

  it("opens the New Connection modal when the sidebar button is clicked", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "connection_list") return Promise.resolve([]);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("No connections yet. Add one to get started.");

    expect(screen.queryByRole("dialog", { name: "New Connection" })).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Add Cluster" }));

    expect(screen.getByRole("dialog", { name: "New Connection" })).toBeInTheDocument();
  });

  it("saves, connects, and adds the new connection to the sidebar when Add is submitted with valid fields", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    const created = {
      ...sampleNewConnection({ name: "Local Kafka", bootstrapServers: "localhost:9092" }),
      id: "conn-1",
      createdAt: "2026-08-22T00:00:00Z",
      updatedAt: "2026-08-22T00:00:00Z",
    };
    let listedConnections: unknown[] = [];
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "connection_list") return Promise.resolve(listedConnections);
      if (command === "connection_create") {
        listedConnections = [created];
        return Promise.resolve(created);
      }
      if (command === "connection_connect") return Promise.resolve("REACHABLE");
      if (command === "connection_check_status") return Promise.resolve("REACHABLE");
      if (command === "connection_is_connected") return Promise.resolve(true);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("No connections yet. Add one to get started.");

    await user.click(screen.getByRole("button", { name: "Add Cluster" }));
    await user.type(screen.getByLabelText("Cluster name"), "Local Kafka");
    await user.type(screen.getByLabelText("Bootstrap servers"), "localhost:9092");
    await user.click(screen.getByRole("button", { name: "Add" }));

    await waitFor(() =>
      expect(invoke).toHaveBeenCalledWith("connection_create", expect.objectContaining({ newConnection: expect.anything() })),
    );
    await waitFor(() => expect(invoke).toHaveBeenCalledWith("connection_connect", { id: "conn-1" }));
    expect(await screen.findByText("Local Kafka")).toBeInTheDocument();
    expect(screen.queryByRole("dialog", { name: "New Connection" })).not.toBeInTheDocument();
  });

  it("still saves the connection, closes the modal, and shows it in the sidebar when the auto-connect attempt fails", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    const created = {
      ...sampleNewConnection({ name: "Local Kafka", bootstrapServers: "localhost:9092" }),
      id: "conn-1",
      createdAt: "2026-08-22T00:00:00Z",
      updatedAt: "2026-08-22T00:00:00Z",
    };
    let listedConnections: unknown[] = [];
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "connection_list") return Promise.resolve(listedConnections);
      if (command === "connection_create") {
        listedConnections = [created];
        return Promise.resolve(created);
      }
      if (command === "connection_connect") return Promise.reject(new Error("unreachable"));
      if (command === "connection_check_status") return Promise.resolve("UNREACHABLE");
      if (command === "connection_is_connected") return Promise.resolve(false);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("No connections yet. Add one to get started.");

    await user.click(screen.getByRole("button", { name: "Add Cluster" }));
    await user.type(screen.getByLabelText("Cluster name"), "Local Kafka");
    await user.type(screen.getByLabelText("Bootstrap servers"), "localhost:9092");
    await user.click(screen.getByRole("button", { name: "Add" }));

    expect(await screen.findByText("Local Kafka")).toBeInTheDocument();
    expect(screen.queryByRole("dialog", { name: "New Connection" })).not.toBeInTheDocument();
  });

  it("closes the modal as soon as the connection is saved, without waiting for the connect attempt", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    const created = {
      ...sampleNewConnection({ name: "Local Kafka", bootstrapServers: "localhost:9092" }),
      id: "conn-1",
      createdAt: "2026-08-22T00:00:00Z",
      updatedAt: "2026-08-22T00:00:00Z",
    };
    let listedConnections: unknown[] = [];
    // A connect that never settles — what a broker rejecting the typed
    // credentials looks like from here for the ~10s the probe takes. The
    // modal must not sit open (and unclosable) for the whole of it.
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "connection_list") return Promise.resolve(listedConnections);
      if (command === "connection_create") {
        listedConnections = [created];
        return Promise.resolve(created);
      }
      if (command === "connection_connect") return new Promise(() => {});
      if (command === "connection_check_status") return new Promise(() => {});
      if (command === "connection_is_connected") return Promise.resolve(false);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("No connections yet. Add one to get started.");

    await user.click(screen.getByRole("button", { name: "Add Cluster" }));
    await user.type(screen.getByLabelText("Cluster name"), "Local Kafka");
    await user.type(screen.getByLabelText("Bootstrap servers"), "localhost:9092");
    await user.click(screen.getByRole("button", { name: "Add" }));

    await waitFor(() => expect(screen.queryByRole("dialog", { name: "New Connection" })).not.toBeInTheDocument());
    await waitFor(() => expect(invoke).toHaveBeenCalledWith("connection_connect", { id: "conn-1" }));
    expect(await screen.findByText("Local Kafka")).toBeInTheDocument();
  });

  it("keeps the modal open, with the failure shown, when saving the connection itself fails", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "connection_list") return Promise.resolve([]);
      if (command === "connection_create") return Promise.reject(new Error("a connection named Local Kafka already exists"));
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("No connections yet. Add one to get started.");

    await user.click(screen.getByRole("button", { name: "Add Cluster" }));
    await user.type(screen.getByLabelText("Cluster name"), "Local Kafka");
    await user.type(screen.getByLabelText("Bootstrap servers"), "localhost:9092");
    await user.click(screen.getByRole("button", { name: "Add" }));

    const dialog = await screen.findByRole("dialog", { name: "New Connection" });
    await waitFor(() => expect(within(dialog).getByRole("alert")).toHaveTextContent("already exists"));
  });

  it("exports every connection when Export All is clicked", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "connection_list") return Promise.resolve([]);
      if (command === "connections_export") return Promise.resolve(undefined);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    save.mockResolvedValue("/tmp/salty-connections.json");
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("No connections yet. Add one to get started.");

    await user.click(screen.getByRole("button", { name: "Export All" }));

    expect(save).toHaveBeenCalledWith(
      expect.objectContaining({
        defaultPath: "salty-connections.json",
        filters: [{ name: "JSON", extensions: ["json"] }],
      }),
    );
    await waitFor(() =>
      expect(invoke).toHaveBeenCalledWith("connections_export", {
        ids: null,
        path: "/tmp/salty-connections.json",
      }),
    );
  });

  it("does not export when the Export All save dialog is cancelled", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "connection_list") return Promise.resolve([]);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    save.mockResolvedValue(null);
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("No connections yet. Add one to get started.");
    vi.mocked(invoke).mockClear();

    await user.click(screen.getByRole("button", { name: "Export All" }));

    expect(invoke).not.toHaveBeenCalledWith("connections_export", expect.anything());
  });

  it("imports connections and shows a summary alert when Import is clicked", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "connection_list") return Promise.resolve([]);
      if (command === "connections_import") return Promise.resolve({ imported: 2, skipped: 1 });
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    open.mockResolvedValue("/tmp/salty-connections.json");
    const alertSpy = vi.spyOn(window, "alert").mockImplementation(() => {});
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("No connections yet. Add one to get started.");

    await user.click(screen.getByRole("button", { name: "Import" }));

    expect(open).toHaveBeenCalledWith(
      expect.objectContaining({ filters: [{ name: "JSON", extensions: ["json"] }], multiple: false }),
    );
    await waitFor(() => expect(alertSpy).toHaveBeenCalledWith(expect.stringMatching(/imported 2.*skipped 1/i)));
  });

  it("does not import when the Import open dialog is cancelled", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "connection_list") return Promise.resolve([]);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    open.mockResolvedValue(null);
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("No connections yet. Add one to get started.");
    vi.mocked(invoke).mockClear();

    await user.click(screen.getByRole("button", { name: "Import" }));

    expect(invoke).not.toHaveBeenCalledWith("connections_import", expect.anything());
  });

  it("opens the Settings panel via the gear icon and shows a closable Settings tab", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "connection_list") return Promise.resolve([]);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("No connections yet. Add one to get started.");

    await user.click(screen.getByLabelText("Open settings"));

    expect(screen.getByRole("heading", { name: "Settings" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Settings" })).toBeInTheDocument();

    await user.click(screen.getByLabelText("Close tab Settings"));

    expect(screen.queryByRole("heading", { name: "Settings" })).not.toBeInTheDocument();
    expect(screen.getByText("Select a cluster, broker, or topic.")).toBeInTheDocument();
  });

  it("keeps Settings open (like a JSON viewer tab) when switching away, and reactivates it when its pill is clicked again", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "tab_create") return Promise.resolve({ id: "tab-1", name: "Tab 1" });
      if (command === "connection_list") return Promise.resolve([]);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("No connections yet. Add one to get started.");

    await user.click(screen.getByLabelText("Open settings"));
    expect(screen.getByRole("heading", { name: "Settings" })).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "Tab 1" }));

    expect(screen.queryByRole("heading", { name: "Settings" })).not.toBeInTheDocument();
    expect(screen.getByText("Select a cluster, broker, or topic.")).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Settings" })).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Settings" })).toHaveAttribute("aria-selected", "false");

    await user.click(screen.getByRole("tab", { name: "Settings" }));

    expect(screen.getByRole("heading", { name: "Settings" })).toBeInTheDocument();
  });

  it("does not render the right pane until a message is selected", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "connection_list") return Promise.resolve([]);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });

    render(<App />);
    await screen.findByText("No connections yet. Add one to get started.");

    expect(screen.queryByTestId("resizable-pane-right")).not.toBeInTheDocument();
  });

  it("hides the left sidebar while a JSON/XML viewer tab is active, and restores it when switching away", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "connection_list") return Promise.resolve([]);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });

    render(<App />);
    await screen.findByText("No connections yet. Add one to get started.");
    expect(screen.getByTestId("resizable-pane-left")).toBeInTheDocument();

    const jsonTabId = useJsonViewerTabsStore.getState().openTab("Partition 0 · Offset 1", { a: 1 });
    useTabsStore.getState().selectTab(jsonTabId);

    await waitFor(() => expect(screen.getByTestId("resizable-pane-left")).not.toBeVisible());

    useTabsStore.setState({ activeTabId: null });

    await waitFor(() => expect(screen.getByTestId("resizable-pane-left")).toBeVisible());
  });

  it("keeps the connection tree's expanded state when switching to a JSON tab and back", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "connection_list") return Promise.resolve([{ id: "1", name: "Local Kafka" }]);
      if (command === "connection_check_status") return Promise.resolve("REACHABLE");
      if (command === "connection_is_connected") return Promise.resolve(true);
      if (command === "connection_list_brokers") return Promise.resolve([]);
      if (command === "connection_list_topics") return Promise.resolve([]);
      if (command === "connection_list_consumer_groups") return Promise.resolve([]);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("Local Kafka");
    await user.click(await screen.findByLabelText("Expand Local Kafka"));
    expect(screen.getByLabelText("Collapse Local Kafka")).toBeInTheDocument();

    const jsonTabId = useJsonViewerTabsStore.getState().openTab("Partition 0 · Offset 1", { a: 1 });
    useTabsStore.getState().selectTab(jsonTabId);
    await waitFor(() => expect(screen.getByTestId("resizable-pane-left")).not.toBeVisible());

    useTabsStore.setState({ activeTabId: null });
    await waitFor(() => expect(screen.getByTestId("resizable-pane-left")).toBeVisible());

    expect(screen.getByLabelText("Collapse Local Kafka")).toBeInTheDocument();
  });

  it("clears the middle pane's selection and the tree's highlighted row when a new tab is opened", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    let nextTabId = 1;
    vi.mocked(invoke).mockImplementation((command: string, args?: unknown) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "tab_create") {
        const id = `tab-${nextTabId++}`;
        const name = (args as { name?: string } | undefined)?.name ?? "New Tab";
        return Promise.resolve({ id, name });
      }
      if (command === "connection_list")
        return Promise.resolve([
          {
            id: "1",
            name: "Local Kafka",
            bootstrapServers: "localhost:9092",
            securityProtocol: "PLAINTEXT",
            saslMechanism: null,
            saslUsername: null,
            createdAt: "2026-08-18T00:00:00Z",
            updatedAt: "2026-08-18T00:00:00Z",
          },
        ]);
      if (command === "connection_check_status") return Promise.resolve("REACHABLE");
      if (command === "connection_is_connected") return Promise.resolve(false);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("Local Kafka");

    await user.click(screen.getByTestId("connection-row-1"));

    expect(screen.getByTestId("connection-row-1")).toHaveClass("connection-row--selected");
    expect(screen.queryByText("Select a cluster, broker, or topic.")).not.toBeInTheDocument();

    await user.click(screen.getByLabelText("New tab"));

    await waitFor(() => expect(screen.getByText("Select a cluster, broker, or topic.")).toBeInTheDocument());
    expect(screen.getByTestId("connection-row-1")).not.toHaveClass("connection-row--selected");
  });

  it("resets the tree's expanded state when switching to a different real tab (unlike a JSON tab excursion, which keeps it)", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    let nextTabId = 1;
    vi.mocked(invoke).mockImplementation((command: string, args?: unknown) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "tab_create") {
        const id = `tab-${nextTabId++}`;
        const name = (args as { name?: string } | undefined)?.name ?? "New Tab";
        return Promise.resolve({ id, name });
      }
      if (command === "connection_list") return Promise.resolve([{ id: "1", name: "Local Kafka" }]);
      if (command === "connection_check_status") return Promise.resolve("REACHABLE");
      if (command === "connection_is_connected") return Promise.resolve(true);
      if (command === "connection_list_brokers") return Promise.resolve([]);
      if (command === "connection_list_topics") return Promise.resolve([]);
      if (command === "connection_list_consumer_groups") return Promise.resolve([]);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("Local Kafka");
    await user.click(await screen.findByLabelText("Expand Local Kafka"));
    expect(screen.getByLabelText("Collapse Local Kafka")).toBeInTheDocument();

    await user.click(screen.getByLabelText("New tab"));

    await waitFor(() => expect(screen.getByLabelText("Expand Local Kafka")).toBeInTheDocument());
  });

  it("preserves an already-visited tab's tree expand state when switching back to it, unlike a brand new tab", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    let nextTabId = 1;
    vi.mocked(invoke).mockImplementation((command: string, args?: unknown) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "tab_create") {
        const id = `tab-${nextTabId++}`;
        const name = (args as { name?: string } | undefined)?.name ?? "New Tab";
        return Promise.resolve({ id, name });
      }
      if (command === "connection_list") return Promise.resolve([{ id: "1", name: "Local Kafka" }]);
      if (command === "connection_check_status") return Promise.resolve("REACHABLE");
      if (command === "connection_is_connected") return Promise.resolve(true);
      if (command === "connection_list_brokers") return Promise.resolve([]);
      if (command === "connection_list_topics") return Promise.resolve([]);
      if (command === "connection_list_consumer_groups") return Promise.resolve([]);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("Local Kafka");
    const firstTabId = useTabsStore.getState().activeTabId as string;
    await user.click(await screen.findByLabelText("Expand Local Kafka"));
    expect(screen.getByLabelText("Collapse Local Kafka")).toBeInTheDocument();

    await user.click(screen.getByLabelText("New tab"));
    expect(screen.getByLabelText("Expand Local Kafka")).toBeInTheDocument();

    useTabsStore.getState().selectTab(firstTabId);

    await waitFor(() => expect(screen.getByLabelText("Collapse Local Kafka")).toBeInTheDocument());
  });

  it("restores a tab's previously-selected item when switching back to it", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    let nextTabId = 1;
    vi.mocked(invoke).mockImplementation((command: string, args?: unknown) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "tab_create") {
        const id = `tab-${nextTabId++}`;
        const name = (args as { name?: string } | undefined)?.name ?? "New Tab";
        return Promise.resolve({ id, name });
      }
      if (command === "connection_list")
        return Promise.resolve([
          {
            id: "1",
            name: "Local Kafka",
            bootstrapServers: "localhost:9092",
            securityProtocol: "PLAINTEXT",
            saslMechanism: null,
            saslUsername: null,
            createdAt: "2026-08-18T00:00:00Z",
            updatedAt: "2026-08-18T00:00:00Z",
          },
        ]);
      if (command === "connection_check_status") return Promise.resolve("REACHABLE");
      if (command === "connection_is_connected") return Promise.resolve(false);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("Local Kafka");
    const firstTabId = useTabsStore.getState().activeTabId;

    await user.click(screen.getByTestId("connection-row-1"));
    expect(screen.getByTestId("connection-row-1")).toHaveClass("connection-row--selected");

    await user.click(screen.getByLabelText("New tab"));
    await waitFor(() => expect(screen.getByText("Select a cluster, broker, or topic.")).toBeInTheDocument());

    useTabsStore.getState().selectTab(firstTabId as string);

    await waitFor(() => expect(screen.getByTestId("connection-row-1")).toHaveClass("connection-row--selected"));
    expect(screen.queryByText("Select a cluster, broker, or topic.")).not.toBeInTheDocument();
  });

  it("wires Tauri's real OS window focus event into React Query's focusManager", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "connection_list") return Promise.resolve([]);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const { focusManager } = await import("@tanstack/react-query");

    render(<App />);
    await screen.findByText("No connections yet. Add one to get started.");

    expect(capturedFocusHandler).not.toBeNull();

    capturedFocusHandler?.(false);
    expect(focusManager.isFocused()).toBe(false);

    capturedFocusHandler?.(true);
    expect(focusManager.isFocused()).toBe(true);
  });

  it("closes the right pane when switching to a different topic in the sidebar while a message is viewed", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "tab_create") return Promise.resolve({ id: "tab-1", name: "Tab 1" });
      if (command === "connection_list")
        return Promise.resolve([
          {
            id: "1",
            name: "Local Kafka",
            bootstrapServers: "localhost:9092",
            securityProtocol: "PLAINTEXT",
            saslMechanism: null,
            saslUsername: null,
            createdAt: "2026-08-18T00:00:00Z",
            updatedAt: "2026-08-18T00:00:00Z",
          },
        ]);
      if (command === "connection_check_status") return Promise.resolve("REACHABLE");
      if (command === "connection_is_connected") return Promise.resolve(true);
      if (command === "connection_list_brokers") return Promise.resolve([]);
      if (command === "connection_list_topics")
        return Promise.resolve([{ name: "orders" }, { name: "payments" }]);
      if (command === "connection_list_consumer_groups") return Promise.resolve([]);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("Local Kafka");
    await user.click(await screen.findByLabelText("Expand Local Kafka"));
    await user.click(screen.getByTestId("category-Topics"));
    await user.click(await screen.findByText("orders"));

    await screen.findByRole("heading", { name: "orders" });

    // Simulates a grid row click (real AG Grid rendering isn't exercised in
    // this test — DataTab.test.tsx already covers that in isolation) —
    // this test's job is the part that wasn't covered anywhere else: does
    // switching topics via a real sidebar click actually hide the right
    // pane end-to-end.
    useMessageViewerStore
      .getState()
      .viewMessage(
        { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: "eA==", payloadSizeBytes: null, headers: [] },
        "1",
        "orders",
      );

    expect(await screen.findByTestId("resizable-pane-right")).toBeInTheDocument();

    await user.click(screen.getByText("payments"));

    await waitFor(() => expect(screen.queryByTestId("resizable-pane-right")).not.toBeInTheDocument());
  });

  it("closes the right pane when switching topics while on a non-Data sub-tab, where DataTab itself isn't even mounted", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    vi.mocked(invoke).mockImplementation((command: string) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "tab_create") return Promise.resolve({ id: "tab-1", name: "Tab 1" });
      if (command === "connection_list")
        return Promise.resolve([
          {
            id: "1",
            name: "Local Kafka",
            bootstrapServers: "localhost:9092",
            securityProtocol: "PLAINTEXT",
            saslMechanism: null,
            saslUsername: null,
            createdAt: "2026-08-18T00:00:00Z",
            updatedAt: "2026-08-18T00:00:00Z",
          },
        ]);
      if (command === "connection_check_status") return Promise.resolve("REACHABLE");
      if (command === "connection_is_connected") return Promise.resolve(true);
      if (command === "connection_list_brokers") return Promise.resolve([]);
      if (command === "connection_list_topics")
        return Promise.resolve([{ name: "orders" }, { name: "payments" }]);
      if (command === "connection_list_consumer_groups") return Promise.resolve([]);
      if (command === "connection_count_topic_messages") return Promise.resolve(0);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("Local Kafka");
    await user.click(await screen.findByLabelText("Expand Local Kafka"));
    await user.click(screen.getByTestId("category-Topics"));
    await user.click(await screen.findByText("orders"));
    await screen.findByRole("heading", { name: "orders" });

    useMessageViewerStore
      .getState()
      .viewMessage(
        { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: "eA==", payloadSizeBytes: null, headers: [] },
        "1",
        "orders",
      );
    expect(await screen.findByTestId("resizable-pane-right")).toBeInTheDocument();

    await user.click(screen.getByRole("tab", { name: "Meta Data" }));
    await user.click(screen.getByText("payments"));

    await waitFor(() => expect(screen.queryByTestId("resizable-pane-right")).not.toBeInTheDocument());
  });

  it("Clear memory only clears the active top-level tab's data, leaving other tabs' cached topics/selection/viewed message untouched", async () => {
    const { invoke } = await import("@tauri-apps/api/core");
    let nextTabId = 1;
    vi.mocked(invoke).mockImplementation((command: string, args?: unknown) => {
      if (command === "tab_list") return Promise.resolve([]);
      if (command === "tab_create") {
        const id = `tab-${nextTabId++}`;
        const name = (args as { name?: string } | undefined)?.name ?? "New Tab";
        return Promise.resolve({ id, name });
      }
      if (command === "connection_list") return Promise.resolve([{ id: "1", name: "Local Kafka" }]);
      if (command === "connection_check_status") return Promise.resolve("REACHABLE");
      if (command === "connection_is_connected") return Promise.resolve(true);
      if (command === "connection_list_brokers") return Promise.resolve([]);
      if (command === "connection_list_topics")
        return Promise.resolve([{ name: "orders" }, { name: "payments" }]);
      if (command === "connection_list_consumer_groups") return Promise.resolve([]);
      if (command === "trim_process_memory") return Promise.resolve(undefined);
      return Promise.reject(new Error(`unexpected command ${command}`));
    });
    const user = userEvent.setup();

    render(<App />);
    await screen.findByText("Local Kafka");
    const tab1Id = useTabsStore.getState().activeTabId as string;

    // Seed tab 1's data while it's the real active tab.
    await user.click(await screen.findByLabelText("Expand Local Kafka"));
    await user.click(screen.getByTestId("category-Topics"));
    await user.click(await screen.findByText("orders"));
    useTabDataStore
      .getState()
      .setTabMessages(dataTabCacheKey(tab1Id, "1", "orders"), [
        { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] },
      ]);
    useMessageViewerStore
      .getState()
      .viewMessage(
        { partition: 0, offset: 1, timestampMs: null, keyBase64: null, payloadBase64: "eA==", payloadSizeBytes: null, headers: [] },
        "1",
        "orders",
      );

    // Open a second tab and seed ITS data while IT is the real active tab.
    // The tree starts fresh/collapsed for a new tab (per the earlier tab-
    // independence fix), so it needs re-expanding here too.
    await user.click(screen.getByLabelText("New tab"));
    const tab2Id = useTabsStore.getState().activeTabId as string;
    expect(tab2Id).not.toBe(tab1Id);
    await user.click(await screen.findByLabelText("Expand Local Kafka"));
    await user.click(screen.getByTestId("category-Topics"));
    await user.click(await screen.findByText("payments"));
    useTabDataStore
      .getState()
      .setTabMessages(dataTabCacheKey(tab2Id, "1", "payments"), [
        { partition: 0, offset: 5, timestampMs: null, keyBase64: null, payloadBase64: null, payloadSizeBytes: null, headers: [] },
      ]);
    useMessageViewerStore
      .getState()
      .viewMessage(
        { partition: 0, offset: 5, timestampMs: null, keyBase64: null, payloadBase64: "eQ==", payloadSizeBytes: null, headers: [] },
        "1",
        "payments",
      );

    // Switch back to tab 1 and clear memory there.
    await user.click(screen.getByRole("tab", { name: "Tab 1" }));
    await user.click(screen.getByLabelText("Clear tab memory"));

    expect(useTabDataStore.getState().messagesByTab[dataTabCacheKey(tab1Id, "1", "orders")]).toBeUndefined();

    // Tab 2's cached data, selection, and viewed message must all survive.
    expect(useTabDataStore.getState().messagesByTab[dataTabCacheKey(tab2Id, "1", "payments")]).toBeDefined();

    await user.click(screen.getByRole("tab", { name: "New Tab" }));
    expect(screen.getByRole("heading", { name: "payments" })).toBeInTheDocument();
    expect(screen.getByTestId("resizable-pane-right")).toBeInTheDocument();
  });
});
