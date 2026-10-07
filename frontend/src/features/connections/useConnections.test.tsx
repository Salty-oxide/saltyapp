import { describe, expect, it, vi, beforeEach } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { setInvokeHandlers } from "../../lib/testInvoke";
import {
  CONNECTED_STATUS_POLL_MS,
  IDLE_STATUS_POLL_MS,
  statusPollInterval,
  useConnect,
  useConnectionConnected,
  useConnectionStatus,
  useConnectionsQuery,
  useDeleteConnection,
  useDisconnect,
  useExportConnections,
  useImportConnections,
  useUpdateConnection,
} from "./useConnections";
import { REACHABILITY_POLL_MS, UNREACHABLE_RECHECK_MS } from "./connectionStatusPolling";
import { sampleNewConnection } from "./connectionTestFixtures";
import { useWorkspaceSelectionStore } from "../workspace/useWorkspaceSelectionStore";
import { useMessageViewerStore } from "../workspace/useMessageViewerStore";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

function createWrapper() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );
}

const newConnection = sampleNewConnection();

beforeEach(() => {
  vi.clearAllMocks();
});

describe("useUpdateConnection", () => {
  it("calls connection_update with the id and updated fields", async () => {
    const updated = { id: "1", ...newConnection, createdAt: "now", updatedAt: "now" };
    const connectionUpdate = vi.fn(() => updated);
    setInvokeHandlers({ connection_update: connectionUpdate });

    const { result } = renderHook(() => useUpdateConnection(), { wrapper: createWrapper() });

    result.current.mutate({ id: "1", connection: newConnection });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(connectionUpdate).toHaveBeenCalledWith({ id: "1", newConnection });
  });
});

describe("statusPollInterval", () => {
  it("polls a healthy connected cluster on the slow steady cadence", () => {
    expect(statusPollInterval(true, "REACHABLE")).toBe(REACHABILITY_POLL_MS);
    expect(statusPollInterval(true)).toBe(REACHABILITY_POLL_MS);
  });

  it("rechecks a connected cluster quickly once it has failed a probe", () => {
    expect(statusPollInterval(true, "UNREACHABLE")).toBe(UNREACHABLE_RECHECK_MS);
  });

  // The in-process reads (`connection-connected`, `connection-auth-block`)
  // stay on the 10s cadence: they cost no socket, and the auth breaker can
  // trip at any time.
  it("keeps the in-process reads on their own 10s cadence", () => {
    expect(CONNECTED_STATUS_POLL_MS).toBe(10_000);
  });

  // Every saved connection polls for as long as the app is open, connected or
  // not. At the connected cadence a user with twenty saved production
  // clusters generates ~170k TCP connect/teardowns a day against broker ports
  // while actively using none of them.
  it("backs off substantially for a cluster the user is not connected to", () => {
    expect(statusPollInterval(false)).toBe(IDLE_STATUS_POLL_MS);
    expect(IDLE_STATUS_POLL_MS).toBeGreaterThan(REACHABILITY_POLL_MS);
  });

  it("still checks an idle connection often enough to be useful", () => {
    // A dot that only refreshes every few minutes stops meaning anything.
    expect(IDLE_STATUS_POLL_MS).toBeLessThanOrEqual(60_000);
  });
});

describe("useConnectionStatus", () => {
  // The first answer must not be delayed by the backed-off interval: a
  // connected cluster's dot has to be right as soon as the tree renders.
  it("checks a connected cluster's status immediately on mount", async () => {
    const checkStatus = vi.fn(() => "REACHABLE");
    setInvokeHandlers({ connection_check_status: checkStatus });

    const { result } = renderHook(() => useConnectionStatus("1", true), { wrapper: createWrapper() });

    await waitFor(() => expect(result.current.data).toBe("REACHABLE"));
    expect(checkStatus).toHaveBeenCalledTimes(1);
  });

  // A saved connection with no session is silent: no socket on mount and none
  // on an interval. This poll used to run for every saved cluster for as long
  // as the app was open — a TCP connect/teardown against production broker
  // ports every minute each — and once the dot stopped colouring itself from
  // reachability while disconnected, the only thing left consuming it was a
  // tooltip's wording.
  it("does not touch the broker at all for a disconnected cluster", async () => {
    const checkStatus = vi.fn(() => "UNREACHABLE");
    setInvokeHandlers({ connection_check_status: checkStatus });

    const { result } = renderHook(() => useConnectionStatus("1", false), { wrapper: createWrapper() });

    // Long enough for a mount fetch to have happened if one were going to.
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(checkStatus).not.toHaveBeenCalled();
    expect(result.current.data).toBe("UNKNOWN");
  });

  function clientWrapper(client: QueryClient) {
    return ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );
  }

  // Real traffic is proof of life: a topic listing that just succeeded says
  // the cluster answered, so a TCP probe on top of it is a wasted socket.
  it("skips the probe when a request to the cluster just succeeded", async () => {
    const checkStatus = vi.fn(() => "UNREACHABLE");
    setInvokeHandlers({ connection_check_status: checkStatus });
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    client.setQueryData(["topics", "1"], []);

    const { result } = renderHook(() => useConnectionStatus("1", true), { wrapper: clientWrapper(client) });

    await waitFor(() => expect(result.current.data).toBe("REACHABLE"));
    expect(checkStatus).not.toHaveBeenCalled();
  });

  it("probes for real when there has been no recent traffic", async () => {
    const checkStatus = vi.fn(() => "REACHABLE");
    setInvokeHandlers({ connection_check_status: checkStatus });
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    renderHook(() => useConnectionStatus("1", true), { wrapper: clientWrapper(client) });

    await waitFor(() => expect(checkStatus).toHaveBeenCalledTimes(1));
  });

  // A confirmation of a failure must be a genuine check, or recent cached
  // traffic could mask a cluster that has since died.
  it("never skips the probe while the last result was UNREACHABLE", async () => {
    const checkStatus = vi.fn(() => "UNREACHABLE");
    setInvokeHandlers({ connection_check_status: checkStatus });
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    client.setQueryData(["connection-status", "1"], "UNREACHABLE");
    client.setQueryData(["topics", "1"], []);

    renderHook(() => useConnectionStatus("1", true), { wrapper: clientWrapper(client) });

    await waitFor(() => expect(checkStatus).toHaveBeenCalledTimes(1));
  });

  // The app's own failed request is the earliest signal that something is
  // wrong; confirm it now instead of waiting out the steady interval.
  it("probes immediately when a request to the cluster fails", async () => {
    const checkStatus = vi.fn(() => "UNREACHABLE");
    setInvokeHandlers({ connection_check_status: checkStatus });
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    renderHook(() => useConnectionStatus("1", true), { wrapper: clientWrapper(client) });
    await waitFor(() => expect(checkStatus).toHaveBeenCalledTimes(1));

    await client
      .fetchQuery({ queryKey: ["topics", "1"], queryFn: () => Promise.reject(new Error("timed out")), retry: false })
      .catch(() => undefined);

    await waitFor(() => expect(checkStatus).toHaveBeenCalledTimes(2));
  });

  it("does not probe because a different cluster's request failed", async () => {
    const checkStatus = vi.fn(() => "REACHABLE");
    setInvokeHandlers({ connection_check_status: checkStatus });
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    renderHook(() => useConnectionStatus("1", true), { wrapper: clientWrapper(client) });
    await waitFor(() => expect(checkStatus).toHaveBeenCalledTimes(1));

    await client
      .fetchQuery({ queryKey: ["topics", "2"], queryFn: () => Promise.reject(new Error("x")), retry: false })
      .catch(() => undefined);

    await new Promise((r) => setTimeout(r, 50));
    expect(checkStatus).toHaveBeenCalledTimes(1);
  });

  it("reports UNKNOWN before the first check resolves", () => {
    setInvokeHandlers({ connection_check_status: () => "REACHABLE" });

    const { result } = renderHook(() => useConnectionStatus("1", true), { wrapper: createWrapper() });

    expect(result.current.data).toBe("UNKNOWN");
  });
});

describe("useConnectionConnected", () => {
  it("reflects the connection_is_connected result", async () => {
    setInvokeHandlers({ connection_is_connected: () => true });

    const { result } = renderHook(() => useConnectionConnected("1"), { wrapper: createWrapper() });

    await waitFor(() => expect(result.current.data).toBe(true));
  });

  it("defaults to false before the query resolves", () => {
    setInvokeHandlers({ connection_is_connected: () => true });
    const { result } = renderHook(() => useConnectionConnected("1"), { wrapper: createWrapper() });
    expect(result.current.data).toBe(false);
  });
});

describe("useConnect", () => {
  it("calls connection_connect with the id and returns the resulting status", async () => {
    const connectionConnect = vi.fn(() => "REACHABLE");
    setInvokeHandlers({ connection_connect: connectionConnect });

    const { result } = renderHook(() => useConnect("1"), { wrapper: createWrapper() });
    result.current.mutate();

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(connectionConnect).toHaveBeenCalledWith({ id: "1" });
    expect(result.current.data).toBe("REACHABLE");
  });
});

describe("useDisconnect", () => {
  it("calls connection_disconnect with the id", async () => {
    const connectionDisconnect = vi.fn(() => undefined);
    setInvokeHandlers({ connection_disconnect: connectionDisconnect });

    const { result } = renderHook(() => useDisconnect(), { wrapper: createWrapper() });
    result.current.mutate("1");

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(connectionDisconnect).toHaveBeenCalledWith({ id: "1" });
  });
});

describe("useDeleteConnection", () => {
  it("clears the deleted connection's selection and viewed message from the workspace", async () => {
    const connectionDelete = vi.fn(() => undefined);
    setInvokeHandlers({ connection_delete: connectionDelete });
    const clearSelectionForConnection = vi.spyOn(useWorkspaceSelectionStore.getState(), "clearForConnection");
    const clearMessageForConnection = vi.spyOn(useMessageViewerStore.getState(), "clearForConnection");

    const { result } = renderHook(() => useDeleteConnection(), { wrapper: createWrapper() });
    result.current.mutate("conn-1");

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(clearSelectionForConnection).toHaveBeenCalledWith("conn-1");
    expect(clearMessageForConnection).toHaveBeenCalledWith("conn-1");
  });
});

describe("useExportConnections", () => {
  it("calls connections_export with the given ids and path", async () => {
    const connectionsExport = vi.fn(() => undefined);
    setInvokeHandlers({ connections_export: connectionsExport });

    const { result } = renderHook(() => useExportConnections(), { wrapper: createWrapper() });
    result.current.mutate({ ids: ["1"], path: "/tmp/export.json" });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(connectionsExport).toHaveBeenCalledWith({ ids: ["1"], path: "/tmp/export.json" });
  });

  it("passes null ids through for an export-all", async () => {
    const connectionsExport = vi.fn(() => undefined);
    setInvokeHandlers({ connections_export: connectionsExport });

    const { result } = renderHook(() => useExportConnections(), { wrapper: createWrapper() });
    result.current.mutate({ ids: null, path: "/tmp/all.json" });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(connectionsExport).toHaveBeenCalledWith({ ids: null, path: "/tmp/all.json" });
  });
});

describe("useImportConnections", () => {
  it("calls connections_import with the path and returns the summary", async () => {
    const connectionsImport = vi.fn(() => ({ imported: 2, skipped: 1 }));
    setInvokeHandlers({ connections_import: connectionsImport });

    const { result } = renderHook(() => useImportConnections(), { wrapper: createWrapper() });
    result.current.mutate("/tmp/import.json");

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(connectionsImport).toHaveBeenCalledWith({ path: "/tmp/import.json" });
    expect(result.current.data).toEqual({ imported: 2, skipped: 1 });
  });

  it("invalidates the connections list on success", async () => {
    const connectionList = vi.fn(() => []);
    setInvokeHandlers({ connection_list: connectionList, connections_import: () => ({ imported: 1, skipped: 0 }) });
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );

    const { result: listResult } = renderHook(() => useConnectionsQuery(), { wrapper });
    await waitFor(() => expect(listResult.current.isSuccess).toBe(true));
    expect(connectionList).toHaveBeenCalledTimes(1);

    const { result: importResult } = renderHook(() => useImportConnections(), { wrapper });
    importResult.current.mutate("/tmp/import.json");
    await waitFor(() => expect(importResult.current.isSuccess).toBe(true));

    await waitFor(() => expect(connectionList).toHaveBeenCalledTimes(2));
  });
});
