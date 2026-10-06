import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { ClusterResourceTree } from "./ClusterResourceTree";
import { useWorkspaceSelectionStore } from "../workspace/useWorkspaceSelectionStore";
import { useTreeUiStore } from "./useTreeUiStore";
import { usePartitionPanelTabStore } from "./usePartitionPanelTabStore";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

function renderWithClient(ui: React.ReactElement) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

function binding(overrides: Record<string, unknown> = {}) {
  return {
    resourceType: "topic",
    resourceName: "orders",
    patternType: "literal",
    principal: "User:alice",
    host: "*",
    operation: "read",
    permission: "allow",
    ...overrides,
  };
}

const CONNECTION = {
  id: "1",
  name: "cluster",
  securityProtocol: "SASL_PLAINTEXT",
  saslUsername: "alice",
};

beforeEach(() => {
  vi.clearAllMocks();
  useWorkspaceSelectionStore.setState({ selection: null, activeTabId: null, byTab: {} });
  useTreeUiStore.setState({ expanded: {}, searchText: {}, hideEmptyConsumerGroups: {} });
  usePartitionPanelTabStore.setState({ activeByTab: {} });
});

describe("the tree's Access Control category", () => {
  it("is offered alongside the other categories", () => {
    setInvokeHandlers({});
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    expect(screen.getByTestId("category-Access Control")).toBeInTheDocument();
  });

  // Listing ACLs needs Describe on Cluster, which a principal with full read
  // access to every topic is routinely not granted — so it is not paid for on
  // every connect, only when someone opens the category.
  it("does not ask the broker for ACLs until it is expanded", async () => {
    const calls: string[] = [];
    setInvokeHandlers({
      acl_list: () => {
        calls.push("acl_list");
        return { availability: "available", bindings: [], bindingErrors: [] };
      },
    });
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    expect(calls).toEqual([]);

    await userEvent.click(screen.getByTestId("category-Access Control"));

    expect(await screen.findByTestId("acl-notice-empty")).toBeInTheDocument();
    expect(calls).toEqual(["acl_list"]);
  });

  it("lists each distinct principal with how many bindings it holds", async () => {
    setInvokeHandlers({
      acl_list: () => ({
        availability: "available",
        bindingErrors: [],
        bindings: [
          binding({ principal: "User:zoe" }),
          binding({ principal: "User:zoe", operation: "write" }),
          binding({ principal: "User:amy" }),
        ],
      }),
      connection_list: () => [CONNECTION],
    });
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await userEvent.click(screen.getByTestId("category-Access Control"));

    expect(await screen.findByText("User:zoe — 2")).toBeInTheDocument();
    expect(screen.getByText("User:amy — 1")).toBeInTheDocument();
  });

  it("marks and lifts the principal this connection authenticates as", async () => {
    setInvokeHandlers({
      acl_list: () => ({
        availability: "available",
        bindingErrors: [],
        bindings: [binding({ principal: "User:zoe" }), binding({ principal: "User:alice" })],
      }),
      connection_list: () => [CONNECTION],
    });
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await userEvent.click(screen.getByTestId("category-Access Control"));
    await screen.findByText("You (User:alice) — 1");

    const rows = screen.getAllByText(/^(You \(User:alice\)|User:zoe) — 1$/);
    expect(rows[0]).toHaveTextContent("You (User:alice)");
  });

  // An mTLS principal is a certificate DN rdkafka does not expose. A guessed
  // "You" row would misinform someone about their own access.
  it("omits the You marker when the connection's principal cannot be known", async () => {
    setInvokeHandlers({
      acl_list: () => ({
        availability: "available",
        bindingErrors: [],
        bindings: [binding({ principal: "User:alice" })],
      }),
      connection_list: () => [{ ...CONNECTION, securityProtocol: "SSL", saslUsername: null }],
    });
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await userEvent.click(screen.getByTestId("category-Access Control"));

    expect(await screen.findByText("User:alice — 1")).toBeInTheDocument();
    expect(screen.queryByText(/^You \(/)).not.toBeInTheDocument();
  });

  it("selects a principal when its row is clicked", async () => {
    setInvokeHandlers({
      acl_list: () => ({
        availability: "available",
        bindingErrors: [],
        bindings: [binding({ principal: "User:amy" })],
      }),
      connection_list: () => [CONNECTION],
    });
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await userEvent.click(screen.getByTestId("category-Access Control"));
    await userEvent.click(await screen.findByText("User:amy — 1"));

    expect(useWorkspaceSelectionStore.getState().selection).toEqual({
      type: "principal",
      connectionId: "1",
      principal: "User:amy",
    });
  });

  // The category is where an unsecured cluster is most likely to be
  // misread, so it says so rather than showing a bare empty list.
  it("explains an unsecured cluster instead of showing an empty list", async () => {
    setInvokeHandlers({
      acl_list: () => ({ availability: "noAuthorizer", bindings: [], bindingErrors: [] }),
      connection_list: () => [CONNECTION],
    });
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await userEvent.click(screen.getByTestId("category-Access Control"));

    expect(await screen.findByTestId("acl-notice-no-authorizer")).toBeInTheDocument();
  });

  it("admits when an empty listing cannot be explained", async () => {
    setInvokeHandlers({
      acl_list: () => ({ availability: "indeterminate", bindings: [], bindingErrors: [] }),
      connection_list: () => [CONNECTION],
    });
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await userEvent.click(screen.getByTestId("category-Access Control"));

    expect(await screen.findByTestId("acl-notice-indeterminate")).toBeInTheDocument();
  });

  // A refused or broken ACL listing must not present the whole connection as
  // broken — the same rule the other categories already follow.
  it("reports a failed listing on the category alone", async () => {
    setInvokeHandlers({
      acl_list: () => {
        throw new Error("Local: Timed out");
      },
      connection_list_topics: () => [{ name: "orders", partitionCount: 1 }],
      connection_list: () => [CONNECTION],
    });
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await userEvent.click(screen.getByTestId("category-Access Control"));

    expect(await screen.findByTestId("category-Access Control-warning")).toBeInTheDocument();
    expect(screen.queryByTestId("category-Topics-warning")).not.toBeInTheDocument();
  });

  it("filters the principal list with the category's search box", async () => {
    setInvokeHandlers({
      acl_list: () => ({
        availability: "available",
        bindingErrors: [],
        bindings: [binding({ principal: "User:zoe" }), binding({ principal: "User:amy" })],
      }),
      connection_list: () => [CONNECTION],
    });
    renderWithClient(<ClusterResourceTree connectionId="1" />);

    await userEvent.click(screen.getByTestId("category-Access Control"));
    await screen.findByText("User:zoe — 1");

    await userEvent.type(screen.getByLabelText("Search Access Control"), "amy");

    expect(screen.getByText("User:amy — 1")).toBeInTheDocument();
    expect(screen.queryByText("User:zoe — 1")).not.toBeInTheDocument();
  });
});
