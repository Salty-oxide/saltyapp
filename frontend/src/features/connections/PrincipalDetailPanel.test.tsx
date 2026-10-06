import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { PrincipalDetailPanel } from "./PrincipalDetailPanel";

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
    principal: "User:writer",
    host: "*",
    operation: "write",
    permission: "allow",
    ...overrides,
  };
}

function connectionRow(overrides: Record<string, unknown> = {}) {
  return {
    id: "1",
    name: "cluster",
    securityProtocol: "SASL_PLAINTEXT",
    saslUsername: "writer",
    ...overrides,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("PrincipalDetailPanel", () => {
  it("lists only the selected principal's bindings", async () => {
    setInvokeHandlers({
      acl_list: () => ({
        availability: "available",
        bindingErrors: [],
        bindings: [binding(), binding({ principal: "User:other", operation: "read" })],
      }),
      connection_list_topics: () => [],
      connection_list: () => [connectionRow()],
    });
    renderWithClient(<PrincipalDetailPanel connectionId="1" principal="User:writer" />);

    expect(await screen.findByTestId("acl-row-User:writer-write")).toBeInTheDocument();
    expect(screen.queryByTestId("acl-row-User:other-read")).not.toBeInTheDocument();
  });

  it("counts the bindings it is showing", async () => {
    setInvokeHandlers({
      acl_list: () => ({
        availability: "available",
        bindingErrors: [],
        bindings: [binding(), binding({ operation: "read" })],
      }),
      connection_list_topics: () => [],
      connection_list: () => [connectionRow()],
    });
    renderWithClient(<PrincipalDetailPanel connectionId="1" principal="User:writer" />);

    expect(await screen.findByText("2 bindings")).toBeInTheDocument();
  });

  it("marks the principal this connection authenticates as", async () => {
    setInvokeHandlers({
      acl_list: () => ({ availability: "available", bindingErrors: [], bindings: [binding()] }),
      connection_list_topics: () => [],
      connection_list: () => [connectionRow({ saslUsername: "writer" })],
    });
    renderWithClient(<PrincipalDetailPanel connectionId="1" principal="User:writer" />);

    expect(await screen.findByText(/this connection/i)).toBeInTheDocument();
  });

  it("does not claim an unrelated principal is this connection", async () => {
    setInvokeHandlers({
      acl_list: () => ({
        availability: "available",
        bindingErrors: [],
        bindings: [binding({ principal: "User:someone-else" })],
      }),
      connection_list_topics: () => [],
      connection_list: () => [connectionRow({ saslUsername: "writer" })],
    });
    renderWithClient(<PrincipalDetailPanel connectionId="1" principal="User:someone-else" />);

    await screen.findByTestId("acl-row-User:someone-else-write");
    expect(screen.queryByText(/this connection/i)).not.toBeInTheDocument();
  });

  it("groups bindings by the kind of resource they govern", async () => {
    setInvokeHandlers({
      acl_list: () => ({
        availability: "available",
        bindingErrors: [],
        bindings: [binding(), binding({ resourceType: "group", resourceName: "analytics", operation: "read" })],
      }),
      connection_list_topics: () => [],
      connection_list: () => [connectionRow()],
    });
    renderWithClient(<PrincipalDetailPanel connectionId="1" principal="User:writer" />);

    expect(await screen.findByText("TOPIC")).toBeInTheDocument();
    expect(screen.getByText("GROUP")).toBeInTheDocument();
  });

  // `User:*` reads as "a principal named star" unless it is spelled out, and
  // the difference decides whether revoking it affects one person or everyone.
  it("warns that the wildcard principal applies to everybody", async () => {
    setInvokeHandlers({
      acl_list: () => ({
        availability: "available",
        bindingErrors: [],
        bindings: [binding({ principal: "User:*" })],
      }),
      connection_list_topics: () => [],
      connection_list: () => [connectionRow()],
    });
    renderWithClient(<PrincipalDetailPanel connectionId="1" principal="User:*" />);

    expect(await screen.findByText(/any-principal wildcard/i)).toBeInTheDocument();
  });

  it("expands a prefixed binding to the topics it currently covers", async () => {
    setInvokeHandlers({
      acl_list: () => ({
        availability: "available",
        bindingErrors: [],
        bindings: [binding({ resourceName: "payments-", patternType: "prefixed" })],
      }),
      connection_list_topics: () => [
        { name: "payments-in", partitionCount: 1 },
        { name: "orders", partitionCount: 1 },
      ],
      connection_list: () => [connectionRow()],
    });
    const user = userEvent.setup();
    renderWithClient(<PrincipalDetailPanel connectionId="1" principal="User:writer" />);

    await user.click(await screen.findByLabelText(/Show topics matching payments-/));

    expect(screen.getByText(/currently matches 1 of 2 topics/i)).toBeInTheDocument();
  });

  it("reports a failed listing as an error", async () => {
    setInvokeHandlers({
      acl_list: () => {
        throw new Error("broker said no");
      },
      connection_list_topics: () => [],
      connection_list: () => [connectionRow()],
    });
    renderWithClient(<PrincipalDetailPanel connectionId="1" principal="User:writer" />);

    expect(await screen.findByRole("alert")).toHaveTextContent("broker said no");
  });
});
