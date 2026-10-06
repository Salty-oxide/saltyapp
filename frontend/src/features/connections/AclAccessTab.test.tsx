import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { AclAccessTab } from "./AclAccessTab";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

function renderWithClient(ui: React.ReactElement) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

/**
 * The backend computes the matrix (`salty_core::resource_access`), so these
 * fixtures are shaped like what it returns rather than like raw bindings —
 * the component under test renders verdicts, it does not derive them.
 */
function access(overrides: Record<string, unknown> = {}) {
  return {
    listing: {
      availability: "available",
      bindingErrors: [],
      bindings: [
        {
          resourceType: "topic",
          resourceName: "orders",
          patternType: "literal",
          principal: "User:reader",
          host: "*",
          operation: "read",
          permission: "allow",
        },
      ],
    },
    access: [
      {
        principal: "User:reader",
        verdicts: [
          { operation: "describe", allowed: true, reason: { kind: "impliedAllow", via: "read" }, viaWildcardPrincipal: false },
          { operation: "read", allowed: true, reason: { kind: "directAllow" }, viaWildcardPrincipal: false },
          { operation: "write", allowed: false, reason: { kind: "defaultDeny" }, viaWildcardPrincipal: false },
        ],
      },
    ],
    ...overrides,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("AclAccessTab", () => {
  it("renders a matrix row per principal with a cell per operation", async () => {
    setInvokeHandlers({ acl_for_resource: () => access(), connection_list_topics: () => [] });
    renderWithClient(<AclAccessTab connectionId="1" resourceType="topic" resourceName="orders" />);

    const row = await screen.findByTestId("acl-matrix-row-User:reader");
    expect(within(row).getAllByRole("cell").map((cell) => cell.textContent)).toEqual([
      "User:reader",
      "✓ implied",
      "✓",
      "✗",
    ]);
  });

  // The distinction the whole verdict design exists to preserve: an implied
  // grant has no binding of its own to revoke, so presenting it identically
  // to a direct one would send someone looking for an ACL that isn't there.
  it("marks an implied grant as implied and names what implied it", async () => {
    setInvokeHandlers({ acl_for_resource: () => access(), connection_list_topics: () => [] });
    renderWithClient(<AclAccessTab connectionId="1" resourceType="topic" resourceName="orders" />);

    const row = await screen.findByTestId("acl-matrix-row-User:reader");
    const describeCell = within(row).getByTestId("verdict-describe");
    expect(describeCell).toHaveTextContent("implied");
    expect(describeCell).toHaveAttribute("title", expect.stringContaining("Implied by Read"));
  });

  // Deny overrides every Allow and is rare enough to skim past, which is why
  // it is styled apart. Rendering one as though it were a grant would be the
  // most misleading thing this table could do.
  it("renders a Deny binding as a denial, visually distinct from an allow", async () => {
    setInvokeHandlers({
      acl_for_resource: () =>
        access({
          listing: {
            availability: "available",
            bindingErrors: [],
            bindings: [
              {
                resourceType: "topic",
                resourceName: "orders",
                patternType: "literal",
                principal: "User:blocked",
                host: "*",
                operation: "write",
                permission: "deny",
              },
            ],
          },
        }),
      connection_list_topics: () => [],
    });
    const { container } = renderWithClient(
      <AclAccessTab connectionId="1" resourceType="topic" resourceName="orders" />,
    );

    const row = await screen.findByTestId("acl-row-User:blocked-write");
    expect(row).toHaveTextContent("Deny");
    expect(row).toHaveClass("acl-row--deny");
    expect(container.querySelectorAll(".acl-permission--deny")).toHaveLength(1);
    expect(container.querySelectorAll(".acl-permission--allow")).toHaveLength(0);
  });

  // The topic list is fetched separately, so it is legitimately absent for the
  // first render. A prefixed binding must still show its pattern; only the
  // "which topics does it cover" toggle depends on that list.
  it("still shows a prefixed pattern before the topic list has loaded", async () => {
    setInvokeHandlers({
      acl_for_resource: () =>
        access({
          listing: {
            availability: "available",
            bindingErrors: [],
            bindings: [
              {
                resourceType: "topic",
                resourceName: "payments-",
                patternType: "prefixed",
                principal: "User:writer",
                host: "*",
                operation: "write",
                permission: "allow",
              },
            ],
          },
        }),
      connection_list_topics: () => new Promise(() => {}),
    });
    renderWithClient(<AclAccessTab connectionId="1" resourceType="topic" resourceName="payments-in" />);

    const row = await screen.findByTestId("acl-row-User:writer-write");
    expect(row).toHaveTextContent("PREFIXED");
    expect(row).toHaveTextContent("payments-");
    expect(screen.queryByLabelText(/Show topics matching/)).not.toBeInTheDocument();
  });

  it("shows the bindings beneath the matrix as the evidence for it", async () => {
    setInvokeHandlers({ acl_for_resource: () => access(), connection_list_topics: () => [] });
    renderWithClient(<AclAccessTab connectionId="1" resourceType="topic" resourceName="orders" />);

    expect(await screen.findByTestId("acl-row-User:reader-read")).toBeInTheDocument();
    expect(screen.getByText(/what the broker reported/i)).toBeInTheDocument();
  });

  it("labels the matrix as derived rather than as something the broker said", async () => {
    setInvokeHandlers({ acl_for_resource: () => access(), connection_list_topics: () => [] });
    renderWithClient(<AclAccessTab connectionId="1" resourceType="topic" resourceName="orders" />);

    expect(await screen.findByText(/derived from the bindings below/i)).toBeInTheDocument();
  });

  it("flags a verdict decided by a wildcard-principal binding", async () => {
    setInvokeHandlers({
      acl_for_resource: () =>
        access({
          access: [
            {
              principal: "User:reader",
              verdicts: [
                { operation: "read", allowed: true, reason: { kind: "directAllow" }, viaWildcardPrincipal: true },
              ],
            },
          ],
        }),
      connection_list_topics: () => [],
    });
    renderWithClient(<AclAccessTab connectionId="1" resourceType="topic" resourceName="orders" />);

    const cell = await screen.findByTestId("verdict-read");
    expect(cell).toHaveTextContent("via *");
    expect(cell).toHaveAttribute("title", expect.stringContaining("User:*"));
  });

  // An unsecured cluster is the case a blank list gets most dangerously
  // wrong: nothing granted reads as nothing permitted, when it means the
  // opposite.
  it("explains an empty listing on a cluster with no authorizer", async () => {
    setInvokeHandlers({
      acl_for_resource: () => ({
        listing: { availability: "noAuthorizer", bindings: [], bindingErrors: [] },
        access: [],
      }),
      connection_list_topics: () => [],
    });
    renderWithClient(<AclAccessTab connectionId="1" resourceType="topic" resourceName="orders" />);

    expect(await screen.findByTestId("acl-notice-no-authorizer")).toHaveTextContent(
      /no authorizer configured/i,
    );
  });

  it("admits when it cannot tell an empty policy from a refusal", async () => {
    setInvokeHandlers({
      acl_for_resource: () => ({
        listing: { availability: "indeterminate", bindings: [], bindingErrors: [] },
        access: [],
      }),
      connection_list_topics: () => [],
    });
    renderWithClient(<AclAccessTab connectionId="1" resourceType="topic" resourceName="orders" />);

    expect(await screen.findByTestId("acl-notice-indeterminate")).toHaveTextContent(/cannot tell why/i);
  });

  it("reports a genuinely empty policy as denying everything", async () => {
    setInvokeHandlers({
      acl_for_resource: () => ({
        listing: { availability: "available", bindings: [], bindingErrors: [] },
        access: [],
      }),
      connection_list_topics: () => [],
    });
    renderWithClient(<AclAccessTab connectionId="1" resourceType="topic" resourceName="orders" />);

    expect(await screen.findByTestId("acl-notice-empty")).toHaveTextContent(/denies everything/i);
  });

  it("surfaces per-binding errors without losing the bindings that did load", async () => {
    setInvokeHandlers({
      acl_for_resource: () => access({ listing: { ...access().listing, bindingErrors: ["one went bad"] } }),
      connection_list_topics: () => [],
    });
    renderWithClient(<AclAccessTab connectionId="1" resourceType="topic" resourceName="orders" />);

    expect(await screen.findByText(/one went bad/)).toBeInTheDocument();
    expect(screen.getByTestId("acl-matrix")).toBeInTheDocument();
  });

  it("reports a failed listing as an error rather than as an empty policy", async () => {
    setInvokeHandlers({
      acl_for_resource: () => {
        throw new Error("broker said no");
      },
      connection_list_topics: () => [],
    });
    renderWithClient(<AclAccessTab connectionId="1" resourceType="topic" resourceName="orders" />);

    expect(await screen.findByRole("alert")).toHaveTextContent("broker said no");
  });

  // A PREFIXED badge with no prefix beside it names the *shape* of the rule
  // that granted access without naming the rule. The prefix is the thing you
  // would go and change, so it has to be on screen without clicking anything.
  it("shows which prefix granted the access, not just that a prefix did", async () => {
    setInvokeHandlers({
      acl_for_resource: () =>
        access({
          listing: {
            availability: "available",
            bindingErrors: [],
            bindings: [
              {
                resourceType: "topic",
                resourceName: "payments-",
                patternType: "prefixed",
                principal: "User:writer",
                host: "*",
                operation: "write",
                permission: "allow",
              },
            ],
          },
        }),
      connection_list_topics: () => [{ name: "payments-in", partitionCount: 1 }],
    });
    renderWithClient(<AclAccessTab connectionId="1" resourceType="topic" resourceName="payments-in" />);

    const row = await screen.findByTestId("acl-row-User:writer-write");
    // Principal *and* pattern, not one in place of the other.
    expect(within(row).getAllByRole("cell").map((cell) => cell.textContent)).toEqual([
      "User:writer",
      "payments-…",
      "PREFIXED▸",
      "Write",
      "Allow",
      "*",
    ]);
  });

  it("shows a literal binding's own resource name in the pattern column", async () => {
    setInvokeHandlers({ acl_for_resource: () => access(), connection_list_topics: () => [] });
    renderWithClient(<AclAccessTab connectionId="1" resourceType="topic" resourceName="orders" />);

    const row = await screen.findByTestId("acl-row-User:reader-read");
    expect(within(row).getAllByRole("cell").map((cell) => cell.textContent)).toEqual([
      "User:reader",
      "orders",
      "LITERAL",
      "Read",
      "Allow",
      "*",
    ]);
  });

  // Resolved against the already-cached topic list, so it costs no broker
  // call — and it is the one thing a flat ACL dump cannot tell you.
  it("expands a prefixed binding to the topics it currently covers", async () => {
    setInvokeHandlers({
      acl_for_resource: () =>
        access({
          listing: {
            availability: "available",
            bindingErrors: [],
            bindings: [
              {
                resourceType: "topic",
                resourceName: "payments-",
                patternType: "prefixed",
                principal: "User:writer",
                host: "*",
                operation: "write",
                permission: "allow",
              },
            ],
          },
        }),
      connection_list_topics: () => [
        { name: "payments-in", partitionCount: 1 },
        { name: "payments-out", partitionCount: 1 },
        { name: "orders", partitionCount: 1 },
      ],
    });
    const user = userEvent.setup();
    renderWithClient(<AclAccessTab connectionId="1" resourceType="topic" resourceName="payments-in" />);

    await user.click(await screen.findByLabelText(/Show topics matching payments-/));

    expect(screen.getByText(/currently matches 2 of 3 topics/i)).toBeInTheDocument();
    expect(screen.getByText("payments-in, payments-out")).toBeInTheDocument();
  });
});
