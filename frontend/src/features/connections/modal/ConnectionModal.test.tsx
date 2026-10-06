import { describe, expect, it, vi, beforeEach } from "vitest";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { setInvokeHandlers } from "../../../lib/testInvoke";
import { ConnectionModal } from "./ConnectionModal";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

function pointerEventAt(type: string, clientX: number, clientY: number): Event {
  const event = new Event(type, { bubbles: true });
  Object.defineProperty(event, "clientX", { value: clientX });
  Object.defineProperty(event, "clientY", { value: clientY });
  return event;
}

function renderWithClient(ui: ReactNode) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("ConnectionModal", () => {
  it("opens on the Properties tab by default", () => {
    renderWithClient(<ConnectionModal onAdd={vi.fn()} onCancel={vi.fn()} />);
    expect(screen.getByRole("tab", { name: "Properties" })).toHaveAttribute("aria-selected", "true");
    expect(screen.getByLabelText("Cluster name")).toBeInTheDocument();
  });

  it("switches to the Security & Authentication tab when clicked", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionModal onAdd={vi.fn()} onCancel={vi.fn()} />);

    await user.click(screen.getByRole("tab", { name: "Security & Authentication" }));

    expect(screen.getByRole("tab", { name: "Security & Authentication" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    // Both halves, on the one tab: the protocol dropdown and the SASL one.
    expect(screen.getByRole("button", { name: /PLAINTEXT/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /None/ })).toBeInTheDocument();
  });

  /** Security and Authentication were two tabs until they were merged. */
  it("offers only configuration tabs, with no separate Authentication or Advanced tab", () => {
    renderWithClient(<ConnectionModal onAdd={vi.fn()} onCancel={vi.fn()} />);

    expect(screen.getAllByRole("tab").map((tab) => tab.textContent)).toEqual([
      "Properties",
      "Security & Authentication",
      "Schema",
      "ksqlDB",
    ]);
  });

  // The modal configures a connection; it does not use one. The cluster panel
  // appends a Query workspace to this same list, and a half-written connection
  // has nothing to query — so that tab must not appear here.
  it("does not offer the cluster panel's Query workspace", () => {
    renderWithClient(<ConnectionModal onAdd={vi.fn()} onCancel={vi.fn()} />);

    expect(screen.queryByRole("tab", { name: "Query" })).not.toBeInTheDocument();
  });

  it("switches to the Schema tab when clicked", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionModal onAdd={vi.fn()} onCancel={vi.fn()} />);

    await user.click(screen.getByRole("tab", { name: "Schema" }));

    expect(screen.getByLabelText("Endpoint")).toBeInTheDocument();
  });

  it("preserves field values entered on one tab after switching away and back", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionModal onAdd={vi.fn()} onCancel={vi.fn()} />);

    await user.type(screen.getByLabelText("Cluster name"), "Local Kafka");
    await user.click(screen.getByRole("tab", { name: "Security & Authentication" }));
    await user.click(screen.getByRole("tab", { name: "Properties" }));

    expect(screen.getByLabelText("Cluster name")).toHaveValue("Local Kafka");
  });

  it("calls onCancel and does not call onAdd when Cancel is clicked", async () => {
    const onAdd = vi.fn();
    const onCancel = vi.fn();
    const user = userEvent.setup();
    renderWithClient(<ConnectionModal onAdd={onAdd} onCancel={onCancel} />);

    await user.click(screen.getByRole("button", { name: "Cancel" }));

    expect(onCancel).toHaveBeenCalledTimes(1);
    expect(onAdd).not.toHaveBeenCalled();
  });

  it("shows a validation error and does not call onAdd when required fields are missing", async () => {
    const onAdd = vi.fn();
    const user = userEvent.setup();
    renderWithClient(<ConnectionModal onAdd={onAdd} onCancel={vi.fn()} />);

    await user.click(screen.getByRole("button", { name: "Add" }));

    expect(screen.getByRole("alert")).toHaveTextContent("Cluster name is required");
    expect(onAdd).not.toHaveBeenCalled();
  });

  it("returns to the tab holding the missing field when Add is clicked from another tab", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionModal onAdd={vi.fn()} onCancel={vi.fn()} />);

    await user.click(screen.getByRole("tab", { name: "Security & Authentication" }));
    await user.click(screen.getByRole("button", { name: "Add" }));

    expect(screen.getByRole("tab", { name: "Properties" })).toHaveAttribute("aria-selected", "true");
    expect(screen.getByLabelText("Cluster name")).toBeInTheDocument();
    expect(screen.getByRole("alert")).toHaveTextContent("Cluster name is required");
  });

  it("shows the add in progress and re-enables the button once it fails", async () => {
    let rejectAdd: (err: Error) => void = () => {};
    const onAdd = vi.fn().mockReturnValue(
      new Promise((_resolve, reject) => {
        rejectAdd = reject;
      }),
    );
    const user = userEvent.setup();
    renderWithClient(<ConnectionModal onAdd={onAdd} onCancel={vi.fn()} />);

    await user.type(screen.getByLabelText("Cluster name"), "Local Kafka");
    await user.type(screen.getByLabelText("Bootstrap servers"), "localhost:9092");
    await user.click(screen.getByRole("button", { name: "Add" }));

    expect(await screen.findByRole("button", { name: "Adding…" })).toBeDisabled();

    await act(async () => {
      rejectAdd(new Error("a connection named Local Kafka already exists"));
    });

    expect(await screen.findByRole("alert")).toHaveTextContent("already exists");
    expect(screen.getByRole("button", { name: "Add" })).toBeEnabled();
  });

  it("keeps Cancel usable while an add is in flight", async () => {
    const onCancel = vi.fn();
    const onAdd = vi.fn().mockReturnValue(new Promise(() => {}));
    const user = userEvent.setup();
    renderWithClient(<ConnectionModal onAdd={onAdd} onCancel={onCancel} />);

    await user.type(screen.getByLabelText("Cluster name"), "Local Kafka");
    await user.type(screen.getByLabelText("Bootstrap servers"), "localhost:9092");
    await user.click(screen.getByRole("button", { name: "Add" }));
    await user.click(screen.getByRole("button", { name: "Cancel" }));

    expect(onCancel).toHaveBeenCalledTimes(1);
  });

  it("calls onAdd with the assembled connection when Add is clicked with valid fields", async () => {
    const onAdd = vi.fn().mockResolvedValue(undefined);
    const user = userEvent.setup();
    renderWithClient(<ConnectionModal onAdd={onAdd} onCancel={vi.fn()} />);

    await user.type(screen.getByLabelText("Cluster name"), "Local Kafka");
    await user.type(screen.getByLabelText("Bootstrap servers"), "localhost:9092");
    await user.click(screen.getByRole("button", { name: "Add" }));

    await waitFor(() => expect(onAdd).toHaveBeenCalledTimes(1));
    expect(onAdd).toHaveBeenCalledWith(
      expect.objectContaining({ name: "Local Kafka", bootstrapServers: "localhost:9092" }),
    );
  });

  it("runs a connection test and shows success", async () => {
    setInvokeHandlers({ connection_test: () => "REACHABLE" });
    const user = userEvent.setup();
    renderWithClient(<ConnectionModal onAdd={vi.fn()} onCancel={vi.fn()} />);

    await user.type(screen.getByLabelText("Cluster name"), "Local Kafka");
    await user.type(screen.getByLabelText("Bootstrap servers"), "localhost:9092");
    await user.click(screen.getByRole("button", { name: "Test" }));

    expect(await screen.findByText("Connection succeeded")).toBeInTheDocument();
  });

  it("runs a connection test and shows failure", async () => {
    setInvokeHandlers({ connection_test: () => "UNREACHABLE" });
    const user = userEvent.setup();
    renderWithClient(<ConnectionModal onAdd={vi.fn()} onCancel={vi.fn()} />);

    await user.type(screen.getByLabelText("Cluster name"), "Local Kafka");
    await user.type(screen.getByLabelText("Bootstrap servers"), "localhost:9092");
    await user.click(screen.getByRole("button", { name: "Test" }));

    expect(await screen.findByText(/connection failed/i)).toBeInTheDocument();
  });

  it("shows a validation error rather than calling the test command when required fields are missing", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionModal onAdd={vi.fn()} onCancel={vi.fn()} />);

    await user.click(screen.getByRole("button", { name: "Test" }));

    expect(screen.getByRole("alert")).toHaveTextContent("Cluster name is required");
  });

  it("moves with the pointer when dragged by its header", () => {
    renderWithClient(<ConnectionModal onAdd={vi.fn()} onCancel={vi.fn()} />);
    const dialog = screen.getByRole("dialog", { name: "New Connection" });
    const header = screen.getByText("New Connection").closest("header") as HTMLElement;

    act(() => {
      header.dispatchEvent(pointerEventAt("pointerdown", 100, 100));
    });
    act(() => {
      window.dispatchEvent(pointerEventAt("pointermove", 140, 115));
    });

    expect(dialog).toHaveStyle({ transform: "translate(40px, 15px)" });
  });

  it("does not move when clicking inside the body, only from the header", () => {
    renderWithClient(<ConnectionModal onAdd={vi.fn()} onCancel={vi.fn()} />);
    const dialog = screen.getByRole("dialog", { name: "New Connection" });

    act(() => {
      screen.getByLabelText("Cluster name").dispatchEvent(pointerEventAt("pointerdown", 100, 100));
    });
    act(() => {
      window.dispatchEvent(pointerEventAt("pointermove", 140, 115));
    });

    expect(dialog).toHaveStyle({ transform: "translate(0px, 0px)" });
  });
});
