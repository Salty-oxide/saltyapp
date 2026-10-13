import { beforeEach, describe, expect, it, vi } from "vitest";
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { invoke } from "@tauri-apps/api/core";
import { setInvokeHandlers } from "../../lib/testInvoke";
import type { FolderAssignment, FolderKind, Organization, ResourceFolder } from "../../lib/tauri";
import { ClusterResourceTree } from "./ClusterResourceTree";
import { useWorkspaceSelectionStore } from "../workspace/useWorkspaceSelectionStore";
import { useTreeUiStore } from "./useTreeUiStore";
import { usePartitionPanelTabStore } from "./usePartitionPanelTabStore";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

const TOPICS = ["orders", "payments", "audit", "billing-events"].map((name) => ({ name, partitionCount: 1 }));
const GROUPS = ["checkout", "analytics"].map((groupId) => ({ groupId, state: "Stable", memberCount: 1 }));

/** An in-memory stand-in for the `organization_*` commands, with the real backend's rules. */
function fakeBackend(initial: Partial<Organization> = {}) {
  const state: Organization = {
    folders: [...(initial.folders ?? [])],
    assignments: [...(initial.assignments ?? [])],
    pinnedTopics: [...(initial.pinnedTopics ?? [])],
  };
  let nextId = 1;
  const handlers = {
    connection_list_topics: () => TOPICS,
    connection_list_consumer_groups: () => GROUPS,
    organization_get: () => structuredClone(state),
    organization_folder_create: ({ kind, name }: { kind: FolderKind; name: string }) => {
      const clean = String(name).trim();
      if (state.folders.some((f) => f.kind === kind && f.name.toLowerCase() === clean.toLowerCase())) {
        throw new Error(`validation error: a folder named '${clean}' already exists`);
      }
      const folder: ResourceFolder = { id: `f${nextId++}`, kind, name: clean };
      state.folders.push(folder);
      return folder;
    },
    organization_folder_rename: ({ id, name }: { id: string; name: string }) => {
      const folder = state.folders.find((f) => f.id === id);
      if (folder) folder.name = name.trim();
    },
    organization_folder_delete: ({ id }: { id: string }) => {
      state.folders = state.folders.filter((f) => f.id !== id);
      state.assignments = state.assignments.filter((a) => a.folderId !== id);
    },
    organization_move_resource: ({
      kind,
      resourceName,
      folderId,
    }: {
      kind: FolderKind;
      resourceName: string;
      folderId: string | null;
    }) => {
      // Like the real backend: a folder id must be null (root) or a folder of this kind.
      if (folderId !== null && !state.folders.some((f) => f.id === folderId && f.kind === kind)) {
        throw new Error(`not found: folder ${String(folderId)}`);
      }
      state.assignments = state.assignments.filter((a) => !(a.kind === kind && a.resourceName === resourceName));
      if (folderId !== null) state.assignments.push({ kind, resourceName, folderId });
    },
    organization_set_topic_pinned: ({ topicName, pinned }: { topicName: string; pinned: boolean }) => {
      state.pinnedTopics = state.pinnedTopics.filter((t) => t !== topicName);
      if (pinned) state.pinnedTopics.push(topicName);
    },
  };
  return { state, handlers };
}

const topicFolder = (id: string, name: string): ResourceFolder => ({ id, kind: "topic", name });
const filedTopic = (resourceName: string, folderId: string): FolderAssignment => ({
  kind: "topic",
  resourceName,
  folderId,
});

function renderTree() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <ClusterResourceTree connectionId="1" />
    </QueryClientProvider>,
  );
}

/** The topic rows in the order they appear in the DOM, folder headers included as `[name]`. */
function listing(container: HTMLElement): string[] {
  return Array.from(container.querySelectorAll("[data-testid^='resource-item-'], [data-testid^='folder-']"))
    .filter((el) => !el.getAttribute("data-testid")!.startsWith("resource-item-partition"))
    .map((el) => {
      const id = el.getAttribute("data-testid")!;
      return id.startsWith("folder-") ? `[${id.slice(7)}]` : id.slice("resource-item-".length);
    });
}

async function openTopics(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByTestId("category-Topics"));
  await screen.findByText("orders");
}

async function chooseFromMenu(label: string) {
  fireEvent.click(await screen.findByRole("menuitem", { name: label }));
}

beforeEach(() => {
  vi.clearAllMocks();
  useWorkspaceSelectionStore.setState({ selection: null });
  useTreeUiStore.setState({ expanded: {}, searchText: {}, hideEmptyConsumerGroups: {} });
  usePartitionPanelTabStore.setState({ activeByTab: {} });
});

describe("pinned topics", () => {
  it("lists pinned topics first, under a Pinned label, with a pin mark", async () => {
    const { handlers } = fakeBackend({ pinnedTopics: ["billing-events"] });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    const { container } = renderTree();
    await openTopics(user);

    await waitFor(() => expect(screen.getByTestId("pinned-label")).toBeInTheDocument());
    expect(listing(container)).toEqual(["billing-events", "orders", "payments", "audit"]);
    expect(within(screen.getByTestId("resource-item-billing-events")).getByLabelText("Pinned")).toBeInTheDocument();
    expect(within(screen.getByTestId("resource-item-orders")).queryByLabelText("Pinned")).not.toBeInTheDocument();
  });

  it("pins a topic from its right-click menu and moves it to the top", async () => {
    const { handlers } = fakeBackend();
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    const { container } = renderTree();
    await openTopics(user);

    fireEvent.contextMenu(screen.getByText("audit"));
    await chooseFromMenu("Pin to top");

    await waitFor(() => expect(listing(container)[0]).toBe("audit"));
    expect(vi.mocked(invoke)).toHaveBeenCalledWith("organization_set_topic_pinned", {
      connectionId: "1",
      topicName: "audit",
      pinned: true,
    });
  });

  it("offers Unpin for a pinned topic and returns it to the list", async () => {
    const { handlers } = fakeBackend({ pinnedTopics: ["audit"] });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    const { container } = renderTree();
    await openTopics(user);
    await screen.findByTestId("pinned-label");

    fireEvent.contextMenu(screen.getByText("audit"));
    await chooseFromMenu("Unpin from top");

    await waitFor(() => expect(screen.queryByTestId("pinned-label")).not.toBeInTheDocument());
    expect(listing(container)).toEqual(["orders", "payments", "audit", "billing-events"]);
  });

  it("does not offer pinning for consumer groups", async () => {
    const { handlers } = fakeBackend();
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    renderTree();
    await user.click(screen.getByTestId("category-Consumers"));
    await screen.findByText("checkout");

    fireEvent.contextMenu(screen.getByText("checkout"));

    expect(await screen.findByRole("menuitem", { name: "Move to new folder…" })).toBeInTheDocument();
    expect(screen.queryByRole("menuitem", { name: "Pin to top" })).not.toBeInTheDocument();
  });
});

describe("topic folders", () => {
  it("creates a folder from the Topics header menu", async () => {
    const { handlers, state } = fakeBackend();
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    const { container } = renderTree();
    await openTopics(user);

    fireEvent.contextMenu(screen.getByTestId("category-Topics"));
    await chooseFromMenu("New folder…");
    await user.type(screen.getByLabelText("New folder name"), "Billing{Enter}");

    await waitFor(() => expect(listing(container)).toContain("[Billing]"));
    expect(state.folders).toEqual([expect.objectContaining({ kind: "topic", name: "Billing" })]);
    expect(screen.queryByLabelText("New folder name")).not.toBeInTheDocument();
  });

  it("keeps the name box open and shows the reason when the name is taken", async () => {
    const { handlers } = fakeBackend({ folders: [topicFolder("f9", "Billing")] });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);

    fireEvent.contextMenu(screen.getByTestId("category-Topics"));
    await chooseFromMenu("New folder…");
    await user.type(screen.getByLabelText("New folder name"), "billing{Enter}");

    expect(await screen.findByRole("alert")).toHaveTextContent("already exists");
    expect(screen.getByLabelText("New folder name")).toBeInTheDocument();
  });

  it("cancels the name box with Escape without creating anything", async () => {
    const { handlers, state } = fakeBackend();
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);

    fireEvent.contextMenu(screen.getByTestId("category-Topics"));
    await chooseFromMenu("New folder…");
    await user.type(screen.getByLabelText("New folder name"), "Nope{Escape}");

    expect(screen.queryByLabelText("New folder name")).not.toBeInTheDocument();
    expect(state.folders).toEqual([]);
  });

  it("moves a topic into a folder from its right-click menu", async () => {
    const { handlers } = fakeBackend({ folders: [topicFolder("f1", "Billing")] });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    const { container } = renderTree();
    await openTopics(user);
    await screen.findByTestId("folder-Billing");

    fireEvent.contextMenu(screen.getByText("payments"));
    await chooseFromMenu("Move to folder: Billing");

    // The folder is closed, so the topic leaves the root list and shows in the count.
    await waitFor(() => expect(listing(container)).not.toContain("payments"));
    expect(within(screen.getByTestId("folder-Billing")).getByText("1")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Folder Billing" }));
    expect(listing(container)).toEqual(["[Billing]", "payments", "orders", "audit", "billing-events"]);
  });

  it("creates a folder and moves the topic into it in one step", async () => {
    const { handlers, state } = fakeBackend();
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    const { container } = renderTree();
    await openTopics(user);

    fireEvent.contextMenu(screen.getByText("orders"));
    await chooseFromMenu("Move to new folder…");
    await user.type(screen.getByLabelText("New folder name"), "Core{Enter}");

    // The new folder opens so the move is visible.
    await waitFor(() => expect(listing(container)).toEqual(["[Core]", "orders", "payments", "audit", "billing-events"]));
    expect(state.assignments).toEqual([expect.objectContaining({ resourceName: "orders", kind: "topic" })]);
  });

  it("removes a topic from its folder", async () => {
    const { handlers } = fakeBackend({
      folders: [topicFolder("f1", "Billing")],
      assignments: [filedTopic("payments", "f1")],
    });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    const { container } = renderTree();
    await openTopics(user);
    await user.click(await screen.findByRole("button", { name: "Folder Billing" }));

    fireEvent.contextMenu(screen.getByText("payments"));
    expect(screen.queryByRole("menuitem", { name: "Move to folder: Billing" })).not.toBeInTheDocument();
    await chooseFromMenu("Remove from folder");

    await waitFor(() => expect(listing(container)).toEqual(["[Billing]", "orders", "payments", "audit", "billing-events"]));
  });

  it("renames a folder from its right-click menu", async () => {
    const { handlers, state } = fakeBackend({ folders: [topicFolder("f1", "Billing")] });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    const { container } = renderTree();
    await openTopics(user);

    fireEvent.contextMenu(await screen.findByRole("button", { name: "Folder Billing" }));
    await chooseFromMenu("Rename folder");
    const input = screen.getByLabelText("Rename folder Billing");
    await user.clear(input);
    await user.type(input, "Finance{Enter}");

    await waitFor(() => expect(listing(container)).toContain("[Finance]"));
    expect(state.folders[0].name).toBe("Finance");
    expect(listing(container)).not.toContain("[Billing]");
  });

  it("deletes a folder and brings its topics back into the Topics list", async () => {
    const { handlers, state } = fakeBackend({
      folders: [topicFolder("f1", "Billing")],
      assignments: [filedTopic("payments", "f1"), filedTopic("billing-events", "f1")],
    });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    const { container } = renderTree();
    await openTopics(user);
    await screen.findByTestId("folder-Billing");
    // Filed topics are not in the root list while the folder exists and is closed.
    expect(listing(container)).toEqual(["[Billing]", "orders", "audit"]);

    fireEvent.contextMenu(screen.getByRole("button", { name: "Folder Billing" }));
    await chooseFromMenu("Delete folder");

    await waitFor(() =>
      expect(listing(container)).toEqual(["orders", "payments", "audit", "billing-events"]),
    );
    expect(state.folders).toEqual([]);
    expect(state.assignments).toEqual([]);
  });
});

describe("searching with folders", () => {
  const organization = {
    folders: [topicFolder("f1", "Billing"), topicFolder("f2", "Platform")],
    assignments: [filedTopic("payments", "f1"), filedTopic("audit", "f2")],
  };

  it("finds a topic inside a closed folder, opens it, and restores it when cleared", async () => {
    const { handlers } = fakeBackend(organization);
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    const { container } = renderTree();
    await openTopics(user);
    await screen.findByTestId("folder-Billing");
    expect(listing(container)).toEqual(["[Billing]", "[Platform]", "orders", "billing-events"]);

    const search = screen.getByLabelText("Search Topics");
    await user.type(search, "pay");

    expect(listing(container)).toEqual(["[Billing]", "payments"]);

    await user.clear(search);
    expect(listing(container)).toEqual(["[Billing]", "[Platform]", "orders", "billing-events"]);
  });

  it("shows every topic of a folder whose name matches", async () => {
    const { handlers } = fakeBackend(organization);
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    const { container } = renderTree();
    await openTopics(user);
    await screen.findByTestId("folder-Platform");

    await user.type(screen.getByLabelText("Search Topics"), "platform");

    expect(listing(container)).toEqual(["[Platform]", "audit"]);
  });
});

describe("consumer folders", () => {
  it("creates a folder under Consumers and files a group in it", async () => {
    const { handlers, state } = fakeBackend();
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    const { container } = renderTree();
    await user.click(screen.getByTestId("category-Consumers"));
    await screen.findByText("checkout");

    fireEvent.contextMenu(screen.getByText("checkout"));
    await chooseFromMenu("Move to new folder…");
    await user.type(screen.getByLabelText("New folder name"), "Shop{Enter}");

    await waitFor(() => expect(listing(container)).toEqual(["[Shop]", "checkout", "analytics"]));
    expect(state.folders).toEqual([expect.objectContaining({ kind: "consumer", name: "Shop" })]);
    expect(state.assignments).toEqual([
      expect.objectContaining({ kind: "consumer", resourceName: "checkout" }),
    ]);
  });

  it("keeps the existing 'hide empty consumer groups' item alongside New folder", async () => {
    const { handlers } = fakeBackend();
    setInvokeHandlers(handlers);
    renderTree();

    fireEvent.contextMenu(screen.getByTestId("category-Consumers"));

    expect(await screen.findByRole("menuitem", { name: "Hide empty consumer groups" })).toBeInTheDocument();
    expect(screen.getByRole("menuitem", { name: "New folder…" })).toBeInTheDocument();
  });

  it("keeps topic and consumer folders separate", async () => {
    const { handlers } = fakeBackend({ folders: [topicFolder("f1", "Billing")] });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    const { container } = renderTree();
    await user.click(screen.getByTestId("category-Consumers"));
    await screen.findByText("checkout");

    expect(listing(container)).toEqual(["checkout", "analytics"]);
  });
});

describe("drag to folder", () => {
  function drag(from: HTMLElement, to: HTMLElement) {
    fireEvent.pointerDown(from, { button: 0, clientX: 10, clientY: 10 });
    fireEvent.pointerMove(from, { clientX: 40, clientY: 40 });
    fireEvent.pointerUp(to, { clientX: 40, clientY: 40 });
  }

  it("moves a topic into a folder dropped on its header", async () => {
    const { handlers, state } = fakeBackend({ folders: [topicFolder("f1", "Billing")] });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    const { container } = renderTree();
    await openTopics(user);

    drag(screen.getByText("payments"), screen.getByRole("button", { name: "Folder Billing" }));

    await waitFor(() => expect(state.assignments).toEqual([filedTopic("payments", "f1")]));
    await waitFor(() => expect(listing(container)).not.toContain("payments"));
  });

  it("returns a topic to the root when dropped on the list background", async () => {
    const { handlers, state } = fakeBackend({
      folders: [topicFolder("f1", "Billing")],
      assignments: [filedTopic("payments", "f1")],
    });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);
    await user.click(await screen.findByRole("button", { name: "Folder Billing" }));

    drag(screen.getByText("payments"), screen.getByLabelText("Search Topics"));

    await waitFor(() => expect(state.assignments).toEqual([]));
  });

  it("does not select the topic that was dragged, but a plain click still selects", async () => {
    const { handlers } = fakeBackend({ folders: [topicFolder("f1", "Billing")] });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);

    const row = screen.getByText("orders");
    drag(row, screen.getByRole("button", { name: "Folder Billing" }));
    fireEvent.click(row);
    expect(useWorkspaceSelectionStore.getState().selection).toBeNull();

    await user.click(screen.getByText("audit"));
    expect(useWorkspaceSelectionStore.getState().selection).toMatchObject({ type: "topic", topicName: "audit" });
  });

  it("treats a press that barely moves as a click, not a drag", async () => {
    const { handlers, state } = fakeBackend({ folders: [topicFolder("f1", "Billing")] });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);

    const row = screen.getByText("orders");
    fireEvent.pointerDown(row, { button: 0, clientX: 10, clientY: 10 });
    fireEvent.pointerMove(row, { clientX: 11, clientY: 11 });
    fireEvent.pointerUp(screen.getByRole("button", { name: "Folder Billing" }), { clientX: 11, clientY: 11 });

    expect(state.assignments).toEqual([]);
  });
});

describe("selecting several topics", () => {
  const ctrlClick = (name: string) => fireEvent.click(screen.getByText(name), { ctrlKey: true });
  const shiftClick = (name: string) => fireEvent.click(screen.getByText(name), { shiftKey: true });
  const isHighlighted = (name: string) =>
    screen.getByTestId(`resource-item-${name}`).querySelector(".resource-item--selected") !== null;

  it("ctrl+click adds topics, highlights them all and empties the middle pane's selection", async () => {
    setInvokeHandlers(fakeBackend().handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);
    await user.click(screen.getByText("orders"));
    expect(useWorkspaceSelectionStore.getState().selection).toMatchObject({ type: "topic", topicName: "orders" });

    ctrlClick("audit");

    expect(useWorkspaceSelectionStore.getState().selection).toBeNull();
    expect(isHighlighted("orders")).toBe(true);
    expect(isHighlighted("audit")).toBe(true);
    expect(isHighlighted("payments")).toBe(false);
  });

  it("ctrl+click back down to one topic opens that topic again", async () => {
    setInvokeHandlers(fakeBackend().handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);
    await user.click(screen.getByText("orders"));
    ctrlClick("audit");

    ctrlClick("orders");

    expect(useWorkspaceSelectionStore.getState().selection).toMatchObject({ type: "topic", topicName: "audit" });
  });

  it("shift+click selects the visible range", async () => {
    setInvokeHandlers(fakeBackend().handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);
    await user.click(screen.getByText("orders"));

    shiftClick("audit");

    expect(useWorkspaceSelectionStore.getState().selection).toBeNull();
    expect(["orders", "payments", "audit"].map(isHighlighted)).toEqual([true, true, true]);
    expect(isHighlighted("billing-events")).toBe(false);
  });

  it("a plain click leaves the multi-selection and opens that topic", async () => {
    setInvokeHandlers(fakeBackend().handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);
    await user.click(screen.getByText("orders"));
    ctrlClick("audit");

    await user.click(screen.getByText("payments"));

    expect(useWorkspaceSelectionStore.getState().selection).toMatchObject({ type: "topic", topicName: "payments" });
    expect(isHighlighted("orders")).toBe(false);
    expect(isHighlighted("audit")).toBe(false);
  });

  it("Escape clears the multi-selection", async () => {
    setInvokeHandlers(fakeBackend().handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);
    await user.click(screen.getByText("orders"));
    ctrlClick("audit");

    fireEvent.keyDown(window, { key: "Escape" });

    expect(isHighlighted("orders")).toBe(false);
    expect(isHighlighted("audit")).toBe(false);
  });

  // The set is kept apart from the workspace selection, so it has to notice
  // when something else is selected — otherwise the topics stay highlighted
  // beside a middle pane that is showing a different thing.
  it("lets go of the multi-selection when anything else is selected", async () => {
    setInvokeHandlers(fakeBackend().handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);
    await user.click(screen.getByText("orders"));
    ctrlClick("audit");
    expect(isHighlighted("orders")).toBe(true);

    act(() => useWorkspaceSelectionStore.getState().selectBroker("1", 2));

    expect(isHighlighted("orders")).toBe(false);
    expect(isHighlighted("audit")).toBe(false);
  });

  it("lets go of the multi-selection when a partition is selected", async () => {
    setInvokeHandlers(fakeBackend().handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);
    await user.click(screen.getByText("orders"));
    ctrlClick("audit");

    act(() => useWorkspaceSelectionStore.getState().selectPartition("1", "payments", 0));

    expect(isHighlighted("orders")).toBe(false);
    expect(isHighlighted("audit")).toBe(false);
  });

  it("keeps the multi-selection while nothing else is selected", async () => {
    setInvokeHandlers(fakeBackend().handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);
    await user.click(screen.getByText("orders"));
    ctrlClick("audit");
    ctrlClick("payments");

    expect(useWorkspaceSelectionStore.getState().selection).toBeNull();
    expect(["orders", "audit", "payments"].map(isHighlighted)).toEqual([true, true, true]);
  });

  it("moves every selected topic from the right-click menu", async () => {
    const { handlers, state } = fakeBackend({ folders: [topicFolder("f1", "Billing")] });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);
    await user.click(screen.getByText("orders"));
    ctrlClick("audit");

    fireEvent.contextMenu(screen.getByText("audit"));
    await chooseFromMenu("Move to folder: Billing");

    await waitFor(() =>
      expect(state.assignments).toEqual([filedTopic("orders", "f1"), filedTopic("audit", "f1")]),
    );
  });

  it("moves every selected topic into a new folder, creating it once", async () => {
    const { handlers, state } = fakeBackend();
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);
    await user.click(screen.getByText("orders"));
    ctrlClick("audit");

    fireEvent.contextMenu(screen.getByText("orders"));
    await chooseFromMenu("Move to new folder…");
    await user.type(screen.getByLabelText("New folder name"), "Mixed{Enter}");

    await waitFor(() => expect(state.assignments).toHaveLength(2));
    expect(state.folders).toHaveLength(1);
    expect(state.assignments.map((a) => a.resourceName).sort()).toEqual(["audit", "orders"]);
  });

  it("moves the whole selection when one of its rows is dragged to a folder", async () => {
    const { handlers, state } = fakeBackend({ folders: [topicFolder("f1", "Billing")] });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);
    await user.click(screen.getByText("orders"));
    ctrlClick("audit");

    const row = screen.getByText("audit");
    fireEvent.pointerDown(row, { button: 0, clientX: 10, clientY: 10 });
    fireEvent.pointerMove(row, { clientX: 40, clientY: 40 });
    fireEvent.pointerUp(screen.getByRole("button", { name: "Folder Billing" }), { clientX: 40, clientY: 40 });

    await waitFor(() => expect(state.assignments).toHaveLength(2));
    expect(state.assignments.map((a) => a.resourceName).sort()).toEqual(["audit", "orders"]);
  });

  it("dragging an unselected row moves only that row", async () => {
    const { handlers, state } = fakeBackend({ folders: [topicFolder("f1", "Billing")] });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    renderTree();
    await openTopics(user);
    await user.click(screen.getByText("orders"));
    ctrlClick("audit");

    const row = screen.getByText("payments");
    fireEvent.pointerDown(row, { button: 0, clientX: 10, clientY: 10 });
    fireEvent.pointerMove(row, { clientX: 40, clientY: 40 });
    fireEvent.pointerUp(screen.getByRole("button", { name: "Folder Billing" }), { clientX: 40, clientY: 40 });

    await waitFor(() => expect(state.assignments).toEqual([filedTopic("payments", "f1")]));
  });
});

describe("selecting several consumer groups", () => {
  const folder = (id: string, name: string): ResourceFolder => ({ id, kind: "consumer", name });
  const ctrlClick = (name: string) => fireEvent.click(screen.getByText(name), { ctrlKey: true });
  const isHighlighted = (name: string) => screen.getByTestId(`resource-item-${name}`).classList.contains("resource-item--selected");

  async function openConsumers(user: ReturnType<typeof userEvent.setup>) {
    await user.click(screen.getByTestId("category-Consumers"));
    await screen.findByText("checkout");
  }

  it("ctrl+click selects several groups and empties the middle pane's selection", async () => {
    setInvokeHandlers(fakeBackend().handlers);
    const user = userEvent.setup();
    renderTree();
    await openConsumers(user);
    await user.click(screen.getByText("checkout"));
    expect(useWorkspaceSelectionStore.getState().selection).toMatchObject({ type: "consumerGroup", groupId: "checkout" });

    ctrlClick("analytics");

    expect(useWorkspaceSelectionStore.getState().selection).toBeNull();
    expect(isHighlighted("checkout")).toBe(true);
    expect(isHighlighted("analytics")).toBe(true);
  });

  it("shift+click selects the range and a plain click opens one group again", async () => {
    setInvokeHandlers(fakeBackend().handlers);
    const user = userEvent.setup();
    renderTree();
    await openConsumers(user);
    await user.click(screen.getByText("checkout"));

    fireEvent.click(screen.getByText("analytics"), { shiftKey: true });
    expect(useWorkspaceSelectionStore.getState().selection).toBeNull();

    await user.click(screen.getByText("analytics"));
    expect(useWorkspaceSelectionStore.getState().selection).toMatchObject({ type: "consumerGroup", groupId: "analytics" });
    expect(isHighlighted("checkout")).toBe(false);
  });

  it("moves every selected group from the menu and by dragging", async () => {
    const { handlers, state } = fakeBackend({ folders: [folder("g1", "Team")] });
    setInvokeHandlers(handlers);
    const user = userEvent.setup();
    renderTree();
    await openConsumers(user);
    await user.click(screen.getByText("checkout"));
    ctrlClick("analytics");

    fireEvent.contextMenu(screen.getByText("analytics"));
    await chooseFromMenu("Move to folder: Team");
    await waitFor(() => expect(state.assignments).toHaveLength(2));

    // Back to the root: open the folder, then drag a selected row onto the list background.
    await user.click(await screen.findByRole("button", { name: "Folder Team" }));
    const row = screen.getByText("analytics");
    fireEvent.pointerDown(row, { button: 0, clientX: 10, clientY: 10 });
    fireEvent.pointerMove(row, { clientX: 40, clientY: 40 });
    fireEvent.pointerUp(screen.getByLabelText("Search Consumers"), { clientX: 40, clientY: 40 });
    await waitFor(() => expect(state.assignments).toEqual([]));
  });
});
