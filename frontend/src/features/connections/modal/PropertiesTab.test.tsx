import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { setInvokeHandlers } from "../../../lib/testInvoke";
import { emptyDraft } from "./draft";
import { PropertiesTab } from "./PropertiesTab";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));
vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: vi.fn() }));

function renderWithClient(ui: ReactNode) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("PropertiesTab", () => {
  it("renders the General section's cluster name, bootstrap servers, and kafka version inputs", () => {
    const onChange = vi.fn();
    renderWithClient(<PropertiesTab draft={emptyDraft()} onChange={onChange} />);

    expect(screen.getByLabelText("Cluster name")).toBeInTheDocument();
    expect(screen.getByLabelText("Bootstrap servers")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /4\.3/ })).toBeInTheDocument();
  });

  it("lists 0.11 through 4.3 as kafka version options", async () => {
    const user = userEvent.setup();
    renderWithClient(<PropertiesTab draft={emptyDraft()} onChange={vi.fn()} />);

    await user.click(screen.getByRole("button", { name: /4\.3/ }));

    expect(screen.getByRole("option", { name: "0.11" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "3.0" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "3.8" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "3.9" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "4.0" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "✓ 4.3" })).toBeInTheDocument();
  });

  it("does not offer 2.9, which Kafka never released", async () => {
    const user = userEvent.setup();
    renderWithClient(<PropertiesTab draft={emptyDraft()} onChange={vi.fn()} />);

    await user.click(screen.getByRole("button", { name: /4\.3/ }));

    expect(screen.queryByRole("option", { name: "2.9" })).not.toBeInTheDocument();
  });

  it("still displays a stored version the list no longer offers", async () => {
    // Dropdown falls back to options[0] for an unknown displayedId, so
    // without appending this the row would render as 0.11 while the DB
    // still said 2.9 — and Update would stay disabled, because the draft
    // never changed. Appending keeps the display honest.
    const user = userEvent.setup();
    const draft = { ...emptyDraft(), kafkaVersion: "2.9" };
    renderWithClient(<PropertiesTab draft={draft} onChange={vi.fn()} />);

    expect(screen.getByRole("button", { name: /2\.9/ })).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /2\.9/ }));
    expect(screen.getByRole("option", { name: "✓ 2.9" })).toBeInTheDocument();
  });

  it("calls onChange when the cluster name is typed", async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();
    renderWithClient(<PropertiesTab draft={emptyDraft()} onChange={onChange} />);

    await user.type(screen.getByLabelText("Cluster name"), "L");

    expect(onChange).toHaveBeenCalledWith({ name: "L" });
  });

  it("replaces the zookeeper section with the KRaft notice on 4.x", () => {
    const draft = { ...emptyDraft(), kafkaVersion: "4.1", zookeeperEnabled: true };
    renderWithClient(<PropertiesTab draft={draft} onChange={vi.fn()} />);

    expect(screen.queryByLabelText("Enable Zookeeper")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Zookeeper host")).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "KRaft" })).toBeInTheDocument();
  });

  it("keeps the zookeeper section on 3.9, the last version that can run it", () => {
    const draft = { ...emptyDraft(), kafkaVersion: "3.9", zookeeperEnabled: true };
    renderWithClient(<PropertiesTab draft={draft} onChange={vi.fn()} />);

    expect(screen.getByLabelText("Enable Zookeeper")).toBeInTheDocument();
    expect(screen.getByLabelText("Zookeeper host")).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "KRaft" })).not.toBeInTheDocument();
  });

  it("does not show zookeeper host/port/chroot fields until zookeeper is enabled", () => {
    const draft = { ...emptyDraft(), kafkaVersion: "3.9" };
    renderWithClient(<PropertiesTab draft={draft} onChange={vi.fn()} />);

    expect(screen.getByLabelText("Enable Zookeeper")).toBeInTheDocument();
    expect(screen.queryByLabelText("Zookeeper host")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Zookeeper port")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Zookeeper chroot path")).not.toBeInTheDocument();
  });

  it("shows zookeeper host/port/chroot fields once zookeeper is enabled", () => {
    const draft = { ...emptyDraft(), kafkaVersion: "3.9", zookeeperEnabled: true };
    renderWithClient(<PropertiesTab draft={draft} onChange={vi.fn()} />);

    expect(screen.getByLabelText("Zookeeper host")).toBeInTheDocument();
    expect(screen.getByLabelText("Zookeeper port")).toBeInTheDocument();
    expect(screen.getByLabelText("Zookeeper chroot path")).toBeInTheDocument();
  });

  it("pings the bootstrap servers and shows a success message", async () => {
    setInvokeHandlers({ connection_ping_bootstrap: () => "REACHABLE" });
    const user = userEvent.setup();
    const draft = { ...emptyDraft(), bootstrapServers: "localhost:9092" };
    renderWithClient(<PropertiesTab draft={draft} onChange={vi.fn()} />);

    await user.click(screen.getByRole("button", { name: "Ping bootstrap servers" }));

    expect(await screen.findByText("Success")).toBeInTheDocument();
  });

  it("shows a failure message when the bootstrap servers ping is unreachable", async () => {
    setInvokeHandlers({ connection_ping_bootstrap: () => "UNREACHABLE" });
    const user = userEvent.setup();
    const draft = { ...emptyDraft(), bootstrapServers: "localhost:9092" };
    renderWithClient(<PropertiesTab draft={draft} onChange={vi.fn()} />);

    await user.click(screen.getByRole("button", { name: "Ping bootstrap servers" }));

    expect(await screen.findByText(/unable to reach/i)).toBeInTheDocument();
  });

  it("disables the bootstrap servers ping button while bootstrap servers is empty", () => {
    renderWithClient(<PropertiesTab draft={emptyDraft()} onChange={vi.fn()} />);
    expect(screen.getByRole("button", { name: "Ping bootstrap servers" })).toBeDisabled();
  });

  it("pings zookeeper and shows a success message", async () => {
    setInvokeHandlers({ connection_ping_zookeeper: () => "REACHABLE" });
    const user = userEvent.setup();
    const draft = {
      ...emptyDraft(),
      kafkaVersion: "3.9",
      zookeeperEnabled: true,
      zookeeperHost: "zk.local",
      zookeeperPort: "2181",
    };
    renderWithClient(<PropertiesTab draft={draft} onChange={vi.fn()} />);

    await user.click(screen.getByRole("button", { name: "Ping zookeeper" }));

    await waitFor(() => expect(screen.getByText("Success")).toBeInTheDocument());
  });

  it("disables the zookeeper ping button until both host and port are filled in", () => {
    const draft = { ...emptyDraft(), kafkaVersion: "3.9", zookeeperEnabled: true };
    renderWithClient(<PropertiesTab draft={draft} onChange={vi.fn()} />);
    expect(screen.getByRole("button", { name: "Ping zookeeper" })).toBeDisabled();
  });

  it("keeps cluster name editable but disables every other field when disabled is true", () => {
    const draft = {
      ...emptyDraft(),
      kafkaVersion: "3.9",
      zookeeperEnabled: true,
      zookeeperHost: "zk.local",
      zookeeperPort: "2181",
    };
    renderWithClient(<PropertiesTab draft={draft} onChange={vi.fn()} disabled />);

    expect(screen.getByLabelText("Cluster name")).toBeEnabled();
    expect(screen.getByLabelText("Bootstrap servers")).toBeDisabled();
    expect(screen.getByRole("button", { name: /3\.9/ })).toBeDisabled();
    expect(screen.getByLabelText("Enable Zookeeper")).toBeDisabled();
    expect(screen.getByLabelText("Zookeeper host")).toBeDisabled();
    expect(screen.getByLabelText("Zookeeper port")).toBeDisabled();
    expect(screen.getByLabelText("Zookeeper chroot path")).toBeDisabled();
    expect(screen.getByRole("button", { name: "Ping bootstrap servers" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Ping zookeeper" })).toBeDisabled();
  });

  it("still disables identity fields on 4.x, but leaves the KRaft link usable", () => {
    // The KRaft notice deliberately sits outside the `disabled` fieldset the
    // ZooKeeper section lived in: `disabled` locks connection *identity*
    // while a cluster is connected, and this notice reads no draft state and
    // only opens a documentation URL. Same reasoning as the Publishing
    // checkbox. Without this test, nesting it back inside that fieldset
    // would break nothing.
    const draft = { ...emptyDraft(), kafkaVersion: "4.1" };
    renderWithClient(<PropertiesTab draft={draft} onChange={vi.fn()} disabled />);

    expect(screen.getByLabelText("Cluster name")).toBeEnabled();
    expect(screen.getByLabelText("Bootstrap servers")).toBeDisabled();
    expect(screen.getByRole("button", { name: /4\.1/ })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Learn more about KRaft" })).toBeEnabled();
  });

  describe("Publishing", () => {
    it("starts with publishing switched off", () => {
      renderWithClient(<PropertiesTab draft={emptyDraft()} onChange={vi.fn()} />);
      expect(
        screen.getByLabelText("Allow publishing messages to this cluster"),
      ).not.toBeChecked();
    });

    it("reports the flag to the draft when ticked", async () => {
      const onChange = vi.fn();
      const user = userEvent.setup();
      renderWithClient(<PropertiesTab draft={emptyDraft()} onChange={onChange} />);

      await user.click(screen.getByLabelText("Allow publishing messages to this cluster"));

      expect(onChange).toHaveBeenCalledWith({ allowPublishing: true });
    });

    it("shows the flag as ticked when the draft says so", () => {
      renderWithClient(
        <PropertiesTab draft={{ ...emptyDraft(), allowPublishing: true }} onChange={vi.fn()} />,
      );
      expect(screen.getByLabelText("Allow publishing messages to this cluster")).toBeChecked();
    });

    it("says that the broker's permissions still apply", () => {
      // The checkbox must not read as though it grants write access — it cannot.
      renderWithClient(<PropertiesTab draft={emptyDraft()} onChange={vi.fn()} />);
      expect(
        screen.getByText(/broker's own permissions always apply/i),
      ).toBeInTheDocument();
    });

    it("stays editable while the cluster is connected, unlike the identity fields", () => {
      // `disabled` freezes the fields that define *which* cluster this is,
      // because a live client cannot be re-described mid-session. This flag
      // changes no client — it is re-read on every publish — and granting or
      // revoking publishing on the cluster you are looking at is the point.
      renderWithClient(<PropertiesTab draft={emptyDraft()} onChange={vi.fn()} disabled />);

      expect(screen.getByLabelText("Bootstrap servers")).toBeDisabled();
      expect(
        screen.getByLabelText("Allow publishing messages to this cluster"),
      ).toBeEnabled();
    });
  });

  it("detects the cluster version and applies it to the dropdown", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    setInvokeHandlers({
      connection_detect_version: () => ({
        mode: "kraft",
        processRoles: "broker,controller",
        interBrokerProtocolVersion: "4.1-IV0",
        suggestedVersion: "4.1",
        note: "derived from inter.broker.protocol.version",
      }),
    });
    const draft = { ...emptyDraft(), bootstrapServers: "localhost:9092", kafkaVersion: "3.7" };
    renderWithClient(<PropertiesTab draft={draft} onChange={onChange} />);

    await user.click(screen.getByRole("button", { name: "Detect cluster version" }));

    await waitFor(() => expect(onChange).toHaveBeenCalledWith({ kafkaVersion: "4.1" }));
    expect(await screen.findByText("KRaft mode")).toBeInTheDocument();
  });

  it("does not offer Detect before there are bootstrap servers to ask", () => {
    renderWithClient(<PropertiesTab draft={emptyDraft()} onChange={vi.fn()} />);

    expect(screen.getByRole("button", { name: "Detect cluster version" })).toBeDisabled();
  });

  it("leaves the version alone when the broker suggests none", async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();
    setInvokeHandlers({
      connection_detect_version: () => ({
        mode: "unknown",
        processRoles: null,
        interBrokerProtocolVersion: null,
        suggestedVersion: null,
        note: "The broker returned neither config.",
      }),
    });
    const draft = { ...emptyDraft(), bootstrapServers: "localhost:9092" };
    renderWithClient(<PropertiesTab draft={draft} onChange={onChange} />);

    await user.click(screen.getByRole("button", { name: "Detect cluster version" }));

    expect(await screen.findByText("Mode could not be determined")).toBeInTheDocument();
    expect(onChange).not.toHaveBeenCalled();
  });
});
