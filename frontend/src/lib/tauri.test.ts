import { describe, expect, it, vi } from "vitest";
import { setInvokeHandlers } from "./testInvoke";
import { api, isKRaftOnly, KAFKA_VERSIONS } from "./tauri";
import { useGeneralSettingsStore } from "../features/settings/useGeneralSettingsStore";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

describe("invoke error normalization", () => {
  it("converts a Tauri command's plain-object rejection into a real Error instance", async () => {
    // This is what a real `CommandError { message: String }` rejection
    // actually looks like once it crosses the Tauri IPC boundary — a plain
    // object, not a JS Error. Every `err instanceof Error` check in the app
    // depends on this being normalized here.
    setInvokeHandlers({
      connection_connect: () => {
        throw { message: "SSL connection closed by peer" };
      },
    });

    await expect(api.connectConnection("1")).rejects.toBeInstanceOf(Error);
    await expect(api.connectConnection("1")).rejects.toThrow("SSL connection closed by peer");
  });

  it("leaves an already-real Error instance unchanged", async () => {
    setInvokeHandlers({
      connection_connect: () => {
        throw new Error("already a real error");
      },
    });

    await expect(api.connectConnection("1")).rejects.toThrow("already a real error");
  });

  it("falls back to String(err) when the rejection has no message field", async () => {
    setInvokeHandlers({
      connection_connect: () => {
        throw "just a string";
      },
    });

    await expect(api.connectConnection("1")).rejects.toThrow("just a string");
  });
});

describe("publishMessages", () => {
  it("sends the messages verbatim, with the user's size and timeout settings", async () => {
    const publish = vi.fn(() => ({ delivered: [], failure: null, notAttempted: 0 }));
    setInvokeHandlers({ connection_publish_messages: publish });
    useGeneralSettingsStore.setState({ maxMessageSizeBytes: 5_000_000, brokerReadTimeoutMs: 20_000 });

    await api.publishMessages("conn-1", "orders", 2, [
      { key: { encoding: "null", text: "" }, value: { encoding: "text", text: "hi" }, headers: [] },
    ]);

    expect(publish).toHaveBeenCalledWith({
      id: "conn-1",
      topic: "orders",
      partition: 2,
      messages: [
        { key: { encoding: "null", text: "" }, value: { encoding: "text", text: "hi" }, headers: [] },
      ],
      maxMessageSizeBytes: 5_000_000,
      writeTimeoutMs: 20_000,
    });
  });

  it("returns the outcome for a publish that failed part-way, rather than rejecting", async () => {
    // A partial publish is the one case where the result matters more than the
    // error: it is the only thing that says which messages are now on the topic.
    const outcome = {
      delivered: [{ index: 0, partition: 2, offset: 7 }],
      failure: { index: 1, kind: "authorization" as const, reason: "no write access" },
      notAttempted: 3,
    };
    setInvokeHandlers({ connection_publish_messages: () => outcome });

    await expect(api.publishMessages("conn-1", "orders", 2, [])).resolves.toEqual(outcome);
  });

  it("rejects with the backend's message when a gate refuses the publish", async () => {
    setInvokeHandlers({
      connection_publish_messages: () => {
        throw { message: "validation error: publishing is disabled for this connection" };
      },
    });

    await expect(api.publishMessages("conn-1", "orders", 0, [])).rejects.toThrow(
      "publishing is disabled for this connection",
    );
  });
});

describe("writeDeniedReason", () => {
  it("asks about one connection and topic", async () => {
    const reason = vi.fn(() => "no write access");
    setInvokeHandlers({ connection_write_denied_reason: reason });

    await expect(api.writeDeniedReason("conn-1", "orders")).resolves.toBe("no write access");
    expect(reason).toHaveBeenCalledWith({ id: "conn-1", topic: "orders" });
  });

  it("reports null for a topic with no recorded denial", async () => {
    setInvokeHandlers({ connection_write_denied_reason: () => null });
    await expect(api.writeDeniedReason("conn-1", "orders")).resolves.toBeNull();
  });
});

describe("KAFKA_VERSIONS", () => {
  it("offers 3.8, 3.9 and the 4.x line", () => {
    expect(KAFKA_VERSIONS).toContain("3.8");
    expect(KAFKA_VERSIONS).toContain("3.9");
    expect(KAFKA_VERSIONS).toContain("4.0");
    expect(KAFKA_VERSIONS).toContain("4.3");
  });

  it("does not offer 2.9, which Kafka never released", () => {
    expect(KAFKA_VERSIONS).not.toContain("2.9");
  });

  it("ends on the newest version, which is what emptyDraft() defaults to", () => {
    expect(KAFKA_VERSIONS[KAFKA_VERSIONS.length - 1]).toBe("4.3");
  });
});

describe("isKRaftOnly", () => {
  it("is false for every version that can still run ZooKeeper", () => {
    expect(isKRaftOnly("0.11")).toBe(false);
    expect(isKRaftOnly("2.8")).toBe(false);
    expect(isKRaftOnly("3.9")).toBe(false);
  });

  it("is true from 4.0 on, which removed ZooKeeper", () => {
    expect(isKRaftOnly("4.0")).toBe(true);
    expect(isKRaftOnly("4.1")).toBe(true);
    expect(isKRaftOnly("4.3")).toBe(true);
  });

  it("is true for a major version beyond the offered list", () => {
    // A version detected from a future broker, or typed into a DB row by
    // hand, must still hide ZooKeeper rather than fall through.
    expect(isKRaftOnly("10.0")).toBe(true);
  });

  it("is false for a version it cannot parse, so unknown hides nothing", () => {
    expect(isKRaftOnly("")).toBe(false);
    expect(isKRaftOnly("not-a-version")).toBe(false);
    expect(isKRaftOnly("2.9")).toBe(false);
  });
});
