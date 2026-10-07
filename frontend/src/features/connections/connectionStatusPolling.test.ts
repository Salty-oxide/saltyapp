import { describe, expect, it } from "vitest";
import { QueryClient } from "@tanstack/react-query";
import {
  hasRecentClusterSuccess,
  REACHABILITY_POLL_MS,
  reachabilityPollInterval,
  UNREACHABLE_RECHECK_MS,
} from "./connectionStatusPolling";

describe("reachabilityPollInterval", () => {
  it("polls a healthy cluster on the slow steady cadence", () => {
    expect(reachabilityPollInterval("REACHABLE")).toBe(REACHABILITY_POLL_MS);
    expect(reachabilityPollInterval("UNKNOWN")).toBe(REACHABILITY_POLL_MS);
    expect(reachabilityPollInterval(undefined)).toBe(REACHABILITY_POLL_MS);
  });

  // The slow cadence is only affordable because a failure is confirmed fast:
  // detection time is steady + recheck, not two steady intervals.
  it("rechecks quickly after a failure so a dead cluster is still noticed promptly", () => {
    expect(reachabilityPollInterval("UNREACHABLE")).toBe(UNREACHABLE_RECHECK_MS);
    expect(UNREACHABLE_RECHECK_MS).toBeLessThan(REACHABILITY_POLL_MS);
  });
});

describe("hasRecentClusterSuccess", () => {
  const NOW = 1_000_000;
  const WINDOW = 30_000;
  const client = () => new QueryClient();

  it("is false when nothing has been fetched for the cluster", () => {
    expect(hasRecentClusterSuccess(client(), "c1", WINDOW, NOW)).toBe(false);
  });

  it("is true when a cluster listing succeeded inside the window", () => {
    const qc = client();
    qc.setQueryData(["topics", "c1"], [], { updatedAt: NOW - 5_000 });
    expect(hasRecentClusterSuccess(qc, "c1", WINDOW, NOW)).toBe(true);
  });

  it("is false once the success is older than the window", () => {
    const qc = client();
    qc.setQueryData(["topics", "c1"], [], { updatedAt: NOW - WINDOW - 1 });
    expect(hasRecentClusterSuccess(qc, "c1", WINDOW, NOW)).toBe(false);
  });

  it("ignores another connection's traffic", () => {
    const qc = client();
    qc.setQueryData(["topics", "c2"], [], { updatedAt: NOW - 1_000 });
    expect(hasRecentClusterSuccess(qc, "c1", WINDOW, NOW)).toBe(false);
  });

  // `connection-connected` and `connection-auth-block` are in-process reads,
  // and `connection-status` is the probe itself: none says the broker answered.
  it.each(["connection-connected", "connection-auth-block", "connection-status", "connections"])(
    "does not count %s, which never touches the broker",
    (root) => {
      const qc = client();
      qc.setQueryData([root, "c1"], true, { updatedAt: NOW - 1_000 });
      expect(hasRecentClusterSuccess(qc, "c1", WINDOW, NOW)).toBe(false);
    },
  );

  it("is false when the cluster's most recent request failed", async () => {
    const qc = client();
    qc.setQueryData(["topics", "c1"], [], { updatedAt: Date.now() - 2_000 });
    await qc
      .fetchQuery({ queryKey: ["brokers", "c1"], queryFn: () => Promise.reject(new Error("down")), retry: false })
      .catch(() => undefined);
    expect(hasRecentClusterSuccess(qc, "c1", WINDOW, Date.now())).toBe(false);
  });

  it("is true again once a request succeeds after the failure", async () => {
    const qc = client();
    await qc
      .fetchQuery({ queryKey: ["brokers", "c1"], queryFn: () => Promise.reject(new Error("down")), retry: false })
      .catch(() => undefined);
    qc.setQueryData(["topics", "c1"], [], { updatedAt: Date.now() + 10 });
    expect(hasRecentClusterSuccess(qc, "c1", WINDOW, Date.now() + 20)).toBe(true);
  });
});
