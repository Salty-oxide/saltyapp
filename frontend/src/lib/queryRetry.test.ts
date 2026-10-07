import { describe, expect, it } from "vitest";
import {
  isAuthError,
  MAX_QUERY_RETRIES,
  NonRetryableQueryError,
  RETRY_DELAY_CAP_MS,
  retryDelay,
  shouldRetry,
} from "./queryRetry";

describe("isAuthError", () => {
  it("recognises the backend's authentication error", () => {
    expect(isAuthError(new Error("authentication error: Invalid username or password"))).toBe(true);
  });

  it("recognises it however the backend cased it", () => {
    expect(isAuthError(new Error("Authentication error: rejected"))).toBe(true);
  });

  it("does not treat a generic kafka error as an auth error", () => {
    expect(isAuthError(new Error("kafka error: failed to fetch topic metadata: Local: Timed out"))).toBe(false);
  });

  it("does not treat a topic authorization failure as an auth error", () => {
    // Authorization is per-resource and the connection's credentials are
    // fine, so this stays retryable.
    expect(isAuthError(new Error("kafka error: Topic authorization failed"))).toBe(false);
  });

  it("is false for a non-Error rejection value", () => {
    expect(isAuthError("something went wrong")).toBe(false);
  });
});

describe("shouldRetry", () => {
  it("never retries an authentication failure", () => {
    // Retrying rejected credentials cannot succeed — it only costs the
    // broker another handshake.
    expect(shouldRetry(0, new Error("authentication error: rejected"))).toBe(false);
  });

  it("retries a transient failure while attempts remain", () => {
    expect(shouldRetry(0, new Error("kafka error: Local: Timed out"))).toBe(true);
  });

  it("stops once the retry allowance is used up", () => {
    expect(shouldRetry(MAX_QUERY_RETRIES, new Error("kafka error: Local: Timed out"))).toBe(false);
  });
});

describe("retryDelay", () => {
  it("backs off further on each successive attempt", () => {
    expect(retryDelay(1)).toBeGreaterThan(retryDelay(0));
  });

  it("never exceeds the cap, however many attempts have failed", () => {
    expect(retryDelay(20)).toBeLessThanOrEqual(RETRY_DELAY_CAP_MS);
  });

  it("jitters, so every open app does not retry the same cluster in lockstep", () => {
    const delays = new Set(Array.from({ length: 50 }, () => retryDelay(2)));
    expect(delays.size).toBeGreaterThan(1);
  });
});

describe("NonRetryableQueryError", () => {
  it("is never retried, however few failures there have been", () => {
    expect(shouldRetry(0, new NonRetryableQueryError("timed out"))).toBe(false);
  });

  it("does not change the policy for ordinary errors", () => {
    expect(shouldRetry(0, new Error("broker unreachable"))).toBe(true);
    expect(shouldRetry(MAX_QUERY_RETRIES, new Error("broker unreachable"))).toBe(false);
  });
});
