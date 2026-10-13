import { beforeEach, describe, expect, it } from "vitest";
import {
  CLUSTER_HEALTH_MAX_AGE_MS,
  cachedClusterHealth,
  forgetClusterHealth,
  rememberClusterHealth,
} from "./clusterHealthCache";
import type { ClusterHealthLoad } from "./clusterHealthLoader";

const load = (total: number): ClusterHealthLoad => ({ topics: [], total, failed: 0, brokerCount: 3 });

beforeEach(() => {
  forgetClusterHealth("a");
  forgetClusterHealth("b");
});

describe("clusterHealthCache", () => {
  it("returns what was remembered while it is recent", () => {
    rememberClusterHealth("a", load(5), 1_000);
    expect(cachedClusterHealth("a", 1_000 + CLUSTER_HEALTH_MAX_AGE_MS - 1)).toEqual({ load: load(5), at: 1_000 });
  });

  it("returns nothing once it is older than the window", () => {
    rememberClusterHealth("a", load(5), 1_000);
    expect(cachedClusterHealth("a", 1_000 + CLUSTER_HEALTH_MAX_AGE_MS + 1)).toBeNull();
  });

  it("keeps connections apart and forgets one without the other", () => {
    rememberClusterHealth("a", load(1), 0);
    rememberClusterHealth("b", load(2), 0);

    forgetClusterHealth("a");

    expect(cachedClusterHealth("a", 1)).toBeNull();
    expect(cachedClusterHealth("b", 1)?.load.total).toBe(2);
  });

  it("replaces an earlier result", () => {
    rememberClusterHealth("a", load(1), 0);
    rememberClusterHealth("a", load(9), 10);
    expect(cachedClusterHealth("a", 11)).toEqual({ load: load(9), at: 10 });
  });
});
