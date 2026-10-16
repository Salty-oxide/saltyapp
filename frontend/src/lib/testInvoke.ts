import { invoke } from "@tauri-apps/api/core";
import { vi } from "vitest";

/**
 * The backend's cached commands answer the same question as their live
 * counterparts, wrapped with when the broker was asked. A test that fakes the
 * live command ("this topic has these partitions") gets the cached one derived
 * from it unless it defines its own — so the data a test describes is what the
 * app sees whichever of the two it asks.
 */
const CACHED_FROM_LIVE: Record<string, string> = {
  connection_cached_brokers: "connection_list_brokers",
  connection_cached_topics: "connection_list_topics",
  connection_cached_consumer_groups: "connection_list_consumer_groups",
  acl_cached_list: "acl_list",
  acl_cached_for_resource: "acl_for_resource",
  connection_cached_partitions: "connection_list_partitions",
  connection_cached_topic_config: "connection_describe_topic_config",
};

export function setInvokeHandlers(given: Record<string, (args: any) => unknown>) {
  const handlers = { ...given };
  for (const [cached, live] of Object.entries(CACHED_FROM_LIVE)) {
    const liveHandler = handlers[live];
    if (!handlers[cached] && liveHandler) {
      handlers[cached] = async (args) => ({ value: await liveHandler(args), fetchedAtMs: Date.now() });
    }
  }
  vi.mocked(invoke).mockImplementation((command: string, args?: unknown) => {
    const handler = handlers[command];
    if (!handler) {
      return Promise.reject(new Error(`no mock handler for command: ${command}`));
    }
    return Promise.resolve(handler(args));
  });
}

/**
 * `vi.mocked(invoke).mockImplementation` for a test that routes by command
 * name itself: the cached commands are answered by the live command's branch,
 * wrapped the way the backend wraps them — see `CACHED_FROM_LIVE`.
 */
export function mockInvokeWith(impl: (command: string, args?: any) => unknown) {
  vi.mocked(invoke).mockImplementation(async (command: string, args?: unknown) => {
    const live = CACHED_FROM_LIVE[command];
    if (live) {
      return { value: await impl(live, args), fetchedAtMs: Date.now() };
    }
    return impl(command, args);
  });
}
