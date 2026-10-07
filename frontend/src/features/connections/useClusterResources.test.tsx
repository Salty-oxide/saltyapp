import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setInvokeHandlers } from "../../lib/testInvoke";
import { retryDelay, RETRY_DELAY_CAP_MS, shouldRetry } from "../../lib/queryRetry";
import {
  FULL_PAYLOAD_TIMEOUT_MS,
  useBrokers,
  useConsumerGroups,
  useFullPayload,
  useTopics,
} from "./useClusterResources";

vi.mock("@tauri-apps/api/core", () => ({ invoke: vi.fn() }));

/** One client for the whole test, so a remount hits the same cache a real tab switch would. */
function renderWith(client: QueryClient, ui: React.ReactElement) {
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

function newClient() {
  return new QueryClient({ defaultOptions: { queries: { retry: false } } });
}

function Topics() {
  const { data } = useTopics("1", true);
  return <div>topics: {data?.length ?? "…"}</div>;
}

function Brokers() {
  const { data } = useBrokers("1", true);
  return <div>brokers: {data?.length ?? "…"}</div>;
}

function Groups() {
  const { data } = useConsumerGroups("1", true);
  return <div>groups: {data?.length ?? "…"}</div>;
}

beforeEach(() => {
  vi.clearAllMocks();
});

/**
 * Each of these is a full cluster-metadata request on the broker. The tree
 * remounts whenever the user switches top-level tabs, and React Query
 * refetches every stale query when the window regains focus — so without a
 * stale window, ordinary app use re-asks a production cluster for its entire
 * topic list over and over.
 */
describe("cluster resource queries", () => {
  it("serves a remounted topic list from cache instead of asking the broker again", async () => {
    const listTopics = vi.fn(() => [{ name: "orders", partitionCount: 3 }]);
    setInvokeHandlers({ connection_list_topics: listTopics });
    const client = newClient();

    const first = renderWith(client, <Topics />);
    await screen.findByText("topics: 1");
    first.unmount();

    renderWith(client, <Topics />);
    await screen.findByText("topics: 1");

    expect(listTopics).toHaveBeenCalledTimes(1);
  });

  it("serves a remounted broker list from cache", async () => {
    const listBrokers = vi.fn(() => [{ id: 1, host: "localhost", port: 9092 }]);
    setInvokeHandlers({ connection_list_brokers: listBrokers });
    const client = newClient();

    const first = renderWith(client, <Brokers />);
    await screen.findByText("brokers: 1");
    first.unmount();

    renderWith(client, <Brokers />);
    await screen.findByText("brokers: 1");

    expect(listBrokers).toHaveBeenCalledTimes(1);
  });

  it("serves a remounted consumer group list from cache", async () => {
    const listGroups = vi.fn(() => [{ groupId: "g1", state: "Stable" }]);
    setInvokeHandlers({ connection_list_consumer_groups: listGroups });
    const client = newClient();

    const first = renderWith(client, <Groups />);
    await screen.findByText("groups: 1");
    first.unmount();

    renderWith(client, <Groups />);
    await screen.findByText("groups: 1");

    expect(listGroups).toHaveBeenCalledTimes(1);
  });

  // A cluster listing used to go stale after a minute, after which any
  // remount (every top-level tab switch) or any window focus re-asked the
  // broker for the whole list. On a desktop app that is alt-tabbed all day
  // that read as the app polling the cluster on a timer forever, long after
  // the list had been fetched successfully. A listing now stays put until
  // something asks for it again.
  it("never refetches a successfully loaded listing on its own, however long it has been cached", async () => {
    vi.useFakeTimers();
    try {
      const listTopics = vi.fn(() => [{ name: "orders", partitionCount: 3 }]);
      setInvokeHandlers({ connection_list_topics: listTopics });
      const client = newClient();

      const first = renderWith(client, <Topics />);
      await vi.waitFor(() => expect(listTopics).toHaveBeenCalledTimes(1));
      first.unmount();

      await vi.advanceTimersByTimeAsync(60 * 60_000);
      renderWith(client, <Topics />);
      await vi.advanceTimersByTimeAsync(60 * 60_000);

      expect(listTopics).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  // Every attempt is a fresh Kafka client, a socket and — on a secured
  // cluster — a TLS/SASL handshake. A listing that failed is shown as failed
  // and left alone until the user asks again, rather than dialling the
  // broker repeatedly on its own.
  it("does not retry a failed listing", async () => {
    const listTopics = vi.fn(() => {
      throw new Error("broker unreachable");
    });
    setInvokeHandlers({ connection_list_topics: listTopics });
    vi.useFakeTimers();
    try {
      // The app's own retry policy, not the test client's — `shouldRetry`
      // allows two retries, and this is the default these queries have to
      // override. Fake timers because that policy backs off ~1-8s between
      // attempts, so a real-time wait short enough for a test would pass
      // whether or not retrying was actually switched off.
      const client = new QueryClient({ defaultOptions: { queries: { retry: shouldRetry, retryDelay } } });

      renderWith(client, <Topics />);
      await vi.waitFor(() => expect(listTopics).toHaveBeenCalled());
      await vi.advanceTimersByTimeAsync(RETRY_DELAY_CAP_MS * 4);

      expect(listTopics).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("refetches when asked to, so opening a category can pull a fresh list", async () => {
    const listTopics = vi.fn(() => [{ name: "orders", partitionCount: 3 }]);
    setInvokeHandlers({ connection_list_topics: listTopics });
    const client = newClient();
    let refetchTopics: () => void = () => {};

    function RefetchableTopics() {
      const { data, refetch } = useTopics("1", true);
      refetchTopics = refetch;
      return <div>topics: {data?.length ?? "…"}</div>;
    }

    renderWith(client, <RefetchableTopics />);
    await screen.findByText("topics: 1");
    expect(listTopics).toHaveBeenCalledTimes(1);

    refetchTopics();

    await waitFor(() => expect(listTopics).toHaveBeenCalledTimes(2));
  });
  /**
   * The one path that deliberately asks for a whole, untruncated payload —
   * and therefore the one that has to give it up promptly.
   *
   * Its cache key includes the offset, so before `gcTime: 0` every message
   * opened was its own entry surviving React Query's five-minute default:
   * ten 50 MB messages read inside that window meant ~500 MB retained, held
   * by nothing the app's own memory ceiling could see.
   */
  it("drops a viewed message's whole payload as soon as nothing is viewing it", async () => {
    setInvokeHandlers({
      connection_fetch_messages: () => ({
        messages: [
          {
            partition: 0,
            offset: 7,
            timestampMs: null,
            keyBase64: null,
            payloadBase64: btoa("a".repeat(5_000)),
            payloadSizeBytes: 5_000,
            headers: [],
          },
        ],
        totalMatching: 1,
      }),
    });
    const client = newClient();
    function Viewer({ offset }: { offset: number }) {
      const { data } = useFullPayload("1", "orders", 0, offset, true);
      return <div>payload: {data?.messages.length ?? "…"}</div>;
    }

    const { unmount } = renderWith(client, <Viewer offset={7} />);
    await waitFor(() => expect(screen.getByText("payload: 1")).toBeInTheDocument());
    expect(client.getQueryData(["full-payload", "1", "orders", 0, 7])).toBeDefined();

    unmount();

    // Gone immediately, not in five minutes.
    await waitFor(() => expect(client.getQueryData(["full-payload", "1", "orders", 0, 7])).toBeUndefined());
  });

  it("holds only the message currently open, not every message visited", async () => {
    setInvokeHandlers({
      connection_fetch_messages: (args: { filter: { offset: number } }) => ({
        messages: [
          {
            partition: 0,
            offset: args.filter.offset,
            timestampMs: null,
            keyBase64: null,
            payloadBase64: btoa("a".repeat(5_000)),
            payloadSizeBytes: 5_000,
            headers: [],
          },
        ],
        totalMatching: 1,
      }),
    });
    const client = newClient();
    function Viewer({ offset }: { offset: number }) {
      const { data } = useFullPayload("1", "orders", 0, offset, true);
      return <div>payload: {data?.messages[0]?.offset ?? "…"}</div>;
    }

    const { rerender } = renderWith(client, <Viewer offset={1} />);
    await waitFor(() => expect(screen.getByText("payload: 1")).toBeInTheDocument());
    rerender(
      <QueryClientProvider client={client}>
        <Viewer offset={2} />
      </QueryClientProvider>,
    );
    await waitFor(() => expect(screen.getByText("payload: 2")).toBeInTheDocument());

    const held = client
      .getQueryCache()
      .getAll()
      .filter((query) => query.queryKey[0] === "full-payload");
    expect(held).toHaveLength(1);
    expect(held[0].queryKey).toEqual(["full-payload", "1", "orders", 0, 2]);
  });
});


/**
 * The whole-payload fetch has to give up. Its backend idle timer restarts on
 * every message and a retry policy sits on top of it, so without a deadline of
 * its own a stuck fetch kept the viewer on its loading state indefinitely.
 */
describe("useFullPayload deadline", () => {
  const message = (offset: number) => ({
    partition: 0,
    offset,
    timestampMs: null,
    keyBase64: null,
    payloadBase64: btoa("x"),
    payloadSizeBytes: 1,
    headers: [],
  });

  /** The app's real retry policy, which is what would otherwise multiply the wait. */
  function appClient() {
    return new QueryClient({ defaultOptions: { queries: { retry: shouldRetry, retryDelay } } });
  }

  function Viewer() {
    const { data, error, isError } = useFullPayload("1", "orders", 0, 7, true);
    if (isError) return <div>error: {error.message}</div>;
    return <div>payload: {data?.messages.length ?? "…"}</div>;
  }

  afterEach(() => {
    vi.useRealTimers();
  });

  it("is 100 seconds", () => {
    expect(FULL_PAYLOAD_TIMEOUT_MS).toBe(100_000);
  });

  it("fails after 100 seconds, tells the backend to stop, and does not retry the timeout", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const fetched = vi.fn(() => new Promise(() => {}));
    const cancelled = vi.fn();
    setInvokeHandlers({ connection_fetch_messages: fetched, connection_cancel_fetch: cancelled });
    renderWith(appClient(), <Viewer />);
    await waitFor(() => expect(fetched).toHaveBeenCalledTimes(1));

    await vi.advanceTimersByTimeAsync(FULL_PAYLOAD_TIMEOUT_MS - 1_000);
    expect(screen.getByText("payload: …")).toBeInTheDocument();
    await vi.advanceTimersByTimeAsync(1_000);

    await waitFor(() => expect(screen.getByText(/error: .*100 seconds/)).toBeInTheDocument());
    const requestId = (fetched.mock.calls[0] as unknown as [{ requestId: string }])[0].requestId;
    expect(cancelled).toHaveBeenCalledWith({ requestId });
    // Well past the app's retry backoff: still the one attempt.
    await vi.advanceTimersByTimeAsync(60_000);
    expect(fetched).toHaveBeenCalledTimes(1);
  });

  it("does not fire the deadline for a fetch that finishes in time", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const cancelled = vi.fn();
    setInvokeHandlers({
      connection_fetch_messages: () => ({ messages: [message(7)], totalMatching: 1 }),
      connection_cancel_fetch: cancelled,
    });
    renderWith(appClient(), <Viewer />);
    await waitFor(() => expect(screen.getByText("payload: 1")).toBeInTheDocument());

    await vi.advanceTimersByTimeAsync(FULL_PAYLOAD_TIMEOUT_MS * 2);

    expect(cancelled).not.toHaveBeenCalled();
    expect(screen.getByText("payload: 1")).toBeInTheDocument();
  });

  it("reports an error, rather than loading forever, when the fetch returns without the message", async () => {
    setInvokeHandlers({
      connection_fetch_messages: () => ({
        messages: [],
        totalMatching: 0,
        pollError: "Broker: Message size too large",
      }),
    });
    renderWith(newClient(), <Viewer />);

    await waitFor(() => expect(screen.getByText(/error: .*Message size too large/)).toBeInTheDocument());
  });

  it("falls back to a plain message when the empty result has no poll error to quote", async () => {
    setInvokeHandlers({ connection_fetch_messages: () => ({ messages: [], totalMatching: 0 }) });
    renderWith(newClient(), <Viewer />);

    await waitFor(() => expect(screen.getByText(/error: .*could not be read/)).toBeInTheDocument());
  });
});
