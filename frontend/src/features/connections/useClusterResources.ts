import { useCallback, useRef } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import {
  AclResourceAccess,
  AclResourceType,
  api,
  Cached,
  ConsumerGroupLag,
  MessageFetchResult,
  MessageFilter,
  ProtobufDecodeResult,
  SchemaFormat,
} from "../../lib/tauri";
import { NonRetryableQueryError } from "../../lib/queryRetry";

/**
 * Every read of cluster data — the listings, ACLs, partitions and topic
 * config — is cached by the backend (`salty_kafka::metadata_cache`, 24 h,
 * purged when the session ends) and reached through the `connection_cached_*`
 * commands. There is deliberately **no TTL here**: a remount or window focus is
 * an IPC call the backend answers from memory, so React Query's `staleTime`
 * stays at its default and nothing needs to be held for "a while".
 *
 * `refresh()` is the only way to reach the broker again: it makes the next
 * read send `refresh: true`, which replaces the backend's entry. It rides
 * `refetch`, so loading and error state behave as for any read.
 *
 * `retry: false` — a listing that failed is shown as failed rather than
 * quietly dialling a broker that is already not answering.
 */
function useBackendCached<T>(
  queryKey: readonly unknown[],
  read: (refresh: boolean) => Promise<Cached<T>>,
  enabled: boolean,
) {
  const force = useRef(false);
  const query = useQuery({
    queryKey,
    queryFn: () => {
      const refresh = force.current;
      force.current = false;
      return read(refresh);
    },
    enabled,
    retry: false,
  });
  const { refetch } = query;
  const refresh = useCallback(async () => {
    force.current = true;
    try {
      await refetch();
    } finally {
      // `refetch` cancels a read already in flight and starts its own, so the
      // flag is consumed by this call's `queryFn`; this only clears it if the
      // refetch never reached `queryFn` (the query was removed meanwhile).
      force.current = false;
    }
  }, [refetch]);
  return { ...query, data: query.data?.value, fetchedAtMs: query.data?.fetchedAtMs, refresh };
}

/** Backs the tree's "Brokers" sub-list. */
export function useBrokers(connectionId: string, enabled: boolean) {
  return useBackendCached(["brokers", connectionId], (refresh) => api.cachedBrokers(connectionId, refresh), enabled);
}

/** Backs the tree's "Topics" sub-list. */
export function useTopics(connectionId: string, enabled: boolean) {
  return useBackendCached(["topics", connectionId], (refresh) => api.cachedTopics(connectionId, refresh), enabled);
}

/** Backs the tree's "Consumers" sub-list. */
export function useConsumerGroups(connectionId: string, enabled: boolean) {
  return useBackendCached(
    ["consumer-groups", connectionId],
    (refresh) => api.cachedConsumerGroups(connectionId, refresh),
    enabled,
  );
}

/**
 * Backs the tree's "Access Control" sub-list.
 *
 * Lazily enabled, like Consumers and for the same reason: listing ACLs needs
 * `Describe` on the `Cluster` resource, which a principal with full access to
 * every topic is routinely not granted. Paying that round trip — and
 * surfacing that refusal — on every connect, for a category the user may
 * never open, is work nobody asked for.
 */
export function useAcls(connectionId: string, enabled: boolean) {
  return useBackendCached(["acls", connectionId], (refresh) => api.cachedAcls(connectionId, refresh), enabled);
}

/**
 * Backs a topic's or consumer group's Access tab.
 *
 * A separate broker call rather than a filter over `useAcls`: the backend
 * sends this one in MATCH mode so the *broker* resolves which literal,
 * prefixed and wildcard patterns govern the name. Filtering client-side would
 * mean reimplementing the authorizer's pattern matching.
 */
export function useResourceAcls(
  connectionId: string,
  resourceType: AclResourceType,
  resourceName: string,
  enabled: boolean,
) {
  return useBackendCached(
    ["acls", connectionId, resourceType, resourceName],
    (refresh) => api.cachedAclsForResource(connectionId, resourceType, resourceName, refresh),
    enabled,
  );
}

/** Backs the topic detail panel's Properties > Messages "Refresh" button. */
export function useCountTopicMessages() {
  return useMutation<number, Error, { connectionId: string; topic: string }>({
    mutationFn: ({ connectionId, topic }) => api.countTopicMessages(connectionId, topic),
  });
}

/** Backs the topic Data tab's Fetch button. `requestId` tags the backend's streamed `"messages-batch"` events so a listener can tell this fetch's rows apart from a superseded one. */
export function useFetchMessages() {
  return useMutation<
    MessageFetchResult,
    Error,
    {
      connectionId: string;
      topic: string;
      filter: MessageFilter;
      requestId: string;
      /** See `api.fetchMessages` — true for the Data tab's Fetch, false for a single-row fetch nothing is listening for. */
      streamUpdates: boolean;
    }
  >({
    mutationFn: ({ connectionId, topic, filter, requestId, streamUpdates }) =>
      api.fetchMessages(connectionId, topic, filter, requestId, streamUpdates),
  });
}

/**
 * Fetches one message's whole payload, for the payload viewer.
 *
 * The Data tab's own fetch deliberately carries only a bounded preview of
 * each payload (see `MessageFilter.maxPayloadPreviewBytes`) — enough for the
 * grid's one-line Value cell, and the reason a 1,000-row fetch of
 * multi-megabyte records no longer moves gigabytes of base64 into the
 * webview. Actually displaying or decoding a message needs the real bytes,
 * so they're pulled one message at a time, only for the message being
 * looked at.
 *
 * Held in React Query's cache rather than patched back into the tab's cached
 * rows: the row cache lives as long as the tab does, so writing full payloads
 * into it would rebuild the retention problem one opened message at a time,
 * whereas these are evicted once nothing is viewing them.
 *
 * `gcTime: 0` is what makes "evicted once nothing is viewing them" true
 * immediately rather than five minutes later (React Query's default), and it
 * is the whole of this hook's memory safety. Every other path into the
 * webview is bounded — the grid truncates each row to
 * `maxPayloadPreviewBytes`, and the tab caches are held under an app-wide
 * ceiling — but this one deliberately asks for the *whole* message, and its
 * cache key includes the offset, so each message opened was its own entry.
 * Ten 50 MB messages opened inside the default window meant ~500 MB retained,
 * counted by nothing. Bounded to one payload at a time, the worst case is the
 * single message on screen, which is the one the user is asking to see.
 *
 * The cost is that clicking back to a message just viewed re-fetches it. That
 * is one bounded single-message round trip, against holding every message
 * visited in the last five minutes — and it is the same trade the Data tab's
 * own row eviction makes.
 */
export function useFullPayload(
  connectionId: string | null,
  topic: string | null,
  partition: number | undefined,
  offset: number | undefined,
  enabled: boolean,
) {
  return useQuery({
    queryKey: ["full-payload", connectionId, topic, partition, offset],
    queryFn: () => fetchFullPayload(connectionId!, topic!, partition!, offset!),
    enabled: enabled && connectionId !== null && topic !== null && partition !== undefined && offset !== undefined,
    // Never refetched while it is on screen...
    staleTime: Infinity,
    // ...and dropped the moment it is not. See the note above.
    gcTime: 0,
  });
}

/**
 * The longest one whole-payload fetch may take, start to finish.
 *
 * The backend's own limits do not add up to a deadline: its idle timer
 * restarts on every message and its read timeout only covers the metadata
 * calls, so a fetch that was merely slow or stuck left the viewer on its
 * loading state with nothing to end it. This is that ending.
 */
export const FULL_PAYLOAD_TIMEOUT_MS = 100_000;

/**
 * The full payload is not coming: it timed out, or the fetch finished without
 * the message. Not retried — retrying would make "100 seconds" three times
 * that. Anything else this query throws keeps the app-wide retry policy.
 */
class FullPayloadUnavailableError extends NonRetryableQueryError {}

async function fetchFullPayload(
  connectionId: string,
  topic: string,
  partition: number,
  offset: number,
): Promise<MessageFetchResult> {
  const requestId = crypto.randomUUID();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      // Abandoning the promise would leave the backend polling the broker for
      // a payload nothing is waiting for any more.
      api.cancelFetch(requestId).catch(() => {});
      reject(
        new FullPayloadUnavailableError(
          `Timed out after ${FULL_PAYLOAD_TIMEOUT_MS / 1000} seconds waiting for the broker to return the message.`,
        ),
      );
    }, FULL_PAYLOAD_TIMEOUT_MS);
  });
  try {
    const result = await Promise.race([
      api.fetchMessages(
        connectionId,
        topic,
        {
          partitions: [partition],
          maxMessagesPerPartition: 1,
          maxTotalMessages: 1,
          fromTimestampMs: null,
          toTimestampMs: null,
          offset,
          // The whole point of this fetch — the only caller that asks for it.
          maxPayloadPreviewBytes: null,
        },
        requestId,
        // Deliberately not streamed. This is the one fetch that carries whole,
        // untruncated payloads, and its events would be emitted — megabytes at
        // a time — only for the Data tab's listener to discard them for not
        // matching its request id.
        false,
      ),
      deadline,
    ]);
    // A fetch that ends without the message (a record over the size limit,
    // say, which arrives as `pollError`) is a success as far as the call is
    // concerned. Left as one, nothing would ever fill the payload in and the
    // viewer would report "loading" for good.
    if (!result.messages.some((m) => m.partition === partition && m.offset === offset)) {
      throw new FullPayloadUnavailableError(
        result.pollError
          ? `The full payload could not be read: ${result.pollError}`
          : "The full payload could not be read from the broker.",
      );
    }
    return result;
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Backs the topic detail panel's Partitions tab, the Metrics skew view, and the
 * sidebar tree's per-topic partition expand. Backend-cached — see
 * `useBackendCached`; `fetchedAtMs` is when the broker was last asked.
 */
export function usePartitions(connectionId: string, topic: string, enabled: boolean = true) {
  return useBackendCached(
    ["partitions", connectionId, topic],
    (refresh) => api.cachedPartitions(connectionId, topic, refresh),
    enabled,
  );
}

/**
 * Backs the topic Metrics tab's From/To window. A window is a question about
 * the log at a moment, not a listing, so the result is neither kept after the
 * tab closes nor refetched on its own.
 */
export function usePartitionMessageCounts(
  connectionId: string,
  topic: string,
  fromMs: number | null,
  toMs: number | null,
  enabled: boolean,
) {
  return useQuery({
    queryKey: ["partition-message-counts", connectionId, topic, fromMs, toMs],
    queryFn: () => api.countPartitionMessages(connectionId, topic, fromMs, toMs),
    enabled,
    gcTime: 0,
    refetchOnWindowFocus: false,
  });
}

/** Backs the topic detail panel's Config tab. Backend-cached — see `useBackendCached`. */
export function useTopicConfig(connectionId: string, topic: string) {
  return useBackendCached(
    ["topic-config", connectionId, topic],
    (refresh) => api.cachedTopicConfig(connectionId, topic, refresh),
    true,
  );
}

/** Backs the consumer group detail panel's "Refresh" button. */
export function useFetchConsumerGroupLag() {
  return useMutation<ConsumerGroupLag, Error, { connectionId: string; groupId: string }>({
    mutationFn: ({ connectionId, groupId }) => api.fetchConsumerGroupLag(connectionId, groupId),
  });
}

/** Backs the topic detail panel's Schema tab. */
export function useTopicSchema(connectionId: string, topic: string, format: SchemaFormat) {
  return useQuery({
    queryKey: ["topic-schema", connectionId, topic, format],
    queryFn: () => api.getTopicSchema(connectionId, topic, format),
  });
}

/** Backs the Schema tab's Save button. */
export function useSetTopicSchema() {
  return useMutation<void, Error, { connectionId: string; topic: string; format: SchemaFormat; schemaText: string }>({
    mutationFn: ({ connectionId, topic, format, schemaText }) =>
      api.setTopicSchema(connectionId, topic, format, schemaText),
  });
}

/** Backs the Schema tab's Clear button. */
export function useDeleteTopicSchema() {
  return useMutation<void, Error, { connectionId: string; topic: string; format: SchemaFormat }>({
    mutationFn: ({ connectionId, topic, format }) => api.deleteTopicSchema(connectionId, topic, format),
  });
}

/** Backs the payload viewer's "Avro" mode button. */
export function useDecodeAvro() {
  return useMutation<unknown, Error, { connectionId: string; topic: string; payloadBase64: string }>({
    mutationFn: ({ connectionId, topic, payloadBase64 }) => api.decodeAvro(connectionId, topic, payloadBase64),
  });
}

export function useDecodeProtobuf() {
  return useMutation<ProtobufDecodeResult, Error, { connectionId: string; topic: string; payloadBase64: string }>({
    mutationFn: ({ connectionId, topic, payloadBase64 }) => api.decodeProtobuf(connectionId, topic, payloadBase64),
  });
}
