import { useMutation, useQuery } from "@tanstack/react-query";
import {
  AclResourceAccess,
  AclResourceType,
  api,
  ConsumerGroupLag,
  MessageFetchResult,
  MessageFilter,
  ProtobufDecodeResult,
  SchemaFormat,
} from "../../lib/tauri";

/**
 * How long a per-topic partition listing stays fresh.
 *
 * Unlike the three cluster listings below, this one is genuinely
 * time-sensitive — a partition's low/high watermarks move with every
 * produce, so cached offsets go wrong quickly in a way a topic *name* never
 * does. A short stale window is the right trade here; "fetch once and leave
 * it" is not.
 */
export const PARTITION_LISTING_STALE_MS = 60_000;

/**
 * Shared policy for the three cluster listings (brokers, topics, consumer
 * groups).
 *
 * Each is a full metadata request on the broker — `fetch_metadata` for
 * topics and brokers, a group listing for consumers — and each costs a fresh
 * Kafka client, a socket, and on a secured cluster a TLS and SASL handshake.
 *
 * These used to carry a 60-second stale window, which sounds conservative
 * and isn't: React Query refetches a *stale* query whenever an observer
 * mounts or the window regains focus, and this app remounts the tree on
 * every top-level tab switch and wires Tauri's real OS-level focus event
 * into `focusManager` (see App.tsx). On a desktop app that gets alt-tabbed
 * all day, that meant the broker was re-asked for the entire topic list over
 * and over, indefinitely, long after it had been fetched successfully — the
 * app looked like it was polling the cluster on a timer.
 *
 * So: fetch once, then hold it. `staleTime: Infinity` is the single lever
 * that does this — mount, focus and reconnect refetches all only apply to
 * stale queries. Refreshing is left to the user, who gets a new call by
 * expanding the category (`onExpand` in `ClusterResourceTree`), and to the
 * explicit invalidation that already follows connect/disconnect.
 *
 * `retry: false` for the same reason, overriding the app-wide `shouldRetry`:
 * a listing that failed is shown as failed rather than quietly dialling a
 * broker that is already not answering.
 */
const CLUSTER_LISTING_OPTIONS = {
  staleTime: Infinity,
  // `staleTime` alone isn't enough: the tree unmounts on every top-level tab
  // switch, and a query with no observers is garbage-collected after
  // `gcTime` (5 minutes by default). Coming back to a tab after a coffee
  // therefore found an empty cache and re-asked the broker — "fetched once"
  // has to survive the tree not being on screen. Freshness on reconnect is
  // handled by invalidating these on connect (see `useConnect`), not by
  // letting the cache lapse.
  gcTime: Infinity,
  retry: false,
} as const;

/** Backs the tree's "Brokers" sub-list. Fetched once when the cluster connects; expanding the category refetches — see `CLUSTER_LISTING_OPTIONS`. */
export function useBrokers(connectionId: string, enabled: boolean) {
  return useQuery({
    queryKey: ["brokers", connectionId],
    queryFn: () => api.listBrokers(connectionId),
    enabled,
    ...CLUSTER_LISTING_OPTIONS,
  });
}

/** Backs the tree's "Topics" sub-list. Fetched once when the cluster connects; expanding the category refetches — see `CLUSTER_LISTING_OPTIONS`. */
export function useTopics(connectionId: string, enabled: boolean) {
  return useQuery({
    queryKey: ["topics", connectionId],
    queryFn: () => api.listTopics(connectionId),
    enabled,
    ...CLUSTER_LISTING_OPTIONS,
  });
}

/** Backs the tree's "Consumers" sub-list. Fetched once when the cluster connects; expanding the category refetches — see `CLUSTER_LISTING_OPTIONS`. */
export function useConsumerGroups(connectionId: string, enabled: boolean) {
  return useQuery({
    queryKey: ["consumer-groups", connectionId],
    queryFn: () => api.listConsumerGroups(connectionId),
    enabled,
    ...CLUSTER_LISTING_OPTIONS,
  });
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
  return useQuery({
    queryKey: ["acls", connectionId],
    queryFn: () => api.listAcls(connectionId),
    enabled,
    ...CLUSTER_LISTING_OPTIONS,
  });
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
  return useQuery<AclResourceAccess>({
    queryKey: ["acls", connectionId, resourceType, resourceName],
    queryFn: () => api.aclsForResource(connectionId, resourceType, resourceName),
    enabled,
    ...CLUSTER_LISTING_OPTIONS,
  });
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
    queryFn: () =>
      api.fetchMessages(
        connectionId!,
        topic!,
        {
          partitions: [partition!],
          maxMessagesPerPartition: 1,
          maxTotalMessages: 1,
          fromTimestampMs: null,
          toTimestampMs: null,
          offset: offset!,
          includePayload: true,
          // The whole point of this fetch — the only caller that asks for it.
          maxPayloadPreviewBytes: null,
        },
        crypto.randomUUID(),
        // Deliberately not streamed. This is the one fetch that carries whole,
        // untruncated payloads, and its events would be emitted — megabytes at
        // a time — only for the Data tab's listener to discard them for not
        // matching its request id.
        false,
      ),
    enabled: enabled && connectionId !== null && topic !== null && partition !== undefined && offset !== undefined,
    // Never refetched while it is on screen...
    staleTime: Infinity,
    // ...and dropped the moment it is not. See the note above.
    gcTime: 0,
  });
}

/** Backs the topic detail panel's Partitions tab, and the sidebar tree's per-topic partition expand. */
export function usePartitions(connectionId: string, topic: string, enabled: boolean = true) {
  return useQuery({
    queryKey: ["partitions", connectionId, topic],
    queryFn: () => api.listPartitions(connectionId, topic),
    enabled,
    staleTime: PARTITION_LISTING_STALE_MS,
  });
}

/** Backs the topic detail panel's Config tab. */
export function useTopicConfig(connectionId: string, topic: string) {
  return useQuery({
    queryKey: ["topic-config", connectionId, topic],
    queryFn: () => api.describeTopicConfig(connectionId, topic),
  });
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
