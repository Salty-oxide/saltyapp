import { api, ConfigEntry } from "../../lib/tauri";
import { analyzeTopic, TopicAnalysis } from "./clusterHealth";

/** Topics read at once — each is two broker round trips, and a cluster can have hundreds of topics. */
export const LOAD_CONCURRENCY = 6;

export interface ClusterHealthLoad {
  topics: TopicAnalysis[];
  /** Topics the cluster has, whether or not each could be read. */
  total: number;
  /** Topics whose partitions could not be read (left out of `topics`). */
  failed: number;
  brokerCount: number;
  /**
   * When the broker was asked for the oldest figure behind the analysis. The
   * per-topic reads are served by the backend's 24 h cache, so this — not the
   * moment the analysis ran — is how old the numbers are. `null` with no topics.
   */
  oldestFetchedAtMs: number | null;
}

export interface LoadOptions {
  /** Called as each topic finishes, with how many have so far. */
  onProgress?: (loaded: number, total: number) => void;
  signal?: AbortSignal;
  /** Skip the backend's cache and read every topic from the broker — the Refresh button. */
  refresh?: boolean;
}

/**
 * Reads every topic's partitions and config and analyses them.
 *
 * Per-topic work is bounded in parallel and abandoned on `signal`. A topic
 * whose partitions cannot be read is counted in `failed` rather than failing
 * the whole load; one whose *config* cannot be read is still analysed, with the
 * broker's default minimum of 1 — a principal may be allowed to see a topic's
 * layout and not its settings.
 */
export async function loadClusterHealth(connectionId: string, options: LoadOptions): Promise<ClusterHealthLoad> {
  const { onProgress, signal, refresh = false } = options;
  const cancelled = () => new Error("Cluster analysis was cancelled.");
  if (signal?.aborted) throw cancelled();

  const [topicRead, brokerRead] = await Promise.all([
    api.cachedTopics(connectionId, refresh),
    api.cachedBrokers(connectionId, refresh),
  ]);
  const topicList = topicRead.value;
  const brokerCount = Math.max(1, brokerRead.value.length);

  const results: TopicAnalysis[] = [];
  let oldest: number | null = Math.min(topicRead.fetchedAtMs, brokerRead.fetchedAtMs);
  const note = (fetchedAtMs: number) => {
    oldest = oldest === null ? fetchedAtMs : Math.min(oldest, fetchedAtMs);
  };
  let failed = 0;
  let done = 0;
  let next = 0;

  async function worker() {
    while (next < topicList.length) {
      if (signal?.aborted) return;
      const topic = topicList[next++];
      try {
        const partitions = await api.cachedPartitions(connectionId, topic.name, refresh);
        const config = await api
          .cachedTopicConfig(connectionId, topic.name, refresh)
          .catch(() => ({ value: [] as ConfigEntry[], fetchedAtMs: partitions.fetchedAtMs }));
        note(partitions.fetchedAtMs);
        note(config.fetchedAtMs);
        results.push(analyzeTopic(topic.name, partitions.value, config.value, brokerCount));
      } catch {
        failed += 1;
      }
      done += 1;
      onProgress?.(done, topicList.length);
    }
  }

  await Promise.all(Array.from({ length: Math.min(LOAD_CONCURRENCY, topicList.length) }, worker));
  if (signal?.aborted) throw cancelled();
  return { topics: results, total: topicList.length, failed, brokerCount, oldestFetchedAtMs: oldest };
}
