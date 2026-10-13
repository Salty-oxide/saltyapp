import { api } from "../../lib/tauri";
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
}

export interface LoadOptions {
  /** Called as each topic finishes, with how many have so far. */
  onProgress?: (loaded: number, total: number) => void;
  signal?: AbortSignal;
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
  const { onProgress, signal } = options;
  const cancelled = () => new Error("Cluster analysis was cancelled.");
  if (signal?.aborted) throw cancelled();

  const [topicList, brokers] = await Promise.all([api.listTopics(connectionId), api.listBrokers(connectionId)]);
  const brokerCount = Math.max(1, brokers.length);

  const results: TopicAnalysis[] = [];
  let failed = 0;
  let done = 0;
  let next = 0;

  async function worker() {
    while (next < topicList.length) {
      if (signal?.aborted) return;
      const topic = topicList[next++];
      try {
        const partitions = await api.listPartitions(connectionId, topic.name);
        const config = await api.describeTopicConfig(connectionId, topic.name).catch(() => []);
        results.push(analyzeTopic(topic.name, partitions, config, brokerCount));
      } catch {
        failed += 1;
      }
      done += 1;
      onProgress?.(done, topicList.length);
    }
  }

  await Promise.all(Array.from({ length: Math.min(LOAD_CONCURRENCY, topicList.length) }, worker));
  if (signal?.aborted) throw cancelled();
  return { topics: results, total: topicList.length, failed, brokerCount };
}
