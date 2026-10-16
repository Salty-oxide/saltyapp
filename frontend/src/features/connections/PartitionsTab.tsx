import { RefreshBar } from "./RefreshBar";
import { usePartitions } from "./useClusterResources";

export interface PartitionsTabProps {
  connectionId: string;
  topicName: string;
}

export function PartitionsTab({ connectionId, topicName }: PartitionsTabProps) {
  const { data: partitions, isLoading, isFetching, fetchedAtMs, refresh } = usePartitions(connectionId, topicName);

  if (isLoading) {
    return <p>Loading partitions…</p>;
  }

  const bar = <RefreshBar fetchedAtMs={fetchedAtMs} refreshing={isFetching} onRefresh={() => void refresh()} />;

  if (!partitions || partitions.length === 0) {
    return (
      <>
        {bar}
        <p>No partitions found for this topic.</p>
      </>
    );
  }

  return (
    <>
      {bar}
      <table className="topic-detail-table">
      <thead>
        <tr>
          <th>ID</th>
          <th>Leader</th>
          <th>Replicas</th>
          <th>ISR</th>
          <th>Low offset</th>
          <th>High offset</th>
        </tr>
      </thead>
      <tbody>
        {partitions.map((partition) => (
          <tr key={partition.id}>
            <td>{partition.id}</td>
            <td>{partition.leader}</td>
            <td>{partition.replicas.join(", ")}</td>
            <td>{partition.isr.join(", ")}</td>
            <td>{partition.lowOffset}</td>
            <td>{partition.highOffset}</td>
          </tr>
        ))}
      </tbody>
      </table>
    </>
  );
}
