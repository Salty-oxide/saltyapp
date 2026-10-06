import { useQuery } from "@tanstack/react-query";
import { api } from "../../lib/tauri";
import { useConnectionsQuery } from "../connections/useConnections";
import { defaultTopicQuery, suggestedStreamName } from "./ksqlRows";
import { KsqlWorkspace } from "./KsqlWorkspace";

export interface TopicKsqlTabProps {
  connectionId: string;
  topicName: string;
}

/**
 * A topic's Query tab.
 *
 * ksqlDB cannot `SELECT` from a raw topic — it needs a registered `STREAM` —
 * so before offering a query this has to find out whether one exists. That
 * lookup is the whole reason this component exists rather than the topic
 * panel rendering `KsqlWorkspace` directly.
 */
export function TopicKsqlTab({ connectionId, topicName }: TopicKsqlTabProps) {
  const { data: connections } = useConnectionsQuery();
  const connection = connections?.find((c) => c.id === connectionId);
  const configured = Boolean(connection?.ksqldbEndpoint?.trim());

  const stream = useQuery({
    queryKey: ["ksql-stream", connectionId, topicName],
    queryFn: () => api.ksqlStreamForTopic(connectionId, topicName),
    // Pointless without a server, and the notice below explains that instead.
    enabled: configured,
    // Registering a stream is something the user does in another tool (or in
    // the cluster's Query tab), so this is re-asked on mount rather than held
    // for the life of the app.
    staleTime: 30_000,
    retry: false,
  });

  // Not an error: most clusters run no ksqlDB server, and a connection
  // without one is not misconfigured.
  if (!configured) {
    return (
      <p className="ksql-notice" role="status" data-testid="ksql-not-configured">
        This connection has no ksqlDB server. Add one on the connection's ksqlDB tab to query{" "}
        <code>{topicName}</code> with SQL.
      </p>
    );
  }

  if (stream.isLoading) return <p>Looking for a stream over {topicName}…</p>;

  if (stream.error) {
    return (
      <p role="alert" className="connection-modal-error">
        Could not reach ksqlDB. {stream.error.message}
      </p>
    );
  }

  if (!stream.data) {
    // The one thing ksqlDB genuinely cannot do, stated plainly rather than
    // shown as an empty grid.
    const suggestion = suggestedStreamName(topicName);
    return (
      <div className="ksql-workspace">
        <p className="ksql-notice ksql-notice--warning" role="status" data-testid="ksql-no-stream">
          No ksqlDB <strong>STREAM</strong> is registered over <code>{topicName}</code>, and ksqlDB cannot
          query a raw topic. Register one from the cluster's Query tab — for a topic whose schema the
          registry knows, that is just:
          <br />
          <code>
            CREATE STREAM {suggestion} WITH (KAFKA_TOPIC=&apos;{topicName}&apos;, VALUE_FORMAT=&apos;AVRO&apos;);
          </code>
        </p>
      </div>
    );
  }

  return (
    <KsqlWorkspace
      connectionId={connectionId}
      scope={topicName}
      initialSql={defaultTopicQuery(stream.data)}
      notice={
        <p className="ksql-notice" role="status">
          Tailing <code>{stream.data}</code>, the stream registered over <code>{topicName}</code>. Rows
          arrive as they are produced — press Stop to end it.
        </p>
      }
    />
  );
}
