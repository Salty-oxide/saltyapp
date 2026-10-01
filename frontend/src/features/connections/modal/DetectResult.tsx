import type { UseMutationResult } from "@tanstack/react-query";
import type { ClusterVersionReport, NewConnection } from "../../../lib/tauri";

const MODE_LABELS: Record<ClusterVersionReport["mode"], string> = {
  kraft: "KRaft mode",
  zookeeper: "ZooKeeper mode",
  unknown: "Mode could not be determined",
};

export interface DetectResultProps {
  mutation: Pick<
    UseMutationResult<ClusterVersionReport, Error, NewConnection>,
    "isSuccess" | "isError" | "data" | "error"
  >;
  /** True when applying the detected version is what removed the ZooKeeper section. */
  hidZookeeper?: boolean;
}

/**
 * The result line beneath the Detect button — `PingResult`'s job for a
 * payload that is more than a `ConnectionStatus`.
 *
 * It shows the two broker configs the mode was derived from, not just the
 * verdict, because the verdict can be `unknown` for two very different
 * reasons (an old broker, or a principal who may not read configs) and the
 * raw values are what let a user tell which.
 */
export function DetectResult({ mutation, hidZookeeper }: DetectResultProps) {
  if (mutation.isSuccess) {
    // `Pick` on `UseMutationResult` keeps each field's own type but drops the
    // discriminated-union correlation between `isSuccess` and `data`, so
    // TypeScript sees `data` as possibly `undefined` here even though React
    // Query guarantees it is set whenever `isSuccess` is true.
    const report = mutation.data as ClusterVersionReport;
    const tone = report.mode === "unknown" ? "ping-result--error" : "ping-result--success";
    return (
      <div role="status" className={`detect-result ${tone}`}>
        <p>{MODE_LABELS[report.mode]}</p>
        {report.processRoles !== null && (
          <p>process.roles = {report.processRoles.length > 0 ? report.processRoles : "(empty)"}</p>
        )}
        {report.interBrokerProtocolVersion !== null && (
          <p>inter.broker.protocol.version = {report.interBrokerProtocolVersion}</p>
        )}
        {report.note !== null && <p>{report.note}</p>}
        {hidZookeeper === true && <p>ZooKeeper settings are hidden — this broker reports KRaft.</p>}
      </div>
    );
  }

  if (mutation.isError) {
    return (
      <p role="alert" className="detect-result ping-result--error">
        {mutation.error instanceof Error ? mutation.error.message : "Unable to detect the cluster version"}
      </p>
    );
  }

  return null;
}
