import { openUrl } from "@tauri-apps/plugin-opener";

/**
 * Apache Kafka's own KRaft documentation.
 *
 * This must stay under `https://kafka.apache.org/` — that is the only host
 * `src-tauri/capabilities/default.json` grants `opener:allow-open-url` for,
 * and a URL outside the scope is refused at runtime without visible
 * feedback. Widening the link means widening the capability.
 */
export const KRAFT_DOCS_URL = "https://kafka.apache.org/documentation/#kraft";

/**
 * Takes the ZooKeeper section's place in the Properties tab whenever the
 * selected Kafka version is 4.x or later.
 *
 * The question it answers is "where do I point this at instead?", and the
 * answer is nowhere: clients only ever talk to `bootstrap.servers`. The
 * KRaft controller quorum is not a client endpoint — `bootstrap.controllers`
 * (KIP-919) is Java-admin-only and librdkafka binds nothing for it — so
 * there is no field to replace the ZooKeeper host with.
 */
export function KRaftNotice() {
  return (
    <section className="connection-modal-section">
      <h3>KRaft</h3>
      <p className="connection-modal-hint">
        Kafka 4.0 removed ZooKeeper. Cluster metadata lives in the KRaft controller quorum, which
        clients never connect to — the bootstrap servers above are the only endpoint Salty needs.
      </p>
      <button
        type="button"
        onClick={() => {
          // Not `void`: a rejection here (no default browser, or a URL that
          // has drifted outside the capability's scope) would otherwise be
          // an unhandled rejection with nothing to show for it. There is no
          // UI to put an error in — this section is an explainer, not a
          // form — so the console is the honest place for it.
          openUrl(KRAFT_DOCS_URL).catch((err: unknown) => {
            console.error("failed to open the KRaft documentation", err);
          });
        }}
      >
        Learn more about KRaft
      </button>
    </section>
  );
}
