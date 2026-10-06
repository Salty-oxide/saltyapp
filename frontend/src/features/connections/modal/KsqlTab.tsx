import { ConnectionTabProps } from "./PropertiesTab";

/**
 * Where this connection's ksqlDB server lives.
 *
 * Its own tab rather than a section on Schema, following the reasoning already
 * recorded for that one: Schema is named for what it holds, and every field on
 * it is a Schema Registry setting. ksqlDB is a different server.
 */
export function KsqlTab({ draft, onChange, disabled = false }: ConnectionTabProps) {
  return (
    <div role="tabpanel" aria-label="ksqlDB" className="connection-modal-tab-panel">
      <fieldset disabled={disabled} className="connection-modal-fieldset">
        <section className="connection-modal-section">
          <h3>ksqlDB</h3>
          <p className="connection-modal-hint">
            Optional. ksqlDB is a separate server that runs alongside Kafka — leave this blank if the
            cluster does not have one, and the Query tabs will say so rather than failing.
          </p>
          <label>
            Endpoint
            <input
              value={draft.ksqldbEndpoint}
              onChange={(e) => onChange({ ksqldbEndpoint: e.target.value })}
              placeholder="http://localhost:8088"
            />
          </label>
          <label>
            Basic auth credentials
            <input
              type="password"
              value={draft.ksqldbBasicAuthCredentials}
              onChange={(e) => onChange({ ksqldbBasicAuthCredentials: e.target.value })}
              placeholder="user:password"
            />
          </label>
        </section>
      </fieldset>
    </div>
  );
}
