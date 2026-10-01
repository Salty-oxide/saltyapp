import { Connection, isKRaftOnly, KAFKA_VERSIONS, NewConnection, SaslMechanism, SecurityProtocol } from "../../../lib/tauri";

/**
 * Editable form state for the New Connection modal. Every field is a plain
 * string/boolean the inputs bind to directly; `toNewConnection` converts
 * this into the wire-format `NewConnection` on Test/Add.
 */
export interface ConnectionDraft {
  name: string;
  bootstrapServers: string;
  kafkaVersion: string;
  zookeeperEnabled: boolean;
  zookeeperHost: string;
  zookeeperPort: string;
  zookeeperChrootPath: string;
  securityProtocol: SecurityProtocol;
  saslMechanism: SaslMechanism | "";
  saslUsername: string;
  saslPassword: string;
  saslOauthUrl: string;
  schemaRegistryEndpoint: string;
  schemaRegistryBasicAuthCredentials: string;
  ksqldbEndpoint: string;
  ksqldbBasicAuthCredentials: string;
  schemaRegistryTrustStoreLocation: string;
  schemaRegistryTrustStorePassword: string;
  schemaRegistryKeystoreLocation: string;
  schemaRegistryKeystorePassword: string;
  schemaRegistryKeystoreKeyPassword: string;
  sslTruststoreLocation: string;
  sslTruststorePassword: string;
  sslKeystoreLocation: string;
  sslKeystorePassword: string;
  sslKeystoreKeyPassword: string;
  /**
   * The Properties tab's "Allow publishing" checkbox. Off in `emptyDraft`, so a
   * connection created through this modal cannot publish until someone
   * deliberately ticks it — matching the column's `DEFAULT 0` and the backend
   * gate that reads it back on every publish.
   */
  allowPublishing: boolean;
}

export function emptyDraft(): ConnectionDraft {
  return {
    name: "",
    bootstrapServers: "",
    kafkaVersion: KAFKA_VERSIONS[KAFKA_VERSIONS.length - 1],
    zookeeperEnabled: false,
    zookeeperHost: "",
    zookeeperPort: "",
    zookeeperChrootPath: "",
    securityProtocol: "PLAINTEXT",
    saslMechanism: "",
    saslUsername: "",
    saslPassword: "",
    saslOauthUrl: "",
    schemaRegistryEndpoint: "",
    schemaRegistryBasicAuthCredentials: "",
    ksqldbEndpoint: "",
    ksqldbBasicAuthCredentials: "",
    schemaRegistryTrustStoreLocation: "",
    schemaRegistryTrustStorePassword: "",
    schemaRegistryKeystoreLocation: "",
    schemaRegistryKeystorePassword: "",
    schemaRegistryKeystoreKeyPassword: "",
    sslTruststoreLocation: "",
    sslTruststorePassword: "",
    sslKeystoreLocation: "",
    sslKeystorePassword: "",
    sslKeystoreKeyPassword: "",
    allowPublishing: false,
  };
}

export function validateDraft(draft: ConnectionDraft): string | null {
  if (draft.name.trim().length === 0) return "Cluster name is required";
  if (draft.bootstrapServers.trim().length === 0) return "Bootstrap servers is required";
  // Not `draft.zookeeperEnabled` alone: on 4.x the ZooKeeper section is
  // hidden, so demanding its fields would block a save on an error naming
  // inputs the user cannot see. `toNewConnection` discards them anyway.
  if (draft.zookeeperEnabled && !isKRaftOnly(draft.kafkaVersion)) {
    if (draft.zookeeperHost.trim().length === 0) {
      return "Zookeeper host is required when Zookeeper is enabled";
    }
    if (draft.zookeeperPort.trim().length === 0) {
      return "Zookeeper port is required when Zookeeper is enabled";
    }
  }
  return null;
}

function nullableTrim(value: string): string | null {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

export function toNewConnection(draft: ConnectionDraft): NewConnection {
  // Kafka 4.0 removed ZooKeeper, so a 4.x connection must never persist
  // ZooKeeper settings: the section is hidden at that point, and a stored
  // `zookeeperEnabled: true` would make the row — and the file
  // `connections_export` writes from it — claim something no 4.x cluster
  // can be doing. The draft itself is left alone, so switching 4.1 -> 3.9
  // inside one modal session brings a typed host back.
  const zookeeperUsable = draft.zookeeperEnabled && !isKRaftOnly(draft.kafkaVersion);
  const zookeeperHost = zookeeperUsable ? nullableTrim(draft.zookeeperHost) : null;
  const zookeeperPort =
    zookeeperUsable && draft.zookeeperPort.trim().length > 0 ? Number(draft.zookeeperPort) : null;
  const zookeeperChrootPath = zookeeperUsable ? nullableTrim(draft.zookeeperChrootPath) : null;

  return {
    name: draft.name.trim(),
    bootstrapServers: draft.bootstrapServers.trim(),
    kafkaVersion: draft.kafkaVersion,
    zookeeperEnabled: zookeeperUsable,
    zookeeperHost,
    zookeeperPort,
    zookeeperChrootPath,
    securityProtocol: draft.securityProtocol,
    saslMechanism: draft.saslMechanism === "" ? null : draft.saslMechanism,
    saslUsername: nullableTrim(draft.saslUsername),
    saslPassword: nullableTrim(draft.saslPassword),
    saslOauthUrl: nullableTrim(draft.saslOauthUrl),
    schemaRegistryEndpoint: nullableTrim(draft.schemaRegistryEndpoint),
    schemaRegistryBasicAuthCredentials: nullableTrim(draft.schemaRegistryBasicAuthCredentials),
    ksqldbEndpoint: nullableTrim(draft.ksqldbEndpoint),
    ksqldbBasicAuthCredentials: nullableTrim(draft.ksqldbBasicAuthCredentials),
    schemaRegistryTrustStoreLocation: nullableTrim(draft.schemaRegistryTrustStoreLocation),
    schemaRegistryTrustStorePassword: nullableTrim(draft.schemaRegistryTrustStorePassword),
    schemaRegistryKeystoreLocation: nullableTrim(draft.schemaRegistryKeystoreLocation),
    schemaRegistryKeystorePassword: nullableTrim(draft.schemaRegistryKeystorePassword),
    schemaRegistryKeystoreKeyPassword: nullableTrim(draft.schemaRegistryKeystoreKeyPassword),
    sslTruststoreLocation: nullableTrim(draft.sslTruststoreLocation),
    sslTruststorePassword: nullableTrim(draft.sslTruststorePassword),
    sslKeystoreLocation: nullableTrim(draft.sslKeystoreLocation),
    sslKeystorePassword: nullableTrim(draft.sslKeystorePassword),
    sslKeystoreKeyPassword: nullableTrim(draft.sslKeystoreKeyPassword),
    allowPublishing: draft.allowPublishing,
  };
}

/**
 * Loads a saved `Connection` (returned by the backend, including secrets —
 * see `Connection`'s doc comment in salty-core) into editable draft
 * state for the cluster detail panel. Secret fields pre-fill the same as
 * every other field; there's no longer a distinction to preserve here.
 */
export function connectionToDraft(connection: Connection): ConnectionDraft {
  return {
    name: connection.name,
    bootstrapServers: connection.bootstrapServers,
    kafkaVersion: connection.kafkaVersion,
    zookeeperEnabled: connection.zookeeperEnabled,
    zookeeperHost: connection.zookeeperHost ?? "",
    zookeeperPort: connection.zookeeperPort !== null ? String(connection.zookeeperPort) : "",
    zookeeperChrootPath: connection.zookeeperChrootPath ?? "",
    securityProtocol: connection.securityProtocol,
    saslMechanism: connection.saslMechanism ?? "",
    saslUsername: connection.saslUsername ?? "",
    saslPassword: connection.saslPassword ?? "",
    saslOauthUrl: connection.saslOauthUrl ?? "",
    schemaRegistryEndpoint: connection.schemaRegistryEndpoint ?? "",
    ksqldbEndpoint: connection.ksqldbEndpoint ?? "",
    ksqldbBasicAuthCredentials: connection.ksqldbBasicAuthCredentials ?? "",
    schemaRegistryBasicAuthCredentials: connection.schemaRegistryBasicAuthCredentials ?? "",
    schemaRegistryTrustStoreLocation: connection.schemaRegistryTrustStoreLocation ?? "",
    schemaRegistryTrustStorePassword: connection.schemaRegistryTrustStorePassword ?? "",
    schemaRegistryKeystoreLocation: connection.schemaRegistryKeystoreLocation ?? "",
    schemaRegistryKeystorePassword: connection.schemaRegistryKeystorePassword ?? "",
    schemaRegistryKeystoreKeyPassword: connection.schemaRegistryKeystoreKeyPassword ?? "",
    sslTruststoreLocation: connection.sslTruststoreLocation ?? "",
    sslTruststorePassword: connection.sslTruststorePassword ?? "",
    sslKeystoreLocation: connection.sslKeystoreLocation ?? "",
    sslKeystorePassword: connection.sslKeystorePassword ?? "",
    sslKeystoreKeyPassword: connection.sslKeystoreKeyPassword ?? "",
    // `?? false` rather than a bare read: a connection row written before this
    // column existed, or an older backend, must mean "not allowed" rather than
    // `undefined` — which would render the checkbox as unchecked while sending
    // `undefined` back on save.
    allowPublishing: connection.allowPublishing ?? false,
  };
}

/**
 * Drives the cluster detail panel's "Update" button — enabled only once the
 * draft has actually diverged from the last-loaded/last-saved snapshot, not
 * just because a field was clicked into.
 */
export function draftsEqual(a: ConnectionDraft, b: ConnectionDraft): boolean {
  return (Object.keys(a) as (keyof ConnectionDraft)[]).every((key) => a[key] === b[key]);
}
