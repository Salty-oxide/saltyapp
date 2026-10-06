import type { NewConnection } from "../../lib/tauri";

/** A fully-populated, all-defaults NewConnection for tests to override from. */
export function sampleNewConnection(overrides: Partial<NewConnection> = {}): NewConnection {
  return {
    name: "Local Kafka",
    bootstrapServers: "localhost:9092",
    kafkaVersion: "3.7",
    zookeeperEnabled: false,
    zookeeperHost: null,
    zookeeperPort: null,
    zookeeperChrootPath: null,
    securityProtocol: "PLAINTEXT",
    saslMechanism: null,
    saslUsername: null,
    saslPassword: null,
    saslOauthUrl: null,
    schemaRegistryEndpoint: null,
    schemaRegistryBasicAuthCredentials: null,
    ksqldbEndpoint: null,
    ksqldbBasicAuthCredentials: null,
    schemaRegistryTrustStoreLocation: null,
    schemaRegistryTrustStorePassword: null,
    schemaRegistryKeystoreLocation: null,
    schemaRegistryKeystorePassword: null,
    schemaRegistryKeystoreKeyPassword: null,
    sslTruststoreLocation: null,
    sslTruststorePassword: null,
    sslKeystoreLocation: null,
    sslKeystorePassword: null,
    sslKeystoreKeyPassword: null,
    allowPublishing: false,
    ...overrides,
  };
}
