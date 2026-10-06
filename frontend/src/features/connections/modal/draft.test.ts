import { describe, expect, it } from "vitest";
import type { Connection } from "../../../lib/tauri";
import { connectionToDraft, draftsEqual, emptyDraft, toNewConnection, validateDraft } from "./draft";

describe("emptyDraft", () => {
  it("defaults to plaintext with zookeeper disabled and no sasl mechanism", () => {
    const draft = emptyDraft();
    expect(draft.securityProtocol).toBe("PLAINTEXT");
    expect(draft.zookeeperEnabled).toBe(false);
    expect(draft.saslMechanism).toBe("");
  });

  it("defaults the kafka version to the newest supported version", () => {
    expect(emptyDraft().kafkaVersion).toBe("4.3");
  });
});

describe("validateDraft", () => {
  it("requires a cluster name", () => {
    const draft = emptyDraft();
    draft.bootstrapServers = "localhost:9092";
    expect(validateDraft(draft)).toBe("Cluster name is required");
  });

  it("requires bootstrap servers", () => {
    const draft = emptyDraft();
    draft.name = "Local";
    expect(validateDraft(draft)).toBe("Bootstrap servers is required");
  });

  it("requires a zookeeper host when zookeeper is enabled", () => {
    const draft = emptyDraft();
    draft.name = "Local";
    draft.bootstrapServers = "localhost:9092";
    draft.kafkaVersion = "3.9"; // a version that can still run zookeeper
    draft.zookeeperEnabled = true;
    draft.zookeeperPort = "2181";
    expect(validateDraft(draft)).toBe("Zookeeper host is required when Zookeeper is enabled");
  });

  it("requires a zookeeper port when zookeeper is enabled", () => {
    const draft = emptyDraft();
    draft.name = "Local";
    draft.bootstrapServers = "localhost:9092";
    draft.kafkaVersion = "3.9"; // a version that can still run zookeeper
    draft.zookeeperEnabled = true;
    draft.zookeeperHost = "zk.local";
    expect(validateDraft(draft)).toBe("Zookeeper port is required when Zookeeper is enabled");
  });

  it("passes for a minimal valid draft", () => {
    const draft = emptyDraft();
    draft.name = "Local";
    draft.bootstrapServers = "localhost:9092";
    expect(validateDraft(draft)).toBeNull();
  });

  it("does not require zookeeper fields when zookeeper is disabled", () => {
    const draft = emptyDraft();
    draft.name = "Local";
    draft.bootstrapServers = "localhost:9092";
    draft.zookeeperEnabled = false;
    expect(validateDraft(draft)).toBeNull();
  });
});

describe("toNewConnection", () => {
  it("trims and nulls out blank optional fields", () => {
    const draft = emptyDraft();
    draft.name = "  Local  ";
    draft.bootstrapServers = "  localhost:9092  ";

    const result = toNewConnection(draft);

    expect(result.name).toBe("Local");
    expect(result.bootstrapServers).toBe("localhost:9092");
    expect(result.zookeeperHost).toBeNull();
    expect(result.zookeeperPort).toBeNull();
    expect(result.zookeeperChrootPath).toBeNull();
    expect(result.saslMechanism).toBeNull();
    expect(result.saslUsername).toBeNull();
    expect(result.saslPassword).toBeNull();
    expect(result.saslOauthUrl).toBeNull();
    expect(result.schemaRegistryEndpoint).toBeNull();
    expect(result.schemaRegistryBasicAuthCredentials).toBeNull();
  });

  it("carries zookeeper fields through when enabled, parsing the port as a number", () => {
    const draft = emptyDraft();
    draft.name = "Local";
    draft.bootstrapServers = "localhost:9092";
    draft.kafkaVersion = "3.9"; // a version that can still run zookeeper
    draft.zookeeperEnabled = true;
    draft.zookeeperHost = "zk.local";
    draft.zookeeperPort = "2181";
    draft.zookeeperChrootPath = "/kafka";

    const result = toNewConnection(draft);

    expect(result.zookeeperEnabled).toBe(true);
    expect(result.zookeeperHost).toBe("zk.local");
    expect(result.zookeeperPort).toBe(2181);
    expect(result.zookeeperChrootPath).toBe("/kafka");
  });

  it("omits zookeeper host/port/chroot even if filled in when zookeeper is disabled", () => {
    const draft = emptyDraft();
    draft.name = "Local";
    draft.bootstrapServers = "localhost:9092";
    draft.zookeeperEnabled = false;
    draft.zookeeperHost = "zk.local";
    draft.zookeeperPort = "2181";

    const result = toNewConnection(draft);

    expect(result.zookeeperHost).toBeNull();
    expect(result.zookeeperPort).toBeNull();
  });

  it("converts an empty sasl mechanism selection to null", () => {
    const draft = emptyDraft();
    draft.name = "Local";
    draft.bootstrapServers = "localhost:9092";
    draft.saslMechanism = "SCRAM-SHA-512";

    expect(toNewConnection(draft).saslMechanism).toBe("SCRAM-SHA-512");
  });

  it("trims the sasl username and carries the password through", () => {
    const draft = emptyDraft();
    draft.name = "Local";
    draft.bootstrapServers = "localhost:9092";
    draft.saslMechanism = "PLAIN";
    draft.saslUsername = "  kafka-user  ";
    draft.saslPassword = "hunter2";

    const result = toNewConnection(draft);

    expect(result.saslUsername).toBe("kafka-user");
    expect(result.saslPassword).toBe("hunter2");
  });

  it("carries all schema registry fields through, trimmed", () => {
    const draft = emptyDraft();
    draft.name = "Local";
    draft.bootstrapServers = "localhost:9092";
    draft.schemaRegistryEndpoint = " https://schema-registry.local ";
    draft.schemaRegistryBasicAuthCredentials = "user:pass";
    draft.schemaRegistryTrustStoreLocation = "/etc/ts.jks";
    draft.schemaRegistryTrustStorePassword = "ts-secret";
    draft.schemaRegistryKeystoreLocation = "/etc/ks.jks";
    draft.schemaRegistryKeystorePassword = "ks-secret";
    draft.schemaRegistryKeystoreKeyPassword = "ks-key-secret";

    const result = toNewConnection(draft);

    expect(result.schemaRegistryEndpoint).toBe("https://schema-registry.local");
    expect(result.schemaRegistryBasicAuthCredentials).toBe("user:pass");
    expect(result.schemaRegistryTrustStoreLocation).toBe("/etc/ts.jks");
    expect(result.schemaRegistryTrustStorePassword).toBe("ts-secret");
    expect(result.schemaRegistryKeystoreLocation).toBe("/etc/ks.jks");
    expect(result.schemaRegistryKeystorePassword).toBe("ks-secret");
    expect(result.schemaRegistryKeystoreKeyPassword).toBe("ks-key-secret");
  });

  it("carries all broker SSL fields through, trimmed", () => {
    const draft = emptyDraft();
    draft.name = "Local";
    draft.bootstrapServers = "localhost:9092";
    draft.sslTruststoreLocation = " /etc/broker-ts.pem ";
    draft.sslTruststorePassword = "broker-ts-secret";
    draft.sslKeystoreLocation = "/etc/broker-ks.p12";
    draft.sslKeystorePassword = "broker-ks-secret";
    draft.sslKeystoreKeyPassword = "broker-ks-key-secret";

    const result = toNewConnection(draft);

    expect(result.sslTruststoreLocation).toBe("/etc/broker-ts.pem");
    expect(result.sslTruststorePassword).toBe("broker-ts-secret");
    expect(result.sslKeystoreLocation).toBe("/etc/broker-ks.p12");
    expect(result.sslKeystorePassword).toBe("broker-ks-secret");
    expect(result.sslKeystoreKeyPassword).toBe("broker-ks-key-secret");
  });
});

function sampleConnection(overrides: Partial<Connection> = {}): Connection {
  return {
    id: "conn-1",
    name: "Local Kafka",
    bootstrapServers: "localhost:9092",
    kafkaVersion: "2.8",
    zookeeperEnabled: true,
    zookeeperHost: "zk.local",
    zookeeperPort: 2181,
    zookeeperChrootPath: "/kafka",
    securityProtocol: "SASL_SSL",
    saslMechanism: "SCRAM-SHA-512",
    saslUsername: "kafka-user",
    saslPassword: "sasl-secret",
    saslOauthUrl: "https://idp.example.com/token",
    schemaRegistryEndpoint: "https://schema-registry.local",
    schemaRegistryBasicAuthCredentials: "sr-user:sr-secret",
    ksqldbEndpoint: "https://ksql.local",
    ksqldbBasicAuthCredentials: "ksql-user:ksql-secret",
    schemaRegistryTrustStoreLocation: "/etc/ts.jks",
    schemaRegistryTrustStorePassword: "sr-ts-secret",
    schemaRegistryKeystoreLocation: "/etc/ks.jks",
    schemaRegistryKeystorePassword: "sr-ks-secret",
    schemaRegistryKeystoreKeyPassword: "sr-ks-key-secret",
    sslTruststoreLocation: "/etc/broker-ts.pem",
    sslTruststorePassword: "broker-ts-secret",
    sslKeystoreLocation: "/etc/broker-ks.p12",
    sslKeystorePassword: "broker-ks-secret",
    sslKeystoreKeyPassword: "broker-ks-key-secret",
    allowPublishing: true,
    createdAt: "2026-08-18T00:00:00Z",
    updatedAt: "2026-08-18T00:00:00Z",
    ...overrides,
  };
}

describe("connectionToDraft", () => {
  it("carries every non-secret field from the connection into the draft", () => {
    const draft = connectionToDraft(sampleConnection());

    expect(draft.name).toBe("Local Kafka");
    expect(draft.bootstrapServers).toBe("localhost:9092");
    expect(draft.kafkaVersion).toBe("2.8");
    expect(draft.zookeeperEnabled).toBe(true);
    expect(draft.zookeeperHost).toBe("zk.local");
    expect(draft.zookeeperPort).toBe("2181");
    expect(draft.zookeeperChrootPath).toBe("/kafka");
    expect(draft.securityProtocol).toBe("SASL_SSL");
    expect(draft.saslMechanism).toBe("SCRAM-SHA-512");
    expect(draft.saslUsername).toBe("kafka-user");
    expect(draft.saslOauthUrl).toBe("https://idp.example.com/token");
    expect(draft.schemaRegistryEndpoint).toBe("https://schema-registry.local");
    expect(draft.schemaRegistryTrustStoreLocation).toBe("/etc/ts.jks");
    expect(draft.schemaRegistryKeystoreLocation).toBe("/etc/ks.jks");
    expect(draft.sslTruststoreLocation).toBe("/etc/broker-ts.pem");
    expect(draft.sslKeystoreLocation).toBe("/etc/broker-ks.p12");
  });

  it("carries every secret field too, since Connection now includes them", () => {
    const draft = connectionToDraft(sampleConnection());

    expect(draft.saslPassword).toBe("sasl-secret");
    expect(draft.schemaRegistryBasicAuthCredentials).toBe("sr-user:sr-secret");
    expect(draft.schemaRegistryTrustStorePassword).toBe("sr-ts-secret");
    expect(draft.schemaRegistryKeystorePassword).toBe("sr-ks-secret");
    expect(draft.schemaRegistryKeystoreKeyPassword).toBe("sr-ks-key-secret");
    expect(draft.sslTruststorePassword).toBe("broker-ts-secret");
    expect(draft.sslKeystorePassword).toBe("broker-ks-secret");
    expect(draft.sslKeystoreKeyPassword).toBe("broker-ks-key-secret");
  });

  it("renders a null/absent secret field as an empty string", () => {
    const draft = connectionToDraft(sampleConnection({ saslPassword: null }));
    expect(draft.saslPassword).toBe("");
  });

  it("renders a null zookeeper port as an empty string rather than 'null'", () => {
    const draft = connectionToDraft(sampleConnection({ zookeeperPort: null, zookeeperHost: null }));
    expect(draft.zookeeperPort).toBe("");
    expect(draft.zookeeperHost).toBe("");
  });

  it("round-trips back through toNewConnection to an equivalent NewConnection", () => {
    const connection = sampleConnection();
    const draft = connectionToDraft(connection);
    const newConnection = toNewConnection(draft);

    expect(newConnection.name).toBe(connection.name);
    expect(newConnection.bootstrapServers).toBe(connection.bootstrapServers);
    expect(newConnection.kafkaVersion).toBe(connection.kafkaVersion);
    expect(newConnection.zookeeperEnabled).toBe(connection.zookeeperEnabled);
    expect(newConnection.zookeeperHost).toBe(connection.zookeeperHost);
    expect(newConnection.zookeeperPort).toBe(connection.zookeeperPort);
    expect(newConnection.securityProtocol).toBe(connection.securityProtocol);
    expect(newConnection.saslMechanism).toBe(connection.saslMechanism);
    expect(newConnection.saslUsername).toBe(connection.saslUsername);
  });
});

describe("draftsEqual", () => {
  it("is true for two independently-created equivalent drafts", () => {
    expect(draftsEqual(emptyDraft(), emptyDraft())).toBe(true);
  });

  it("is false when a single field differs", () => {
    const a = emptyDraft();
    const b = { ...emptyDraft(), name: "Changed" };
    expect(draftsEqual(a, b)).toBe(false);
  });

  it("is false when a boolean field differs", () => {
    const a = emptyDraft();
    const b = { ...emptyDraft(), zookeeperEnabled: true };
    expect(draftsEqual(a, b)).toBe(false);
  });

  it("is true again once the differing field is reverted", () => {
    const original = emptyDraft();
    const draft = { ...original, name: "Changed" };
    const reverted = { ...draft, name: original.name };
    expect(draftsEqual(original, reverted)).toBe(true);
  });
});

describe("allowPublishing through the draft", () => {
  it("starts off in a new draft, so a connection created here cannot publish", () => {
    expect(emptyDraft().allowPublishing).toBe(false);
  });

  it("carries the saved connection's flag into the draft", () => {
    expect(connectionToDraft(sampleConnection({ allowPublishing: true })).allowPublishing).toBe(true);
    expect(connectionToDraft(sampleConnection({ allowPublishing: false })).allowPublishing).toBe(
      false,
    );
  });

  it("treats a connection with no flag at all as not allowed", () => {
    // A row written before the column existed, or an older backend: the absence
    // of permission must read as "no", never as undefined.
    const legacy = sampleConnection();
    delete (legacy as { allowPublishing?: boolean }).allowPublishing;
    expect(connectionToDraft(legacy).allowPublishing).toBe(false);
  });

  it("sends the flag on save", () => {
    expect(toNewConnection({ ...emptyDraft(), allowPublishing: true }).allowPublishing).toBe(true);
    expect(toNewConnection(emptyDraft()).allowPublishing).toBe(false);
  });

  it("counts as a change, so Update becomes available when it is toggled", () => {
    const before = emptyDraft();
    const after = { ...before, allowPublishing: true };
    expect(draftsEqual(before, after)).toBe(false);
  });
});

describe("Kafka 4.x and ZooKeeper", () => {
  it("never persists zookeeper settings on a 4.x connection", () => {
    // Kafka 4.0 removed ZooKeeper, so a stored zookeeperEnabled: true on a
    // 4.x row would make the DB — and connections_export's file — claim
    // something no 4.x cluster can be doing.
    const draft = {
      ...emptyDraft(),
      name: "prod",
      bootstrapServers: "localhost:9092",
      kafkaVersion: "4.1",
      zookeeperEnabled: true,
      zookeeperHost: "zk.internal",
      zookeeperPort: "2181",
      zookeeperChrootPath: "/kafka",
    };

    const wire = toNewConnection(draft);

    expect(wire.zookeeperEnabled).toBe(false);
    expect(wire.zookeeperHost).toBeNull();
    expect(wire.zookeeperPort).toBeNull();
    expect(wire.zookeeperChrootPath).toBeNull();
  });

  it("still persists zookeeper settings on 3.9, the last version that can run it", () => {
    const draft = {
      ...emptyDraft(),
      name: "legacy",
      bootstrapServers: "localhost:9092",
      kafkaVersion: "3.9",
      zookeeperEnabled: true,
      zookeeperHost: "zk.internal",
      zookeeperPort: "2181",
      zookeeperChrootPath: "/kafka",
    };

    const wire = toNewConnection(draft);

    expect(wire.zookeeperEnabled).toBe(true);
    expect(wire.zookeeperHost).toBe("zk.internal");
    expect(wire.zookeeperPort).toBe(2181);
    expect(wire.zookeeperChrootPath).toBe("/kafka");
  });

  it("does not demand a zookeeper host on 4.x, where the section is hidden", () => {
    // Without this guard, someone who had ZooKeeper enabled and then picked
    // 4.1 is blocked from saving by a validation error about fields they
    // can no longer see.
    const draft = {
      ...emptyDraft(),
      name: "prod",
      bootstrapServers: "localhost:9092",
      kafkaVersion: "4.1",
      zookeeperEnabled: true,
      zookeeperHost: "",
      zookeeperPort: "",
    };

    expect(validateDraft(draft)).toBeNull();
  });

  it("still demands a zookeeper host on 3.9 when zookeeper is enabled", () => {
    const draft = {
      ...emptyDraft(),
      name: "legacy",
      bootstrapServers: "localhost:9092",
      kafkaVersion: "3.9",
      zookeeperEnabled: true,
      zookeeperHost: "",
      zookeeperPort: "",
    };

    expect(validateDraft(draft)).toBe("Zookeeper host is required when Zookeeper is enabled");
  });

  it("agrees between validation and persistence about whether zookeeper applies", () => {
    // The guard in validateDraft and the gate in toNewConnection are the same
    // predicate. If they ever diverge, a user is blocked on a field that was
    // never going to be saved — or worse, not blocked on one that was.
    const base = { ...emptyDraft(), name: "c", bootstrapServers: "localhost:9092", zookeeperEnabled: true };

    for (const kafkaVersion of ["3.9", "4.0", "4.3"]) {
      const draft = { ...base, kafkaVersion, zookeeperHost: "", zookeeperPort: "" };
      const demandsAHost = validateDraft(draft) !== null;
      const persistsZookeeper = toNewConnection({ ...draft, zookeeperHost: "zk", zookeeperPort: "2181" })
        .zookeeperEnabled;
      expect(demandsAHost).toBe(persistsZookeeper);
    }
  });
});
