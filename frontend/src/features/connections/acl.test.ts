import { describe, expect, it } from "vitest";
import { AclBinding, Connection, TopicSummary } from "../../lib/tauri";
import {
  bindingCountsByPrincipal,
  distinctPrincipals,
  groupByResourceType,
  isExpandablePattern,
  operationLabel,
  orderPrincipals,
  patternTypeLabel,
  resourceTypeLabel,
  selfPrincipal,
  topicsMatchingPrefix,
} from "./acl";

function binding(overrides: Partial<AclBinding> = {}): AclBinding {
  return {
    resourceType: "topic",
    resourceName: "orders",
    patternType: "literal",
    principal: "User:alice",
    host: "*",
    operation: "read",
    permission: "allow",
    ...overrides,
  };
}

function connection(overrides: Partial<Connection> = {}): Connection {
  return {
    securityProtocol: "SASL_PLAINTEXT",
    saslUsername: "alice",
    ...overrides,
  } as Connection;
}

describe("selfPrincipal", () => {
  it("reads a SASL username as the Kafka principal it becomes", () => {
    expect(selfPrincipal(connection({ saslUsername: "alice" }))).toBe("User:alice");
  });

  it("trims a username rather than producing a principal with a stray space", () => {
    expect(selfPrincipal(connection({ saslUsername: "  alice  " }))).toBe("User:alice");
  });

  it("names an unauthenticated plaintext connection as Kafka does", () => {
    expect(selfPrincipal(connection({ securityProtocol: "PLAINTEXT", saslUsername: null }))).toBe(
      "User:ANONYMOUS",
    );
  });

  // The whole point of the function. An mTLS principal is a certificate DN
  // and a GSSAPI one a Kerberos principal; rdkafka hands back neither, so
  // there is nothing to derive. A wrong "You" row would misinform someone
  // about their own access, which is worse than having no row at all.
  it("returns nothing for a TLS connection, whose principal is a certificate DN we cannot see", () => {
    expect(selfPrincipal(connection({ securityProtocol: "SSL", saslUsername: null }))).toBeNull();
  });

  it("returns nothing when there is no connection yet", () => {
    expect(selfPrincipal(undefined)).toBeNull();
  });

  it("returns nothing rather than User: for a blank username", () => {
    expect(selfPrincipal(connection({ securityProtocol: "SASL_SSL", saslUsername: "   " }))).toBeNull();
  });
});

describe("orderPrincipals", () => {
  it("lifts the connection's own principal to the top", () => {
    expect(orderPrincipals(["User:amy", "User:zoe"], "User:zoe")).toEqual(["User:zoe", "User:amy"]);
  });

  it("leaves the order alone when the connection's principal holds no ACLs", () => {
    expect(orderPrincipals(["User:amy", "User:zoe"], "User:nobody")).toEqual(["User:amy", "User:zoe"]);
  });

  it("leaves the order alone when the principal is unknown", () => {
    expect(orderPrincipals(["User:amy", "User:zoe"], null)).toEqual(["User:amy", "User:zoe"]);
  });

  it("does not duplicate the principal it lifts", () => {
    expect(orderPrincipals(["User:amy", "User:zoe"], "User:amy")).toEqual(["User:amy", "User:zoe"]);
  });
});

describe("distinctPrincipals", () => {
  it("collapses repeats and sorts, so the tree is stable between fetches", () => {
    const bindings = [
      binding({ principal: "User:zoe" }),
      binding({ principal: "User:amy" }),
      binding({ principal: "User:zoe", operation: "write" }),
    ];

    expect(distinctPrincipals(bindings)).toEqual(["User:amy", "User:zoe"]);
  });

  it("returns nothing for no bindings", () => {
    expect(distinctPrincipals([])).toEqual([]);
  });
});

describe("bindingCountsByPrincipal", () => {
  it("counts every binding a principal holds", () => {
    const counts = bindingCountsByPrincipal([
      binding({ principal: "User:zoe" }),
      binding({ principal: "User:zoe", operation: "write" }),
      binding({ principal: "User:amy" }),
    ]);

    expect(counts.get("User:zoe")).toBe(2);
    expect(counts.get("User:amy")).toBe(1);
  });
});

describe("topicsMatchingPrefix", () => {
  const topics: TopicSummary[] = [
    { name: "payments-in", partitionCount: 1 },
    { name: "payments-out", partitionCount: 1 },
    { name: "orders", partitionCount: 1 },
  ];

  it("finds every topic the prefix currently covers", () => {
    expect(topicsMatchingPrefix("payments-", topics)).toEqual(["payments-in", "payments-out"]);
  });

  it("reports none when the prefix covers nothing yet", () => {
    // Not an error state: a prefixed grant legitimately covers topics that do
    // not exist, which is exactly why the panel says "currently".
    expect(topicsMatchingPrefix("archive-", topics)).toEqual([]);
  });

  it("reports none before the topic list has loaded", () => {
    expect(topicsMatchingPrefix("payments-", undefined)).toEqual([]);
  });
});

describe("isExpandablePattern", () => {
  // Only PREFIXED reaches beyond the name it carries. A literal matches
  // itself, and the table already shows that plainly.
  it("is true only for prefixed patterns", () => {
    expect(isExpandablePattern("prefixed")).toBe(true);
    expect(isExpandablePattern("literal")).toBe(false);
    expect(isExpandablePattern("match")).toBe(false);
    expect(isExpandablePattern("any")).toBe(false);
  });
});

describe("groupByResourceType", () => {
  it("groups a principal's bindings and orders topics before groups", () => {
    const grouped = groupByResourceType([
      binding({ resourceType: "group", resourceName: "analytics" }),
      binding({ resourceType: "topic" }),
      binding({ resourceType: "topic", operation: "write" }),
    ]);

    expect(grouped.map(([type, rows]) => [type, rows.length])).toEqual([
      ["topic", 2],
      ["group", 1],
    ]);
  });

  it("returns nothing for a principal with no bindings", () => {
    expect(groupByResourceType([])).toEqual([]);
  });
});

describe("labels", () => {
  // The user's ACLs were written with kafka-acls.sh, which says --cluster.
  // librdkafka's "broker" would send them looking for a flag that does not
  // exist.
  it("calls the cluster resource what Kafka's own tooling calls it", () => {
    expect(resourceTypeLabel("broker")).toBe("Cluster");
  });

  it("capitalises the other resource types", () => {
    expect(resourceTypeLabel("topic")).toBe("Topic");
    expect(resourceTypeLabel("group")).toBe("Group");
  });

  it("renders pattern types the way kafka-acls.sh prints them", () => {
    expect(patternTypeLabel("prefixed")).toBe("PREFIXED");
    expect(patternTypeLabel("literal")).toBe("LITERAL");
  });

  it("capitalises operations as Kafka names them", () => {
    expect(operationLabel("read")).toBe("Read");
    expect(operationLabel("describeConfigs")).toBe("DescribeConfigs");
  });
});
