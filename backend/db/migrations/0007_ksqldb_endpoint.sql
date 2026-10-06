-- Where this connection's ksqlDB server is, and how to authenticate to it.
--
-- Nullable and absent by default: ksqlDB is a separate server that most
-- clusters do not run, and a connection without one is not misconfigured —
-- the workspace says so and offers nothing else.
--
-- `ksqldb_basic_auth_credentials` holds `user:password`, matching
-- `schema_registry_basic_auth_credentials` rather than inventing a second
-- shape for the same thing. It is a secret, so it is excluded from
-- `PortableConnection` and never reaches an exported connections file — the
-- same rule every other credential here follows.
ALTER TABLE connections ADD COLUMN ksqldb_endpoint TEXT;
ALTER TABLE connections ADD COLUMN ksqldb_basic_auth_credentials TEXT;
