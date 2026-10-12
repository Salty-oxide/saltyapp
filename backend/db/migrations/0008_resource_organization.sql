-- Pins and folders for the sidebar tree's Topics and Consumers lists.
--
-- No `REFERENCES` / `ON DELETE CASCADE`, for the reason 0004 gives: this app
-- never enables SQLite's `foreign_keys` pragma, so a declared cascade would
-- silently never fire. Deleting a folder, and deleting a connection, clean up
-- explicitly instead — see `salty_db::organization`.
CREATE TABLE resource_folders (
    id TEXT PRIMARY KEY,
    connection_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('topic', 'consumer')),
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- Folder names are unique per connection and kind, ignoring case.
CREATE UNIQUE INDEX resource_folders_name
    ON resource_folders (connection_id, kind, name COLLATE NOCASE);

-- The primary key is what makes "an item lives in at most one folder" a fact
-- of the schema rather than a convention.
CREATE TABLE resource_folder_items (
    connection_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('topic', 'consumer')),
    resource_name TEXT NOT NULL,
    folder_id TEXT NOT NULL,
    PRIMARY KEY (connection_id, kind, resource_name)
);

CREATE INDEX resource_folder_items_folder ON resource_folder_items (folder_id);

CREATE TABLE pinned_topics (
    connection_id TEXT NOT NULL,
    topic_name TEXT NOT NULL,
    pinned_at TEXT NOT NULL,
    PRIMARY KEY (connection_id, topic_name)
);
