-- Journal and snapshots of url-manager in cluster mode (pekko-persistence-cassandra 1.1, events-by-tag off).
-- The tables of the plugin, as it creates them itself; applied before the first start (init container of the stand,
-- UrlManagerIT): autocreate by the plugin races with the first recoveries.
-- For ScyllaDB; for Cassandra drop the `AND tablets` lines. Tablets are off: vnodes give a ring every driver knows.
-- Replication 1 - the stand with one node; production: 3 per datacenter, the journal read and written with QUORUM.

CREATE KEYSPACE IF NOT EXISTS url_manager
    WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}
    AND tablets = {'enabled': false};

CREATE TABLE IF NOT EXISTS url_manager.all_persistence_ids (
    persistence_id text PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS url_manager.messages (
    persistence_id text,
    partition_nr bigint,
    sequence_nr bigint,
    timestamp timeuuid,
    event blob,
    event_manifest text,
    meta blob,
    meta_ser_id int,
    meta_ser_manifest text,
    ser_id int,
    ser_manifest text,
    timebucket text,
    writer_uuid text,
    tags set<text>,
    PRIMARY KEY ((persistence_id, partition_nr), sequence_nr, timestamp)
) WITH CLUSTERING ORDER BY (sequence_nr ASC, timestamp ASC);

CREATE TABLE IF NOT EXISTS url_manager.metadata (
    persistence_id text PRIMARY KEY,
    deleted_to bigint,
    properties map<text, text>
);

CREATE KEYSPACE IF NOT EXISTS url_manager_snapshot
    WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}
    AND tablets = {'enabled': false};

CREATE TABLE IF NOT EXISTS url_manager_snapshot.snapshots (
    persistence_id text,
    sequence_nr bigint,
    meta blob,
    meta_ser_id int,
    meta_ser_manifest text,
    ser_id int,
    ser_manifest text,
    snapshot blob,
    snapshot_data blob,
    timestamp bigint,
    PRIMARY KEY (persistence_id, sequence_nr)
) WITH CLUSTERING ORDER BY (sequence_nr DESC);
