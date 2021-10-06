
CREATE KEYSPACE "local_group_default_T_pageviews_per_project_v2" WITH replication = {'class': 'NetworkTopologyStrategy', 'eqiad': '3'}  AND durable_writes = true;

CREATE TABLE "local_group_default_T_pageviews_per_project_v2".meta (
    key text,
    tid timeuuid,
    "_del" timeuuid,
    value text,
    PRIMARY KEY (key, tid)
) WITH CLUSTERING ORDER BY (tid DESC)
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE "local_group_default_T_pageviews_per_project_v2".data (
    "_domain" text,
    project text,
    access text,
    agent text,
    granularity text,
    timestamp text,
    "_tid" timeuuid,
    "_del" timeuuid,
    v bigint,
    views int,
    PRIMARY KEY (("_domain", project, access, agent, granularity), timestamp, "_tid")
) WITH CLUSTERING ORDER BY (timestamp ASC, "_tid" DESC)
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';
