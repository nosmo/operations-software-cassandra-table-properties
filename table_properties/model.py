import json


class DataCenter():
    def __init__(self, data_center_name: str, replication_factor: int):
        self.name = data_center_name
        self.repl_factor = replication_factor

class Replication():
    def __init__(self, repl_class: str, data_centers: list):
        self.replication_class = repl_class
        self.data_centers = data_centers

# class	LZ4Compressor	The compression algorithm to use. Default compressor are: LZ4Compressor, SnappyCompressor and DeflateCompressor. Use 'enabled' : false to disable compression. Custom compressor can be provided by specifying the full class name as a “string constant”:#constants.
# enabled	true	Enable/disable sstable compression.
# chunk_length_in_kb	64	On disk SSTables are compressed by block (to allow random reads). This defines the size (in KB) of said block. Bigger values may improve the compression rate, but increases the minimum size of data to be read from disk for a read
# crc_check_chance	1.0	When compression is enabled, each compressed block includes a checksum of that block for the purpose of detecting disk bitrot and avoiding the propagation of corruption to other replica. This option defines the probability with which those checksums are checked during read. By default they are always checked. Set to 0 to disable checksum checking and to 0.5 for instance to check them every other read |

class Compression():
    def __init__(self, class_name, enabled: bool, chunk_length_in_kb: int,
        crc_check_chance: float):
        self.class_name = class_name
        self.enabled = enabled
        self.chunk_length_in_kb = chunk_length_in_kb
        self.crc_check_chance = crc_check_chance

# enabled (default: true)
# Whether minor compactions should run. Note that you can have ‘enabled’: true as a compaction option and then do ‘nodetool enableautocompaction’ to start running compactions.
# tombstone_threshold (default: 0.2)
# How much of the sstable should be tombstones for us to consider doing a single sstable compaction of that sstable.
#
# tombstone_compaction_interval (default: 86400s (1 day))
# Since it might not be possible to drop any tombstones when doing a single sstable compaction we need to make sure that one sstable is not constantly getting recompacted - this option states how often we should try for a given sstable.
#
# log_all (default: false)
# New detailed compaction logging, see below.
#
# unchecked_tombstone_compaction (default: false)
# The single sstable compaction has quite strict checks for whether it should be started, this option disables those checks and for some usecases this might be needed. Note that this does not change anything for the actual compaction, tombstones are only dropped if it is safe to do so - it might just rewrite an sstable without being able to drop any tombstones.
#
# only_purge_repaired_tombstone (default: false)
# Option to enable the extra safety of making sure that tombstones are only dropped if the data has been repaired.
#
# min_threshold (default: 4)
# Lower limit of number of sstables before a compaction is triggered. Not used for LeveledCompactionStrategy.
#
# max_threshold (default: 32)
# Upper limit of number of sstables before a compaction is triggered. Not used for LeveledCompactionStrategy.

class CompactionOptions():
    def __init__(self,
        enabled: bool = True,
        tombstone_compaction_interval: float = 0.2,
        log_all: bool = False,
        unchecked_tombstone_compaction: bool = False,
        only_purge_repaired_tombstone: bool = False
    ):
        self.enabled = enabled
        self.tombstone_compaction_interval = tombstone_compaction_interval
        self.log_all = log_all
        self.unchecked_tombstone_compaction = unchecked_tombstone_compaction
        self.only_purge_repaired_tombstone = only_purge_repaired_tombstone

# STCS Options:
# min_sstable_size (default: 50MB)
# Sstables smaller than this are put in the same bucket.
#
# bucket_low (default: 0.5)
# How much smaller than the average size of a bucket a sstable should be before not being included in the bucket. That is, if bucket_low * avg_bucket_size < sstable_size (and the bucket_high condition holds, see below), then the sstable is added to the bucket.
#
# bucket_high (default: 1.5)
# How much bigger than the average size of a bucket a sstable should be before not being included in the bucket. That is, if sstable_size < bucket_high * avg_bucket_size (and the bucket_low condition holds, see above), then the sstable is added to the bucket.

class SizeTieredCompactionStrategy(CompactionOptions):
    def __init_(self,
        enabled: bool = True,
        tombstone_compaction_interval: float = 0.2,
        log_all: bool = False,
        unchecked_tombstone_compaction: bool = False,
        only_purge_repaired_tombstone: bool = False,
        min_threshold: int = 4,
        max_threshold: int = 32,
        min_sstable_size: str = "50MB",
        bucket_low: float = 0.5, 
        bucket_high: float = 1.5
    ):
        CompactionOptions.__init__(enabled, tombstone_compaction_interval,
            log_all, unchecked_tombstone_compaction, 
            only_purge_repaired_tombstone)

        self.min_threshold = min_threshold
        self.max_threshold = max_threshold
        self.min_sstable_size = min_sstable_size
        self.bucket_low = bucket_low
        self.bucket_high = bucket_high

# LCS options
# sstable_size_in_mb (default: 160MB)
# The target compressed (if using compression) sstable size - the sstables can end up being larger if there are very large partitions on the node.
#
# fanout_size (default: 10)
# The target size of levels increases by this fanout_size multiplier. You can reduce the space amplification by tuning this option.

class LeveledCompactionStrategy(CompactionOptions):
    def __init__(self,
        enabled: bool = True,
        tombstone_compaction_interval: float = 0.2,
        log_all: bool = False,
        unchecked_tombstone_compaction: bool = False,
        only_purge_repaired_tombstone: bool = False,
        sstable_size_in_mb: str = "160MB",
        fanout_size: int = 10
    ):
        CompactionOptions.__init__(enabled, tombstone_compaction_interval,
            log_all, unchecked_tombstone_compaction, 
            only_purge_repaired_tombstone)

        self.sstable_size_in_mb = sstable_size_in_mb
        self.fanout_size = fanout_size

# compaction_window_unit (default: DAYS)
# A Java TimeUnit (MINUTES, HOURS, or DAYS).
#
# compaction_window_size (default: 1)
# The number of units that make up a window.
# unsafe_aggressive_sstable_expiration (default: false)
# Expired sstables will be dropped without checking its data is shadowing other 
# sstables. This is a potentially risky option that can lead to data loss or 
# deleted data re-appearing, going beyond what unchecked_tombstone_compaction 
# does for single sstable compaction. Due to the risk the jvm must also be 
# started with -Dcassandra.unsafe_aggressive_sstable_expiration=true.

class TimeWindowCompactionStrategy(CompactionOptions):
    def __init__(self,
        enabled: bool = True,
        tombstone_compaction_interval: float = 0.2,
        log_all: bool = False,
        unchecked_tombstone_compaction: bool = False,
        only_purge_repaired_tombstone: bool = False,
        compaction_window_unit: str = 'DAYS',
        compaction_window_size: int = 1
    ):
        CompactionOptions.__init__(enabled, tombstone_compaction_interval,
            log_all, unchecked_tombstone_compaction, 
            only_purge_repaired_tombstone)

# comment	simple	none	A free-form, human-readable comment.
# speculative_retry	simple	99PERCENTILE	Speculative retry options.
# additional_write_policy	simple	99PERCENTILE	Speculative retry options.
# gc_grace_seconds	simple	864000	Time to wait before garbage collecting tombstones (deletion markers).
# bloom_filter_fp_chance	simple	0.00075	The target probability of false positive of the sstable bloom filters. Said bloom filters will be sized to provide the provided probability (thus lowering this value impact the size of bloom filters in-memory and on-disk)
# default_time_to_live	simple	0	The default expiration time (“TTL”) in seconds for a table.
# compaction	map	see below	Compaction options.
# compression	map	see below	Compression options.
# caching	map	see below	Caching options.
# memtable_flush_period_in_ms	simple	0	Time (in ms) before Cassandra flushes memtables to disk.
# read_repair	simple	BLOCKING	Sets read repair behavior (see below)

class Table():
    def __init__(self, name: str, comment: str, speculative_retry: str,
        additional_write_policy: str, gc_grace_seconds: int,
        bloom_filter_fp_chance: float, default_time_to_live: int,
        compaction, compression, caching, memtable_flush_period_in_ms: int,
        read_repair: str):

        self.name = name
        self.comment = comment
        self.speculative_retry = speculative_retry
        self.additional_write_policy = additional_write_policy
        self.gc_grace_seconds = gc_grace_seconds
        self.bloom_filter_fp_chance = bloom_filter_fp_chance
        self.default_time_to_live = default_time_to_live
        self.compaction = compaction
        self.compression = compression
        self.caching = caching
        self.memtable_flush_period_in_ms = memtable_flush_period_in_ms
        self.read_repair = read_repair


class Keyspace():
    def __init__(self, keyspace_name: str, replication: Replication,
        durable_writes: bool, tables: list = []):
        self.name = keyspace_name
        self.replication = replication
        self.durable_writes = durable_writes
        self.tables = tables

class Keyspaces():
    def __init__(self, keyspaces: list):
        if not isinstance(keyspaces, list):
            raise Exception("Keyspaces constructor argument not a list")

        if not all(isinstance(keyspace, Keyspace) for keyspace in keyspaces):
            raise Exception("""Keyspaces constructor arguments must be of 
                type 'Keyspace'""")

        self.keyspaces = keyspaces

    def toJSON(self):
            return json.dumps(self, default=lambda o: o.__dict__, 
                sort_keys=True, indent=4)


def main():
    import json

    r = Replication("NetworkTopologyStrategy", [ DataCenter("datacenter1", 3) ])
    ks = Keyspace("test_keyspace", r, True)


    print(Keyspaces([ks]).toJSON())
    # json.dumps(r)

if __name__ == "__main__":
    main()