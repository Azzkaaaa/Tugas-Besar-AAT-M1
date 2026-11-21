# Bounded Audit metadata pages

The four-node resumed practice baseline exceeded the checksum API's 20-second
deadline and returned an explicit partial checksum (8,465 references). A complete
canonical metadata capture subsequently contained 20,514 populated records. The
partial digest cannot prove no data loss.

The first correction raised Audit metadata pages from 64 to 256 references.
At the later 120,000-event practice catalog, coastal checksums still reached the
20-second deadline and correctly returned partial results. Pages now contain at
most 1,024 metadata references (at most 16,384 retained across 16 shards). The merge
retains at most one bounded page per shard, keeps four initial readers, preserves
global order and reports failed ranges/deadlines as before. This reduces sequential
WAN page refills without increasing full-payload admission, changing consistency,
or extending the API deadline. The regression spans more than two pages on each
shard and verifies ordering, inactive-row exclusion and healthy-shard retention.

Focused Cassandra and M2 suites passed again (1.100s and 17.715s). The runtime checksum must still be
remeasured on the intended cluster; this source change alone is not acceptance.
