# Testing S3 throttling (503 SlowDown)

This document records how `test/boost/s3_slowdown_test.cc` provokes a real
"503 SlowDown" from S3, the measurements behind its design, and why the same
thing cannot be done from a cluster test.

## The test

`s3_slowdown_test` writes many tiny sstables concurrently and then unlinks them,
through `sstables::storage` — the abstraction that both `filesystem_storage` and
`s3_storage` implement. Nothing in the test names S3 beyond the storage options
handed to `test_env`, so it keeps exercising the same code path if throttling
handling is later added underneath, and it can be pointed at local storage
unchanged.

It is skipped unless `ENABLE_S3_SLOWDOWN_TEST` is set, because it needs a real
bucket:

```
export S3_SERVER_ADDRESS_FOR_TEST=s3.<region>.amazonaws.com
export S3_SERVER_PORT_FOR_TEST=443
export S3_BUCKET_FOR_TEST=<bucket>
export AWS_DEFAULT_REGION=<region>
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
export ENABLE_S3_SLOWDOWN_TEST=1
pytest --mode dev test/boost/s3_slowdown_test.cc
```

Knobs: `S3_SLOWDOWN_SSTABLES` (default 4000), `S3_SLOWDOWN_CONCURRENCY`
(default 512). Objects are one byte each and are deleted again at the end.

Throttling is detected by tapping the stream seastar logs to:
`aws::default_aws_retry_strategy` reports every retry together with the message
the server sent, so no production code has to change to observe it.

## Why the write path, and not reads

Reads cannot provoke throttling from a single node. Measured against a real
bucket in eu-central-1:

- 2,000,000 HEAD requests over 8 minutes on one prefix, sustained ~6,000/s,
  peaking ~6,600/s: **zero** throttling statuses. S3 answered by dropping TCP
  connections instead — 724 retries, all `Connection reset by peer` /
  `Software caused connection abort`, none surfaced as a failure.
- Spreading the same load over 56 keys instead of 1 made no difference
  (4,693 vs 4,677 requests/s), so the plateau was client-side, not a single-key
  hotspot.

`PUT`/`COPY`/`POST`/`DELETE` share a per-prefix budget appreciably lower than the
one for `GET`/`HEAD`, and what counts is the *arrival* rate rather than the
completion rate. A representative run (4000 sstables, concurrency 512, one
shard):

| phase | value |
|---|---|
| sstables written | 4000 in 25.0 s = 160/s |
| PUT | 56,000 = **2,240/s** (exactly 14.0 per sstable) |
| POST / HEAD / DELETE | 16,000 each = 640/s |
| GET | 8,276 = 331/s |
| total | ~112,000 requests = ~4,500/s |
| throttling statuses | 765 |
| retries exhausted | 0 |
| sstables lost | 0 |

Of the 765 throttling statuses, 722 carried S3's SlowDown `<Message>` ("Please
reduce your request rate.") and 43 arrived with a body `aws_error::parse` could
not use, falling back to `aws_error::from_http_code` →
`HTTP_SERVICE_UNAVAILABLE`. Both are retryable and both were retried correctly.
First throttle appeared 7.3 s into the run.

Note that 2,240 PUT/s is well under the ~3,500/s AWS documents for writes: with
512 requests in flight plus backing-off retries, S3 sees more attempts per second
than the storm completes.

Each sstable is ~14 objects totalling ~9 KB (~643 bytes per object), so total
volume is irrelevant — 4000 sstables is ~36 MB. The cost driver is request count.

## Why this cannot be a cluster test

A cluster test cannot get within two orders of magnitude of the required rate.

1. **One sstable write to S3 takes ~1.6-3.3 s.** 200 sstables at concurrency 64
   took 5.0 s (40/s), 4000 at concurrency 512 took 25-26 s (~155/s). Each sstable
   is ~14 objects and the component writes are only partly pipelined.
2. **Flushes are serialized to one per shard.** `_flush_serializer(1)` in
   `replica/dirty_memory_manager.hh`; every flush path takes that same permit —
   dirty-memory pressure, the per-table flush timer in `replica/table.cc`, and
   `nodetool flush` via `replica::database::flush_all_memtables()`. A flush
   produces one sstable per compaction group, so 100 tables give 100 sstables
   *serially*, not 100 in parallel. Node flush concurrency equals `--smp`.

So a shard flushing back-to-back yields ~0.6 sstables/s ≈ 9 PUT/s, against the
~150 sstables/s ≈ 2,200 PUT/s needed — roughly 240 shards doing nothing but
flushing. The boost test works precisely *because* it bypasses the flush
serializer and writes sstables directly with 512 in flight.

Dead ends, so they need not be re-investigated:

- The Cassandra-era memtable knobs are inert in Scylla:
  `memtable_total_space_in_mb`, `memtable_flush_writers` and
  `memtable_cleanup_threshold` are all `value_status::Invalid`/`Unused` in
  `db/config.cc`. The memtable budget is hard-coded to 50% of shard memory in
  `replica/database.cc`, so smaller memtables are only reachable via a smaller
  `-m`.
- Compaction is not a multiplier — it merges many sstables into fewer.

If node-level behaviour under throttling is wanted, the route is an error
injection rather than real S3. There is none today that produces a
retryable/throttling error: the existing S3 injections
(`s3_client_fail_authorization`, `kill_s3_inflight_req`,
`break_s3_inflight_req` in `utils/s3/client.cc`) throw non-retryable errors or a
connection reset. The place to add one is `client::wrap_handler`, just before the
`if (possible_error)` block, setting `aws_error_type::SLOW_DOWN` with
`retryable::yes` so the real retry path runs.

## Loose ends found while writing this

- **`endpoint_config::max_connections` has no effect on sstable traffic.**
  `storage_manager` builds its client through the
  `client::make(endpoint, region, iam_role_arn, factory, connections_per_shard)`
  overload, which constructs a fresh `endpoint_config` carrying only
  port/https/region/role and `connections_per_shard` — `max_connections` is
  dropped. `object_storage_connections_per_shard` is therefore the only knob that
  matters there. Its default of 128 caps in-flight requests per shard, which is
  what pinned throughput at a suspiciously exact 4,677 requests/s for both
  concurrency 1024 and 4096 until it was raised.
- **No request timeout is plumbed through `s3::client`.** Driving raw PUTs at
  concurrency 1024-2048 wedged twice: the storm wrote all but exactly one object
  and then stopped making progress, once with no log output for 16 minutes.
  `default_aws_retry_strategy` sleeps `(1 << attempt) * 25ms` for up to 10
  retries, so a single throttled request can sit ~51 s in backoff. Under a
  sustained real 503 storm a node could plausibly hit the same stall. Not
  attributed — it may be an artefact of that concurrency; the sstable-layer path
  at concurrency 512 does not reproduce it.
- **The test under-reports its own throttle count.** Its verdict covers the write
  phase only, because `sstable::unlink()` returns before the objects are actually
  gone and the background deletion keeps getting throttled afterwards. In the run
  above the test reported 225 statuses while the log held 765.
