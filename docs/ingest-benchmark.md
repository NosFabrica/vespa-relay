# Ingest benchmark

How fast a real corpus gets into the store, through each path that writes it: a client
publishing to the relay, and the mirror downloading from upstream. One protocol, one corpus,
one table, so a change to either path is judged against the same work.

## The corpus

Real events, because the costs that matter scale with what a document carries: kind 3 is the
worst case this store sees (hundreds of `p` tags each, every one a `tag_index` entry), and a
synthetic note corpus hides that. Pull it with amethyst's `amy`:

```bash
# from an amethyst checkout
./gradlew :cli:installDist && AMY=$PWD/cli/build/install/amy/bin/amy
$AMY count --kind 3 --relay wss://relay.ditto.pub --json          # size it first: 3,016,179 on 2026-10-16
$AMY fetch --kind 3 --relay wss://relay.ditto.pub --paginate --limit 0 \
  --out ditto-kind3.jsonl.gz --json                                # one event per line, verified, not cached
```

A cut-off walk resumes with `--until` the oldest `created_at` already written; overlapping
lines are the same event and dedupe on read. Run the export from a copy of amy's install
directory if you will rebuild amethyst while it runs: `installDist` replaces the jars under a
running JVM, which then dies on its next class load.

Benchmarks use fixed slices (`head -n 100000`), not the whole file. The walk is newest-first,
and event size is not uniform along it: the newest 20k kind 3s average 9 KB, the first 100k
3.3 KB, with a tail past 500 KB. Compare arms on the same slice, and read `mb_s` beside `ev_s`.

## An arm

`relay/tools/ingest-bench/bench.sh NAME [VAR=value …]` runs one arm:

1. a fresh Vespa (container removed, volume deleted, `VESPA_MEM_LIMIT=8g`), unless
   `KEEP_STORE=1`, which keeps what the previous arm wrote: the resend pass;
2. `MODE=relay` (default): the relay booted with the `VAR=value` pairs as its environment, then
   `amy publish --file` pushing the slice over one websocket with `INFLIGHT` (2048) EVENTs
   unanswered at once. `MODE=sync`: the same slice through `CorpusIngestBench`, the mirror's
   `IngestPipeline` wired as `SyncMain` wires it, with the pairs as its `BENCH_*` settings;
3. one row appended to `build/ingest-bench/results.tsv`, the run's logs under `runs/`.

The measured processes (Vespa, the relay, the publisher or the bench JVM) run on `CPUS`, so
a download or a build elsewhere on the box stays off them. Every arm on one table shares one
`CPUS` and one slice.

| column | what it is |
|---|---|
| `ev_s`, `mb_s` | events and bytes answered per second, up to the last answer |
| `accepted` / `duplicate` / `refused` / `unanswered` | the OKs by kind: `OK true`, `OK true duplicate:`, `OK false`, none at all |
| `ok_p50_ms`, `ok_p99_ms` | EVENT to OK, as the publisher saw it; with 2048 in flight this is mostly queueing |
| `relay_cpu_s`, `vespa_cpu_s` | CPU seconds spent during the arm, by process; the bench JVM is not split out in `MODE=sync` |
| `relay_us_ev`, `vespa_us_ev` | the same per event sent: what an event costs each side |

`refused` on this corpus is the relay's own limits (`max_event_tags` 2000, `max_message_length`
256 KiB), the same in every arm. `unanswered` must be 0; a non-zero value is a bug in a path,
not a result. The relay's last `store-metrics` line (every 15 s in a bench) breaks the store's
time into stages: `write` is the feed, `dedup` / `versions` / `guards` the reads around it,
`lock.ingest.hold` how long the writer lock was held.

## Arms

```bash
T=relay/tools/ingest-bench/bench.sh; export CORPUS=slice-100k.jsonl AMY=…/amy
$T queue-64       RELAY_INGEST=queue                      # quartz's IngestQueue, as shipped until now
KEEP_STORE=1 $T queue-64-dup RELAY_INGEST=queue           # the same slice again: every event a resend
$T queue-4096     RELAY_INGEST=queue RELAY_INGEST_BATCH=4096
$T pipe-2x1000    RELAY_INGEST=pipeline                   # the mirror's IngestPipeline, its defaults
KEEP_STORE=1 $T pipe-2x1000-dup RELAY_INGEST=pipeline
$T pipe-2x4096    RELAY_INGEST=pipeline RELAY_INGEST_BATCH=4096
MODE=sync $T sync-2x1000 BENCH_CONCURRENCY=2 BENCH_BATCH=1000 BENCH_PASSES=2   # no socket; pass 2 is the resend
```

## Results

2026-10-16, one 4-core box: the measured side on 3 cores (`CPUS=0-2`), Vespa at
`VESPA_MEM_LIMIT=8g`, a corpus download on the fourth. The slice is the first 100,000 lines of
the ditto export (344.8 MB). `refused` is 125 in every relay arm: the relay's own tag and size
limits, which the mirror does not apply, so the sync arms store those 125 too.

| arm | ev/s | MB/s | OK p50 / p99 ms | relay µs/ev | Vespa µs/ev |
|---|---|---|---|---|---|
| `queue-64` (quartz's IngestQueue, as shipped) | 427 | 1.47 | 3,401 / 6,798 | 1,600 | 3,650 |
| `pipe-2x1000` (IngestPipeline, the new default) | **620** | 2.14 | 2,092 / 5,356 | 1,380 | 2,880 |
| `pipe-2x1000`, 4 publishers | 573 | 1.97 | 10,004 / 26,020 | 1,430 | 2,930 |
| `pipe-2x4096`, 4 publishers | 595 | 2.05 | 8,896 / 28,082 | 1,370 | 2,820 |
| `queue-4096`, 4 publishers | broken: 7,136 unanswered | | | | |
| `sync-2x1000` (the mirror, no socket) | 682 | 2.35 | | | 3,800¹ |
| `sync-2x4096` | 750 | 2.59 | | | 3,570¹ |
| `queue-64-dup` (every event a resend) | 2,055 | 7.08 | 405 / 1,609 | 510 | 610 |
| `pipe-2x1000-dup` | 2,024 | 6.98 | 279 / 1,764 | 460 | 720 |
| `sync-2x1000`, pass 2 (resends) | 30,961 | 106.75 | | | ¹ |

¹ Both passes of a sync arm share one Vespa reading, the schema deploy included.

What it says, so far:

- **The pipeline is 45% faster on the relay's publishes**, and spends 21% less engine CPU per
  event: fewer, wider commits, and the version probe answering supersession before the lock.
- **Fresh writes are engine- and CPU-bound on this box.** Relay plus Vespa plus the publisher
  spend about 4.6 ms of CPU per kind 3; three cores cap that near 650 ev/s, which is where every
  fresh arm lands, and more publishers only lengthen the queue (p99 26 s). Going faster means
  spending less CPU per event, not more concurrency.
- **A saturated engine fails wide batches instead of slowing them.** `queue-4096` with four
  publishers stalled: a 4096-document `putAll` under a saturated Vespa outran the store's 30 s
  per-operation feed timeout, the feed client's breaker opened, and a batch failed whole.
- **Resends cost the wire, not the store**: 31k ev/s through the pipeline alone, 2k through the
  relay. When the store has nothing to write, framing, parsing, policies and the OKs (and the
  publisher's own JSON) are the whole cost, ~460 µs of relay CPU per event.

### Follow-ups: the feed writer and the access log

A JFR profile of the `pipe-2x1000` arm (the harness's `JAVA_OPTS` reaches the relay alone;
`AMY_JAVA_OPTS` is the publisher's) put 12% of the relay's samples in the store building each
feed body as a `JsonObject` tree, and most of the rest of its write in the Vespa feed client's
own HTTP/2 request per document. The store now writes the body in one pass (vespa-eventstore,
`EventDoc.feedJson`, byte-identical): 320-360 µs to 183-206 µs per body. Same slice, same box:

| arm | ev/s | relay µs/ev | Vespa µs/ev |
|---|---|---|---|
| `pipe-2x1000`, tree (three runs) | 620, 599, 614¹ | 1,330-1,380 | 2,720-2,880 |
| `pipe-2x1000`, one-pass writer (two runs) | 585, 634 | 1,280-1,300 | 2,710-2,880 |
| the same with `VESPA_ACCESS_LOG=disabled` (two runs) | **679, 689** | 1,230-1,250 | **2,520-2,550** |

¹ 614 and 623 were the writer's first, two-pass form.

The writer is a 3-5% cut in the relay's CPU and invisible in throughput here, where Vespa
takes two thirds of the CPU; it is the relay's own capacity it buys back. The access log is
worth 12%: the store feeds one HTTP request per document, so the container logged a line per
event. `.env.example` already set it to `disabled`, but `docker-compose.yml` never forwarded the
variable to either process, so a compose deployment ran with the log on.

Two client artifacts were found and fixed on the way, both of which had inflated `duplicate`
and `unanswered` in earlier runs: OkHttp closes a websocket whose send queue would pass 16 MiB
(2,048 of these events are 38 MiB), and an EVENT over `max_message_length` was answered with a
NOTICE and no OK. `publishStream` now bounds its window in bytes, and quartz answers the
oversized EVENT with `OK false`.

### Feed compression

An audit of the store's write path found the feed client compressing every document: its
default, `auto`, gzips any body over 512 bytes, and nothing set it. For a 400-tag kind 3 (72 KB,
29 KB gzipped) that is about 2 ms of the relay's CPU, four times what writing the body costs, and
Vespa inflates it again. The store now reads `VESPA_FEED_COMPRESSION` (vespa-eventstore
`75be510`), and `docker-compose.yml` sets `none` for its Vespa on the same host. Same slice and
box, `VESPA_ACCESS_LOG=disabled`, two alternating pairs:

| arm | ev/s | relay µs/ev | Vespa µs/ev |
|---|---|---|---|
| `auto` (gzip) | 693, 725 | 1,180-1,210 | 2,430-2,440 |
| `none` | **792, 832** | **940-990** | **2,200-2,280** |

From 427 ev/s at the start of this page to 832 on the same box and slice. Across a WAN the bytes
are the cost instead; set `gzip` there.

The same audit changed the harness after these rows were taken: `vespa_cpu_s` now ends at the
last answer, as `relay_cpu_s` does (every row above includes the 16 s the harness then waits for
the last metrics line, so later rows read slightly lower); the relay is tracked by the pid the
harness started, never by a process pattern that would also match a compose relay's JVM; and an
arm refuses to wipe the store while the compose relay is running. It also sized the relay's queue
to one batch per worker (2,000 waiting instead of 4,096 at the defaults). Both checked on the
final build (store `75be510686`, quartz `020e714858`), `none`, same slice and box:

| arm | ev/s | relay µs/ev | Vespa µs/ev |
|---|---|---|---|
| final build, `none` (two runs) | **822, 844** | 930-950 | 2,080-2,110¹ |

¹ The window that ends at the last answer.
