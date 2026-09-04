# Instrumentation

Moved from AGENTS.md on 2026-09-04, unchanged. This is the long form of the AGENTS.md section "Instrumentation".

## Instrumentation — use it before theorising

Most of this exists because something was diagnosed wrongly from inference.
Reach for it first.

- **health line** (once a minute) — heap, ingest queue depth vs capacity, ev/s
  IN and OUT of the ingest queue, relays transferring, connected, fatal count,
  events lost to store errors. A full queue and an empty queue are opposite
  diagnoses that look identical everywhere else — and so are the two FULL
  ones, which is why the line names the workers in a batch and the age of the
  oldest when it says `wedged`. The rate is a pair for the same reason: `0
  ev/s` on a full queue was the drain alone, and it reads the same for a store
  that has stopped answering and for a fan-out that has gone quiet. The `in`
  half (`arrivingPerSec` on the page, `IngestPipeline.submitted` differenced)
  is what tells them apart.
- **`SYNC_WIRE_LOG`** — `sent` logs every REQ/CLOSE; `full` adds every message
  received. Empty still logs `NOTICE`, `CLOSED` and failed sends, which are the
  relay explaining itself. It lowers quartz's log floor itself, because
  `QUARTZ_LOG_LEVEL=WARN` would otherwise silently discard its own output.
- **`SYNC_STREAMS`** — run one stream alone, so a measurement isn't three
  streams competing for one socket budget, heap and ingest queue.
- **`ingest stages`** — per-stage timing (`dedup`, `write`, `proj.fetch`,
  `proj.write`, `versions`, plus the router's own `verify` and `dedup.pre`).
  This is what identified a projection read-back as 90% of ingest. Signature
  verification was NOT on this line for as long as it existed, so "is verify
  the limit?" was a question no instrument here could answer; anything else
  added to the ingest path belongs on it for the same reason.
- **`store` on `/stats.json`, and the `store call SLOW` lines beside the health
  line** — WHICH store calls this process has outstanding right now, WHO asked
  for each, and WHAT it asked for. Reach for it the moment `oldestBatchSec`,
  `wedged` or a full ingest queue is on screen: those say a worker has been
  inside a batch pass for 794 seconds and cannot say which of the pass's three
  store calls it is in — the id probe, the version probe and the write go to
  three different engine paths with three different remedies. `caller` is this
  router's own subsystem names (`ingest.dedup`, `ingest.versions`,
  `ingest.write`, `visit.negentropy`, `heal.resolve`, `audit.retraction`,
  `push.upstream`, `monitor.verdicts`, `monitor.publish`, `source.relayLists`),
  each greppable straight back to the line that makes the call; `callers` rolls
  the same names up into issued/answered/failed/cancelled/outstanding, which is
  the only thing in this repository that can answer *whose requests are filling
  the engine's queue*; `ages` bands the outstanding set so a busy router and a
  wedged one are different shapes rather than one number. `outstandingAtIssue`
  is the client half of *slow store or long queue* — the store's own request
  dispatcher is 1,024 wide, far above anything this router runs, so a slow call
  issued with a handful outstanding did not wait on our side of the wire.
  `slowAfterSec` is the router's own slow bound, published so the page marks a
  row where the log names one. **The live half is one partition three ways** —
  `outstanding`, the callers' shares of it, and the age bands all come off ONE
  read of the outstanding set, so they close exactly and the card reports it
  when they do not; the lifetime counters beside them (`issued`, `returned`,
  `failed`, `cancelled`) are a rate and agree with the live count only once the
  router goes quiet.
  See `StoreCalls`. **Do not wait for `bottleneck` to agree with it**: measured
  against a deliberately frozen store (`docker pause vespa` under a live
  mirror), three store calls had been hung for 74 seconds while `bottleneck`
  still read `mixed` — which the card draws as "keeping up — nothing here is
  the constraint". That is honest as far as it goes, since the queue was not
  full and `wedged` needs ten minutes of every worker held; it is also the
  whole blind spot this section fills, and the `store call SLOW` lines were
  already naming the calls a minute in.
  **Two halves of it are NOT ours to publish and want a store
  change**: an `X-Caller` header on the wire, and a server-side service-start
  timestamp — `VespaHttp` builds its own OkHttp client with no header or
  interceptor seam, so neither is reachable from this repository.
- **the abort partition on the visits row, and the `visit … aborted` lines
  beside it** — WHY visits are ending early, which on a mirror that has stopped
  converging is the only question. `abortedVisits` is a total and unactionable
  on its own; the eight counters that split it (`abortedAuthRequired`,
  `abortedClosed`, `abortedQuiet`, `abortedUnreachable`, `abortedUnpageable`,
  `abortedGaveUp`, `abortedFailed`, `abortedBackpressured`) sum back to it
  exactly, and each has a different remedy — a key the relay accepts, its own
  CLOSED sentence read, a slower revisit, nothing at all. Read
  `abortedBackpressured` FIRST, with `visitsHeldByIngest` beside it: they are
  about this mirror's ingest queue rather than about any relay, and before they
  existed their share sat inside `Quiet` and `Unpageable` — 90% of all aborts
  on staging, blamed on relays that were fine. Reach for it the moment `contentViaOutbox`
  stops reconciling: 92.5% aborting says the resync cannot converge, and only
  the split says what would fix it. The log half is one line per
  (stream, relay, reason) per half hour, carrying the ask and — where the relay
  explained itself — the sentence, which the counters cannot hold and
  `SYNC_WIRE_LOG` could only give you for every relay at once. See
  `VisitAborts` and `RelayComplaints`. `narrowedRelays` beside them is not a
  fault: it counts relays that have told us how wide a filter they take, and
  each one is a relay that could never finish an ask before.
- **the `prime relays` table on the mirror's page** — TWO AXES, and reading
  either one alone is how this table was wrong first. `syncStatus` is the PAST
  (`notStarted` / `paging` / `complete` / `refused`); `behind` is the PRESENT —
  how old the newest event we hold from that pair is, bucketed `current` /
  `today` / `thisWeek` / `older` / `nothing`. **`complete` says the past below
  is settled and says nothing at all about whether we are current**, so a pair
  that is complete with a dead tail and nothing since last week is a worse
  finding than one still paging and live — and the first version ranked them
  the other way round, green chip at the bottom of the sort. The `fault` mark
  and the row order now span both: a refusal, a pair never reached, or one that
  is cold with NO TAIL watching it. A cold pair that IS tailed is excluded on
  purpose — something is listening, so what is old is the relay's content and
  not our copy of it, and without that exclusion every low-traffic relay on the
  roster would sit in the fault band forever and retire the mark. The third
  reading is the TERMS a relay serves us on, both of which were measured and
  invisible per relay: the monitor's signed NIP-77 verdict (`negentropy` —
  against a `false` relay a `paging` row can never settle by itself, which is a
  configuration question rather than a puzzle) and the filter width its own
  refusal taught us (`kindCap`), beside the relay's own sentence.
- **…and the same table's per-relay reading** — WHERE EACH RELAY STANDS,
  which everything else the mirror publishes is an aggregate over. `roster`
  counts them, the coverage card charts their bands folded per stream, and the
  in-flight tables name the handful a worker is holding this instant — so *is
  this relay synced*, the question an operator actually arrives with and
  usually about ONE relay, could be answered only while that relay was being
  visited. Four answers per (relay, stream) pair: `complete` (every band
  settled, with the last reconcile's age), `paging` (still walking back — watch
  `coveredFrom`, which not moving between two polls IS the finding),
  `refused` (visited, no band written, with the reason and the relay's own
  sentence) and `notStarted`. **The last two are the pair that could not be
  told apart before**: both are the same absence in the band file, they are
  opposite findings, and the coverage card can draw neither because its
  denominator is *relays this stream has touched*. Worst first, the status
  counts published whole even when the row list is cut, and `settled/asks` on a
  `paging` row because a unit owes one ask per bound provider and the status
  alone covers 39-of-40 and 1-of-40 the same way. **The page is not where ONE
  relay is looked up** — that is `jq` over `/stats.json`, which carries the same
  rows; the table's job is what is wrong on this mirror. See
  `RelayStatusReport`.
- **`RelayReachLiveProbe`** — the same question asked of a LIST of relays
  ahead of the deployment: dial each, send the real ask, print the ending, the
  relay's own sentence, whether our AUTH was accepted, and the width it will
  take. This is what turned #185's four log-derived lists into per-relay
  verdicts in one run, and its `-DreachNoAuth=true` control arm is the only
  thing that can price the NIP-42 responder — thirteen relays flip from
  `AUTH_REQUIRED` to serving when it is attached. Reach for it before believing
  any claim of the form *the router cannot read these relays*: a production log
  shows a relay's FIRST word, and for an auth-gated relay that word is always
  `auth-required:` whether or not the AUTH that follows gets us in.
- **paging progress** — percentage and ETA measured on the *time axis*, because
  a paged fetch has no event denominator. Its predecessor computed
  `downloaded/downloaded` and printed `100%, ETA ~0:00` for hours.
- **`IngestCostBench`** — what one arriving event costs ingest, split by the
  verdict it ends on, end to end through the real pipeline against a real
  Vespa. It lives in `:peers` with the pipeline it measures — this said
  `:sync:test` for a while, which matches no test and reports BUILD SUCCESSFUL
  having run nothing. Skipped unless `BENCH_VESPA_URL` names a live engine:
  `BENCH_VESPA_URL=http://localhost:8080 BENCH_N=20000 ./gradlew :peers:test
  --tests '*IngestCostBench*' -PtestHeap=6g --rerun-tasks -i` (Gradle treats env
  vars as invisible, so without `--rerun-tasks` a second run is silently
  UP-TO-DATE and prints the FIRST run's numbers).

  **`-PtestHeap` is not optional at six figures.** The test task takes Gradle's
  512m default, and a `BENCH_N=100000` corpus — the events, plus a `NostrSignerSync`
  per author — is several GB. It is opt-in rather than the default because no
  unit test needs it and every CI box would pay for it.

  **Standing a Vespa up for it, on a machine that is not the deployment.** The
  compose service is enough (`VESPA_MEM_LIMIT=9g docker compose up -d vespa`;
  the committed 34g default is a limit sized for the real host and a small box
  will not honour it). Do NOT wait on its healthcheck first: that check requires
  a search returning a content node, a content node requires a deployed
  application package, and nothing deploys one until the relay boots or the
  store's `autoDeploy` runs — so a bare `up vespa` sits at `health: starting`
  forever while the container logs `No response / error from config server. This
  is normal before an application package is deployed.` The bench bootstraps
  itself (`VespaEventStore.open(autoDeploy = true)` → `deployIfAbsent`, which
  deploys AND waits for serving), so just run it and let it deploy — but WAIT FOR
  THE CONFIG SERVER FIRST. `deployIfAbsent` gives up after two minutes, and a
  container still booting eats that whole budget, so the bench throws before
  printing a single line (and a grep for `COST-BENCH` shows an empty file and a
  clean exit code, which reads like a bench that ran and found nothing):
  `until curl -sS http://localhost:19071/state/v1/health | grep -q '"code" : "up"'; do sleep 5; done`

  Measured on a 4-core box sharing its cores with the engine, 72k-doc corpus,
  20k-event batches — the ratios are what travel, not the absolute times:

  | arriving event | µs/event, probe off → on | where it goes |
  |---|---:|---|
  | fresh (written) | 508 | `write` 73%, `verify` 12% |
  | duplicate | 66 → 17, 35 → 11 | `verify` was 2/3 of it; gone |
  | stale replaceable | 130 → 40, 68 → 34 | `versions`+`verify` were 75%; gone |
  | newer replaceable (written) | 778 | `write` 66%; the probe is ~6% overhead |

  Each pair is interleaved, so a warming engine cannot be read as a difference
  between arms. **Verification is not a rounding error** — 12% of a fresh batch,
  two thirds of a duplicate one — and it costs ~70-95µs in situ against ~48µs
  isolated, because the router competes with Vespa for the same cores.

  The asymmetry is the thing to understand before touching either probe: on a
  stream that mostly REJECTS, both probes are worth 2-4x; on one that mostly
  ACCEPTS they are duplicated work the store will redo, and the version probe
  costs ~6%. Which regime a deployment is in is not knowable here and changes
  over a backfill's life, so `ProbeGate` measures it instead of anyone
  declaring it. The break-evens its thresholds come from: ~35% duplicates for
  the id probe (10-23µs/id against the ~44µs a drop saves), ~20% stale for the
  version probe (21-29µs against ~110).

  **EVERY ROW ABOVE IS A PURE BATCH, AND A MIRROR NEVER SEES ONE.** They price
  one verdict at a time, and the omission matters more than the numbers do: a
  100%-duplicate batch is dropped whole by the probe, so it never reaches the
  write and never takes the writer lock. A real batch carries a couple of
  percent that must be written, and that is where the lock is. At
  `BENCH_N=100000` on the same 4-core box, the `98% dup / 2% fresh` arm:

  | batch | us/event, probe off -> on |
  |---|---:|
  | 100% duplicate | 37 -> 11 (**3.3x**) |
  | 98% duplicate / 2% fresh | 48 -> 46 (**1.04x**) |

  **At a mirror's real mix the id probe is worth a few percent, not 3.3x**, and
  the mix costs 2.4x what its parts predict (0.98 x 11 + 0.02 x 349 = 19us
  against 46 measured). Neither is a slow stage. Both are batch COMPOSITION.

  ### Survivors per lock hold is what decides ingest throughput

  The store takes ONE writer mutex for the whole of `commit`, so commits never
  run in parallel however many ingest workers exist - more workers only lengthen
  the queue for it. What a lock hold is worth is how many surviving events it
  writes, and that is `batchSize x (1 - dropRate)`. At 98% dropped, a 512-event
  batch carries **ten**. The `98/2 shape sweep` arm - same work, three shapes:

  | `concurrency x batch` | real batchSize | commits for 100k | survivors each | us/event | ev/s |
  |---|---:|---:|---:|---:|---:|
  | `8 x 1024` | **512** | 195 | ~10 | 150 | 6,685 |
  | `2 x 8192` | 8192 | 12 | ~164 | **17** | **60,492** |
  | `1 x 16384` | 16384 | 6 | ~328 | 18 | 56,439 |

  Reproduced on an independent run against a fresh Vespa: 5,778 / 53,030 /
  50,886 ev/s. Unlike the burst sweep above, this ordering does NOT flip — the
  9x is the finding it looks like.

  **9x, on identical work, from shape alone.** The eight-worker row spent
  `lock.ingest.wait 99.1s` of aggregate thread time across a 15s wall - each
  worker queued ~12.4 of 15 seconds - to perform `write 0.2s` of writing.
  Concurrency past 1-2 buys nothing once batches are wide, exactly as a
  serializing mutex predicts; width is the whole lever.

  **And the configured batch is not the batch.** `capacity = (batch * 4)
  .coerceIn(4096, MAX_INBOUND_QUEUE)`, then `batchSize = min(batch, capacity /
  workers)` - so at eight workers `SYNC_INGEST_BATCH=1024` yields 512, and no
  setting can exceed 2048. `MAX_INBOUND_QUEUE` was sized for MEMORY ("16k events
  is a few hundred MB") and now binds write efficiency too: one constant, two
  unrelated concerns.

  ### …and on an all-fresh burst, shape does not matter at all

  The row above is a mirror's steady state. A sudden burst of genuinely NEW
  events is the opposite regime, and the same three shapes over 100k fresh
  events answer it flatly:

  | `concurrency x batch` | ev/s, order UP | ev/s, order DOWN |
  |---|---:|---:|
  | `8 x 1024` | 2,405 | **2,337** |
  | `2 x 8192` | **2,858** | 2,307 |
  | `1 x 16384` | 2,375 | 2,092 |

  **The two orders disagree about which shape wins, and that IS the result.**
  Every arm writes another 100k documents, so a later shape meets a bigger
  index; run one order only and that drift is indistinguishable from an effect
  of width — which is how a first pass produced an "if anything decreasing with
  width" reading that does not survive. Ascending, `2 x 8192` wins; descending,
  `8 x 1024` does. A rank order that flips when you reverse the arms is noise
  and corpus growth, not shape. **On an all-fresh burst, pipeline shape does not
  move throughput**: everything sits at 2.1-2.9k ev/s.

  The one signal that does survive both passes is small and points the other
  way: `1 x 16384` is last in each, including the descending pass where it ran
  FIRST against the smallest corpus. A single worker cannot overlap anything —
  its verify and `dedup.pre` sit in series with its own write — which is the
  mechanism that predicts exactly that. It is 1-10%, and the reason to prefer
  two workers over one.

  In every row `write`
  is 96-99% of `hold`: the lock is held essentially the whole wall clock, and
  essentially all of that holding is the write itself. There is no non-write
  time inside the lock to reclaim, so **removing or striping the writer mutex
  cannot make a burst faster** — a burst is engine-bound, and the feed client
  already pipelines 32 connections inside a single `putAll`.

  The eight-worker row is the cleanest proof: `lock.ingest.wait 243.9s` across a
  37.6s wall — each worker queued ~30 of 37.6 seconds — for the SAME throughput
  as one worker that waits for nothing. **Contention here is a symptom, not a
  cause**; the workers are queueing for a resource that is already saturated.
  Read `lock.*.wait` against `write`/`hold` before concluding a lock is a
  bottleneck, or the 98/2 and burst rows look identical and are opposites.

  Two things follow. Wider batches are safe for bursts as well as 9x better for
  the steady state — the 6% here is inside the noise of a shared box. And if a
  burst must be absorbed FASTER, the lever is the feed client or Vespa, not the
  router's pipeline: `VESPA_FEED_CONNECTIONS` / `VESPA_FEED_STREAMS` /
  `VESPA_FEED_INFLIGHT_FACTOR`, and `VespaFeed.statusLine()` — acks, inflight
  window, latency, exceptions, already computed and never printed by the router
  — is the instrument that says whether the client is throttling itself or the
  engine is pushing back. Nothing here can tell those apart today.

  What a burst still costs is not throughput but STARVATION: for its ~40s every
  other writer in the process (verdict edits, the healer, the sweep) queues on
  that same mutex, which is what `EDIT_DEADLINE_MS` was written for. This bench
  runs no other writers, so it does not measure that; `lock.gate.wait` during a
  burst is the number nobody has.

  ### What staging said when it ran, and what the bench could not have known

  Deployed at `concurrency=2, batch=8192` against the real store (~200M docs, 67
  concurrent visits, negentropy reads on the same engine), ingest **oscillates
  between ~11,400 ev/s and 136** with the queue pinned at its ceiling. Two
  numbers matter and neither survives a 500k-document bench:

  - **Absolute throughput does not transfer.** A 98/2 batch of 8192 takes ~0.16s
    here and tens of seconds there. Dedup cost scales with the INDEX, and the
    bench's corpus is ~400x smaller. Use the bench for ratios between shapes,
    never for what a batch costs.
  - **The accepted fraction is not a constant.** Lifetime it is 2.1%, but over
    one two-minute window it was **21.5%** — and throughput fell with it,
    because writes dominate the moment they stop being rare. "98/2" is an
    average, not a workload.

  That floor is what re-derived [WEDGE_AFTER_MS]: at 136 ev/s two workers on
  8192-event batches take 120 seconds, which was the whole threshold. It is ten
  minutes now, and the lesson generalises — **a threshold derived from a bench
  corpus is a guess about production until production runs it.**

  The operator-level fix needs no code: `SYNC_INGEST_CONCURRENCY=2
  SYNC_INGEST_BATCH=8192`. **The formula was deliberately NOT changed**, because
  widening batches is not free in the other direction: every other writer - the
  monitor's verdict edits, the healer, the sweep - queues on that same mutex,
  and `RelayVerdictRecord.EDIT_DEADLINE_MS` exists because they already wait
  "~10s per 20k-event batch, several deep under load". Wider batches make ingest
  faster and that tail longer. Measure both before moving the default.

