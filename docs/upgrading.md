# Upgrading a running deployment

What a deployment notices when it moves to a release, newest first. Schema
changes that need a reindex are in [migrations.md](migrations.md); this file is
about everything else an operator has to do or will see.

## The search page asks the sync about your posts (November 2026)

The line under the search box that says whether the mirror has a reader's own
posts now asks the sync process, which knows, instead of counting the posts on
two relays. The relay stops reading the manifest file the sync left on the
shared volume, and `/stats.json` goes to schema 4 without its `sync` section.

### Do before the first boot

- **Set `SYNC_PUBLIC_URL` on the relay** to the address readers' browsers reach
  the sync at, e.g. `https://sync.example.com`, and route `/coverage/` there
  from your proxy (the sync serves it on `SYNC_STATUS_PORT`, 7778). Only that
  path needs to be public. Unset, the line under the search box says nothing
  about posts.
- **Drop `SYNC_MANIFEST_FILE` and `STATS_COUNTERS_INTERVAL_SECONDS`** from your
  `.env`. Both do nothing now, and the relay and the sync name them at boot if
  they are set. The old `sync-manifest.json` on the volume can be deleted.

### What changes in behaviour

- **The posts line counts relays, not posts.** "3 of 4 relays read in full"
  comes from the sync's own walk, so it no longer depends on a stranger's
  `COUNT`. A reader no stream follows is told the mirror isn't following their
  posts yet.
- **`/stats.json` has no `sync` member and one tier, `charts`.** A client that
  read `sync.mirrors.kinds` should ask the sync's `/coverage/<pubkey>` instead.
- **The sync answers `GET /coverage/<pubkey>?relay=…`** to any origin: every
  stream that reads that author on the relays named (or is bound to them
  anywhere) and how far each has got, and the named relays nothing reads. It caches an author for
  30 s and gives at most 20 fresh answers a second; past that it answers 429.

## Store refusals and the phrase band (November 2026)

vespa-eventstore `b21f996c1849`. The schema moves, with no reindex: new fieldsets
and rank functions over fields the corpus already has, which the relay deploys at
boot.

### What changes in behaviour

- **A content node that is down is an error, not an empty store.** A NIP-45
  `COUNT` fails instead of answering 0, and the sync keeps the roster it had
  instead of dropping to its declared relays until the next rebuild.
- **A COUNT over several filters no longer holds every match.** The same answer,
  without the heap a broad anonymous COUNT could exhaust.
- **NIP-50 ranks the query as typed above its words scattered.** Results move on
  multi-word searches.

## The monitor is its own process (November 2026)

The monitor left the sync process. It is `vespa-monitor` in the same image and
the `monitor` service in `docker-compose.yml`, under the same `sync` profile, so
`docker compose --profile sync up -d` starts it.

### Do before the first boot

- **Move the monitor's settings to the monitor.** `MONITOR_CONFIG`,
  `MONITOR_CONFIG_FILE`, `MONITOR_STATUS_PORT` and `MONITOR_STATUS_FILE` on the
  sync process stop its boot and say where they moved, and so does a
  `monitor { }` block in `sync.conf`: move its contents to `monitor.conf`. Under
  compose with `MONITOR_CONFIG_LOCAL` there is nothing to change.
- **Check the memory budget.** The monitor container takes `MONITOR_MEM_LIMIT`
  (default `6g`, not yet measured on its own). Its heap was part of the sync's
  12g before.
- **A probe's dial budget** is `connectionTimeout` in `monitor.conf` now
  (default 20 s); `sync.conf`'s no longer reaches the monitor.

### What you will see

- **A `vespa-monitor` container**, with the monitor page on 7779 as before and
  its own `boot:` and health lines. `restart sync` no longer restarts the
  monitor; `restart monitor` does, and neither touches the other.
- **The monitor's lines start `monitor:`, not `router:`.** The fold, stability
  and fitness lines, `store call SLOW` and the socket and Tor lines included;
  grep `docker compose logs monitor` for `monitor:`. `router:` is the sync's.
- **No monitor config, no monitor.** Without `MONITOR_CONFIG_LOCAL` the monitor
  prints that this deployment measures nothing and exits 0, and compose leaves
  it down. A sync whose streams discover their relays is no longer refused for
  having no monitor declared: it rosters from whatever verdicts `verdictsFrom`
  names, and the boot says whose.
- **A health line naming a starved stream.** A stream whose relays come from
  kind-30166 verdicts that the store does not hold (`router: health stream `X`
  takes its relays from kind-30166 verdicts and the store holds none…`) means
  no monitor is signing them: start the monitor service, or point
  `verdictsFrom` at the monitor that does.
- **The sync's `connectionTimeout`** is named at boot when it is not the
  default, since the monitor no longer reads it.
- **An event a probe happens to see is no longer mirrored.** It went into the
  mirror's ingest when both ran in one process; the monitor drops it now. A
  handful per relay, and the mirror's own walks fetch them.

## Score repair and quartz 2.13.0 (November 2026)

vespa-eventstore `2e4f323c4aad` and quartz 2.13.0. Nothing to deploy: the
schema does not move.

### Do after the first boot

- **Audit the scores once.** A restart could delete reputation documents: the
  boot drain re-derived the authors a dying process left marked against a
  provider pass that read no kind 10040 on the cold engine, and each came out
  empty. On search.brainstorm.world one author lacked 422 of 698 providers'
  cells, every one older than 2026-11-02 07:40 UTC, so ranked reads gated
  trusted authors out. The store now refuses that derivation; the cells lost
  before stay lost until repaired. Boot once with `VERIFY_TRUST_ON_START=check`
  and read its `trust-verify:` line, then once with `repair`, then unset it
  ([configuration.md](configuration.md)). Run the repair on this release, not
  before: the guard has to be in place, or the next restart can undo it.

### What you will see

- **A cold boot may report the trust drain refusing.** The first refusal is
  printed with its stack (`… failed and stay queued: the provider pass read no
  kind 10040, yet N of M subject(s) hold scored documents …`) and later ones
  are counted under the store's `trust.drain` background failures: the engine
  is not serving its 10040s yet, so the drain keeps the marked authors queued
  and retries every few seconds instead of re-deriving them. It clears on its
  own once the lists are served.
- **The audit is on `/trust.json`** as the `trust-verify` step while it runs.
- **The sync's relay client.** A fleet subscribed at start is dialed once
  rather than redialed on the first tick; a resolver that did not answer no
  longer parks a relay for five minutes as a dead domain; no AUTH goes out on
  a client being torn down (quartz 2.10.0 to 2.12.1).

## The store reads its own settings (October 2026)

vespa-eventstore `d75be63d9db0` and quartz 2.9.0. Nothing to deploy: the
schema does not move.

### Do before the first boot

- **Check every `VESPA_*` setting parses.** Both processes now read all of the
  store's variables strictly at boot ([configuration.md](configuration.md),
  "Ranges the boot enforces"), where the store used to read most of them
  leniently: `VESPA_VISIT_STREAM=no` was read as on (only `0` and `false`
  turned it off) and now means off, and a value no reading fits stops the
  boot, with every bad variable named in one line.
- **Check `ALLOW_PUBKEYS`, `DENY_PUBKEYS` and `RELAY_CONTACT_PUBKEY`.** Each
  entry must now be one whole `npub1…` (or `nprofile1…`), optionally behind
  `nostr:`. Trailing characters after the key, or an `nsec` pasted onto it,
  used to be read past and now stop the boot naming the setting.

### What you will see

- **`RELAY_STATE_FILE` keeps more.** The NIP-86 event allow list, roles and
  claims now survive a restart beside the bans; an `allowevent` used to be lost
  on restart. The file the old relay wrote reads as before.
- **The upstream push waits for each `OK`.** An `up`/`both` stream sends four
  events at a time and waits for the upstream's answer, where it used to send
  at a fixed 25 per second without one; one reconcile per upstream per pass,
  budgeted to one `SYNC_UP_INTERVAL_SECONDS`. `pushed N event(s) upstream`
  now counts events the upstream accepted, and the log line names what was
  left for the next pass.
- **`wss://host:443` and `ws://host:80` are `wss://host` and `ws://host`**
  everywhere, the stats rollup included, so a relay listed both ways counts
  once; a NIP-62 vanish naming the relay with its default port now covers it.
- Search expansion serves an upper-case hex `e`/`p` member of a list
  (lowercased) where it used to drop it, and a NIP-85 card whose subject is
  upper-case hex ranks nobody (it never did; it no longer leaves a stored cell).
- A NIP-50 `sort:` the store does not recognise (`sort:bogus`) is no longer held
  by the per-connection search lane; the store always served it as plain
  recall.
- The sign-in NOTICE follows the store's lens: a reader whose FIRST
  `30382:rank` provider has signed nothing here is now told so (`no kind 30382
  from <service> here yet`), even when a later provider's cards are mirrored.
- `/stats.json`'s `trust.scoredPubkeys` reads one to three lower: the store's
  own marker documents are no longer counted as scored pubkeys, so a store with
  no scores now shows zero and the stats page's empty-web-of-trust warning.
- The stats rollup's groupings show on `/pulse.json` as the `Aggregate`
  activity, and with `VESPA_QUERY_URLS` set they rotate across every query
  container instead of asking the first.

## The image runs JDK 27 (October 2026)

The runtime stage of the image moved from JDK 21 to JDK 27 (Temurin, Ubuntu 26.04); the build is unchanged.
Nothing needs doing. What changes: compact object headers are on by default, so live heap per open connection
is about 9% lower; a `JAVA_TOOL_OPTIONS` that names a flag JDK 27 removed would now stop the JVM, so check any
you set beyond compose's defaults; and the boot no longer warns about Netty loading its native libraries.

## HTTP commands have no per-address cap (October 2026)

`HTTP_RELAY_PER_CLIENT`, `HTTP_RELAY_CLIENT_HEADER` and
`HTTP_RELAY_TRUSTED_PROXIES` are gone; a deployment that sets them boots, says
at startup that they do nothing, and ignores them. `HTTP_RELAY_TOTAL` is the one bound on NIP-FE commands, and a
command past it is a `503`; nothing answers `429` any more. Raise the total if
one busy address used to be what kept it from filling.

## The relay is the only writer (October 2026)

The sync process no longer writes the store. Every write it makes (ingest's
batches, the monitor's NIP-66 records, the retraction audit's deletes) goes to
the relay over one socket signed in as the relay's key, and the relay writes it
through its mirror lane under the same policies and signature check as any
publish ([configuration.md](configuration.md), `SYNC_INGEST_URL`).

### Do before the first boot

- **Give both services the same `RELAY_NSEC`.** The sync refuses to boot
  without one, and against a relay that states another key or none in its
  NIP-11 `self`. A deployment that ran the sync anonymously must now create a
  key (any `nsec1…`; it becomes the relay's public identity, see
  `.env.example`).
- **Remove `SYNC_INGEST`.** `local` stops the boot with the reason; a leftover
  `relay` is accepted and does nothing.
- **Mind the relay's write rules.** Mirrored events now meet them as anyone's
  publish does: with `ALLOW_PUBKEYS` or `ALLOW_KINDS` set, the relay stores only
  what they allow, and its tag and size limits refuse what they refuse for any
  client.

### What you will see

- A fresh backfill runs about 20-26% slower than local writes did (200k
  production events, 3 cores), and the relay takes the CPU the sync used to.
  Steady-state mirroring is paced by the upstreams and does not notice.
- Live subscribers whose filters match receive backfilled history.
- The sync's ingest no longer yields to `SYNC_PRESSURE_URL`; the relay's mirror
  lane does. The sync's repairs still yield to it.
- The sync no longer deploys the schema. It waits, up to 10 minutes, until an
  application serves at `VESPA_URL`, which on a fresh Vespa is the relay's boot
  deploy, and logs `schema: AUTO_DEPLOY is the relay's alone` if `AUTO_DEPLOY`
  is set in its environment.
- If the relay is not answering when the sync boots, the sync starts after a
  minute and keeps reading the relay's key every 30s; when the relay answers
  with another key, or none, the sync stops with the same reason the boot
  would have given.

## The hardening release (module audit, October 2026)

### Do before the first boot

- **The image runs as uid 10001.** A bind mount over `/var/lib/vespa-relay`
  (compose's `${PARSE_AUDIT_LOCAL_DIR:-./.audit}`) must be writable by it:
  `sudo chown -R 10001:10001 ./.audit`. A state file the process cannot read
  now stops the boot and names the file and that command, rather than starting
  from nothing. On Kubernetes, `fsGroup: 10001`.
- **Vespa is pinned** (`vespaengine/vespa:8.763.13` in compose). A `docker
  compose pull` no longer moves the engine; bumping it is a deliberate change
  (see [operations.md](operations.md)).

### Settings that booted before and are now refused

Each refusal names the key and the bound, so a refused boot says what to fix.

- Relay: `MAX_MESSAGE_LENGTH` below 512; `DEFAULT_LIMIT` above `MAX_LIMIT`
  (an unset `DEFAULT_LIMIT` follows a lowered `MAX_LIMIT`); `MIN_POW_DIFFICULTY`
  above 256; a negative `CREATED_AT_*`; `STATS_INTERVAL_SECONDS` or
  `STATS_COUNTERS_INTERVAL_SECONDS` negative or above a week (0 is off); an
  unreadable ban file (`RELAY_STATE_FILE`).
- Router (`sync.conf`, `monitor.conf`): a scalar `negentropySyncThePastSeconds`
  or `refetchThePastSeconds` below 3600, 0 included (0 used to be read as an
  hour); a `refetch` tier with `every = 0`; `connectionTimeout = 0`; any
  `*Concurrency`, `maxRelaysPerList` or tier `maxAge` of 0; a source's
  `refreshSeconds` below 10 or `maxAgeSeconds` below 60; the monitor's
  `sweepSeconds` below 300 or a `fastLaneSeconds` from 1 to 29; an invalid
  `urls` or `exclude` entry (each used to be skipped with a warning; a skipped
  `exclude` dialled the relay it was meant to keep out).
- A tier's `every` is honoured as written, as it always was: a half-hourly
  negentropy band still boots.

### Behaviour an operator will see

- **Blank and unset are the same.** Compose passes `RELAY_ICON: ""` when the
  variable is unset; the NIP-11 icon is now the relay's own favicon there
  rather than an empty string, so the relay's kind 0 is republished once.
  Blank `RELAY_DESCRIPTION`, `RELAY_CONTACT` and the like are left out of
  NIP-11 instead of published as `""`.
- **Pages carry a Content-Security-Policy** (`script-src 'self'`), plus
  `X-Frame-Options: DENY`, `nosniff` and `no-referrer`. A page cannot be
  framed. A NIP-07 signer that injects an inline script rather than a
  `chrome-extension://` one would be refused; test the signers your readers use.
- **Each pulse site has its own cookie** (`pulse_session_<origin>`), so the
  relay's and the mirror's no longer sign each other out. Admins sign in once
  again after the upgrade; the old `pulse_session` cookie expires on its own.
- **The monitor stops signing `silent`** for a 429, a 5xx, an unrecognised
  failure or a relay that refused some shapes. Those are set aside unsigned,
  with a cooldown so a rate-limiting relay is not re-dialled on every fast-lane
  tick, and a standing `prime` the monitor can no longer re-measure is withdrawn.
- **`/pressure` gains `liveGate: { failed, shed }`**: cold-author live
  deliveries refused because the trust read failed or found no slot.
