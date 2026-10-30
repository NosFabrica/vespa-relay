# Upgrading a running deployment

What a deployment notices when it moves to a release, newest first. Schema
changes that need a reindex are in [migrations.md](migrations.md); this file is
about everything else an operator has to do or will see.

## The store reads its own settings (October 2026)

vespa-eventstore `7e05038862db` and quartz 2.8.0. Nothing to deploy: the
schema does not move.

### Do before the first boot

- **Check every `VESPA_*` setting parses.** Both processes now read all of the
  store's variables strictly at boot ([configuration.md](configuration.md),
  "Ranges the boot enforces"), where the store used to read most of them
  leniently: `VESPA_VISIT_STREAM=no` was read as on (only `0` and `false`
  turned it off) and now means off, and a value no reading fits stops the
  boot, with every bad variable named in one line.

### What you will see

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
