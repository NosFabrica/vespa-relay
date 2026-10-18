# Upgrading a running deployment

What a deployment notices when it moves to a release, newest first. Schema
changes that need a reindex are in [migrations.md](migrations.md); this file is
about everything else an operator has to do or will see.

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
- **Roster and `gatedBy` verdict sources age a grade on its label's own stamp**,
  not on the record's `created_at`, which every co-writer bumps. A stale
  `prime` no longer re-enters the roster when another pass re-signs its record.
- **`/pressure` gains `liveGate: { failed, shed }`**: cold-author live
  deliveries refused because the trust read failed or found no slot.
