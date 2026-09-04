# The sync boot behind a store read that never answers

Reproduces the staging wedge of 2026-09-04 — `main` parked for the life of
the process in `MonitorEngine.retireOwnStaleVerdicts`, the status sites
never bound, `/sync/` and `/monitor/` answering 502 — against a local Vespa,
and shows the fix.

`tarpit.mjs` is a proxy in front of a Vespa on `:8080` that forwards every
request in its own protocol (the feed client's h2c prior-knowledge HTTP/2 and
the query client's HTTP/1.1 or upgraded HTTP/2) and holds open, forever, any
`/search/` whose body names kind 30166: the monitor's own graded records,
which the retraction walk pages through at boot. That is #167's shape
(deadline-less store reads ahead of the walk) reduced to the one query that
matters, so everything else the boot does still answers.

`runsync.sh <installDist dir> <label>` boots `vespa-sync` from that
distribution against the tarpit for 75 s with one static-url stream and a
throwaway signer, probes `:7778` and `:7779` every 15 s, then prints the boot
lines. Run it against two `./gradlew :sync:installDist` trees, one from
before the fix and one from after, and compare.

## What it showed (local Vespa, 1.39M events, 2026-09-04)

Before (`c2e644e`, the deployment that wedged):

```
old t=15s  :7778 -> refused   :7779 -> refused
old t=30s  :7778 -> refused   :7779 -> refused
old t=45s  :7778 -> refused   :7779 -> refused
old t=60s  :7778 -> refused   :7779 -> refused
boot lines: only "sync identity: …" — no status page, no stream count
tarpit: holding POST /search/ … kind in (30166) and pubkey in ("79a55e…")
```

After (this branch):

```
new t=15s  :7778 -> 200   :7779 -> 200
…
vespa-sync status page on http://localhost:7778/ (refreshed every 30s)
vespa-sync monitor page on http://localhost:7779/
router: 1 down stream(s) on the pool (1 declared relay(s)) + 0 up relay(s)
router: could not retire stale-epoch verdicts: Job was cancelled   <- at shutdown, the held walk cancelled with the scope
```

The same query is held either way; the difference is what the process does
while it is: both pages answer inside 15 s, the streams start, and the
retraction reports its own outcome when the process stops instead of being
the reason it never started.
