# Peers, config and status-report test decisions

What the router-config, relay-discovery and status-report tests used to carry
in their comments and no other decisions file records. The negative-bound,
`since = 0`, `limit = 0` and ungated-stream rules are in `router-config.md`;
the owed-asks join, the two-axis sort and the `sweeping` count in
`sync-status.md`; the abort rate that motivated the per-relay table in
`sync-support.md`.

**A verdict-source filter is not narrowed to `#l: ["prime"]` for the operator.**
A `relaySource` asking for every kind-30166 verdict was twice quietly given
that tag, first by a read that hardcoded the value and then by the loader
making the read explicit, so a filter written wide asked for one label.
`RouterConfigTest` pins that a bare `{ "kinds": [30166] }` keeps no tag
predicate: narrowing what the operator wrote is the same mistake whichever
layer does it, and `dead` is a value a stream may legitimately select.

**`legs` is compared against the band-bearing relays, not the group's relay
count.** A relay sweeping its first leg has a cursor and no band, so it raises
the relay count without contributing a leg; measured against that count, two
bands merged onto one relay read as one leg per relay and the merge the
`legs` member exists to disclose disappeared. `SyncCoverageReportTest` pins
the case with one first-sweep relay beside a two-band one.
