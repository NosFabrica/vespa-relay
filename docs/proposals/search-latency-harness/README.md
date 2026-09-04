# The scripts behind docs/search-latency.md

Plain node, no dependencies, the way `web/src/test/js` is written. They ran
against a local single-node Vespa holding a read-only capture of staging
(`capture.mjs`, `capture_cards.mjs`, fed with the store's `exportLoad`), and
against `wss://search-staging.brainstorm.world/` for the wire measurements.

- `pageflow.mjs` / `pageflow3.mjs` — the page's REQ sequence for one typed
  word, before and after the change (staging).
- `bench.mjs` — the store's own YQL (from `searchTrace`, `SEARCH_YQL=1`,
  saved as `yql-all.txt`) re-sent with a match-phase cut (`ATTR`, `VARIANTS`).
- `proof.mjs`, `descent.mjs` — the trust-keyed cut and the exact trust
  descent with its bound; `maxrank.mjs` fills the experimental `max_rank` on
  the reputation documents, `hist.mjs` / `share.mjs` / `top.mjs` read the
  corpus back.

They are kept as measurement notebooks, not as tooling: none is run by the
build.
