// stats.html's entry: one page for the relay, the mirror and the monitor, each panel guarded on
// the section it reads, so whichever service serves it draws only what that service publishes.

import { mountStatsPage } from "../shared/statspage.js";
// Imported unconditionally: one page serves all three services (see PANELS).
import { activityPanel, kindActivityCard, kindsCard, relaysCard, settleGrain, tiles, trustCard, zapsCard } from "../relay/cards.js";
import { syncCard } from "../sync/cards.js";
import { monitorCard, verdictsCard } from "../monitor/cards.js";

/** The document schema this page reads; see SCHEMA_VERSION in StatsRollup. */
const SCHEMA = 2;

/**
 * The body, panel by panel. `reads` is the redraw key: a panel is rebuilt only
 * when a section it reads was recomputed, so a poll does not destroy what the
 * reader typed. A panel whose section is absent draws nothing, not an empty card.
 */
const has = (doc, ...members) => members.some((m) => doc[m]);

const PANELS = [
  { name: "totals", reads: ["corpus", "authors", "kinds"], build: (doc) => (has(doc, "corpus") ? tiles(doc) : []) },
  { name: "trust", reads: ["trust"], build: (doc) => (has(doc, "trust") ? [trustCard(doc.trust)] : []) },
  { name: "activity", reads: ["activity"], build: (doc) => (has(doc, "activity") ? [activityPanel(doc.activity)] : []) },
  { name: "kindActivity", reads: ["kindActivity"], build: (doc) => (has(doc, "kindActivity") ? [kindActivityCard(doc.kindActivity)] : []) },
  { name: "zaps", reads: ["zaps"], build: (doc) => (has(doc, "zaps") ? [zapsCard(doc.zaps)] : []) },
  { name: "relays", reads: ["relayDistribution"], build: (doc) => (has(doc, "relayDistribution") ? [relaysCard(doc.relayDistribution)] : []) },
  { name: "kinds", reads: ["kinds"], build: (doc) => (has(doc, "kinds") ? [kindsCard(doc.kinds)] : []) },
  // The mirror's card is guarded on the coverage itself, not the section: the
  // relay's document carries a `sync` section too, holding only the manifest.
  // `relays` counts as coverage: an entirely refused roster has no bands.
  { name: "sync", reads: ["sync"], build: (doc) => (doc.sync?.data?.progress || doc.sync?.data?.streams || doc.sync?.data?.relays ? [syncCard(doc.sync)] : []) },
  { name: "monitor", reads: ["monitor"], build: (doc) => (has(doc, "monitor") ? [monitorCard(doc.monitor)].filter(Boolean) : []) },
  // `reads: []`: built once, because it holds what the reader asked for over a
  // websocket. `doc.relay` is the served relay's ws url; a page on the monitor's
  // port must not derive its socket from `location`.
  { name: "verdicts", reads: [], build: (doc) => (has(doc, "monitor") ? [verdictsCard(doc.relay)] : []) },
];

mountStatsPage({
  panels: PANELS,
  // Every schema this page can read, one per publishing service.
  schema: { relay: SCHEMA, sync: 1, monitor: 1 },
  // The cadences named in the footer, in reading order; a tier not listed here
  // still gets a line.
  tiers: [["counters", "Counters"], ["charts", "Charts"], ["status", "Status"]],
  pendingNote:
    "This is computed behind the server. On the relay the first pass over a large corpus takes a while (check the " +
    "log for `stats:` lines, and that STATS_INTERVAL_SECONDS is not 0); on the mirror and the monitor it is a fold " +
    "over state already in memory and lands on the first tick (check the sync log for `router:` lines).",
  // A document may have stopped carrying the grain on screen.
  beforePanels: (doc) => settleGrain(doc.activity),
});
