// stats.html's entry: one page for the relay, the mirror and the monitor, each panel guarded on
// the section it reads, so whichever service serves it draws only what that service publishes.

import { ago, el, isoOf } from "../shared/page.js";
import { mountStatsPage } from "../shared/statspage.js";
import { Relay } from "../shared/relay.js";
import { LiveCounts, socketUrlOf } from "../relay/live.js";
import { liveCards } from "../relay/cards.js";
// Imported unconditionally: one page serves all three services (see PANELS).
import { activityPanel, kindActivityCard, kindsCard, relaysCard, settleGrain, tiles, trustCard, zapsCard } from "../relay/charts.js";
import { syncCard } from "../sync/cards.js";
import { monitorCard, verdictsCard } from "../monitor/cards.js";

/** The document schema this page reads; see SCHEMA_VERSION in StatsRollup. */
const SCHEMA = 3;

/**
 * The relay's document, as against the mirror's (which names no relay) and the monitor's (which
 * carries `monitor`). Only the relay's page counts the store over a socket.
 */
const isRelayDoc = (doc) => !!doc.relay && !doc.monitor;

/** Started on the first relay document and kept for the life of the tab. */
let live = null;

function livePanels() {
  if (!live) {
    // Same-origin, beside this page: `doc.relay` is the public url, which a local or tunnelled
    // reader may not be able to reach.
    const socketUrl = socketUrlOf(location.href);
    const relay = new Relay(socketUrl, { lensless: true });
    let cards = null;
    live = new LiveCounts(relay, { onChange: () => cards && cards.update() });
    cards = liveCards(live, socketUrl);
    live.cards = cards.cards;
    // Closed on pagehide, which keeps the page eligible for the back-forward cache.
    window.addEventListener("pagehide", () => { try { relay.ws && relay.ws.close(); } catch (e) {} });
    live.start();
  }
  return live.cards;
}

/** When the charts pass that computed everything under the divider ran, in epoch seconds; null when unstated. */
function rolledUpAt(doc) {
  const iso = doc.tiers?.charts?.generatedAt || doc.corpus?.generatedAt;
  const t = iso ? Date.parse(iso) : NaN;
  return Number.isNaN(t) ? null : Math.floor(t / 1000);
}

/**
 * The line between the live panels and the rollup: everything under it is as old as it says.
 * Its age is re-read on a timer, since the panel is only rebuilt when the rollup is.
 */
function asOfDivider(doc) {
  const at = rolledUpAt(doc);
  const line = el("div", "asof");
  const label = el("span");
  const tick = () => { label.textContent = at == null ? "As of the last rollup" : `As of ${ago(at)}`; };
  tick();
  if (at != null) label.title = `Everything below was rolled up at ${isoOf(at)}; the numbers above are live.`;
  line.appendChild(label);
  return { line, tick };
}
let divider = null;
setInterval(() => divider && divider.tick(), 15000);

/**
 * The body, panel by panel. `reads` is the redraw key: a panel is rebuilt only
 * when a section it reads was recomputed, so a poll does not destroy what the
 * reader typed. A panel whose section is absent draws nothing, not an empty card.
 */
const has = (doc, ...members) => members.some((m) => doc[m]);

const PANELS = [
  // `reads: []`: built once, because it holds its own socket and redraws itself.
  { name: "live", reads: [], build: (doc) => (isRelayDoc(doc) ? livePanels() : []) },
  {
    name: "asOf",
    reads: ["corpus", "kinds"],
    build: (doc) => {
      divider = isRelayDoc(doc) && has(doc, "corpus", "kinds") ? asOfDivider(doc) : null;
      return divider ? [divider.line] : [];
    },
  },
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
