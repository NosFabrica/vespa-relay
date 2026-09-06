// The mirror's cards: what each stream is doing, what the background work is doing, and
// how far the walk has got. Every judgement is made in `shared/sync.js`; this only renders.

import { cardHead, dayOf, el, fmt, fmtDur, short } from "../shared/page.js";
import { backgroundPanel, chip, setStages, setTerms, term } from "../shared/processors.js";
import { STUCK_CALL_SEC, STUCK_LEG_SEC, constraintOf, heldRows, poolsOf, relayStatusOf, socketsOf, storeOf, streamSections } from "../shared/sync.js";

/** A cursor within this of now is drawn to the minute, older ones to the day. */
const FINE_CURSOR_SEC = 2 * 24 * 60 * 60;
const cursorOf = (t, nowSec) =>
  nowSec != null && nowSec - t < FINE_CURSOR_SEC
    ? new Date(t * 1000).toISOString().slice(0, 16).replace("T", " ")
    : dayOf(t);

/** A schedule's period, in days once past two; `fmtDur` would render a week as `168h 0m`. */
const DAY_SEC = 24 * 60 * 60;
const fmtPeriod = (sec) => (sec >= 2 * DAY_SEC ? `${Math.round(sec / DAY_SEC)}d` : fmtDur(sec));

/** The first line of the card: throughput, and the constraint verdict as the one chip. */
function statusRow(progress) {
  const row = el("div", "sy-status");
  row.appendChild(el("i", "sy-dot"));
  row.append("running");
  const health = progress.health || {};
  // Spelled with its own spaces: consecutive text nodes collapse into one flex item.
  // Both ends of the ingest queue, since the drain alone cannot tell a stalled store from a quiet fan-out.
  if (health.arrivingPerSec != null) row.append(` · ${fmt(health.arrivingPerSec)} events/s into ingest`);
  if (health.eventsPerSec != null) row.append(` · ${fmt(health.eventsPerSec)} events/s out to the store`);
  // Only `ingest` and `wedged` name a fault.
  const constraint = constraintOf(health);
  if (constraint) row.appendChild(chip(constraint.text, constraint.tone, constraint.why));
  // Coloured on the queue alone; near the ceiling is the healthy state.
  const sockets = socketsOf(health);
  if (sockets) {
    row.appendChild(
      chip(
        `${fmt(sockets.open)}/${fmt(sockets.ceiling)} sockets${sockets.queued ? ` · ${fmt(sockets.queued)} queued` : ""}`,
        sockets.starved ? "warn" : null,
        sockets.starved
          ? `${fmt(sockets.queued)} call(s) waiting for a slot. ${term("socketsQueued")}`
          : `${term("sockets")} ${term("socketCeiling")}`,
      ),
    );
  }
  if (progress.fatals) row.appendChild(chip(`${fmt(progress.fatals)} fatal error(s)`, "warn", term("fatals")));
  // The feed's line is machine output, so it is the tooltip. `EXC` is another repo's
  // wording; reworded, this degrades to a neutral chip rather than a wrong verdict.
  if (health.feed) {
    const broken = health.feed.includes("EXC");
    row.appendChild(chip(broken ? "feed errors" : "feed", broken ? "warn" : null, `${health.feed} — ${term("feed")}`));
  }
  return row;
}

/**
 * The mirror-wide cut, two lines, only above more than one stream. Off the same [poolsOf]
 * read the sections use, so the summary cannot disagree with the rows under it.
 */
function mirrorPanel(progress, sections, held) {
  if (sections.length < 2) return null;
  const pools = poolsOf(progress, held);
  if (!pools) return null;
  const box = el("div");
  box.appendChild(poolLine(pools.totals));
  box.appendChild(poolTally(pools.groups, pools.totals));
  return box;
}

/**
 * How big the pool is, in one line. The first marks partition the roster; the tail count
 * crosses them, so it is drawn last and says so in its title.
 */
function poolLine(totals, whose = "the roster") {
  const line = el("div", "sy-sub");
  const mark = (text, why) => {
    const span = el("span", null, line.children.length ? ` · ${text}` : text);
    if (why) span.title = why;
    line.appendChild(span);
  };
  // Absent is not zero.
  if (totals.relays != null) mark(`${fmt(totals.relays)} relay(s)`, term("roster"));
  // Units only where they differ from relays.
  if (totals.units != null && totals.units !== totals.relays) {
    mark(`${fmt(totals.units)} stream-visit(s)`, term("rosterVisits"));
  }
  mark(`${fmt(totals.working)} with a worker now`, term("visiting"));
  if (totals.queued != null) mark(`${fmt(totals.queued)} queued for one`, term("awaitingVisit"));
  if (totals.waiting != null) {
    mark(`${fmt(totals.waiting)} between visits`,
      `The rest of ${whose}: neither running nor queued, waiting out the ` +
      "revisit delay its last visit earned. Most of a healthy rotation is here — a relay is revisited on what it " +
      "has been yielding lately, not on a shared clock.");
  }
  mark(`${fmt(totals.tailed)} holding a live tail`,
    "Not a fourth share of the three before it: a tailed relay keeps its tail while it is revisited, so the same " +
    "relay is counted here and in `with a worker now` at once. " + term("liveHeld"));
  return line;
}

/** The scrolling headed table every list on this card shares; the caller appends the rows to `table`. */
function headedTable(columns, fit = false, groups = null) {
  const scroll = el("div", fit ? "sy-legs-box sy-fit" : "sy-legs-box");
  const table = el("table", "sy-legs");
  // `halves` is derived from the band's spans so the band and the rule down the body agree.
  const halves = new Set();
  if (groups) {
    const band = el("tr", "sy-group");
    let at = 0;
    for (const [label, span] of groups) {
      const th = el("th", null, label || "");
      th.colSpan = span;
      band.appendChild(th);
      if (at) halves.add(at);
      at += span;
    }
    table.appendChild(band);
  }
  const head = el("tr");
  columns.forEach(([label, key, right], i) => {
    const th = el("th", `${right ? "n" : ""}${halves.has(i) ? " half" : ""}`.trim() || null, label);
  // A column with no glossary member behind it gets no tooltip.
    if (key) th.title = term(key);
    head.appendChild(th);
  });
  table.appendChild(head);
  scroll.appendChild(table);
  return { scroll, table, halves };
}

/**
 * One row per job: what the stream may spend on it and when it comes due. Two faults
 * share a row, so the cell carries the mark: `turned away` for a cap, `due` for a schedule.
 */
function jobsTable(rows) {
  if (!rows.length) return null;
  const { scroll, table, halves } = headedTable(
    [["job", "job", false],
     ["may run", "streamCap", true], ["in use", "inUse", true], ["turned away", "deferred", true],
     ["every", "everySec", true], ["due", "due", true], ["never run", "neverRun", true],
     ["waiting", "waiting", true], ["next in", "nextInSec", true]],
    true,
    // Without the band, `due` beside `turned away` reads as cause and effect.
    [[null, 1], ["what it may spend", 3], ["when the past is re-read", 5]],
  );
  for (const r of rows) {
    const l = r.limit;
    const sc = r.schedule;
    const tr = el("tr");
    const cell = (cls, text) => {
      const td = el("td", `${cls || ""}${halves.has(tr.children.length) ? " half" : ""}`.trim() || null, text);
      tr.appendChild(td);
    };
    cell(l?.biting || sc?.backedUp ? "hot" : null, r.label);
    // `uncapped` is a word, not a blank; no limit row at all draws `—`.
    cell("n", !l ? "—" : l.streamCap != null ? fmt(l.streamCap) : "uncapped");
    cell("n", l && l.inUse != null ? fmt(l.inUse) : "—");
    cell(`n${l?.biting ? " hot" : ""}`, l ? fmt(l.deferred) : "—");
    cell("n", sc && sc.everySec != null ? fmtPeriod(sc.everySec) : "—");
    cell(`n${sc?.backedUp ? " hot" : ""}`, sc ? fmt(sc.due) : "—");
    cell("n", sc ? fmt(sc.neverRun) : "—");
    cell("n", sc ? fmt(sc.waiting) : "—");
    // Nothing waiting is no countdown: every ask is already due.
    cell("n", sc && sc.nextInSec != null ? fmtPeriod(sc.nextInSec) : "—");
    table.appendChild(tr);
  }
  return scroll;
}

/** How many units are in each pool across every stream, off the same `groups` the sections draw from. */
function poolTally(groups, totals) {
  const line = el("div", "sy-sub");
  for (const g of groups) {
    const span = el("span", null, `${line.children.length ? " · " : ""}${g.label} ${fmt(g.rows.length)}`);
    span.title = g.what;
    line.appendChild(span);
  }
  // The denominator on the end, or the line reads as a partition.
  if (totals.units != null) {
    const of = el("span", null, ` · of ${fmt(totals.units)} stream-visit(s)`);
    of.title = term("rosterVisits");
    line.appendChild(of);
  }
  return line;
}

/**
 * One stream, whole: name and phase, its share of the roster, one row per job, then the
 * pools it is holding. A stream holding nothing gets one line instead of four empty tables.
 */
function streamSection(section) {
  const box = el("div", "sy-stream");
  const top = el("div", "sy-top");
  // A null stream is the rows no configured stream claimed.
  top.appendChild(el("span", "sy-name", section.stream || "not attributed to a stream"));
  if (section.phase) {
    const meta = el("span", "sy-meta",
      `${section.phase}${section.phaseForSec != null ? ` for ${fmtDur(section.phaseForSec)}` : ""}`);
    // `rotating` reads as a stall and is not; the glossary entry hangs here.
    const phaseWhy = term(section.phase);
    if (phaseWhy) meta.title = phaseWhy;
    top.appendChild(meta);
  }
  box.appendChild(top);

  // A rotating stream with an empty roster is waiting on the fitness pass.
  const waiting = !!(section.rotation && section.rotation.waiting);
  if (waiting) {
    const line = el("div", "sy-sub");
    const none = el("s", null, "no certified relay yet — waiting on the fitness pass to sign the first `prime`");
    none.title = term("roster");
    line.appendChild(none);
    box.appendChild(line);
  } else {
    box.appendChild(poolLine(section.totals, "this stream's roster"));
  }

  // No sub-head over the jobs; the table's band already names its halves.
  const jobs = jobsTable(section.jobs);
  if (jobs) {
    const part = el("div", "sy-part");
    part.appendChild(jobs);
    box.appendChild(part);
  }

  if (!section.holding) {
    // Not after the waiting line, which has already said why it holds nothing.
    if (!waiting) box.appendChild(el("div", "sy-sub sy-quiet", "holding nothing right now — no visit, no tail"));
    return box;
  }
  // `sy-sub-h` is a level below the card's `sy-h` bands.
  const pools = el("div", "sy-part");
  pools.appendChild(el("p", "sy-sub-h", "what it is holding"));
  for (const group of section.groups) pools.appendChild(poolBlock(group));
  box.appendChild(pools);
  return box;
}

/**
 * Which store calls are out, and whose. Drawn only where the document carries the
 * section: "this router does not say" is not "nothing is outstanding".
 */
function storePanel(progress) {
  const s = storeOf(progress && progress.store);
  if (!s) return null;
  const box = el("div", "sy-stream");
  const top = el("div", "sy-top");
  top.appendChild(el("span", "sy-name", "store calls"));
  const meta = el("span", "sy-meta", `${fmt(s.outstanding)} outstanding`);
  meta.title = term("outstanding");
  top.appendChild(meta);
  box.appendChild(top);

  // `failed` is here because a store the schema drifted under fails calls in milliseconds.
  const line = el("div", "sy-sub");
  const fact = (text, key, loud) => {
    const part = el(loud ? "s" : "span", null, `${line.children.length ? " · " : ""}${text}`);
    const why = term(key);
    if (why) part.title = why;
    line.appendChild(part);
  };
  fact(`${short(s.issued)} call(s) since boot`, "issued");
  fact(`${short(s.returned)} answered`, "returned");
  if (s.failed) fact(`${fmt(s.failed)} FAILED`, "failed", true);
  if (s.cancelled) fact(`${fmt(s.cancelled)} cancelled at shutdown`, "cancelled");
  // Only when the router moved `SYNC_STORE_SLOW_SEC`; a card must not mark by a rule it never states.
  if (s.stuckSec !== STUCK_CALL_SEC) fact(`marked past ${fmtDur(s.stuckSec)}`, "slowAfterSec");
  box.appendChild(line);

  if (!s.outstanding) {
    // Empty is an answer; a part that vanished would look like a build without it.
    box.appendChild(el("div", "sy-sub sy-quiet", "nothing outstanding right now — no call is waiting on the store"));
    return box;
  }

  const part = el("div", "sy-part");
  part.appendChild(el("p", "sy-sub-h", "what is out right now"));
  part.appendChild(callsTable(s.rows));
  if (s.more) {
    part.appendChild(el("p", "sy-sub",
      `${fmt(s.more)} more outstanding call(s) not named here — the whole list is in \`/stats.json\``));
  }
  if (s.callers.length) {
    part.appendChild(el("p", "sy-sub-h", "whose calls they are"));
    part.appendChild(callersTable(s.callers));
  }
  if (s.ages.length) {
    part.appendChild(el("p", "sy-sub-h", "how old they are"));
    part.appendChild(agesLine(s.ages));
    // The router's own partition failing to close is reported, not smoothed.
    if (!s.accountedFor) {
      const bad = el("div", "sy-tr-note warn", "these bands do not sum to `outstanding` — see `ages` in the JSON");
      bad.title = term("ages");
      part.appendChild(bad);
    }
  }
  box.appendChild(part);
  return box;
}

/** The outstanding calls. `others out` is how many this process already had out when it went. */
function callsTable(rows) {
  const { scroll, table } = headedTable([
    ["caller", "caller", false],
    ["op", "op", false],
    ["asked for", "asked", false],
    ["running", "elapsedSec", true],
    ["others out", "outstandingAtIssue", true],
  ]);
  for (const r of rows) {
    // `hot` is the log's own threshold.
    const tr = el("tr", r.hot ? "hot" : null);
    tr.appendChild(el("td", null, r.caller));
    tr.appendChild(el("td", null, r.op));
    // Several ops genuinely carry no filter.
    tr.appendChild(el("td", null, r.asked || "no filter"));
    tr.appendChild(el("td", "n", fmtDur(r.elapsedSec)));
    tr.appendChild(el("td", "n", r.outstandingAtIssue != null ? fmt(r.outstandingAtIssue) : "—"));
    table.appendChild(tr);
  }
  return scroll;
}

/** One row per subsystem. `issued = answered + failed + cancelled + out`, so every zero is drawn. */
function callersTable(rows) {
  const { scroll, table } = headedTable([
    ["caller", "caller", false],
    ["out", "outstanding", true],
    ["oldest", "oldestOutstandingSec", true],
    ["issued", "issued", true],
    ["answered", "returned", true],
    ["failed", "failed", true],
    ["cancelled", "cancelled", true],
  ]);
  for (const r of rows) {
    const tr = el("tr", r.hot ? "hot" : null);
    tr.appendChild(el("td", null, r.caller));
    tr.appendChild(el("td", "n", fmt(r.outstanding)));
    // Nothing out is no age; `0s` would read as a call just started.
    tr.appendChild(el("td", "n", r.oldestOutstandingSec != null ? fmtDur(r.oldestOutstandingSec) : "—"));
    tr.appendChild(el("td", "n", short(r.issued)));
    tr.appendChild(el("td", "n", short(r.returned)));
    tr.appendChild(el("td", `n${r.failed ? " hot" : ""}`, fmt(r.failed)));
    tr.appendChild(el("td", "n", fmt(r.cancelled)));
    table.appendChild(tr);
  }
  return scroll;
}

/** The outstanding set by age band, as one line. */
function agesLine(ages) {
  const line = el("div", "sy-sub");
  for (const a of ages) {
    const label = a.fromSec === 0 ? "under 1s" : `${fmtDur(a.fromSec)}+`;
    const span = el(a.hot ? "s" : "span", null,
      `${line.children.length ? " · " : ""}${label} ${fmt(a.calls)}`);
    span.title = term("fromSec");
    line.appendChild(span);
  }
  return line;
}

/** One pool: its heading, how many relays are in it, and which. */
function poolBlock(group) {
  const box = el("div", "sy-pool");
  const head = el("div", "sy-pool-head");
  const name = el("span", "sy-pool-name", group.label);
  name.title = group.what;
  head.appendChild(name);
  // The count alone; the denominator is the section's.
  head.appendChild(el("span", "sy-pool-n", fmt(group.rows.length)));
  // The shared stage word, in place of a column repeating it on every row.
  if (group.doing) {
    const doing = el("span", "sy-pool-doing", group.doing);
    doing.title = term("doing");
    head.appendChild(doing);
  }
  box.appendChild(head);
  // Empty is an answer.
  if (!group.rows.length) {
    box.appendChild(el("div", "sy-sub sy-quiet", "none right now"));
    return box;
  }
  box.appendChild(poolTable(group));
  return box;
}

/** The rows of one pool. `quiet` is the column that decides, and the row is coloured off it alone. */
function poolTable(group) {
  const cursors = group.rows.some((r) => r.pagingUntil != null);
  // One clock for the table, so two rows cannot draw the same cursor two ways.
  const nowSec = Date.now() / 1000;
  const columns = [["relay", null, false]];
  if (group.streams) columns.push(["stream", null, false]);
  // Only where the group's own rows disagree; otherwise the word is in the heading.
  if (!group.doing) columns.push(["doing", "doing", false]);
  if (cursors) columns.push(["back to", "pagingUntil", false]);
  columns.push(["held", "heldForSec", true], ["events", "events", true], ["quiet", "quietForSec", true]);
  const { scroll, table } = headedTable(columns);
  for (const r of group.rows) {
    const tr = el("tr", r.quietForSec >= STUCK_LEG_SEC ? "hot" : null);
    const url = el("td", "u");
    // An LTR isolate inside the rtl cell, which ellipsises on the left, keeps the address intact.
    const inner = el("span", null, r.short);
    inner.dir = "ltr";
    url.appendChild(inner);
    url.title = r.relay;
    tr.appendChild(url);
    if (group.streams) tr.appendChild(el("td", null, r.stream || "—"));
    // A leg with no slot says so.
    if (!group.doing) tr.appendChild(el("td", null, r.doing || (r.slotless ? "not on a transfer slot" : "—")));
    if (cursors) {
      // A skewed reader clock draws a coarser or finer cursor, never a wrong one.
      const at = el("td", "sy-at", r.pagingUntil != null ? cursorOf(r.pagingUntil, nowSec) : "—");
      tr.appendChild(at);
    }
    tr.appendChild(el("td", "n", fmtDur(r.heldForSec)));
    tr.appendChild(el("td", "n", fmt(r.events)));
    tr.appendChild(el("td", "n", fmtDur(r.quietForSec)));
    table.appendChild(tr);
  }
  return scroll;
}

/**
 * One row per (relay, stream) pair, worst first in the document's order. The counts above
 * the table are whole even when the rows are cut.
 */
function relayPanel(d) {
  const r = relayStatusOf(d.relays);
  if (!r) return null;
  const box = el("div");

  // Both lines off the document's own partitions, never re-counted from `rows`, which is cut.
  const head = el("div", "sy-pool-head");
  head.appendChild(el("span", "sy-pool-name",
    `${fmt(r.current)} of ${fmt(r.pairs)} pair(s) current`));
  for (const c of r.freshness) head.appendChild(chip(`${fmt(c.pairs)} ${c.label}`, c.pairs ? c.tone : null, term("behind")));
  box.appendChild(head);

  const past = el("div", "sy-pool-head");
  past.appendChild(el("span", "sy-pool-name", "the past behind it"));
  for (const c of r.chips) past.appendChild(chip(`${fmt(c.pairs)} ${c.label}`, c.pairs ? c.tone : null, term("syncStatus")));
  box.appendChild(past);

  // Said only when it is not zero: a permanent "0 unwatched" would read as a gauge rather than
  // the config question it is. This one is not about a relay — it is the two blocks disagreeing.
  if (r.unwatched) {
    const drift = el("div", "sy-pool-head");
    drift.appendChild(el("span", "sy-pool-name", "what the monitor is not watching"));
    drift.appendChild(chip(`${fmt(r.unwatched)} of ${fmt(r.pairs)} pair(s) unwatched`, "warn", term("unwatched")));
    box.appendChild(drift);
    box.appendChild(el("p", "sy-sub",
      "the mirror syncs these and no current verdict of ours grades them — monitor.conf names a " +
      "different set of relays from the streams in sync.conf"));
  }

  const nowSec = Date.now() / 1000;
  const { scroll, table } = headedTable(
    [["relay", null, false], ["stream", null, false],
     ["newest", "behindSec", false],
     ["status", "syncStatus", false], ["back to", "coveredFrom", false], ["verified", "verifiedAgoSec", true],
     // One column: the sentence is the only cell that wraps.
     ["terms", "negentropy", false]],
    false,
    [["", 2], ["how current", 1], ["how far back", 3], ["on what terms", 1]],
  );
  for (const row of r.rows) {
    const tr = el("tr", row.hot ? "hot" : null);
    const url = el("td", "u");
    // The url in its own LTR isolate inside the rtl cell, as in [poolTable].
    const inner = el("span", null, row.short);
    inner.dir = "ltr";
    url.appendChild(inner);
    url.title = row.relay;
    tr.appendChild(url);
    tr.appendChild(el("td", null, row.stream || "—"));

    // The tail belongs here, not beside the status: old content on a tailed pair is a quiet
    // relay, not a mirror falling behind.
    const fresh = el("td");
    fresh.appendChild(el("span", null, row.behindSec != null ? `${fmtPeriod(row.behindSec)} old` : "—"));
    if (row.tailed) fresh.appendChild(chip("live", "live", term("tailed")));
    fresh.title = term("behind");
    tr.appendChild(fresh);

    // The backfill's own axis.
    const st = el("td");
    st.appendChild(el("span", null, row.label));
    if (row.progress) {
      const done = el("span", "sy-quiet", ` ${row.progress}`);
      done.title = term("settled");
      st.appendChild(done);
    }
    if (row.visiting) st.appendChild(chip("visiting", "busy", term("visiting")));
    st.title = term("syncStatus");
    tr.appendChild(st);
    tr.appendChild(el("td", "sy-at", row.coveredFrom != null ? cursorOf(row.coveredFrom, nowSec) : "—"));
    // `—` where no reconcile has run yet.
    tr.appendChild(el("td", "n", row.verifiedAgoSec != null ? `${fmtPeriod(row.verifiedAgoSec)} ago` : "—"));

    // A `paging` row beside `no neg` will not settle by itself.
    const terms = el("td", "sy-said");
    if (row.negentropy === true) terms.appendChild(chip("neg", "live", term("negentropy")));
    if (row.negentropy === false) terms.appendChild(chip("no neg", "warn", term("negentropy")));
    if (row.kindCap != null) terms.appendChild(chip(`≤${row.kindCap} kinds`, "busy", term("kindCap")));
    // Not a term the relay serves us on but the reason two of them are absent: nothing measured it.
    if (row.unwatched) terms.appendChild(chip("unwatched", "warn", term("unwatched")));
    // The relay's own sentence last, after the measured terms.
    if (row.why) {
      const said = el("span", null, row.why);
      if (row.refusedAgoSec != null) said.title = `last refused ${fmtPeriod(row.refusedAgoSec)} ago`;
      terms.appendChild(said);
    }
    if (!terms.children.length) terms.appendChild(el("span", "sy-quiet", "—"));
    tr.appendChild(terms);
    table.appendChild(tr);
  }
  box.appendChild(scroll);
  if (r.omitted) {
    box.appendChild(el("p", "sy-sub",
      `${fmt(r.omitted)} more pair(s) not listed — the counts above are complete, and every row naming a fault or an unwatched relay is above the cut`));
  }
  return box;
}

/** Every walked band at once, as how many relays reach each point in the frame. */
const DEPTH_BUCKETS = 72;

function coveragePanel(d) {
  const streams = d.streams || [];
  if (!streams.length || d.from == null || d.to == null) return null;
  const box = el("div");
  const settled = streams.reduce((a, s) => a + (s.reconciled || 0), 0);
  const open = streams.reduce((a, s) => a + (s.paged || 0), 0);
  const facts = [];
  if (d.relays != null) facts.push(`${fmt(d.relays)} relay(s) on ${fmt(d.hosts || 0)} host(s) have coverage recorded`);
  // `reconciled` and `paged` are per (stream, relay), so "walks" keeps them apart from relays.
  if (settled || open) {
    facts.push(`${fmt(settled + open)} walk(s) — ${fmt(settled)} settled, ${fmt(open)} not proven exhaustive`);
  }
  box.appendChild(el("div", "sy-sub", facts.join(" · ")));

  const span = Math.max(1, d.to - d.from);
  const depth = new Array(DEPTH_BUCKETS).fill(0);
  let rows = 0;
  for (const s of streams) {
    for (const r of s.rows || []) {
      if (r.min == null || r.max == null) continue;
      rows++;
      const a = Math.max(0, Math.min(DEPTH_BUCKETS - 1, Math.floor(((r.min - d.from) / span) * DEPTH_BUCKETS)));
      const b = Math.max(0, Math.min(DEPTH_BUCKETS - 1, Math.floor(((r.max - d.from) / span) * DEPTH_BUCKETS)));
      for (let i = a; i <= b; i++) depth[i]++;
    }
  }
  if (!rows) return box;
  const peak = Math.max(...depth, 1);
  const strip = el("div", "sy-depth");
  depth.forEach((n, i) => {
    const bar = el("i");
    // A bucket nothing reaches keeps the stylesheet's 1px floor, so a gap reads as a gap.
    bar.style.height = Math.round((n / peak) * 100) + "%";
    const at = d.from + (i / DEPTH_BUCKETS) * span;
    bar.title = `${dayOf(at)} — ${fmt(n)} of ${fmt(rows)} walked band(s) reach here`;
    strip.appendChild(bar);
  });
  box.appendChild(strip);
  const axis = el("div", "sy-axis");
  axis.appendChild(el("span", null, dayOf(d.from)));
  axis.appendChild(el("span", null, `peak ${fmt(peak)} of ${fmt(rows)} band(s)`));
  axis.appendChild(el("span", null, "now"));
  box.appendChild(axis);
  return box;
}

/**
 * The sync card: is the sync working, and how far has it got. Every quantity is a mark
 * whose meaning is on its `title`; a stream appears in exactly one place, its section.
 */
function syncCard(section) {
  const d = (section && section.data) || {};
  const streams = d.streams || [];
  const progress = d.progress;
  const card = el("div", "card");
  // `relays` counts as state: a router whose whole roster is refused has no walked streams.
  cardHead(card, "Sync coverage", streams.length || progress || d.relays ? null : "No sync state in this document.", section);
  setTerms(d.terms);
  // The shift happens once per document, here.
  setStages(progress?.health?.stages);
  if (progress) card.appendChild(statusRow(progress));

  // Held rows are collected once and grouped twice, so the summary and the sections agree.
  const held = heldRows(progress);
  const sections = streamSections(progress, held);
  const mirror = mirrorPanel(progress, sections, held);
  if (mirror) {
    card.appendChild(el("p", "sy-h", "the whole mirror"));
    card.appendChild(mirror);
  }
  if (sections.length) {
    card.appendChild(el("p", "sy-h", sections.length > 1 ? "the streams" : "the stream"));
    for (const s of sections) card.appendChild(streamSection(s));
  }
  // Disclosed at card level because the mirror's summary is withheld below two streams.
  if (held.omitted) {
    card.appendChild(el("p", "sy-sub",
      `${fmt(held.omitted)} held relay(s) the router did not name — nothing says which stream or pool they are in`));
  }

  // The half of the background work that moves events; the rest is `monitorCard`'s.
  const background = backgroundPanel((progress && progress.processors) || []);
  if (background) {
    card.appendChild(el("p", "sy-h", "the pipeline"));
    card.appendChild(background);
  }

  // Under the pipeline because it is what the pipeline waits on.
  const store = storePanel(progress);
  if (store) {
    card.appendChild(el("p", "sy-h", "the store"));
    card.appendChild(store);
  }

  // Before the coverage strip, which is a chart of the survivors.
  const relays = relayPanel(d);
  if (relays) {
    card.appendChild(el("p", "sy-h", "prime relays"));
    card.appendChild(relays);
  }

  const coverage = coveragePanel(d);
  if (coverage) {
    card.appendChild(el("p", "sy-h", "coverage so far"));
    card.appendChild(coverage);
    card.appendChild(el("p", "card-sub",
      `Frame starts ${dayOf(d.from)} — the oldest span anything here reaches. Not a target: these filters carry ` +
      `no lower bound, so depth means "as deep as anything reaches", not "finished".`));
  }
  return card;
}


export { syncCard };
