// The monitor's cards: what this router has decided about each relay url,
// and the passes that decided it. Reads the `monitor` section's processor
// rows and, on request, the kind-30166 records off the relay's own websocket.

import { ago, cardHead, el, fmt, isoOf, short, shownOf } from "../shared/page.js";
import { backgroundPanel, setTerms } from "../shared/processors.js";
import { Relay } from "../shared/relay.js";
import { MONITOR_KIND, PRIME, groupByHost, summarise, walkRecords } from "../shared/verdicts.js";

/**
 * The round-up and the three passes that decide which relays may be
 * dialled, and the corpus they decide over. Drawn only when a router has
 * written processor rows: a serve-only relay runs no monitor, and an empty
 * card there would read as a broken one.
 */
function monitorCard(section) {
  const d = (section && section.data) || {};
  const rows = (d.progress && d.progress.processors) || [];
  if (!rows.length) return null;
  // Set here, not inherited from whichever card drew first: two cards on one
  // section must not depend on the order `PANELS` builds them in.
  setTerms(d.terms);
  const card = el("div", "card");
  cardHead(card, "Relay monitor", null, section);
  // On the heading, not a `card-sub`: that slot is a state, and explanations
  // on this page go on the label.
  const heading = card.querySelector("h2");
  if (heading) {
    heading.title =
      "What the router has decided about the relay urls it discovers — which are one server wearing several " +
      "addresses, which cannot answer the same question twice, which are graded `prime`, and which are " +
      "unreachable. The round-up at the top is where those urls come from at all: it walks the store for " +
      "every url the relay lists name and hands the three passes their candidate set, which is minutes of " +
      "every sweep. Every stream's relay list is what these passes admit; the per-url verdicts they sign are " +
      "in Monitor verdicts below.";
  }
  card.appendChild(backgroundPanel(rows, true));
  return card;
}
/**
 * What the monitor has decided about each url, read as kind-30166 records
 * over this relay's own websocket, so a verdict unreadable here is unreadable
 * by any client. Not drawn until asked: the page polls itself every minute,
 * and paging thousands of records on that cadence would make a debugging aid
 * a load source. `reads: []` in PANELS keeps the poller from rebuilding it.
 */
function verdictsCard(relayUrl) {
  const card = el("div", "card");
  cardHead(card, "Monitor verdicts (NIP-66)", null, null);
  card.appendChild(el("p", "card-sub",
    `Reads kind ${MONITOR_KIND} out of this relay over its own websocket — the same ask any client makes. ` +
    "Grouped by host, because a duplicate is never a property of one url: it is a property of a url next to another one."));

  // Not `.grp-head`: that is a sticky heading for the inside of a scroll box.
  const bar = el("div", "vd-bar");
  const load = el("button", "btn", "Read verdicts from this relay");
  const filter = el("input");
  filter.type = "search";
  // Named so render()'s restore loop cannot carry one search box's text into another.
  filter.name = "verdicts";
  filter.placeholder = "filter by host or url";
  filter.setAttribute("aria-label", "filter verdicts by host or url");
  filter.hidden = true;
  const status = el("span", "status", "");
  bar.append(load, filter, status);
  card.appendChild(bar);

  const sumBox = el("div");
  card.appendChild(sumBox);

  const scroll = el("div", "wrap tall");
  scroll.hidden = true;
  card.appendChild(scroll);
  const none = el("p", "cov-none");
  none.hidden = true;
  card.appendChild(none);

  let groups = [];

  const applyFilter = () => {
    const q = filter.value.trim().toLowerCase();
    let live = 0;
    for (const g of groups) {
      // A host hit keeps the whole group: the comparison between its urls is
      // what the panel exists to give.
      const wholeHost = !q || g.host.includes(q);
      let kept = 0;
      for (const r of g.rows) {
        const hit = wholeHost || r.needle.includes(q);
        r.node.hidden = !hit;
        if (hit) kept++;
      }
      // Heads go with rows; hiding only rows leaves a heading per host with
      // nothing beneath it.
      g.box.hidden = !kept;
      if (kept) live++;
    }
    const empty = !!q && !live;
    none.hidden = !empty;
    if (empty) none.textContent = `No url in this store's verdicts contains “${filter.value.trim()}”.`;
  };
  filter.addEventListener("input", applyFilter);

  // Without a relay to ask, no button: a websocket to nowhere reads as a
  // store holding no verdicts.
  if (!relayUrl) {
    load.remove();
    filter.remove();
    status.textContent = "";
    card.appendChild(el("p", "cov-none",
      "This document names no relay to read verdicts from — the sync process publishes RELAY_URL here, " +
      "and this page is served on the monitor's own port rather than the relay's."));
    return card;
  }

  load.addEventListener("click", async () => {
    load.disabled = true;
    status.textContent = "asking…";
    try {
      const { events, complete, self, others } = await readVerdicts((n) => { status.textContent = `${fmt(n)} record(s)…`; }, relayUrl);
      const nowSec = Math.floor(Date.now() / 1000);
      const byHost = groupByHost(events, nowSec);
      const sum = summarise(byHost, nowSec);
      groups = drawVerdicts(scroll, byHost, nowSec);
      // `replaceChildren`, not `fill`: that helper carries filter text and
      // scroll position across a rebuild, and this box holds neither.
      sumBox.replaceChildren(tally([
        ["urls", sum.urls],
        ["hosts", sum.hosts],
        ["folded away", sum.folded],
        ["measured, kept", sum.cleared],
        // Not `not folded`, which the fold's pill wears on graded rows too.
        ["no verdict yet", sum.silent],
        // The Tor tile is drawn at zero, unlike the three after it: zero there
        // is the answer an operator running Tor came for.
        ...(sum.graded ? [["graded", sum.graded], ["prime", sum.prime], ["prime on Tor", sum.primeTor]] : []),
        ...(sum.expired ? [["verdict expired", sum.expired]] : []),
        ...(sum.unstable ? [["refused as inconsistent", sum.unstable]] : []),
      ]));
      // Partial first, then whose: every number above is over one monitor's
      // records, and reads as a census of the store otherwise.
      const whose = self
        ? (others === null
            ? `this relay's own monitor (${self.slice(0, 8)}…)`
            : others > 0
              ? `this relay's own monitor (${self.slice(0, 8)}…) — ${fmt(others)} more 30166 record(s) here belong to other monitors and are not counted`
              : `this relay's own monitor (${self.slice(0, 8)}…), the only one with records here`)
        : "EVERY monitor in this store — this relay advertises no key of its own, so these are not necessarily its verdicts";
      status.textContent =
        (complete ? "" : `partial read — stopped at ${fmt(events.length)} record(s); the store holds more. `) +
        whose +
        (sum.inferred ? `. ${fmt(sum.inferred)} survivor(s) inferred from the folds that point at them` : "");
      status.className = complete && self ? "status" : "status vd-stale";
      filter.hidden = false;
      scroll.hidden = false;
      applyFilter();
    } catch (e) {
      // The relay's own message: "0 records" and "the socket closed" are
      // different findings.
      status.textContent = "";
      none.hidden = false;
      none.textContent = `Could not read verdicts: ${(e && e.message) || e}`;
    } finally {
      load.disabled = false;
    }
  });
  return card;
}

/** A row of labelled numbers, the panel's own tiles. */
function tally(pairs) {
  const box = el("div", "vd-sum");
  for (const [label, n] of pairs) {
    const cell = el("div");
    cell.appendChild(el("b", null, fmt(n)));
    cell.append(label);
    box.appendChild(cell);
  }
  return box;
}

/**
 * Every kind-30166 record out of this relay, newest first, paged on `until`
 * because one REQ is bounded by the relay's own limit. Scoped to the relay's
 * own monitor key when it advertises one: a mirror holds strangers' 30166s by
 * design, and an unscoped count would include relays this router never
 * probed. Without a key the panel says so rather than guessing an author.
 */
async function readVerdicts(onProgress, relayUrl) {
  // `lensless`: a count of what the store holds must not be narrowed by
  // anybody's web of trust.
  const relay = new Relay(relayUrl, { lensless: true });
  // Asked of the relay itself: this page is served on the monitor's port, so
  // `location` would be the status site.
  const { maxPage, self } = await relayIdentity(relayUrl);
  try {
    // `self` is the relay's own key, which `RELAY_NSEC` also gives the sync
    // process so its records speak as the relay it feeds.
    const scope = self ? { authors: [self] } : {};
    const walk = await walkRecords({
      // Longer than `req`'s default: a timeout here is indistinguishable
      // from an empty store.
      ask: (limit, until) =>
        relay.req({ kinds: [MONITOR_KIND], limit, ...scope, ...(until == null ? {} : { until }) }, 25000),
      pageSize: Math.min(500, maxPage),
      maxPage,
      onProgress,
    });
    // What the scope left out, by one COUNT. A relay that will not answer it
    // leaves this null, rendered as unknown rather than zero.
    const total = self ? await relay.count({ kinds: [MONITOR_KIND] }) : walk.events.length;
    return { ...walk, self, others: typeof total === "number" ? Math.max(0, total - walk.events.length) : null };
  } finally {
    // [Relay] has no close(), and this panel opens one socket per click.
    try { relay.ws && relay.ws.close(); } catch (e) {}
  }
}

/**
 * The relay's NIP-11 document: the biggest single ask it will answer, and
 * its own key. `self` is the relay's key, not `pubkey`, which is the admin
 * contact; scoping to the contact would return nothing. With no stated
 * ceiling the walk cannot grow its page, so a long run of records is
 * reported incomplete rather than stepped over.
 */
async function relayIdentity(relayUrl) {
  try {
    // Same host and port; the NIP-11 document is served under the CORS the
    // relay already sets.
    const origin = relayUrl.replace(/^ws/, "http").replace(/\/$/, "");
    const doc = await fetch(`${origin}/`, { headers: { Accept: "application/nostr+json" } }).then((r) => r.json());
    const stated = doc && doc.limitation && Number(doc.limitation.max_limit);
    return {
      maxPage: stated > 0 ? Math.min(stated, 5000) : 500,
      self: (doc && typeof doc.self === "string" && /^[0-9a-f]{64}$/i.test(doc.self) && doc.self) || null,
    };
  } catch (e) {
    return { maxPage: 500, self: null };
  }
}
/**
 * Draw the host groups into [box]; returns the filterable groups. A url is
 * drawn as its path: the host is the box's heading, and these lists are
 * minted paths on one host.
 */
function drawVerdicts(box, groups, nowSec) {
  box.replaceChildren();
  const out = [];
  for (const group of groups) {
    const wrap = el("div", "vd-group");
    const head = el("div", "vd-ghead");
    const folded = group.folded;
    const meter = el("div", "vd-meter");
    const fill = el("i");
    fill.style.width = `${group.urls.length ? Math.round((folded / group.urls.length) * 100) : 0}%`;
    meter.appendChild(fill);
    meter.title = `${fmt(folded)} of ${fmt(group.urls.length)} url(s) fold away`;
    head.append(
      el("div", "vd-name", group.host),
      el("span", "vd-count",
        `${fmt(group.urls.length)} url(s) → ${fmt(group.survivors)} dialled` +
        (group.expired ? ` · ${fmt(group.expired)} expired` : "")),
      meter,
    );
    wrap.appendChild(head);
    const rows = [];
    for (const u of group.urls) {
      const row = el("div", "vd-row");
      const left = el("div", "vd-path");
      const right = el("div", "vd-when");
      // The bare url is named; a "/" row looks like a rendering failure.
      const path = u.url.replace(/^wss?:\/\/[^/]+/i, "") || "/";
      left.append(el("span", null, path === "/" ? "(no path)" : path));
      // The fold column says only what the fold decided; the stability
      // verdict is drawn apart, so a host mid-measurement does not read as
      // one nothing has looked at.
      let tag;
      if (u.fold) {
        tag = el("span", u.foldCurrent ? "vd-pill fold" : "vd-pill expired", u.foldCurrent ? "fold" : "expired");
        // The survivor heads this box, so the arrow names only its path.
        left.append(el("span", "vd-into", ` → ${u.fold.replace(/^wss?:\/\/[^/]+/i, "") || "/"}`));
      } else if (u.cleared) {
        // The same currency test the fold row makes: a retired cleared
        // verdict is back in the queue, not settled.
        tag = el("span", u.foldCurrent ? "vd-pill keep" : "vd-pill expired", u.foldCurrent ? "keep" : "expired");
        if (u.foldCurrent) row.classList.add("vd-keep");
      } else if (u.synthetic) {
        // Inferred from the folds, not stated by the monitor, and said so.
        tag = el("span", "vd-pill keep", "survivor");
        row.classList.add("vd-keep");
        left.append(el("span", "vd-into", " · no record of its own"));
      } else {
        tag = el("span", "vd-pill none", "not folded");
      }
      right.append(tag);
      if (u.fold || u.cleared) {
        // `ago` takes the instant and draws a missing one as an em dash;
        // skipping the span would read as "just measured".
        const at = u.foldMeasuredAt;
        const when = el("span", null, ` ${ago(at)}`);
        when.title = isoOf(at);
        // `foldCurrent` from the grouping, not `isCurrent` on the TTL alone: a
        // verdict under superseded rules is inside the TTL and still not acted on.
        if (!u.foldCurrent) when.className = "vd-stale";
        right.append(when);
      }
      // The grade on the left, beside the url: the right column answers what
      // the fold decided, and a second pill there would compete with it. A
      // retired grade is struck, not hidden.
      if (u.grade) {
        const tone = u.gradeCurrent ? (u.grade === PRIME ? "prime" : "refused") : "retired";
        const badge = el("span", `vd-grade ${tone}`, u.grade);
        badge.title =
          (u.gradeEvidence ? `${u.gradeEvidence} — ` : "") +
          `graded ${ago(u.gradeMeasuredAt)}` +
          (u.gradeCurrent ? "" : "; the router has retired this grade and will re-take it");
        left.append(" ", badge);
      }
      // Struck, not hidden, when retired: the reader sees what was decided
      // and that the router no longer acts on it.
      if (u.stable === false) {
        const badge = el("span", u.stableCurrent ? "badge bad" : "badge retired", "inconsistent");
        if (!u.stableCurrent) badge.title = "the router has retired this refusal and will re-measure";
        left.append(" ", badge);
      }
      row.append(left, right);
      // One evidence line, named. The fold's wins when both exist; the
      // stability sentence keeps its own title.
      if (u.foldEvidence) {
        const why = el("div", "vd-why", `fold: ${u.foldEvidence}`);
        why.title = u.foldEvidence;
        row.appendChild(why);
      } else if (u.stableEvidence) {
        const s = u.stable === false ? "inconsistent" : u.stable === true ? "consistent" : "measured";
        const text = `stability (${s}, ${ago(u.stableMeasuredAt)}): ${u.stableEvidence}`;
        const why = el("div", "vd-why", text);
        why.title = text;
        row.appendChild(why);
      }
      // The rest of the record is what the other writers on this address
      // said; on a shared replaceable event, a row showing `same-as` and
      // nothing else is a clobbered record.
      const meta = [];
      // The record's clock is when the router last connected, not when it
      // measured; labelled apart from the age beside the pill, and the only
      // date on a pre-stamp verdict.
      if (u.recordAt) meta.push(["record", ago(u.recordAt), isoOf(u.recordAt)]);
      if (u.network) meta.push(["", u.network]);
      for (const r of u.requirements) meta.push(["req", r]);
      if (u.rttOpen) meta.push(["open", `${u.rttOpen}ms`]);
      if (u.rttRead) meta.push(["read", `${u.rttRead}ms`]);
      if (u.rttWrite) meta.push(["write", `${u.rttWrite}ms`]);
      if (u.software) meta.push(["", u.software.replace(/^git\+https?:\/\//, "")]);
      // A count: the number says whether it answered at all.
      if (u.supportedNips.length) meta.push(["nips", String(u.supportedNips.length)]);
      if (u.hasDoc) meta.push(["", "nip-11 doc"]);
      // Counted, never dropped: an unknown tag should look different from none.
      if (u.extra) meta.push(["", `+${u.extra} other tag(s)`]);
      if (meta.length) {
        const line = el("div", "vd-meta");
        for (const [k, v, title] of meta) {
          const cell = el("span", k === "req" ? "vd-req" : null);
          // The instant behind the relative age, as on the pill.
          if (title) cell.title = title;
          if (k && k !== "req") cell.append(el("span", "k", k + " "));
          cell.append(v);
          line.appendChild(cell);
        }
        row.appendChild(line);
      }
      wrap.appendChild(row);
      rows.push({ node: row, needle: (u.url + " " + (u.fold || "")).toLowerCase() });
    }
    box.appendChild(wrap);
    // The group is what the filter hides, with its rows.
    out.push({ box: wrap, host: group.host.toLowerCase(), rows });
  }
  return out;
}

export { monitorCard, verdictsCard };
