// The pulse page: everything it draws, and the sign-in it draws first.
//
// A MODULE FILE RATHER THAN AN INLINE SCRIPT, unlike this repo's other pages.
// This one is served only to administrators and carries the one document here
// that is not public, so its Content-Security-Policy is `script-src 'self'`
// with no `unsafe-inline` — which an inline script cannot satisfy. The imports
// are module-relative (`../shared/…`), which resolves the same way a
// document-relative one would if this page were ever mounted behind a prefix.
//
// Nothing here holds a credential: the session cookie is HttpOnly and this
// script cannot read it.

import { ago, cardHead, el, fitTiles, fmt, fmtDur, hideTip, isoOf, moveTip, short, showTip } from "../shared/page.js";
import {
  POLL_MS, activityRowsOf, admissionOf, dominantOf, engineRowsOf, gaugesOf,
  locksOf, outcomeSplitOf, showsClients, slowestOf, stageRowsOf, uncertain, whereOf,
} from "../shared/pulse.js";
import { NotAnAdmin, SignInRequired, canSign, fetchGuarded, signIn, signOut } from "../shared/pulseauth.js";

/** The document schema this page reads; see PulseDocument.SCHEMA. */
const SCHEMA = 1;

const docUrl = "./pulse.json";
const bodyEl = document.getElementById("body");
const scopeEl = document.getElementById("scope");
const footEl = document.getElementById("foot");

/** The previous document, kept solely so the counters above can be differenced into rates. */
let prev = null;
/** The last document drawn, so a failed poll leaves the numbers up instead of blanking them. */
let shown = null;

// ── formatting ──────────────────────────────────────────────────────────────

/** A rate, or an em dash while there is no baseline to difference against. */
const perSec = (n) => (n == null ? "—" : n >= 100 ? fmt(Math.round(n)) : n >= 1 ? n.toFixed(1) : n.toFixed(2));
/** Milliseconds at the precision they are worth: sub-millisecond reads are the common case here. */
const ms = (n) => (n == null ? "—" : n >= 1000 ? `${(n / 1000).toFixed(1)}s` : n >= 10 ? `${Math.round(n)}ms` : `${n.toFixed(n >= 1 ? 1 : 2)}ms`);
const pct = (f) => (f == null ? "—" : `${(f * 100).toFixed(f < 0.01 && f > 0 ? 2 : 0)}%`);
/** A duration in milliseconds as the seconds a lock hold is read in. */
const heldFor = (msec) => fmtDur(Math.max(0, Math.round(msec / 1000)));

/** A right-aligned numeric cell. */
const num = (text, title) => {
  const td = el("td", "r", text);
  if (title) td.title = title;
  return td;
};
/** A proportion as a bar, for a table cell. */
const barCell = (share, cls) => {
  const td = el("td");
  const frac = el("div", "frac");
  const i = el("i", cls);
  i.style.width = `${Math.max(0, Math.min(1, share || 0)) * 100}%`;
  frac.appendChild(i);
  td.appendChild(frac);
  return td;
};
const table = (heads, rows, tall) => {
  const wrap = el("div", tall ? "wrap tall" : "wrap");
  const t = el("table", "legs");
  const thead = el("thead");
  const tr = el("tr");
  for (const [label, cls] of heads) tr.appendChild(el("th", cls, label));
  thead.appendChild(tr);
  t.appendChild(thead);
  const tbody = el("tbody");
  for (const r of rows) tbody.appendChild(r);
  t.appendChild(tbody);
  wrap.appendChild(t);
  return wrap;
};
const tile = (key, value, unit, title) => {
  const d = el("div", "tile");
  const v = el("div", "v", value);
  if (unit) v.appendChild(el("small", null, unit));
  d.appendChild(v);
  d.appendChild(el("div", "k", key));
  if (title) d.title = title;
  return d;
};
const card = (title, sub) => {
  const c = el("div", "card");
  cardHead(c, title, sub);
  return c;
};

// ── panels ──────────────────────────────────────────────────────────────────

/**
 * The strip: what this process is doing per second right now, and the two
 * instantaneous readings that say whether it is keeping up. Rates need two
 * polls, so the first render draws em dashes rather than zeros — "not measured
 * yet" and "idle" are different facts and must not look alike.
 */
function healthStrip(doc, rows) {
  const row = el("div", "tiles");
  const admit = admissionOf(doc, prev);
  const reads = rows.filter((r) => r.activity === "Query" || r.activity === "Count");
  const readsPerSec = reads.some((r) => r.callsPerSec != null)
    ? reads.reduce((s, r) => s + (r.callsPerSec || 0), 0)
    : null;
  const ingest = rows.find((r) => r.activity === "BatchInsert" || r.activity === "Insert");
  const slow = slowestOf(doc);

  row.appendChild(tile("reads", perSec(readsPerSec), "/s", "REQ and COUNT port calls per second, differenced from the last poll"));
  row.appendChild(tile("events admitted", perSec(admit?.admittedPerSec), "/s", "Events this process stored, per second"));
  row.appendChild(
    tile(
      "admitted share",
      admit ? pct(admit.share) : "—",
      null,
      admit ? `${fmt(admit.admitted)} of ${fmt(admit.offered)} events offered since boot were stored` : "Nothing has been offered yet",
    ),
  );
  row.appendChild(
    tile(
      slow ? `${slow.call.toLowerCase()} p99` : "slowest p99",
      slow ? ms(slow.p99Ms) : "—",
      null,
      slow ? `${slow.label}: p50 ${ms(slow.p50Ms)}, p99 ${ms(slow.p99Ms)} over ${fmt(slow.measured)} calls` : "No call shape on this page keeps a histogram yet",
    ),
  );
  // No gauge tiles here. Every number in this strip is a RATE differenced from
  // the last poll; an instantaneous reading standing beside them reads as one,
  // and the one thing a gauge must never be is differenced. They have their
  // own panel.
  if (ingest) row.appendChild(tile("docs written", perSec(ingest.docsPerSec), "/s", "Documents leaving the store's write path, per second"));
  return fitTiles(row);
}

/**
 * Where the store's own time goes: one row per activity, its ports indented
 * under it. `calls/doc` is the store's own performance contract in a number —
 * a bulk path that books several port calls per document is talking to the
 * engine more than the work justifies.
 */
function activityPanel(rows) {
  const c = card("What the store is doing", "Wall time inside the engine calls this process made, by the work that made them. Cumulative since boot; the rate column is differenced from the last poll.");
  if (!rows.length) {
    c.appendChild(el("p", "why", "No port call has been booked yet. A store that has served nothing publishes no activities rather than a table of zeros."));
    return c;
  }
  const top = dominantOf(rows);
  const body = [];
  for (const r of rows) {
    const tr = el("tr", "act");
    tr.appendChild(el("td", null, r.label));
    tr.appendChild(num(fmt(r.calls)));
    tr.appendChild(num(perSec(r.callsPerSec)));
    tr.appendChild(num(ms(r.ms)));
    tr.appendChild(barCell(r.share, "level"));
    tr.appendChild(num(pct(r.share)));
    tr.appendChild(num(fmt(r.docs)));
    tr.appendChild(num(""));
    tr.appendChild(num(""));
    body.push(tr);
    for (const p of r.ports) {
      const pr = el("tr", "port");
      pr.appendChild(el("td", null, p.call));
      pr.appendChild(num(fmt(p.calls)));
      pr.appendChild(num(perSec(p.callsPerSec)));
      pr.appendChild(num(ms(p.ms)));
      pr.appendChild(el("td"));
      // calls/doc, warm where the port is chatty.
      const cpd = num(p.docs > 0 ? p.callsPerDoc.toFixed(p.callsPerDoc < 1 ? 3 : 1) : "—", "Port calls per document this call shape touched");
      if (p.chatty) cpd.style.color = "var(--warn)";
      pr.appendChild(el("td", "r", ""));
      pr.appendChild(num(fmt(p.docs)));
      pr.appendChild(cpd);
      // Absent, not zero: no histogram is kept for a write shape, and "p99 0ms"
      // would read as instant when it means unmeasured.
      pr.appendChild(num(p.p99Ms == null ? "—" : ms(p.p99Ms), p.p99Ms == null ? "No histogram is kept for this call shape" : `p50 ${ms(p.p50Ms)} over ${fmt(p.measured)} calls`));
      body.push(pr);
    }
  }
  c.appendChild(
    table(
      [["activity / port"], ["calls", "r"], ["per sec", "r"], ["time", "r"], ["share", null], ["", "r"], ["docs", "r"], ["calls/doc", "r"], ["p99", "r"]],
      body,
    ),
  );
  if (top) {
    c.appendChild(el("p", "note", `${pct(top.share)} of this process's store time is ${top.label}.`));
  }
  const chatty = rows.flatMap((r) => r.ports.filter((p) => p.chatty).map((p) => `${r.label} · ${p.call} at ${p.callsPerDoc.toFixed(1)} calls per document`));
  if (chatty.length) {
    c.appendChild(el("p", "alarm", `Chatty against the engine: ${chatty.join("; ")}.`));
  }
  return c;
}

/**
 * What the ENGINE did with the queries, per rank profile. Its own time is not
 * our wall time, and the gap between them is the network and the summary
 * fetch; `matched → served` is how much of the work reached a client.
 */
function enginePanel(rows) {
  const c = card("What the engine did", "Vespa's own timings and counts, per rank profile, as it reported them on each response.");
  if (!rows.length) {
    c.appendChild(el("p", "why", "No engine response has carried timings yet."));
    return c;
  }
  const body = rows.map((e) => {
    const tr = el("tr");
    tr.appendChild(el("td", null, e.profile));
    tr.appendChild(num(fmt(e.queries)));
    tr.appendChild(num(perSec(e.queriesPerSec)));
    tr.appendChild(num(ms(e.meanEngineMs), `${ms(e.engineMs)} of engine time, ${ms(e.summaryMs)} fetching summaries`));
    tr.appendChild(num(short(e.docsMatched)));
    tr.appendChild(num(short(e.hitsServed)));
    tr.appendChild(num(pct(e.servedShare), "How much of what the engine matched ever reached a client"));
    const deg = num(fmt(e.degraded));
    if (e.degraded > 0) deg.style.color = "var(--warn)";
    tr.appendChild(deg);
    tr.appendChild(num(fmt(e.rungs)));
    return tr;
  });
  c.appendChild(
    table(
      [["rank profile"], ["queries", "r"], ["per sec", "r"], ["mean engine", "r"], ["matched", "r"], ["served", "r"], ["served/matched", "r"], ["degraded", "r"], ["rungs", "r"]],
      body,
    ),
  );
  c.appendChild(el("p", "note", "A profile matching far more than it serves is doing work no client sees — which is exactly what the observer gate is for, and where a narrower filter pays."));
  return c;
}

/**
 * What became of the events offered, per activity. The bar is the answer to
 * "should I narrow this sync": a mirror that is 80% duplicates is fetching
 * what it already holds.
 */
function outcomesPanel(doc) {
  const o = doc.outcomes;
  if (!o || !o.byActivity?.length) return null;
  const c = card("What became of the events", `${fmt(o.admitted)} of ${fmt(o.offered)} events offered to this process were stored.`);
  for (const raw of o.byActivity) {
    const row = outcomeSplitOf(raw);
    const head = el("p", "foot", `${row.label} — ${fmt(row.offered)} offered`);
    c.appendChild(head);
    const bar = el("div", "disp");
    for (const r of row.reasons) {
      const i = el("i", r.reason === "admitted" ? "delivered" : r.reason === "duplicate" ? "nothingNew" : "pending");
      i.style.width = `${r.share * 100}%`;
      i.title = `${r.reason}: ${fmt(r.events)} (${pct(r.share)})`;
      bar.appendChild(i);
    }
    c.appendChild(bar);
    const keys = el("p", "keys foot");
    for (const r of row.reasons) {
      keys.appendChild(el("b", r.reason === "admitted" ? "delivered" : r.reason === "duplicate" ? "nothingNew" : "pending"));
      keys.append(`${r.reason} ${pct(r.share)}  `);
    }
    c.appendChild(keys);
  }
  return c;
}

/**
 * The write path in the present tense and in the past. `held` answers the only
 * question worth asking while ingest is stalled; the wait split answers the
 * one after it — what was it stalled BEHIND.
 */
function locksPanel(doc) {
  const { held, wait } = locksOf(doc);
  if (!held.length && !wait.length) return null;
  const c = card("Locks", "What holds a store mutex at this instant, and where the waiting went.");
  if (held.length) {
    for (const h of held) {
      const p = el("p", h.heldMs > 5000 ? "alarm" : "foot");
      p.append(`${h.stage} held for ${heldFor(h.heldMs)}`);
      if (h.doing) p.append(` — ${h.doing}`);
      if (h.mutex) p.append(` (${h.mutex})`);
      c.appendChild(p);
    }
  } else {
    c.appendChild(el("p", "foot", "Nothing holds a store mutex right now."));
  }
  for (const w of wait) {
    c.appendChild(el("p", "foot", `${w.stage} — ${ms(w.ms)} waited in total`));
    if (!w.behind.length) continue;
    const body = w.behind.map((b) => {
      const tr = el("tr");
      tr.appendChild(el("td", null, b.holder));
      tr.appendChild(barCell(b.share, "level"));
      tr.appendChild(num(pct(b.share)));
      tr.appendChild(num(ms(b.ms)));
      return tr;
    });
    c.appendChild(table([["behind"], ["", null], ["", "r"], ["waited", "r"]], body));
  }
  if (wait.length) {
    c.appendChild(
      el(
        "p",
        "note",
        "Attributed to whoever held the lock when the waiter arrived. Over a long wait the lock may change hands several times, and all of that wait is charged to the first holder — which is right for the case this exists to catch, one pathological holder stalling everyone, and over-attributes to the head of a uniformly busy queue.",
      ),
    );
  }
  return c;
}

/** The write path's stage split — the same rows the mirror's status page draws, on the same counters. */
function stagesPanel(rows) {
  if (!rows.length) return null;
  const c = card("Write path", "Every ingest stage this process has booked, busiest first. `busy` is seconds of work per second of wall clock: 1.0 saturates one thread, and above 1.0 is concurrency.");
  const body = rows.map((s) => {
    const tr = el("tr");
    tr.appendChild(el("td", null, s.stage));
    tr.appendChild(num(ms(s.ms)));
    tr.appendChild(num(s.busy == null ? "—" : s.busy.toFixed(2)));
    // A stage booked from a duration measured elsewhere carries no denominator,
    // and a mean over one that does not exist would be a fiction.
    tr.appendChild(num(s.calls == null ? "—" : fmt(s.calls)));
    tr.appendChild(num(s.meanMs == null ? "—" : ms(s.meanMs)));
    tr.appendChild(num(s.maxMs == null ? "—" : ms(s.maxMs)));
    return tr;
  });
  c.appendChild(table([["stage"], ["total", "r"], ["busy", "r"], ["calls", "r"], ["mean", "r"], ["worst", "r"]], body));
  return c;
}

/** The gauges, drawn apart from every counter so nobody differences a queue depth into a rate. */
function gaugesPanel(doc) {
  const gauges = gaugesOf(doc);
  if (!gauges.length) return null;
  const c = card("Right now", "Instantaneous readings — a depth, a level, a count in flight. Not counters: differencing any of these between two polls gives nonsense, which is why they are not in the strip above.");
  const row = el("div", "tiles");
  for (const g of gauges) row.appendChild(tile(g.label, fmt(g.value), null, g.gauge));
  c.appendChild(fitTiles(row));
  return c;
}

/**
 * Who and what is driving the load. A bounded sketch, so the list is the heavy
 * hitters and the tail is forgotten; each row's own overestimate bound is
 * published rather than hidden.
 */
function hotspotsPanel(doc) {
  const h = doc.hotspots;
  if (!h || (!h.observers?.length && !h.terms?.length)) return null;
  const c = card("What is driving the load", "A bounded sketch of the heaviest observers and search terms — the heavy hitters are kept and the tail is forgotten, so a row's weight is an estimate with the bound beside it.");
  const section = (title, hits) => {
    if (!hits.length) return;
    c.appendChild(el("p", "foot", title));
    const body = hits.map((x) => {
      const tr = el("tr");
      tr.appendChild(el("td", "q", x.key));
      tr.appendChild(num(fmt(x.weight)));
      const err = num(`±${fmt(x.error)}`);
      if (uncertain(x)) {
        err.style.color = "var(--warn)";
        err.title = "The sketch's overestimate for this row is more than half its weight — it may not belong in this list at all.";
      }
      tr.appendChild(err);
      return tr;
    });
    c.appendChild(table([[title === "Observers" ? "observer" : "term"], ["weight", "r"], ["error", "r"]], body));
  };
  section("Observers", h.observers || []);
  section("Terms", h.terms || []);
  return c;
}

/** The slow-read ring: bounded by the ring, never by how many distinct queries exist. */
function slowPanel(doc) {
  const reads = doc.slowReads || [];
  if (!reads.length) return null;
  const c = card(
    "Slow reads",
    `${fmt(reads.length)} engine calls beat this store's slow threshold, newest first. A slow read here is a slow ENGINE CALL, not a slow query: one REQ fans out into companion queries and admission probes, and which of them was slow is the question a four-second search leaves you with. A ring — the oldest is overwritten, so this is bounded whatever the traffic.`,
  );
  // NOT reversed. `CostLedger.snapshot` already sorts the ring newest-first and
  // the document preserves that order, so reversing here drew the oldest read
  // at the top under a card that says "newest first" — which is the wrong end
  // of a ring somebody opened during an incident.
  const body = reads.map((s) => {
    const tr = el("tr");
    const when = el("td", "r", ago(s.at));
    when.title = isoOf(s.at);
    tr.appendChild(when);
    tr.appendChild(el("td", null, s.activity));
    tr.appendChild(el("td", null, s.profile));
    tr.appendChild(num(ms(s.wallMs), `engine ${ms(s.engineMs)}, summaries ${ms(s.summaryMs)}`));
    tr.appendChild(num(short(s.docsMatched)));
    tr.appendChild(num(fmt(s.hits)));
    // One line, with the whole query on hover: a YQL carrying two hundred ids
    // is one row that takes a screen, and thirty of them are the whole page.
    const q = el("td", "q one", whereOf(s.detail));
    q.title = s.detail;
    tr.appendChild(q);
    return tr;
  });
  c.appendChild(
    table([["when", "r"], ["activity"], ["profile"], ["wall", "r"], ["matched", "r"], ["served", "r"], ["query"]], body, true),
  );
  return c;
}

// ── the page ────────────────────────────────────────────────────────────────

function render(doc) {
  if (doc.title) {
    document.title = doc.title;
    document.querySelector("h1").textContent = doc.title;
  }
  scopeEl.textContent = doc.scope || "";
  const rows = activityRowsOf(doc, prev);
  const parts = [
    healthStrip(doc, rows),
    activityPanel(rows),
    enginePanel(engineRowsOf(doc, prev)),
    outcomesPanel(doc),
    locksPanel(doc),
    stagesPanel(stageRowsOf(doc, prev)),
    gaugesPanel(doc),
    hotspotsPanel(doc),
    slowPanel(doc),
  ];
  if (!showsClients(doc)) {
    const c = card("Not on this page", null);
    c.appendChild(
      el(
        "p",
        "why",
        "This process publishes no client-derived sections. The heaviest observers, the heaviest search terms and the slow-read log describe the people using this relay rather than the relay itself, so they are served only where an operator has asked for them.",
      ),
    );
    parts.push(c);
  }
  // The whole body is rebuilt every poll, so anything the reader was doing
  // inside it is lost unless it is carried across. Scroll is the one that
  // matters: the slow-read table is a scroll box, and resetting it to the top
  // twice a second makes it unreadable. Restored by index — the panels are
  // built in a fixed order, so box N is the same box it was.
  const scrolled = [...bodyEl.querySelectorAll(".wrap.tall")].map((b) => b.scrollTop);
  bodyEl.replaceChildren(...parts.filter(Boolean));
  [...bodyEl.querySelectorAll(".wrap.tall")].forEach((b, i) => {
    if (scrolled[i]) b.scrollTop = scrolled[i];
  });
  renderFoot(doc);
  shown = doc;
  prev = doc;
}

function renderFoot(doc) {
  footEl.replaceChildren();
  footEl.append(`Up ${fmtDur(doc.uptimeSeconds)}; every total on this page is cumulative over that. `);
  const a = el("a", null, docUrl);
  a.href = docUrl;
  footEl.append("Source: ");
  footEl.appendChild(a);
  footEl.append(` (schema ${doc.schema}, read at ${doc.generatedAt}).`);
  if (doc.feed) footEl.appendChild(el("p", "foot", doc.feed));
  if (authed) {
    const line = el("p", "foot");
    // The pubkey only when this page load signed in; a reload that inherited a
    // live session is authorised without this script knowing as whom, and
    // guessing would be worse than saying less.
    line.append(admin ? `Signed in as ${admin.slice(0, 12)}… ` : "Signed in. ");
    const out = el("button", "linkish", "sign out");
    out.addEventListener("click", async () => {
      await signOut();
      admin = null;
      authed = false;
      // `load()`, not a card built here: the refusal it gets back carries the
      // relay's own `session` block, and a card built with none would sign the
      // url this browser dialled — which behind a proxy is not the one the
      // relay checks, so the next sign-in would fail for no visible reason.
      await load();
    });
    line.appendChild(out);
    footEl.appendChild(line);
  }
  if (doc.schema > SCHEMA) {
    footEl.appendChild(el("p", "err", `This page was written for schema ${SCHEMA} — some panels may be missing or misread.`));
  }
}

/**
 * Who we are signed in as, when this page load did the signing — never a
 * credential, since the cookie is HttpOnly and invisible to this script. Null
 * on a reload that inherited a live session, where the page is authorised and
 * this page does not know as whom.
 */
let admin = null;

/** Whether the last read was admitted. The poll rides this, so a signed-out page stops asking. */
let authed = false;

/** The sign-in prompt: the only thing an unauthenticated visitor is ever shown. */
function signInCard(why, session) {
  bodyEl.replaceChildren();
  shown = null;
  prev = null;
  footEl.replaceChildren();
  authed = false;
  const c = el("div", "card");
  c.appendChild(el("h2", null, "Administrators only"));
  c.appendChild(
    el("p", "why",
      "This page reads what the store is spending its resources on — including, where the operator has turned it on, " +
      "which observer lenses and search terms are driving the load and the text of slow queries. It is served only to " +
      "an administrator of this relay."),
  );
  if (!canSign()) {
    // The honest dead end, with the one thing that fixes it.
    c.appendChild(el("p", "err",
      "No Nostr extension found in this browser (window.nostr / NIP-07). Signing in needs one, because the proof is a " +
      "signature by an administrator's key — this page never sees the key itself."));
  } else {
    const b = el("button", "signin", "Sign in with your Nostr extension");
    b.addEventListener("click", async () => {
      b.disabled = true;
      b.textContent = "Waiting for your extension…";
      try {
        admin = await signIn(session);
        await load();
      } catch (e) {
        signInCard(e instanceof NotAnAdmin
          ? `That key is not an administrator of this relay (${e.pubkey ? e.pubkey.slice(0, 12) + "…" : "unknown"}). ` +
            "An operator adds it to RELAY_ADMIN_PUBKEYS."
          : String(e && e.message ? e.message : e), e instanceof SignInRequired ? e.session : session);
      }
    });
    c.appendChild(b);
  }
  if (why) c.appendChild(el("p", "foot", why));
  bodyEl.appendChild(c);
  scopeEl.textContent = "Not signed in.";
  document.title = "Eventstore pulse";
}

async function load() {
  try {
    // `no-store` on the route, so every poll is a real read; that is the point.
    const res = await fetchGuarded(docUrl);
    authed = true;
    if (res.status === 503) {
      scopeEl.textContent = "This process serves no metered store.";
      shown = null;
      prev = null;
      bodyEl.replaceChildren();
      const c = el("div", "card pending");
      c.appendChild(el("h2", null, "Nothing to measure"));
      c.appendChild(el("p", "why", "The process answering this port has no metered event store, so there are no counters to read."));
      bodyEl.appendChild(c);
      return;
    }
    render(await res.json());
  } catch (e) {
    // A SESSION THAT ENDED IS NOT AN ERROR. Sessions expire on a fixed clock and
    // die with the process; the page returns to the prompt rather than leaving
    // stale numbers under a red line that says "could not refresh".
    if (e instanceof SignInRequired || e instanceof NotAnAdmin) {
      // "Your session ended" only when there WAS one. On a first visit there is
      // nothing to have ended, and saying so reads as a bug in the page.
      const expired = authed;
      admin = null;
      authed = false;
      signInCard(
        expired && e instanceof SignInRequired ? "Your session ended. One more signature starts another." : null,
        e instanceof SignInRequired ? e.session : null,
      );
      return;
    }
    const why = String(e && e.message ? e.message : e);
    // A failed poll is not a failed page: what is on screen is still the last
    // thing this process reported, and the footer says when.
    if (shown) {
      const note = el("p", "err", `Could not refresh: ${why}. The numbers above are from the last successful read.`);
      note.dataset.refresh = "";
      const prior = footEl.querySelector("[data-refresh]");
      if (prior) prior.replaceWith(note);
      else footEl.appendChild(note);
      return;
    }
    scopeEl.textContent = "Could not load the pulse document.";
    bodyEl.replaceChildren();
    const c = el("div", "card");
    c.appendChild(el("p", "err", why));
    bodyEl.appendChild(c);
  }
}

addEventListener("mousemove", moveTip);
addEventListener("mouseleave", hideTip);
load();
// A fixed cadence, not a document-stated one: this document has no rollup to
// follow, and the rates on screen are only as fresh as the interval. Gated on
// being signed in, so a page sitting on the prompt is not knocking on a route
// that will refuse it every two seconds — and so the prompt is not rebuilt out
// from under the button while somebody is clicking it.
setInterval(() => { if (authed) load(); }, POLL_MS);
