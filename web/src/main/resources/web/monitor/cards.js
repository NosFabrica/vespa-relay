// THE MONITOR'S CARDS — what this router has decided about each relay url, and
// the four passes that decided it.
//
// Both were somewhere else, and both moved to the plane that produces them.
// `monitorCard` was a panel on the mirror's page, drawn out of a processor
// array both planes shared; `verdictsCard` was on the RELAY's, because that was
// the only page with a websocket to read kind-30166 records over. Neither
// belonged there: sync coverage answers "is the mirror keeping up" and this
// answers "what is out there, and how much of it can we use".
//
// The rows come from the monitor's own `Processors` now, so nothing sorts them
// by name at render time.

import { ago, cardHead, el, fmt, isoOf, short, shownOf } from "../shared/page.js";
import { backgroundPanel, setTerms } from "../shared/processors.js";
import { Relay } from "../shared/relay.js";
import { MONITOR_KIND, PRIME, groupByHost, summarise, walkRecords } from "../shared/verdicts.js";

/**
 * WHICH RELAYS MAY BE DIALLED AT ALL — the round-up and the three passes that
 * decide it, and the corpus they decide over.
 *
 * ## Why this is not part of Sync coverage
 *
 * It was, and the card asked two questions at once. These four rows run on the
 * alias monitor's own clock, nothing about them is configured by a stream, their
 * unit is a RELAY URL rather than an event, and what they produce is a signed
 * kind-30166 record that outlives this process — the same records the card
 * below looks up one url at a time. Sync coverage answers "is the mirror
 * keeping up"; this answers "what is out there, and how much of it can we
 * use". An operator arrives with one of those, and the split is what lets them
 * stop reading at the answer.
 *
 * ## The tree first, then the passes
 *
 * The corpus is the subject all four share, so it heads the card rather than
 * hanging under whichever pass happens to publish it — and it is captioned with
 * that pass's name, because it IS that pass's reading and unlabelled it would
 * pass for the card's own arithmetic. See `funnelOf`.
 *
 * Drawn only when a router has written processor rows. A serve-only relay runs
 * no monitor, and an empty card there would read as a broken one.
 */
function monitorCard(section) {
  const d = (section && section.data) || {};
  // Every row this document carries. It used to be the monitor's SHARE of a
  // shared array, sorted out by name at render time; the plane keeps its own
  // report now, so a row is here because this plane registered it.
  const rows = (d.progress && d.progress.processors) || [];
  if (!rows.length) return null;
  // The document's own glossary, set again here rather than inherited from
  // whichever card drew first: two cards reading one section must not depend on
  // the order `PANELS` happens to build them in.
  setTerms(d.terms);
  const card = el("div", "card");
  cardHead(card, "Relay monitor", null, section);
  // ON THE HEADING, not as a `card-sub`: that slot is a STATE — "no kind
  // histogram in this document" — and the panels here deliberately stopped
  // explaining themselves in prose. A card whose name is new to the reader
  // still owes them a sentence, so it goes where every other explanation on
  // this page goes, on the label itself.
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
 * WHAT THE MONITOR HAS DECIDED ABOUT EACH RELAY URL, asked over the wire.
 *
 * Every other panel here reads `/stats.json`, which is a rollup: the fold's
 * summary says 373 urls collapsed onto 909 relays and stops there. That number
 * cannot answer the question an operator actually has when a duplicate is still
 * being dialled — *what does this store say about THIS url, and when was it
 * measured?* The verdict is a signed kind-30166 record and nothing on this page
 * read one.
 *
 * A plain NIP-01 REQ over this relay's own websocket, so the panel is a
 * protocol check as much as a view: a verdict that cannot be read here cannot
 * be read by any client either. The note at the top of this file records that
 * the kind histogram lost exactly this property when it stopped asking NIP-45
 * COUNTs, and judged the check better as a test than a page. That was right
 * about counts, which have a rollup to fall back on. It is wrong about
 * verdicts, which have no other reader at all.
 *
 * NOT DRAWN UNTIL ASKED, and that is why it takes a button. This page polls
 * itself every minute; opening a websocket and paging thousands of records on
 * that cadence would make a debugging aid into a load source. `reads: []` in
 * PANELS keeps the poller from rebuilding it, so what the reader loaded and
 * typed survives every refresh underneath it.
 */
function verdictsCard(relayUrl) {
  const card = el("div", "card");
  cardHead(card, "Monitor verdicts (NIP-66)", null, null);
  card.appendChild(el("p", "card-sub",
    `Reads kind ${MONITOR_KIND} out of this relay over its own websocket — the same ask any client makes. ` +
    "Grouped by host, because a duplicate is never a property of one url: it is a property of a url next to another one."));

  // ONE toolbar, not `.grp-head` — which this borrowed and should not have.
  // That class is a STICKY heading for the inside of a scroll box: it carries
  // `position: sticky`, a z-index and its own background, none of which mean
  // anything at the top of a card, and the background made the control row read
  // as a header band.
  const bar = el("div", "vd-bar");
  const load = el("button", "btn", "Read verdicts from this relay");
  const filter = el("input");
  filter.type = "search";
  // Named like the others so render()'s restore loop cannot carry one search
  // box's text into another — there are three on this page now.
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
      // A hit on the HOST keeps the whole group — that is what someone typing a
      // hostname means, and hiding the non-matching urls of the very host they
      // asked for would take away the comparison the panel exists to give.
      const wholeHost = !q || g.host.includes(q);
      let kept = 0;
      for (const r of g.rows) {
        const hit = wholeHost || r.needle.includes(q);
        r.node.hidden = !hit;
        if (hit) kept++;
      }
      // Heads and rows go together. Hiding only rows left a heading on screen
      // for every host in the store, each with nothing beneath it.
      g.box.hidden = !kept;
      if (kept) live++;
    }
    const empty = !!q && !live;
    none.hidden = !empty;
    if (empty) none.textContent = `No url in this store's verdicts contains “${filter.value.trim()}”.`;
  };
  filter.addEventListener("input", applyFilter);

  // NOT DRAWN AT ALL without a relay to ask. A serve-only or fill-only
  // deployment publishes no `relay` in this document, and a button that opens a
  // websocket to nowhere reads as a store holding no verdicts — the one
  // conclusion this panel exists to make impossible.
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
      // The numbers as NUMBERS, over the list rather than as a run-on sentence
      // beside the button. `not folded` is the one that separates two very
      // different diagnoses: a record carrying no verdict tag of ours was
      // written by somebody else's monitor — this panel deliberately asks
      // without an `authors` filter — or by this router back when a passive
      // NIP-66 watcher still wrote one per socket. Either way a store full of
      // them beside zero folds says the FOLD has not run, where no records at
      // all says nothing has.
      // `replaceChildren`, not `fill`: that helper exists to carry a reader's
      // filter text and scroll position across a rebuild, and this box holds
      // neither. Using it here would imply a preservation that is not happening.
      sumBox.replaceChildren(tally([
        ["urls", sum.urls],
        ["hosts", sum.hosts],
        ["folded away", sum.folded],
        ["measured, kept", sum.cleared],
        // "NO VERDICT AT ALL", and it used to be spelled `not folded` — the
        // same two words the fold's own pill wears on every row it did not
        // fold. They count different things, and adding the grade pulled them
        // apart on a live store: 77 urls carry no verdict of any kind, while
        // 540 rows are drawn `not folded` because the FOLD has not folded them
        // — 510 of those graded by the fitness pass minutes earlier. A reader
        // comparing the tile to the rows under it saw one number contradict
        // the column beneath it.
        ["no verdict yet", sum.silent],
        // The GRADE's numbers, which no tile carried while the grade was
        // hidden inside the software column. `prime` is the one that answers
        // "how much of this store is actually in a roster right now", and
        // `prime on Tor` splits it by the transport that decides whether this
        // deployment can reach any of it without a circuit.
        //
        // The Tor tile is drawn at zero, unlike `expired` and the two below it.
        // Those say "this state does not occur here"; this one says "nothing we
        // admit is behind a hidden service", which is an answer an operator
        // running Tor came for — and hiding it would read as a panel that does
        // not know the difference rather than as a store with none.
        ...(sum.graded ? [["graded", sum.graded], ["prime", sum.prime], ["prime on Tor", sum.primeTor]] : []),
        ...(sum.expired ? [["verdict expired", sum.expired]] : []),
        ...(sum.unstable ? [["refused as inconsistent", sum.unstable]] : []),
      ]));
      // What the read did NOT cover comes first: every number above is over the
      // records we actually got, and one taken from a partial read means
      // something different from one taken from a whole store.
      // WHOSE RECORDS THESE ARE comes before what they say. Every number below
      // is over one monitor's observations, and a reader who assumes otherwise
      // reads the panel as a census of the store.
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
      // The relay's own message. A panel that speaks the protocol has to report
      // a protocol failure as one — "0 records" and "the socket closed" are not
      // the same finding, and this is the panel whose whole job is telling
      // states apart.
      status.textContent = "";
      none.hidden = false;
      none.textContent = `Could not read verdicts: ${(e && e.message) || e}`;
    } finally {
      load.disabled = false;
    }
  });
  return card;
}

/** A row of labelled numbers — the panel's own small tiles. */
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
 * Page every kind-30166 record out of this relay, newest first.
 *
 * Paged on `until`, because a fan-out of five figures carries five figures of
 * records and one REQ is bounded by whatever this relay's own limit happens to
 * be — the same reason `AliasProbe` walks rather than asking once. The cursor
 * steps strictly below the oldest `created_at` seen when a page adds nothing
 * new, or a page that is entirely one timestamp cannot move it at all.
 *
 * No `authors` filter. The question is what this STORE holds, which includes
 * records signed by another monitor or by a router sharing the key — and a
 * verdict we cannot see because we guessed the wrong author reads exactly like
 * a verdict that was never written, which is the confusion this panel exists to
 * end. The author is drawn per row instead.
 */
async function readVerdicts(onProgress, relayUrl) {
  // `lensless`: this socket never authenticates, and a vespa-relay refuses an
  // unauthenticated read that names no lens, so every filter below goes out
  // with `include:spam` (shared/lens.js). Right as well as necessary — a
  // verdict panel counting what the STORE holds must not be narrowed by
  // anybody's web of trust, least of all silently.
  const relay = new Relay(relayUrl, { lensless: true });
  // What that relay says it will serve in one ask, and WHO IT IS — asked of the
  // relay itself rather than assumed, which is why the url has to be published
  // in the document: this page is served on the MONITOR's port, and dialling
  // `location` would open a websocket to the status site.
  const { maxPage, self } = await relayIdentity(relayUrl);
  try {
    // SCOPED TO THIS RELAY'S OWN MONITOR, and that scoping is load-bearing on a
    // real store rather than tidiness.
    //
    // A router discovers relays from other monitors' reports — the documented
    // outbox source reads `kinds: [10002, 10050, 30002, 30166]` — so a mirror
    // holds strangers' 30166s by design, and public monitors publish them by
    // the tens of thousands. Unscoped, every count on this panel would be our
    // verdicts plus an unknown quantity of somebody else's observations, which
    // is worse than no number: "1,621 folded away" would silently include
    // relays this router never probed.
    //
    // `self` is the relay's OWN key, which `RELAY_NSEC` also gives the sync
    // process precisely so its monitor records speak as the relay it feeds.
    // Absent — an anonymous relay, or one whose sync was given a different key
    // — there is nothing to scope to, and the panel says so rather than
    // guessing an author and hiding every verdict signed under another.
    const scope = self ? { authors: [self] } : {};
    const walk = await walkRecords({
      // Longer than the class's 10s default: a page of signed records off a
      // loaded relay is a bigger answer than the search box ever asks for, and
      // a timeout here is indistinguishable from an empty store to the reader.
      ask: (limit, until) =>
        relay.req({ kinds: [MONITOR_KIND], limit, ...scope, ...(until == null ? {} : { until }) }, 25000),
      pageSize: Math.min(500, maxPage),
      maxPage,
      onProgress,
    });
    // …and then count what we DIDN'T read, so the scoping never hides anything.
    // A COUNT is one round trip against an index; walking every monitor's
    // records to arrive at the same number is not worth a debugging panel's
    // bandwidth. A relay that will not answer it leaves this null, which the
    // caller renders as "unknown" rather than as zero.
    const total = self ? await relay.count({ kinds: [MONITOR_KIND] }) : walk.events.length;
    return { ...walk, self, others: typeof total === "number" ? Math.max(0, total - walk.events.length) : null };
  } finally {
    // Always, and through the socket rather than a method: [Relay] has no
    // close() — the pages that use it keep one connection for their lifetime.
    // This panel opens one per click, so a leak here is one socket per press
    // that the reader cannot see and cannot close.
    try { relay.ws && relay.ws.close(); } catch (e) {}
  }
}

/**
 * What this relay says about itself, out of its own NIP-11 document: the
 * biggest single ask it will answer, and its own key.
 *
 * `maxPage` defaults to 500, which is the conservative reading rather than a
 * guess at generosity: with no stated ceiling the walk cannot grow its page,
 * and a run of records longer than that is reported as an incomplete read
 * instead of stepped over.
 *
 * `self` is the relay's OWN key and NOT `pubkey`, which is the admin contact —
 * two different fields that a reader coming from NIP-11 will expect to be one.
 * Scoping the panel to the contact key would return nothing at all.
 */
async function relayIdentity(relayUrl) {
  try {
    // The relay's HTTP origin, from its ws url — same host, same port, and the
    // NIP-11 document is served there under the CORS the relay already sets for
    // browser clients.
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
 * Draw the host groups into [box]; returns the filterable groups.
 *
 * A url is drawn as its PATH, not its whole url. The host is already the box's
 * heading, so repeating `wss://articles.layer3.news/` on all 23 rows spent the
 * width that tells them apart on the part they share — and these lists are
 * nothing but minted paths on one host, so the path is the entire signal.
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
      // The path alone, with the bare url named rather than drawn as an empty
      // cell — "/" is a row that looks like a rendering failure.
      const path = u.url.replace(/^wss?:\/\/[^/]+/i, "") || "/";
      left.append(el("span", null, path === "/" ? "(no path)" : path));
      // The VERDICT's age, not the record's. Kind 30166 is addressable and
      // shared — quartz's monitor rewrites it on every connection — so
      // `created_at` tracks the last time we TALKED to the relay, not the last
      // time we MEASURED it. Reading the record's clock is the bug that made
      // half these verdicts look immortal on the Kotlin side.
      // THE TWO VERDICTS ARE DRAWN SEPARATELY, and collapsing them misread the
      // live store. A url the stability pass has measured but the fold has not
      // was drawn as "no verdict" with the STABILITY sentence under it, so a
      // host mid-measurement read as a host nothing had ever looked at — beside
      // evidence that plainly contradicted the word above it. The fold column
      // now says only what the fold decided.
      let tag;
      if (u.fold) {
        tag = el("span", u.foldCurrent ? "vd-pill fold" : "vd-pill expired", u.foldCurrent ? "fold" : "expired");
        // The survivor is at the top of this same box, so the arrow names the
        // path it points at rather than repeating the whole url.
        left.append(el("span", "vd-into", ` → ${u.fold.replace(/^wss?:\/\/[^/]+/i, "") || "/"}`));
      } else if (u.cleared) {
        // THE SAME TEST THE FOLD ROW MAKES, and it was missing here. A cleared
        // verdict the router has retired — aged out, or measured under rules it
        // no longer applies — was still drawn as "keep", i.e. measured and
        // settled, while the url was in fact back in the queue to be
        // re-measured. Half the verdicts in a store are this form, so after a
        // rules bump that is most of the page saying the opposite of the truth.
        tag = el("span", u.foldCurrent ? "vd-pill keep" : "vd-pill expired", u.foldCurrent ? "keep" : "expired");
        if (u.foldCurrent) row.classList.add("vd-keep");
      } else if (u.synthetic) {
        // Inferred from what the folds point at, not stated by the monitor —
        // and said so, because the two are different claims.
        tag = el("span", "vd-pill keep", "survivor");
        row.classList.add("vd-keep");
        left.append(el("span", "vd-into", " · no record of its own"));
      } else {
        tag = el("span", "vd-pill none", "not folded");
      }
      right.append(tag);
      if (u.fold || u.cleared) {
        // `ago` takes the INSTANT, not the elapsed time — it does the
        // subtraction itself. Handing it a duration drew every verdict as
        // "20679d ago", which is `now` minus five minutes read as an epoch.
        //
        // Drawn even when the record does not say when it was measured — `ago`
        // renders that as an em dash. Skipping the whole span instead left a
        // pre-stamp verdict with no date at all beside its pill, which reads as
        // "just measured" rather than as the undatable record it is.
        const at = u.foldMeasuredAt;
        const when = el("span", null, ` ${ago(at)}`);
        when.title = isoOf(at);
        // The verdict's own answer, computed once per row when the host was
        // grouped. Asking `isCurrent` again here would ask it a narrower
        // question — a verdict measured yesterday under SUPERSEDED RULES is
        // inside the TTL and still not acted on — and the date would then read
        // fresh beside a pill saying expired.
        if (!u.foldCurrent) when.className = "vd-stale";
        right.append(when);
      }
      // THE FITNESS GRADE, which this panel could not draw at all until the
      // monitor stopped writing it on `s`. It is the verdict that decides
      // whether the relay is in any stream's roster — the most consequential of
      // the three — and it was being rendered as the relay's software.
      //
      // On the LEFT, beside the url, rather than as a third pill on the right:
      // the right-hand column answers "what did the fold decide", and a second
      // pill there would read as a competing answer to that question. An
      // out-of-epoch or aged-out grade is drawn struck rather than hidden, the
      // same bargain the two verdict pills make.
      if (u.grade) {
        const tone = u.gradeCurrent ? (u.grade === PRIME ? "prime" : "refused") : "retired";
        const badge = el("span", `vd-grade ${tone}`, u.grade);
        badge.title =
          (u.gradeEvidence ? `${u.gradeEvidence} — ` : "") +
          `graded ${ago(u.gradeMeasuredAt)}` +
          (u.gradeCurrent ? "" : "; the router has retired this grade and will re-take it");
        left.append(" ", badge);
      }
      if (u.stable === false) left.append(" ", el("span", "badge bad", "inconsistent"));
      row.append(left, right);
      // ONE evidence line, and NAMED — the two verdicts answer different
      // questions about the same url, and an unlabelled sentence is read as
      // belonging to whatever word is nearest. The fold's is the one that
      // explains the row, so it wins when both exist; the stability sentence is
      // still on the title of its own line.
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
      // THE REST OF THE RECORD. Everything above is what this router decided;
      // this is what the other writers on the same address said, and on a
      // replaceable event with several writers its presence is itself the
      // finding — a row showing `same-as` and nothing else is what a clobbered
      // record looks like.
      const meta = [];
      // THE RECORD'S OWN CLOCK, which is not the verdict's and is labelled so.
      //
      // It is written every time this router CONNECTS to the relay — quartz's
      // passive monitor rewrites the same address on a 5-minute flush — so it
      // answers a different question from the age beside the pill: that one is
      // when we last MEASURED, this one is when we last TALKED. Reading the
      // second as the first is the bug that made half these verdicts immortal,
      // which is exactly why both belong on screen with their own names rather
      // than one standing in for the other.
      //
      // It also puts a date back on the rows that lost one: a verdict written
      // before the measured-at element existed shows `—` above, and this line
      // is the only remaining evidence of when the record was last touched.
      if (u.recordAt) meta.push(["record", ago(u.recordAt), isoOf(u.recordAt)]);
      if (u.network) meta.push(["", u.network]);
      for (const r of u.requirements) meta.push(["req", r]);
      if (u.rttOpen) meta.push(["open", `${u.rttOpen}ms`]);
      if (u.rttRead) meta.push(["read", `${u.rttRead}ms`]);
      if (u.rttWrite) meta.push(["write", `${u.rttWrite}ms`]);
      // The relay's own claim about itself, and it is drawn as the SOFTWARE it
      // is. This line used to render the monitor's own fitness grade here,
      // because the grade was written on `s` — so a dead relay showed a chip
      // saying `dead` in the slot where every other NIP-66 reader shows
      // strfry's repository url. The grade is a pill on the right now.
      if (u.software) meta.push(["", u.software.replace(/^git\+https?:\/\//, "")]);
      // A count, not the list: a relay advertising thirty NIPs would take the
      // whole row, and the number is what says whether it answered at all.
      if (u.supportedNips.length) meta.push(["nips", String(u.supportedNips.length)]);
      if (u.hasDoc) meta.push(["", "nip-11 doc"]);
      // Counted, never dropped: a record carrying a tag this page has not heard
      // of should look different from one carrying nothing.
      if (u.extra) meta.push(["", `+${u.extra} other tag(s)`]);
      if (meta.length) {
        const line = el("div", "vd-meta");
        for (const [k, v, title] of meta) {
          const cell = el("span", k === "req" ? "vd-req" : null);
          // An exact timestamp behind the relative one, the same way the
          // verdict's own age carries its instant on the title.
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
    // The GROUP is what the filter hides, and the rows within it. Hiding only
    // rows left every host's heading on screen — a search for one host drew a
    // column of a thousand headings with nothing under them, which read as a
    // thousand matches.
    out.push({ box: wrap, host: group.host.toLowerCase(), rows });
  }
  return out;
}

export { monitorCard, verdictsCard };
