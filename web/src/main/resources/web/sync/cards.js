// THE MIRROR'S CARDS — what each stream is doing, what the background work is
// doing, and how far the walk has got.
//
// Lifted out of the relay's stats.html unchanged, comments included, when the
// sync process started serving its own page. Every judgement these draw was
// already in `shared/sync.js` — the split moved the RENDERING to the service
// that produces the numbers, and changed neither.

import { cardHead, dayOf, el, fmt, fmtDur, short } from "../shared/page.js";
import { backgroundPanel, chip, setStages, setTerms, term } from "../shared/processors.js";
import { STUCK_CALL_SEC, STUCK_LEG_SEC, constraintOf, heldRows, poolsOf, relayStatusOf, socketsOf, storeOf, streamSections } from "../shared/sync.js";

/**
 * A LIVE cursor, which is the one place a day is not enough precision.
 *
 * `pagingUntil` now moves with every event a leg receives, and a walk that
 * opened at `now` spends its first stretch inside TODAY — where a day-granular
 * label cannot move however fast the walk does, which is the exact reading the
 * per-event cursor exists to give. So the minute is drawn while the cursor is
 * still within [FINE_CURSOR_SEC] of now, and the plain day after that, where the
 * minute would be false precision on a walk measured in years.
 *
 * An auditing leg's cursor is the older edge of the negentropy window it is
 * comparing, so `back to` reads the same direction on both stages — a date near
 * now means the leg has only just started down, never that it is done.
 */
const FINE_CURSOR_SEC = 2 * 24 * 60 * 60;
const cursorOf = (t, nowSec) =>
  nowSec != null && nowSec - t < FINE_CURSOR_SEC
    ? new Date(t * 1000).toISOString().slice(0, 16).replace("T", " ")
    : dayOf(t);

/**
 * A SCHEDULE'S period, which is the one duration on this card measured in days.
 *
 * `fmtDur` is the phase clock's — seconds, minutes, hours — and it is right for
 * every other duration here: a leg held for 26 hours reads better as `26h 0m`
 * than as `1d 2h`, because what is being judged is how long a worker has been
 * stuck. These two are the opposite: a week's audit period rendered `168h 0m`
 * and a thirty-day re-fetch `720h 0m` are numbers a reader has to divide before
 * they mean anything, and what is being judged is the calendar.
 *
 * Local rather than a second tier inside `fmtDur`, so the phase clock is not
 * changed under every other mark on the page to fix two cells.
 */
const DAY_SEC = 24 * 60 * 60;
const fmtPeriod = (sec) => (sec >= 2 * DAY_SEC ? `${Math.round(sec / DAY_SEC)}d` : fmtDur(sec));

/**
 * IS IT WORKING — one line, and the first thing on the card.
 *
 * The throughput and the constraint verdict when there is one. The verdict is
 * the only part that names a fault, which is why it is the only part drawn as
 * a chip.
 *
 * IT USED TO OPEN WITH "running" / "not running", and that half is gone. This
 * page was served by the RELAY, off files the mirror wrote to a shared volume,
 * so it had to infer whether the writer still existed: a `writtenAt` heartbeat,
 * a `staleForSec` computed against the reader's clock, a 150-second threshold,
 * and a past tense for the constraint. The mirror serves this page itself now.
 * The document is only here because the process is, so the dot stays as a
 * heading mark and the sentence it used to carry has nothing left to say.
 */
function statusRow(progress) {
  const row = el("div", "sy-status");
  row.appendChild(el("i", "sy-dot"));
  row.append("running");
  const health = progress.health || {};
  // Spelled with its own spaces: consecutive text nodes collapse into ONE
  // anonymous flex item, so the row's `gap` never lands between them.
  // BOTH ENDS OF THE INGEST QUEUE, in then out. The row used to carry the
  // drain alone, as "events/s into the store", and on staging with the queue
  // pinned full it read `0 events/s` — which is what a store that has stopped
  // answering looks like and ALSO what a fan-out that has gone quiet looks
  // like, since the drain counts what left a batch. What is still being sent
  // to ingest is the reading that tells them apart, so it is drawn first and
  // the drain is named for what it is.
  if (health.arrivingPerSec != null) row.append(` · ${fmt(health.arrivingPerSec)} events/s into ingest`);
  if (health.eventsPerSec != null) row.append(` · ${fmt(health.eventsPerSec)} events/s out to the store`);
  // The five verdicts are different faults with different fixes, and only two
  // are a fault at all: `ingest` (full and keeping up badly) and `wedged` (full
  // and not draining) — see BOTTLENECK.
  const constraint = constraintOf(health);
  if (constraint) row.appendChild(chip(constraint.text, constraint.tone, constraint.why));
  // THE SOCKET BUDGET, and it was published to `/stats.json` for a long time
  // without being drawn anywhere: the one number that says whether the router
  // can reach more relays was readable only by fetching the JSON by hand.
  // Coloured on the QUEUE alone — see [socketsOf] for why "near the ceiling"
  // is the healthy state and not a warning.
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
  // A count, not a list: the document publishes how many, and one is already
  // the whole message.
  if (progress.fatals) row.appendChild(chip(`${fmt(progress.fatals)} fatal error(s)`, "warn", term("fatals")));
  // THE STORE'S OWN SENTENCE about its write path — on HOVER, not on the row.
  // It reads `feed ok 4211 inflight 32 lat 18ms`, which is machine output, and
  // it sat here beside curated phrases like "ingest is the limit" as the
  // longest and least readable thing on the line while answering nothing at a
  // glance: a reader still has to know that a wide in-flight window with high
  // latency is the ENGINE pushing back and a narrow one with low latency is the
  // CLIENT not pushing. So the chip is a label and the sentence is the tooltip.
  //
  // The one tone is taken on a SUBSTRING, deliberately the weakest possible
  // read of another repo's prose: `EXC` appears only when the feed client has
  // seen transport exceptions, which is unambiguously a fault, and if that
  // library ever rewords it this degrades to a neutral chip rather than to a
  // wrong verdict. Every other judgement stays with the reader, beside the
  // numbers it needs.
  if (health.feed) {
    const broken = health.feed.includes("EXC");
    row.appendChild(chip(broken ? "feed errors" : "feed", broken ? "warn" : null, `${health.feed} — ${term("feed")}`));
  }
  return row;
}

/**
 * WHAT THE WHOLE MIRROR IS SPENDING — two lines, above the stream sections
 * that divide them.
 *
 * ## Why it is two lines and not the four tables it used to be
 *
 * The panel here used to draw the mirror's own four pool tables and then, above
 * more than one stream, the SAME rows again cut by stream. Two cuts of one set
 * on one card: every held relay appeared twice, and with one stream the
 * mirror's cut and the stream's were the same four tables drawn under two
 * headings. What the mirror-wide cut answered that a stream's cannot is a
 * QUESTION OF PROPORTION — how much of this router is re-fetching history
 * rather than keeping up — and that is a tally, not a table of urls.
 *
 * So the rows are drawn once, inside the stream that owns them, and this is
 * the two lines that cannot be read off them: how big the pool is, and how its
 * work divides. Both come off the same [poolsOf] read the sections are built
 * from, so a summary cannot disagree with the rows under it.
 *
 * What a truncated list left out is NOT here, though it used to be. This panel
 * is withheld below two streams (see below), and a disclosure that only
 * appeared on multi-stream deployments is not a disclosure — it is at card
 * level now, in [syncCard].
 *
 * ## …and only above more than one stream
 *
 * With one stream the pool IS that stream: same roster, same units, same
 * split, drawn a second time under a heading that says "the mirror" instead of
 * the stream's name. That is the duplication this panel was rebuilt to end,
 * and drawing it anyway "for consistency" would reintroduce it in the one
 * deployment shape where it is guaranteed to be redundant.
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
 * HOW BIG THE POOL IS — one line, above what divides it.
 *
 * The tables answer "how many are doing this" and cannot answer "out of how
 * many", which is the first thing asked of any of them: nine relays working is
 * a healthy rotation against a roster of four hundred and a stalled one
 * against a roster of twelve.
 *
 * ONE FUNCTION FOR BOTH CUTS, because they are one line, and [whose] is the
 * only difference that is not derivable — the mirror's roster or one stream's.
 * The units mark comes off the numbers themselves: pool-wide a relay two
 * streams both want is one relay and two units, so the two denominators
 * differ and both are named; inside one stream they are the same number, and
 * so is a mirror with one stream, and drawing it twice would invite reading
 * one number as two. Everything else, including the tail caveat, is the same
 * sentence in one place — written twice, the "between visits" prose had
 * already drifted between the copies.
 *
 * The first marks PARTITION the roster; the tail count crosses them, because a
 * tailed relay keeps its tail while it is revisited. Drawn last and said in
 * its title, so the line is never read as parts of one whole.
 */
function poolLine(totals, whose = "the roster") {
  const line = el("div", "sy-sub");
  const mark = (text, why) => {
    const span = el("span", null, line.children.length ? ` · ${text}` : text);
    if (why) span.title = why;
    line.appendChild(span);
  };
  // A router that publishes no pool row is not a router with an empty pool, so
  // the totals simply go away rather than rendering as zero.
  if (totals.relays != null) mark(`${fmt(totals.relays)} relay(s)`, term("roster"));
  // RELAYS FIRST AND UNITS SECOND where they differ, because they are
  // different denominators and every count of work below is in the second.
  // Where they are equal there is one denominator and it has been named.
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

/**
 * A SCROLLING TABLE WITH A HEADER ROW — the chrome every list on this card
 * shares, built once.
 *
 * Three panels drew it: the same box, the same table class, and the same loop
 * over `[label, glossaryKey, rightAlign]` triples. Three copies of the chrome
 * is three edits for any change to it, and the alignment was being re-typed
 * inline when the stylesheet already carries the rule for the cells beneath.
 *
 * Returns both halves because the caller owns the rows: `table` to append them
 * to, `scroll` to hand back to the card.
 */
function headedTable(columns, fit = false, groups = null) {
  const scroll = el("div", fit ? "sy-legs-box sy-fit" : "sy-legs-box");
  const table = el("table", "sy-legs");
  // A SECOND HEADER ROW where the columns fall into named halves — see
  // [jobsTable], the one table here wide enough to need it. Spans rather than
  // repeated labels, so the two halves are visibly two.
  // WHERE ONE GROUP ENDS AND THE NEXT BEGINS, derived from the spans rather
  // than listed a second time: the band and the rule down the body have to
  // fall in the same two places, and a second list of column indices is a
  // second thing to keep in step with the first.
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
    // The glossary is the document's own — a column with no member behind it
    // (the stream's name) gets no tooltip rather than an invented one.
    if (key) th.title = term(key);
    head.appendChild(th);
  });
  table.appendChild(head);
  scroll.appendChild(table);
  return { scroll, table, halves };
}

/**
 * WHAT THIS STREAM MAY SPEND ON EACH JOB, AND WHEN THAT JOB COMES DUE — one
 * table, one row per job.
 *
 * ## Two tables became one, and lost a column doing it
 *
 * The budgets and the schedule were separate panels at card level, each with a
 * `stream` column repeating the name once per job and each with its own `job`
 * column carrying the same four words. Six rows of "content, content, content,
 * content, indexers, indexers" in one, three in the other, and the same job
 * named twice on the same card. Both are keyed by (stream, job): the stream is
 * the section they now sit in, and the job is one row.
 *
 * The join is worth more than the space it saves. `may run` at its ceiling is
 * not a fault; `may run` at its ceiling with `turned away` climbing and 400
 * asks `waiting` on a 30-day clock is a cap biting, and those three numbers
 * were in two tables at opposite ends of the card. See [jobsOf].
 *
 * ## Absent, not zero
 *
 * A job with a cap and no clock (a dial width, a tail budget) draws `—` across
 * the schedule half. That is what "this job is not on a schedule" looks like,
 * and a zero there would read as a period of nothing.
 *
 * ## Which cell is coloured
 *
 * Two independent faults share a row now, so the ROW cannot carry the mark:
 * `hot` on the whole row would say "something about this job" and make the
 * reader find out which. The cap biting colours `turned away`, the schedule
 * backing up colours `due`, and the job's name is coloured for either so the
 * row is still findable at a glance down a card.
 */
function jobsTable(rows) {
  if (!rows.length) return null;
  const { scroll, table, halves } = headedTable(
    [["job", "job", false],
     ["may run", "streamCap", true], ["in use", "inUse", true], ["turned away", "deferred", true],
     ["every", "everySec", true], ["due", "due", true], ["never run", "neverRun", true],
     ["waiting", "waiting", true], ["next in", "nextInSec", true]],
    true,
    // The two halves named over the columns that belong to each. Without it
    // nine columns read as one undifferentiated row of numbers, and `due`
    // beside `turned away` invites reading the second as a cause of the first.
    [[null, 1], ["what it may spend", 3], ["when the past is re-read", 5]],
  );
  for (const r of rows) {
    const l = r.limit;
    const sc = r.schedule;
    const tr = el("tr");
    // The same divide the band above draws, carried down the body — and taken
    // from the band's own arithmetic, never re-listed.
    const cell = (cls, text) => {
      const td = el("td", `${cls || ""}${halves.has(tr.children.length) ? " half" : ""}`.trim() || null, text);
      tr.appendChild(td);
    };
    cell(l?.biting || sc?.backedUp ? "hot" : null, r.label);
    // UNCAPPED IS NOT ZERO, and the word is drawn rather than an empty cell: a
    // blank here would read as a cap of nothing, which is the opposite. A job
    // with no limit row at all is a different claim again — the router
    // published none — and draws the same `—` as an absent clock.
    cell("n", !l ? "—" : l.streamCap != null ? fmt(l.streamCap) : "uncapped");
    cell("n", l && l.inUse != null ? fmt(l.inUse) : "—");
    cell(`n${l?.biting ? " hot" : ""}`, l ? fmt(l.deferred) : "—");
    cell("n", sc && sc.everySec != null ? fmtPeriod(sc.everySec) : "—");
    cell(`n${sc?.backedUp ? " hot" : ""}`, sc ? fmt(sc.due) : "—");
    cell("n", sc ? fmt(sc.neverRun) : "—");
    cell("n", sc ? fmt(sc.waiting) : "—");
    // Nothing waiting is no countdown — every ask is already due, which the
    // three columns to the left have just said.
    cell("n", sc && sc.nextInSec != null ? fmtPeriod(sc.nextInSec) : "—");
    table.appendChild(tr);
  }
  return scroll;
}

/**
 * THE MIRROR-WIDE SPLIT IN ONE LINE — how many units are in each pool, across
 * every stream.
 *
 * This is the whole of what the mirror's own cut of the tables was for: the
 * per-stream sections answer "which stream is re-fetching" and give up the
 * answer to "how much of this mirror is". One line restores it, above the
 * sections whose counts add up to it.
 *
 * Off the same `groups` those sections are drawn from, never off `visiting` or
 * `liveHeld` — a summary that disagreed with the rows under it would be worse
 * than no summary.
 */
function poolTally(groups, totals) {
  const line = el("div", "sy-sub");
  for (const g of groups) {
    const span = el("span", null, `${line.children.length ? " · " : ""}${g.label} ${fmt(g.rows.length)}`);
    span.title = g.what;
    line.appendChild(span);
  }
  // OUT OF WHAT, on the end: the counts above are shares of the pool's units
  // and the line is read as a partition without it. Omitted where the router
  // does not publish the size, rather than drawn as zero.
  if (totals.units != null) {
    const of = el("span", null, ` · of ${fmt(totals.units)} stream-visit(s)`);
    of.title = term("rosterVisits");
    line.appendChild(of);
  }
  return line;
}

/**
 * ONE STREAM, WHOLE — everything about it in one bordered section, and nothing
 * about it anywhere else on the card.
 *
 * ## What this replaced
 *
 * A stream used to be drawn five times: a line under *streams* saying what it
 * was riding, a pool section that opened by saying the same thing off
 * different members, and its name in the first column of two card-level
 * tables, once per job. The five were four independent walks of the same
 * array, so the reader did the join — and the two roster lines could disagree,
 * because one counted the drawn rows and the other read `liveHeld`.
 *
 * One section, in the order the questions are asked of a stream:
 *
 *  1. **what it is** — its name and its phase, on one line.
 *  2. **what it is riding** — its share of the roster, ONCE, from
 *     [streamSections]' single read.
 *  3. **what it may spend, and when its clocks come due** — one row per job,
 *     because they are the config half and they explain what follows: a pool
 *     smaller than its work is a cap, and an empty one is a schedule. It leads
 *     rather than trails for a reason the flat tables did not have — the pools
 *     below are five scrolling tables, and burying four rows of configuration
 *     under them makes an operator scroll past the answer to find the reason.
 *  4. **what it is holding** — the four pools and their relays.
 *
 * ## A stream holding nothing gets a line, not four empty tables
 *
 * "Nothing is auditing right now" is an answer and the pools still say it —
 * but a stream holding nothing ANYWHERE said it five times, in five headings
 * with five "none right now" underneath, which is 200 pixels to say one thing.
 * That is the starved-stream case and the not-yet-certified case, and both are
 * better served by the sentence than by the tables. A stream holding anything
 * keeps all five, empties included: which of the four is empty is the reading,
 * and a pool that vanished when it emptied would look like a build without it.
 */
function streamSection(section) {
  const box = el("div", "sy-stream");
  const top = el("div", "sy-top");
  // Null names the rows no configured stream claimed — see [streamSections].
  // Said in words rather than left blank: an unlabelled section reads as a
  // rendering fault, and this one is a finding.
  top.appendChild(el("span", "sy-name", section.stream || "not attributed to a stream"));
  if (section.phase) {
    const meta = el("span", "sy-meta",
      `${section.phase}${section.phaseForSec != null ? ` for ${fmtDur(section.phaseForSec)}` : ""}`);
    // THE PHASE WORD'S OWN DEFINITION. `rotating` reads as a stall and is not,
    // and its glossary entry had nowhere to hang: the word was drawn as bare
    // text, so "rotating for 58m" was a sentence a reader could only guess at.
    const phaseWhy = term(section.phase);
    if (phaseWhy) meta.title = phaseWhy;
    top.appendChild(meta);
  }
  box.appendChild(top);

  // WHAT IT IS RIDING — one line, and the only one. A rotating stream with an
  // empty roster is the exception: it is not riding anything, it is waiting on
  // the fitness pass, and that is the reading that changes what an operator
  // does next.
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

  // THE CONFIG HALF — see the reading order in this function's head, and
  // [jobsTable] for why it is one table and not the two it was.
  //
  // NO SUB-HEAD over it, unlike the pools below: the table names its own two
  // halves in the band above its columns, and a heading reading "what it may
  // spend, and when it comes due" six pixels above headings reading `what it
  // may spend` and `when the past is re-read` is the same label twice.
  const jobs = jobsTable(section.jobs);
  if (jobs) {
    const part = el("div", "sy-part");
    part.appendChild(jobs);
    box.appendChild(part);
  }

  // …AND WHAT IT IS HOLDING.
  if (!section.holding) {
    // …and NOT under the line above, which has already said why this stream is
    // holding nothing. "Waiting on the first `prime`" followed by "holding
    // nothing" is one fact told twice, and the second telling is the weaker of
    // the two — it names the symptom the first one just explained.
    if (!waiting) box.appendChild(el("div", "sy-sub sy-quiet", "holding nothing right now — no visit, no tail"));
    return box;
  }
  // The one labelled part left. `sy-sub-h` is a level BELOW the card's own
  // `sy-h` bands: those separate subjects — the mirror, the streams, the
  // pipeline — and this separates one subject's answers.
  const pools = el("div", "sy-part");
  pools.appendChild(el("p", "sy-sub-h", "what it is holding"));
  for (const group of section.groups) pools.appendChild(poolBlock(group));
  box.appendChild(pools);
  return box;
}

/**
 * WHICH STORE CALLS ARE OUT, AND WHOSE — the half of a wedge the pipeline row
 * could never say.
 *
 * ## Why it is its own part of the card
 *
 * The pipeline row above it says `2 of 2 worker(s) in a batch, oldest 794s`.
 * That is where every investigation of a stalled mirror got to and stopped: a
 * batch pass makes three different store calls, against three different engine
 * paths, with three different remedies, and the row reports all three as one
 * number. This names the call.
 *
 * It is beside the pipeline rather than inside it because its subject is the
 * STORE and not ingest: the negentropy pager, the healer, the retraction audit
 * and the monitor's verdict reads are all on it, and a section folded under
 * "the pipeline" would read as ingest's own traffic.
 *
 * ## Three tables' worth of question, in one part
 *
 *  1. **which calls** — longest-running first, which is the router's order and
 *     the opposite of a stream's legs: holding a relay for an hour is how this
 *     mirror works, while a store call that has not come back is the anomaly.
 *  2. **whose** — one row per subsystem, whoever holds the most first. This is
 *     the answer to "what is filling the engine's queue", which no number on
 *     this page could give while every plane hit one anonymous store.
 *  3. **the shape** — the outstanding set banded by age. A thousand calls all
 *     under a second is a busy router; eight hundred under a second with two
 *     past fifteen minutes is the finding, and the total cannot tell them apart.
 *
 * Drawn only where the document carries the section: a router too old to book
 * its calls publishes none, and "this router does not say" is not the same
 * claim as "nothing is outstanding".
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

  // THE LIFETIME LINE, and the reason `failed` is on it at all: a store the
  // schema has drifted under fails calls in milliseconds, where one that has
  // stopped answering shows up as an age with nothing failing. Those are
  // opposite faults and the second is the one the tables below describe, so the
  // first needs somewhere to be seen.
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
  // WHERE THE LINE IS, and only when it is not where a reader would assume.
  // The rows below are marked at the ROUTER's `SYNC_STORE_SLOW_SEC`, so a
  // deployment that moved it would otherwise have a card marking rows by a
  // rule nothing on it states — while saying so on every poll of every
  // default deployment is a line to read past forever.
  if (s.stuckSec !== STUCK_CALL_SEC) fact(`marked past ${fmtDur(s.stuckSec)}`, "slowAfterSec");
  box.appendChild(line);

  if (!s.outstanding) {
    // EMPTY IS AN ANSWER here, exactly as it is for a pool: a router between
    // batches genuinely has nothing out, and a part that vanished when it
    // emptied would look like a build without it.
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
    // The router's own partition failing to close. Reported rather than
    // silently smoothed, on `accountedFor`'s terms — a card that does not add
    // up to the counts on it must say so instead of letting a reader subtract.
    if (!s.accountedFor) {
      const bad = el("div", "sy-tr-note warn", "these bands do not sum to `outstanding` — see `ages` in the JSON");
      bad.title = term("ages");
      part.appendChild(bad);
    }
  }
  box.appendChild(part);
  return box;
}

/**
 * The outstanding calls themselves.
 *
 * `caller` and `op` lead because together they are the identification — which
 * subsystem, and which store method — and `asked` is next because it is what
 * makes the row actionable: "2048 id(s)" beside `ingest.dedup existingIds` is
 * the whole sentence three investigations had to guess at.
 *
 * `waiting` is last and is the one column that is not about this call: it is
 * how many calls this process already had out when this one went, which is the
 * only part of "slow store or long queue" our side of the wire can measure.
 */
function callsTable(rows) {
  const { scroll, table } = headedTable([
    ["caller", "caller", false],
    ["op", "op", false],
    ["asked for", "asked", false],
    ["running", "elapsedSec", true],
    ["others out", "outstandingAtIssue", true],
  ]);
  for (const r of rows) {
    // Past the threshold the log warns at, and off nothing else — the page and
    // the log must not carry two definitions of the same word.
    const tr = el("tr", r.hot ? "hot" : null);
    tr.appendChild(el("td", null, r.caller));
    tr.appendChild(el("td", null, r.op));
    // A call that carries no filter says so rather than leaving the cell
    // blank: several ops genuinely have none, and an empty cell reads as a
    // report that declined to say.
    tr.appendChild(el("td", null, r.asked || "no filter"));
    tr.appendChild(el("td", "n", fmtDur(r.elapsedSec)));
    tr.appendChild(el("td", "n", r.outstandingAtIssue != null ? fmt(r.outstandingAtIssue) : "—"));
    table.appendChild(tr);
  }
  return scroll;
}

/**
 * One row per subsystem — the answer to "whose requests are these".
 *
 * The row CLOSES: `issued = answered + failed + cancelled + out`, which is why
 * every column is drawn including its zeroes. A reader checking a row against
 * itself is the point; a table that hid its zeroes would make the identity
 * unverifiable and the arithmetic look wrong.
 */
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
    // Nothing out is no age — and a `0s` there would read as a call that had
    // just started, which is the opposite of a caller sitting idle.
    tr.appendChild(el("td", "n", r.oldestOutstandingSec != null ? fmtDur(r.oldestOutstandingSec) : "—"));
    tr.appendChild(el("td", "n", short(r.issued)));
    tr.appendChild(el("td", "n", short(r.returned)));
    tr.appendChild(el("td", `n${r.failed ? " hot" : ""}`, fmt(r.failed)));
    tr.appendChild(el("td", "n", fmt(r.cancelled)));
    table.appendChild(tr);
  }
  return scroll;
}

/**
 * The outstanding set by age, as one line rather than a table.
 *
 * A line because the bands are a SHAPE and the shape is legible in one glance:
 * six numbers in a column invite reading each one, where `< 1s 812 · 15m+ 2` is
 * the finding itself. Empty bands are dropped here — the router publishes them
 * all so the partition can be checked, and drawing five zeroes to reach the one
 * band that matters is the noise this line exists to avoid.
 */
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
  // WHAT THIS POOL IS, on the heading rather than under it. Four descriptions
  // as visible text is the paragraph-per-stream shape this card was rebuilt to
  // get rid of; the meaning belongs on the mark, like every other one here.
  name.title = group.what;
  head.appendChild(name);
  // THE COUNT, AND NOT THE DENOMINATOR. It used to read `12 of 412`, on all
  // five headings, three lines under a roster line that had just said 412 —
  // the same number five times inside one section. Out of how many is a
  // property of the section, said once at its top; how many are in this pool
  // is the property of the heading.
  head.appendChild(el("span", "sy-pool-n", fmt(group.rows.length)));
  // The shared stage word, lifted out of a column that would have repeated it
  // on every row.
  if (group.doing) {
    const doing = el("span", "sy-pool-doing", group.doing);
    doing.title = term("doing");
    head.appendChild(doing);
  }
  box.appendChild(head);
  // EMPTY IS AN ANSWER, and it is drawn rather than skipped: "no relay is
  // auditing right now" is a finding, and a pool that vanished when it emptied
  // would look exactly like a build with no such pool.
  if (!group.rows.length) {
    box.appendChild(el("div", "sy-sub sy-quiet", "none right now"));
    return box;
  }
  box.appendChild(poolTable(group));
  return box;
}

/**
 * The rows themselves.
 *
 * `held`, `events` and `quiet` are on every table because they read the same
 * way in every pool: how long we have had this socket, what has come down it,
 * and how long since anything did. The last is the one that decides — on a
 * visit it separates a real backlog from a walk that has stopped, and on a
 * tail a relay with nothing to say from a subscription that died upstream —
 * which is why the row is coloured off it and off nothing else.
 */
function poolTable(group) {
  const cursors = group.rows.some((r) => r.pagingUntil != null);
  // ONE CLOCK FOR THE TABLE, read once. It was read per row, which is a
  // `Date.now()` per held relay on a list that is three hundred rows on this
  // deployment — and, more to the point, let two rows in one table be measured
  // against two different instants. It only decides a LABEL's precision, so the
  // cost of that was never a wrong date; it was a table that could draw the
  // same cursor two ways.
  const nowSec = Date.now() / 1000;
  const columns = [["relay", null, false]];
  if (group.streams) columns.push(["stream", null, false]);
  // Only where the group's own rows disagree — otherwise the word is in the
  // heading above and this column would be one value repeated.
  if (!group.doing) columns.push(["doing", "doing", false]);
  if (cursors) columns.push(["back to", "pagingUntil", false]);
  columns.push(["held", "heldForSec", true], ["events", "events", true], ["quiet", "quietForSec", true]);
  const { scroll, table } = headedTable(columns);
  for (const r of group.rows) {
    // Quiet past the threshold is the one row shape worth colouring — see
    // STUCK_LEG_SEC for why the floor is ten minutes and not less.
    const tr = el("tr", r.quietForSec >= STUCK_LEG_SEC ? "hot" : null);
    const url = el("td", "u");
    // The url in its own LTR isolate inside the rtl cell: the cell being rtl is
    // what puts the ellipsis on the LEFT, where a path-suffixed alias differs,
    // and the isolate is what stops that direction rewriting the address.
    const inner = el("span", null, r.short);
    inner.dir = "ltr";
    url.appendChild(inner);
    url.title = r.relay;
    tr.appendChild(url);
    if (group.streams) tr.appendChild(el("td", null, r.stream || "—"));
    // A leg with no slot says so here rather than leaving the column blank: the
    // stage IS the answer for most of a fan-out's workers.
    if (!group.doing) tr.appendChild(el("td", null, r.doing || (r.slotless ? "not on a transfer slot" : "—")));
    if (cursors) {
      // The reader's own clock, and it only decides the LABEL's precision — a
      // skewed one draws a coarser or finer cursor, never a wrong one.
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
 * WHERE EACH PRIME RELAY STANDS — one row per relay a stream is allowed to
 * dial, and what the sync of it has actually reached.
 *
 * ## This is not the per-relay table that was removed
 *
 * [coveragePanel] below replaced a 10,462-row list of BANDS behind a filter
 * box, and that was right: its rows were spans, its subject was "how deep is
 * the mirror", and a distribution answers that in one glance where the table
 * needed all 10,462 read. This asks the opposite question — *which relays are
 * being synced at all* — and a distribution cannot answer it, because the two
 * states that matter most (a relay never reached, and one refused on every
 * visit) have NO BAND and so appear in no distribution. Its subject is the
 * ROSTER, not the band file, which is also why its denominator is honest where
 * the coverage card's cannot be.
 *
 * ## Worst first, and the counts are the key to it
 *
 * The order is the document's — refused, then never started, then paging, then
 * complete — because an operator opens this because something is wrong, and a
 * table sorted by relay name would put the four broken rows on page nine. The
 * counts above the table are published WHOLE even when the row list is cut, so
 * a truncation can never be read as a smaller problem.
 *
 * ## A row is a (relay, STREAM) pair
 *
 * Because that is what has a status: one relay can be complete for `indexers`
 * and never started for `contentViaOutbox`, and a row per relay would have to
 * invent a verdict over the two. The stream column is what makes that legible;
 * on a one-stream deployment it is one repeated word and harmless.
 */
function relayPanel(d) {
  const r = relayStatusOf(d.relays);
  if (!r) return null;
  const box = el("div");

  // TWO HEADLINES, ONE PER AXIS, and the order is the reading: how current we
  // are first, because that is the question an operator arrives with, then how
  // far the backfill behind it has got. Both off the document's own partitions
  // and never re-counted from `rows`, which is cut — see [relayStatusOf].
  const head = el("div", "sy-pool-head");
  head.appendChild(el("span", "sy-pool-name",
    `${fmt(r.current)} of ${fmt(r.pairs)} pair(s) current`));
  for (const c of r.freshness) head.appendChild(chip(`${fmt(c.pairs)} ${c.label}`, c.pairs ? c.tone : null, term("behind")));
  box.appendChild(head);

  const past = el("div", "sy-pool-head");
  past.appendChild(el("span", "sy-pool-name", "the past behind it"));
  for (const c of r.chips) past.appendChild(chip(`${fmt(c.pairs)} ${c.label}`, c.pairs ? c.tone : null, term("syncStatus")));
  box.appendChild(past);

  const nowSec = Date.now() / 1000;
  // THREE GROUPS, because the row answers three questions and a flat eight
  // columns made a reader work out which cell belonged to which. `headedTable`
  // already draws the band and rules the divides — the jobs table has used it
  // since the budgets and the schedule became one table.
  const { scroll, table } = headedTable(
    [["relay", null, false], ["stream", null, false],
     ["newest", "behindSec", false],
     ["status", "syncStatus", false], ["back to", "coveredFrom", false], ["verified", "verifiedAgoSec", true],
     // ONE COLUMN, not three. The terms and the relay's own sentence answer
     // the same question and the sentence is the only cell here that WRAPS —
     // split across columns they pushed the table wider than its card and cut
     // the sentence off at the right edge, which is the one thing on the row
     // that says what to do.
     ["terms", "negentropy", false]],
    false,
    [["", 2], ["how current", 1], ["how far back", 3], ["on what terms", 1]],
  );
  for (const row of r.rows) {
    const tr = el("tr", row.hot ? "hot" : null);
    const url = el("td", "u");
    // The url in its own LTR isolate inside the rtl cell — see [poolTable],
    // which is where this rule is explained.
    const inner = el("span", null, row.short);
    inner.dir = "ltr";
    url.appendChild(inner);
    url.title = row.relay;
    tr.appendChild(url);
    tr.appendChild(el("td", null, row.stream || "—"));

    // HOW CURRENT — the age of the newest thing we hold, and whether anything
    // is listening for the next one. The tail belongs HERE and not beside the
    // status: it is what carries the present between visits, so old content on
    // a tailed pair is a quiet relay rather than a mirror falling behind, and
    // that is the whole difference between the two readings.
    const fresh = el("td");
    // fmtPeriod, not fmtDur: these are CALENDAR ages, and the phase clock
    // renders nine days as `216h 0m` — a number a reader has to divide before
    // it means anything, which is the complaint fmtPeriod already exists for
    // one table over.
    fresh.appendChild(el("span", null, row.behindSec != null ? `${fmtPeriod(row.behindSec)} old` : "—"));
    if (row.tailed) fresh.appendChild(chip("live", "live", term("tailed")));
    fresh.title = term("behind");
    tr.appendChild(fresh);

    // HOW FAR BACK — the backfill's own axis, and nothing on it says anything
    // about the present.
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
    // The number to watch on a `paging` row: unchanged between two polls means
    // the walk is not advancing, and nothing else here says so per relay.
    tr.appendChild(el("td", "sy-at", row.coveredFrom != null ? cursorOf(row.coveredFrom, nowSec) : "—"));
    // The last completed reconcile. `—` where none has ever run, which is not a
    // fault: the clock is a week in the shipped example and a young relay has
    // not had one.
    tr.appendChild(el("td", "n", row.verifiedAgoSec != null ? `${fmtPeriod(row.verifiedAgoSec)} ago` : "—"));

    // ON WHAT TERMS — what this relay lets us do, which decides what the two
    // columns to the left can ever reach. A relay the monitor measured as
    // refusing a NEG-OPEN can never have its history reconciled, so a `paging`
    // row beside `no neg` is one that will not settle by itself; a width cap is
    // why its asks go out in chunks.
    const terms = el("td", "sy-said");
    if (row.negentropy === true) terms.appendChild(chip("neg", "live", term("negentropy")));
    if (row.negentropy === false) terms.appendChild(chip("no neg", "warn", term("negentropy")));
    if (row.kindCap != null) terms.appendChild(chip(`≤${row.kindCap} kinds`, "busy", term("kindCap")));
    // The relay's own sentence LAST, after the terms we measured: those are
    // standing facts about the relay and this is what it said the last time it
    // turned us away, which is the detail a reader lands on.
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
      `${fmt(r.omitted)} more pair(s) not listed — the counts above are complete, and every row naming a fault is above the cut`));
  }
  return box;
}

/**
 * HOW DEEP THE COVERAGE IS — every walked band at once, as how many relays
 * reach each point in the frame.
 *
 * This replaces the per-relay table. That table was 10,462 rows behind a filter
 * box, and answering "is the mirror deep or only recent" from it meant reading
 * every row. The distribution answers it in one glance and the per-relay spans
 * are still in the document for anything that needs them.
 */
const DEPTH_BUCKETS = 72;

function coveragePanel(d) {
  const streams = d.streams || [];
  if (!streams.length || d.from == null || d.to == null) return null;
  const box = el("div");
  const settled = streams.reduce((a, s) => a + (s.reconciled || 0), 0);
  const open = streams.reduce((a, s) => a + (s.paged || 0), 0);
  const facts = [];
  if (d.relays != null) facts.push(`${fmt(d.relays)} relay(s) on ${fmt(d.hosts || 0)} host(s) have coverage recorded`);
  // A DIFFERENT DENOMINATOR, said out loud. `reconciled` and `paged` are per
  // stream-and-relay, so one relay two streams both walked counts twice and
  // these do NOT add up to the relay count above them. Naming them "walk(s)"
  // is what stops the two numbers reading as parts of one whole.
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
    // A bucket nothing reaches keeps its 1px floor from the stylesheet, so the
    // gap reads as a gap rather than as the strip ending early.
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
 * IS THE SYNC WORKING, AND HOW FAR HAS IT GOT — and nothing else.
 *
 * ## Marks, not sentences
 *
 * The live half of this card used to be prose: a paragraph of clauses per
 * stream, a second for the accounting, a third naming the held relays, and a
 * fourth per processor. Measured on a two-stream deployment it came to 520
 * words, and it had the failure modes prose always has against a table of
 * numbers — it is read serially, two streams cannot be compared without
 * holding both paragraphs in your head, and a number inside a clause cannot be
 * scanned down a column.
 *
 * So: every quantity is a mark, and the MEANING lives on the mark's `title`,
 * taken from the glossary the document already ships (see [term]). The card
 * spends visible words on exactly two things — damage, and the one caveat
 * under the coverage strip.
 *
 * ## What this card stopped saying, and why
 *
 * It used to carry the worker/transfer pair, a per-leg table, an eight-way
 * outcome partition, a legend, a filter box and one row per relay — 10,462 of
 * them on this deployment. Measured against the two questions above, almost
 * none of it answered either.
 *
 * The test that removed them: A MARK THAT READS THE SAME ON A HEALTHY SYSTEM
 * EVERY TIME IS NOT A MARK. `100 of 512 workers downloading` was the clearest
 * case — both numbers are config ceilings (`concurrency` and `admissionWidth`),
 * so every busy stream on every deployment renders exactly that, forever. It
 * was the implementer's argument for splitting the two gates, printed where an
 * operator has to scroll past it. That argument belongs in the KDoc, where it
 * already is.
 *
 * The per-relay table went for the opposite reason: it varied so much that
 * reading it was the work. "Is coverage deep or only recent" needed all 10,462
 * rows; it is now one distribution.
 *
 * ## THE STREAM IS THE SECTION, and that is the second thing this card lost
 *
 * What survived the cut above was still drawn per PANEL rather than per
 * subject: a *streams* list of one-liners, a *pools* panel that repeated every
 * one of those lines and hung the tables under it, and two card-level tables
 * whose first column was the stream's name written once per job. Four walks of
 * `progress.streams`, four places to look up one stream, and the reader doing
 * the join. On a two-stream deployment the word `content` appeared eleven
 * times and its roster nine.
 *
 * Worse than the repetition, the two roster lines came off DIFFERENT members —
 * one counted the drawn rows, the other read `liveHeld` — so a stream could be
 * told it was holding 288 tails and 12 tails, four lines apart, and both
 * numbers were honestly derived.
 *
 * So a stream is one bordered section now ([streamSection]) and appears in
 * exactly one place, with the join done once in [streamSections] where it can
 * be checked. What is left at CARD level is what is not about any one stream:
 *
 *  - **the status line** — is the process working, and where is the constraint.
 *  - **the mirror's two lines** — how big the pool is and how its work divides,
 *    which is the only thing the mirror-wide cut of the tables ever said that a
 *    stream's own cannot. Drawn above more than one stream and not at all
 *    below that, where it would be the single stream's line again.
 *  - **the pipeline** — ingest, the healer, the push, and the pool's own
 *    lifetime counters. The pool row stopped repeating its roster split there;
 *    see `processorFact`.
 *  - **the coverage strip** — where this mirror has WALKED, which is
 *    accumulated over every run and is not a property of a live stream at all.
 *
 * ## What moved OUT of it
 *
 * The alias fold, the stability gate and fitness are the monitor's own PAGE
 * now, with the corpus tree that describes their subject. They were here
 * because they used to arrive in the same array, and the card asked two
 * questions at once: *is the mirror keeping up* is answered by streams, queue
 * depth and ingest, while *which relays may we dial at all* is answered by a
 * round-up and three passes on a six-hour clock whose unit is a url and whose
 * output is a signed record. An operator arrives with one of those questions,
 * never both.
 *
 * They no longer arrive in one array either: each plane keeps its own
 * `Processors` and publishes its own document, so the rule that used to sort
 * them at render time (`splitProcessors`) has nothing left to sort.
 *
 * Nothing was removed from `/stats.json`. Every number this card stopped
 * drawing is still published; what changed is which of them a person is made
 * to read, and where.
 */
function syncCard(section) {
  const d = (section && section.data) || {};
  const streams = d.streams || [];
  const progress = d.progress;
  const card = el("div", "card");
  // `relays` counts as state here for [stats.html]'s reason: a router whose
  // whole roster is refused has no walked streams and no progress worth the
  // word, and the sub-heading would say the document is empty over a table
  // naming every relay it could not sync.
  cardHead(card, "Sync coverage", streams.length || progress || d.relays ? null : "No sync state in this document.", section);
  // The glossary this card's marks explain themselves with, before anything
  // below draws. It is the document's own, so a chip cannot describe a member
  // in words the router would not use — see `term` and `SyncVocabularyTest`.
  setTerms(d.terms);
  // The stage totals this document carries, handed to the module that draws the
  // ingest row — the same shape as `setTerms` above, and for the same reason.
  // That row shows the DELTA against the previous document, so the shift has to
  // happen once per document rather than once per card.
  setStages(progress?.health?.stages);
  if (progress) card.appendChild(statusRow(progress));

  // ONE SECTION PER STREAM, and everything about a stream inside it. The join
  // — held rows, budgets and schedule under the stream that owns them — is
  // [streamSections]', not this loop's: it is the half that can drop a row
  // silently.
  // COLLECTED ONCE and grouped twice: the summary and the sections are the same
  // held rows, so walking the document per cut would both allocate a second
  // object per held relay and let the two disagree.
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
  // WHAT NO SECTION CAN ACCOUNT FOR. Zero from this router — a row IS a worker
  // or a tail, and both sets are published whole — and drawn when it is not,
  // because a card that adds up to less than the counts on it must say so
  // rather than let a reader do the subtraction.
  //
  // At CARD level and off the read itself, not inside the mirror's summary: the
  // summary is withheld below two streams, and a truncation that disclosed
  // itself only on multi-stream deployments would be worse than one that never
  // disclosed at all.
  if (held.omitted) {
    card.appendChild(el("p", "sy-sub",
      `${fmt(held.omitted)} held relay(s) the router did not name — nothing says which stream or pool they are in`));
  }

  // The half of the background work that moves EVENTS. The half that decides
  // which relays may be dialled at all is its own card — see `monitorCard`.
  const background = backgroundPanel((progress && progress.processors) || []);
  if (background) {
    card.appendChild(el("p", "sy-h", "the pipeline"));
    card.appendChild(background);
  }

  // …AND WHAT THE STORE IS DOING WITH THEM. Under the pipeline because it is
  // what the pipeline is waiting on, and its own part because its subject is
  // the store rather than ingest — the pager, the healer, the audit and the
  // monitor's verdict reads are all on it too.
  const store = storePanel(progress);
  if (store) {
    card.appendChild(el("p", "sy-h", "the store"));
    card.appendChild(store);
  }

  // BEFORE the coverage distribution, and the order is the reading: this says
  // which relays are being synced, that one says how deep the ones that are
  // have got. A depth chart over a roster half of which is refused is a chart
  // of the survivors.
  const relays = relayPanel(d);
  if (relays) {
    card.appendChild(el("p", "sy-h", "prime relays"));
    card.appendChild(relays);
  }

  const coverage = coveragePanel(d);
  if (coverage) {
    card.appendChild(el("p", "sy-h", "coverage so far"));
    card.appendChild(coverage);
    // THE FRAME IS NOT A TARGET, and this is the one caveat worth the words:
    // these filters carry no lower bound, so "full" is not a defined state and
    // a deep strip means "as far back as anything here reaches", not "done".
    card.appendChild(el("p", "card-sub",
      `Frame starts ${dayOf(d.from)} — the oldest span anything here reaches. Not a target: these filters carry ` +
      `no lower bound, so depth means "as deep as anything reaches", not "finished".`));
  }
  return card;
}


export { syncCard };
