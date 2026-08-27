// THE MIRROR'S CARDS — what each stream is doing, what the background work is
// doing, and how far the walk has got.
//
// Lifted out of the relay's stats.html unchanged, comments included, when the
// sync process started serving its own page. Every judgement these draw was
// already in `shared/sync.js` — the split moved the RENDERING to the service
// that produces the numbers, and changed neither.

import { cardHead, dayOf, el, fmt, fmtDur, short } from "../shared/page.js";
import { backgroundPanel, chip, setTerms, term } from "../shared/processors.js";
import { STUCK_LEG_SEC, constraintOf, heldRows, limitsOf, poolsByStreamOf, poolsOf, rotationOf, scheduleOf } from "../shared/sync.js";

/**
 * WHAT THE ROUTER IS DOING RIGHT NOW — one cycle, live, with an outcome.
 *
 * This sits above the coverage bars because it answers the questions asked
 * first and because the coverage card cannot answer any of them. Coverage is
 * accumulated across every run this router has ever made; this is one cycle.
 *
 * ## Marks, not sentences
 *
 * The half of this card that describes the live state used to be prose: a
 * paragraph of clauses per stream, a second for the accounting, a third naming
 * the held relays, and a fourth per processor. Measured on a two-stream
 * deployment it came to 520 words, and it had the failure modes prose always
 * has against a table of numbers — it is read serially, two streams cannot be
 * compared without holding both paragraphs in your head, and a number inside a
 * clause cannot be scanned down a column. The same facts as chips, bars and
 * one table come to 66.
 *
 * So: every quantity is a mark, and the MEANING lives on the mark's `title`,
 * taken from the glossary the document already ships (see [term]). The card
 * spends words on exactly one thing — damage — through `.alarm`.
 *
 * ## Three readings the arrangement is meant to make impossible
 *
 *  - **"16,747/16,752" as progress.** That number counts legs that RETURNED,
 *    including every one that came back unreachable or capped. The stacked bar
 *    is what actually became of them, and it is drawn from members that sum to
 *    the total by construction — a stack cannot be drawn from counts that do
 *    not.
 *  - **"discovered" as "dialled".** Most of the gap between a fan-out's two big
 *    numbers is duplicate urls rather than lost relays, so the url partition is
 *    its own bar above the disposition rather than a clause beside it.
 *  - **A stopped router as a quiet one.** `staleForSec` is a heartbeat, and the
 *    `live` chip is the first mark on the card — a phase frozen mid-`fetching`
 *    is exactly what a dead process leaves behind.
 */

/**
 * WHAT THE ROUTER IS DOING RIGHT NOW — one cycle, live, with an outcome.
 *
 * This sits above the coverage bars because it answers the questions asked
 * first and because the coverage card cannot answer any of them. Coverage is
 * accumulated across every run this router has ever made; this is one cycle.
 *
 * ## Marks, not sentences
 *
 * The half of this card that describes the live state used to be prose: a
 * paragraph of clauses per stream, a second for the accounting, a third naming
 * the held relays, and a fourth per processor. Measured on a two-stream
 * deployment it came to 520 words, and it had the failure modes prose always
 * has against a table of numbers — it is read serially, two streams cannot be
 * compared without holding both paragraphs in your head, and a number inside a
 * clause cannot be scanned down a column. The same facts as chips, bars and one
 * table come to 66.
 *
 * So: every quantity is a mark, and the MEANING lives on the mark's `title`,
 * taken from the glossary the document already ships (see [term]). The card
 * spends words on exactly one thing — damage — through `.alarm`.
 *
 * ## Where the decisions live
 *
 * Not here. Which legs are worth naming, what a bar is a proportion OF, whether
 * a partition divides, whether a health object is drawable — all of that is
 * `/web/shared/sync.js`, because it is the half that can be wrong silently and
 * the half nothing could reach while it was inline. What is left below is DOM
 * and the document's own words. See that module's head for the five bugs that
 * shipped behind a string grep.
 *
 * ## Three readings the arrangement is meant to make impossible
 *
 *  - **"16,747/16,752" as progress.** That number counts legs that RETURNED,
 *    including every one that came back unreachable or capped. The stacked bar
 *    is what actually became of them, and it is drawn from members that sum to
 *    the total by construction — a stack cannot be drawn from counts that do
 *    not.
 *  - **"discovered" as "dialled".** Most of the gap between a fan-out's two big
 *    numbers is duplicate urls rather than lost relays, so the url partition is
 *    its own bar above the disposition rather than a clause beside it.
 *  - **A stopped router as a quiet one.** `staleForSec` is a heartbeat, and the
 *    `live` chip is the first mark on the card — a phase frozen mid-`fetching`
 *    is exactly what a dead process leaves behind.
 */

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
  if (health.eventsPerSec != null) row.append(` · ${fmt(health.eventsPerSec)} events/s into the store`);
  // The four verdicts are different faults with different fixes, and only the
  // ingest one is a fault at all — see BOTTLENECK.
  const constraint = constraintOf(health);
  if (constraint) row.appendChild(chip(constraint.text, constraint.tone, constraint.why));
  // A count, not a list: the document publishes how many, and one is already
  // the whole message.
  if (progress.fatals) row.appendChild(chip(`${fmt(progress.fatals)} fatal error(s)`, "warn", term("fatals")));
  return row;
}

/**
 * EVERY RELAY THIS MIRROR IS HOLDING RIGHT NOW, SPLIT BY WHAT IT IS BEING
 * ASKED FOR — one table per pool.
 *
 * ## Why four tables and not one column
 *
 * It WAS one table, per stream, with a `doing` column. That answered "what is
 * this leg doing" for a leg you had already found, and could not answer the
 * question an operator arrives with: how much of this mirror is keeping up,
 * how much is re-downloading history, how much is reconciling, and how much is
 * just sitting on a socket. Those are four different bills, and reading them
 * off a column meant counting rows by eye — on a list that is 128 rows wide by
 * construction and re-sorted every poll.
 *
 * The split is the router's, not the page's: it publishes `pool` beside
 * `doing` precisely so a reader can group without grepping prose. See
 * `poolsOf`, which is where the grouping and every judgement in it lives.
 *
 * ## Card-level, and cut by stream once there is more than one
 *
 * The panel is drawn off the whole document rather than out of the stream
 * blocks, because the first question is about the mirror. But the budgets are
 * configured PER STREAM — `visitConcurrency`, `maxLiveConcurrency`,
 * `refetchConcurrency`, `negentropyConcurrency` — so the second question is
 * always which stream is spending them, and a `stream` column answers that by
 * making an operator count rows by eye.
 *
 * So above one stream the four tables are drawn once per stream, under a tally
 * line that keeps the mirror-wide comparison the sections give up. Both cuts
 * are the same rows out of one [poolsOf] read (see [poolsByStreamOf]), so the
 * sections sum to the tally by construction. With ONE stream the two cuts are
 * the same four tables, and only the mirror's are drawn.
 *
 * ## What the table drops when it can
 *
 * A column whose every cell reads the same is not a column. The pool's shared
 * `doing` word is lifted into the heading when its rows agree — which is all
 * four pools on most ticks — leaving the audit pool's, whose two stages are a
 * history sweep and a provider's retraction comparison and want telling apart.
 * The cursor is its OWN column rather than a suffix on that one, so lifting
 * the word never takes `back to <date>` with it: on a catch-up that is the
 * only mark that moves.
 */
function poolsPanel(progress) {
  // COLLECTED ONCE and grouped twice: the two cuts are the same rows, and
  // walking the document per cut allocated a second object per held relay for
  // a set that cannot have changed in between.
  const held = heldRows(progress);
  if (!held.rows.length) return null;
  const pools = poolsOf(progress, held);
  const box = el("div");
  box.appendChild(poolLine(pools.totals));
  const perStream = poolsByStreamOf(progress, held);
  if (perStream.length > 1) {
    box.appendChild(poolTally(pools.groups, pools.totals));
    for (const section of perStream) box.appendChild(streamPools(section));
  } else {
    for (const group of pools.groups) box.appendChild(poolBlock(group, pools.totals));
  }
  // What no pool can account for. Zero from this router — a row IS a worker or
  // a tail, and both sets are published whole — and drawn when it is not,
  // because a panel that adds up to less than the counts beside it must say so
  // rather than let a reader do the subtraction.
  if (pools.omitted) {
    box.appendChild(el("div", "sy-sub",
      `${fmt(pools.omitted)} held relay(s) the router did not name — nothing says which pool they are in`));
  }
  return box;
}

/**
 * HOW BIG THE POOL IS — one line, above the tables that divide it.
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
function headedTable(columns) {
  const scroll = el("div", "sy-legs-box");
  const table = el("table", "sy-legs");
  const head = el("tr");
  for (const [label, key, right] of columns) {
    const th = el("th", right ? "n" : null, label);
    // The glossary is the document's own — a column with no member behind it
    // (the stream's name) gets no tooltip rather than an invented one.
    if (key) th.title = term(key);
    head.appendChild(th);
  }
  table.appendChild(head);
  scroll.appendChild(table);
  return { scroll, table };
}

/**
 * WHAT EACH STREAM MAY SPEND, and what it has spent.
 *
 * The pool is one engine shared by every stream, so "how much CPU may this
 * stream take" is not answerable by looking at any relay — it is a property of
 * the admission gates, and it lived only in the config file until it was
 * published. A table because it is a comparison: the interesting reading is
 * one stream's share against another's.
 *
 * `deferred` is the column that makes the rest actionable. In use == cap is
 * not a fault on its own; in use == cap with work being turned away is the cap
 * biting, and that is the only row shape here worth a colour.
 */
function limitsPanel(progress) {
  const rows = limitsOf(progress);
  if (!rows.length) return null;
  const { scroll, table } = headedTable([["stream", null, false], ["job", "job", false],
                                        ["may run", "streamCap", true], ["in use", "inUse", true],
                                        ["turned away", "deferred", true]]);
  for (const r of rows) {
    const tr = el("tr", r.biting ? "hot" : null);
    tr.appendChild(el("td", null, r.stream || "—"));
    tr.appendChild(el("td", null, r.label));
    // UNCAPPED IS NOT ZERO, and the word is drawn rather than an empty cell:
    // a blank here would read as a cap of nothing, which is the opposite.
    tr.appendChild(el("td", "n", r.streamCap != null ? fmt(r.streamCap) : "uncapped"));
    tr.appendChild(el("td", "n", r.inUse != null ? fmt(r.inUse) : "—"));
    tr.appendChild(el("td", "n", fmt(r.deferred)));
    table.appendChild(tr);
  }
  return scroll;
}

/**
 * WHEN THE SCHEDULED RE-READS COME DUE — the audit's clock and the re-fetch's.
 *
 * The counters next door say the work HAPPENED. Only this says it happened
 * because it was due: an ask leaves `waiting` by its clock running out and by
 * nothing else, so a `waiting` column that drains at the period is the
 * schedule working, and one that holds steady while the audit counters climb
 * is a rule being broken.
 *
 * `never run` is drawn as its own column for the reason the router counts it
 * apart: an ask with no completed pass is due by definition, which is the
 * whole of a fresh deployment. Folded into `due` it would make a mirror that
 * has never audited anything look identical to one whose period has elapsed.
 */
function schedulePanel(progress) {
  const rows = scheduleOf(progress);
  if (!rows.length) return null;
  const { scroll, table } = headedTable([["stream", null, false], ["job", "job", false],
                                        ["every", "everySec", true], ["due", "due", true],
                                        ["never run", "neverRun", true], ["waiting", "waiting", true],
                                        ["next in", "nextInSec", true]]);
  for (const r of rows) {
    const tr = el("tr", r.backedUp ? "hot" : null);
    tr.appendChild(el("td", null, r.stream || "—"));
    tr.appendChild(el("td", null, r.label));
    tr.appendChild(el("td", "n", r.everySec != null ? fmtPeriod(r.everySec) : "—"));
    tr.appendChild(el("td", "n", fmt(r.due)));
    tr.appendChild(el("td", "n", fmt(r.neverRun)));
    tr.appendChild(el("td", "n", fmt(r.waiting)));
    // Nothing waiting is no countdown — every ask is already due, which the
    // three columns to the left have just said.
    tr.appendChild(el("td", "n", r.nextInSec != null ? fmtPeriod(r.nextInSec) : "—"));
    table.appendChild(tr);
  }
  return scroll;
}

/**
 * THE MIRROR-WIDE SPLIT IN ONE LINE — how many units are in each pool, across
 * every stream.
 *
 * Drawn only when the tables below have been cut by stream, and drawn for
 * exactly that reason: the per-stream sections answer "which stream is
 * re-fetching" and give up the answer to "how much of this mirror is", which
 * was the whole point of four tables. One line restores it, above the sections
 * whose counts add up to it.
 *
 * Off the same `groups` the tables are drawn from, never off `visiting` or
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
 * ONE STREAM'S FOUR POOLS — its name, its own share of the roster, and the
 * same four tables the mirror draws.
 *
 * The section exists because the caps do: a stream is given its own
 * `visitConcurrency`, `maxLiveConcurrency`, `refetchConcurrency` and
 * `negentropyConcurrency`, and "is this stream spending what it was given" is
 * unanswerable from a shared table with a stream column. Here the count in
 * each heading is the number the matching cap is set against, and the line
 * above them is what it is a share of.
 *
 * A stream holding NOTHING still gets its section, four empty tables and all.
 * That is the shape of a starved stream and of a stream whose schedule has not
 * come due, and both are answers — a section that vanished when it emptied
 * would leave an operator scrolling for a stream that is on the card.
 */
function streamPools(section) {
  const box = el("div", "sy-stream-pools");
  const head = el("div", "sy-top");
  // Null names the rows no configured stream claimed — see [poolsByStreamOf].
  // Said in words rather than left blank: an unlabelled section reads as a
  // rendering fault, and this one is a finding.
  head.appendChild(el("span", "sy-name", section.stream || "not attributed to a stream"));
  head.appendChild(poolLine(section.totals, "this stream's roster"));
  box.appendChild(head);
  for (const group of section.groups) box.appendChild(poolBlock(group, section.totals));
  return box;
}

/** One pool: its heading, how much of the pool is in it, and the relays. */
function poolBlock(group, totals) {
  const box = el("div", "sy-pool");
  const head = el("div", "sy-pool-head");
  const name = el("span", "sy-pool-name", group.label);
  // WHAT THIS POOL IS, on the heading rather than under it. Four descriptions
  // as visible text is the paragraph-per-stream shape this card was rebuilt to
  // get rid of; the meaning belongs on the mark, like every other one here.
  name.title = group.what;
  head.appendChild(name);
  // OUT OF HOW MANY, on every heading — and out of UNITS, because a row here
  // is one stream's work on one relay and not the relay itself.
  head.appendChild(el("span", "sy-pool-n",
    totals.units ? `${fmt(group.rows.length)} of ${fmt(totals.units)}` : `${fmt(group.rows.length)}`));
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
    box.appendChild(el("div", "sy-sub", "none right now"));
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
      const at = el("td", "sy-at", r.pagingUntil != null ? cursorOf(r.pagingUntil, Date.now() / 1000) : "—");
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
 * WHAT ONE STREAM IS DOING: the phase word, how long it has been in it, and the
 * two numbers a rotation has — the relays it is riding and the tails it holds.
 *
 * There used to be four more marks here (a pass meter, an ETA, how far back the
 * walk had reached, and the pass's own disposition), drawn from the fan-out
 * that walked a stream's relay list in cycles. Every stream rides the visit
 * pool now: there is no pass to be a fraction OF, and where each relay has got
 * to is per relay — the pool tables below, which is the question those marks
 * were standing in for.
 *
 * The per-relay rows used to hang here, one list per stream with a `doing`
 * column. They are one panel for the whole card now, cut by POOL first — which
 * is the split that answers what this mirror is spending itself on, and the
 * one a column of prose could not — and then by stream inside it wherever
 * there is more than one. See [poolsPanel].
 */
function streamBlock(s) {
  const box = el("div", "sy-stream");
  const top = el("div", "sy-top");
  top.appendChild(el("span", "sy-name", s.name));
  const meta = el("span", "sy-meta",
    `${s.phase || "starting"}${s.phaseForSec != null ? ` for ${fmtDur(s.phaseForSec)}` : ""}`);
  // THE PHASE WORD'S OWN DEFINITION. `rotating` reads as a stall and is not,
  // and its glossary entry had nowhere to hang: the word was drawn as bare
  // text, so "rotating for 58m" was a sentence a reader could only guess at.
  const phaseWhy = term(s.phase);
  if (phaseWhy) meta.title = phaseWhy;
  top.appendChild(meta);
  box.appendChild(top);
  // A ROTATING STREAM'S WHOLE STATE, and the row was the phase word alone until
  // it was published: a stream riding four hundred relays and one riding none
  // drew the identical line.
  const rot = rotationOf(s);
  if (rot) {
    const line = el("div", "sy-sub");
    if (rot.waiting) {
      // Drawn LOUD, because it is the one rotating state that is not the
      // stream working: nothing is certified for it yet, and the fitness row
      // in the section below is where the wait is.
      const none = el("s", null, "no certified relay yet — waiting on the fitness pass to sign the first `prime`");
      none.title = term("roster");
      line.appendChild(none);
    } else {
      const riding = el("span", null, `riding ${fmt(rot.roster)} relay(s)`);
      riding.title = term("roster");
      line.appendChild(riding);
      if (rot.tails != null) {
        const tails = el("span", null, ` · ${fmt(rot.tails)} holding a live tail`);
        tails.title = term("liveHeld");
        line.appendChild(tails);
      }
    }
    box.appendChild(line);
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
 * ## What is left
 *
 * One status line (working?), one block per stream (progress), one table per
 * POOL (which relays, and what each is being asked for), one line per
 * pipeline job (where a slow mirror is actually diagnosed — a full ingest queue
 * means every download is backpressured behind it), and one depth strip (how
 * far back the coverage actually goes).
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
  cardHead(card, "Sync coverage", streams.length || progress ? null : "No sync state in this document.", section);
  // The glossary this card's marks explain themselves with, before anything
  // below draws. It is the document's own, so a chip cannot describe a member
  // in words the router would not use — see `term` and `SyncVocabularyTest`.
  setTerms(d.terms);
  if (progress) card.appendChild(statusRow(progress));

  const lanes = (progress && progress.streams) || [];
  if (lanes.length) {
    card.appendChild(el("p", "sy-h", "streams"));
    for (const s of lanes) card.appendChild(streamBlock(s));
  }

  // WHAT EACH STREAM MAY SPEND on those pools, and WHEN the scheduled jobs
  // come due. Under the pools because both are read after "what is running
  // right now": the caps explain a pool that is smaller than its work, and the
  // schedule explains a pool that is empty.
  const budgets = limitsPanel(progress);
  const schedule = schedulePanel(progress);

  // WHICH RELAYS, AND WHAT EACH IS BEING ASKED FOR — the four pools, under the
  // streams because "is it getting anywhere" is read first and "what is it
  // doing right now" immediately after. Drawn off the whole progress document
  // and then cut by stream, because the caps under it are per stream and this
  // is what they are spent on. See [poolsPanel].
  const pools = poolsPanel(progress);
  if (pools) {
    card.appendChild(el("p", "sy-h", "the pools"));
    card.appendChild(pools);
  }
  if (budgets) {
    card.appendChild(el("p", "sy-h", "what each stream may spend"));
    card.appendChild(budgets);
  }
  if (schedule) {
    card.appendChild(el("p", "sy-h", "when the past is re-read"));
    card.appendChild(schedule);
  }

  // The half of the background work that moves EVENTS. The half that decides
  // which relays may be dialled at all is its own card — see `monitorCard`.
  const background = backgroundPanel((progress && progress.processors) || []);
  if (background) {
    card.appendChild(el("p", "sy-h", "the pipeline"));
    card.appendChild(background);
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
