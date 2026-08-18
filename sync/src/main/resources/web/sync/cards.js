// THE MIRROR'S CARDS — what each stream is doing, what the background work is
// doing, and how far the walk has got.
//
// Lifted out of the relay's stats.html unchanged, comments included, when the
// sync process started serving its own page. Every judgement these draw was
// already in `shared/sync.js` — the split moved the RENDERING to the service
// that produces the numbers, and changed neither.

import { cardHead, dayOf, el, fmt, fmtDur, short } from "/web/shared/page.js";
import {
  HELD_SHOWN, STUCK_LEG_SEC, STUCK_PASS_SEC, constraintOf, funnelOf, heldOf, legsOf, measuringOf,
  probeProgress, rotationOf, splitProcessors,
} from "/web/shared/sync.js";

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
 * The glossary the document ships, for the current card's `title` attributes.
 *
 * Set by [syncCard] before anything below draws. The words are the ROUTER's —
 * `SyncVocabulary` publishes them and `SyncVocabularyTest` pins the two
 * directions, no published count without a term and no term without a count —
 * so a mark that explains itself here cannot drift from the member it explains.
 */
let TERMS = {};

/** What the document says a member means, or nothing at all. */
function term(key) {
  return TERMS[key] || "";
}

/** One labelled pill. `tone` is `live`, `busy`, `warn`, or nothing. */
function chip(text, tone, title) {
  const c = el("span", tone ? `chip ${tone}` : "chip", text);
  if (title) c.title = title;
  return c;
}

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
 * EVERY RELAY THIS STREAM IS HOLDING RIGHT NOW, and WHAT EACH ONE IS DOING.
 *
 * The list was five rows behind a `+507 more`, which is the shape that cannot
 * answer the question it exists for: the leg worth finding is by construction
 * not in the healthy head of the list. It is the whole set now — the router
 * caps its own rows at the widest admission gate and still discloses anything
 * past it.
 *
 * `doing` is the column the clocks could never supply. `held 2h 15m, 3 events`
 * is four different faults depending on the stage, and two of them — quiet
 * because negentropy is computing a difference, quiet because a paged walk has
 * stopped delivering — were indistinguishable from every number this row
 * carries. See the `doing` entry in the document's own glossary.
 */
function legsPanel(s) {
  const { rows, more } = legsOf(s.inFlight);
  if (!rows.length) return null;
  const box = el("div");
  // WHERE THE LEGS ARE, before the list itself. On a wide fan-out the shape is
  // the finding — "412 in the guards, 100 paging" is a healthy rotation, and
  // the same 512 all waiting for a slot is a saturated pool.
  const stages = new Map();
  for (const r of rows) stages.set(r.doing || "stage not published", (stages.get(r.doing || "stage not published") || 0) + 1);
  const summary =
    [...stages.entries()].sort((a, b) => b[1] - a[1]).map(([what, n]) => `${fmt(n)} ${what}`).join(" · ");
  box.appendChild(el("div", "sy-sub",
    `${fmt(rows.length)} leg(s) in flight — ${summary}${more ? ` · ${fmt(more)} more the router did not name` : ""}`));

  const scroll = el("div", "sy-legs-box");
  const table = el("table", "sy-legs");
  const head = el("tr");
  for (const [label, key, right] of [["relay", null, false], ["doing", "doing", false],
                                     ["held", "heldForSec", true], ["events", "events", true],
                                     ["quiet", "quietForSec", true]]) {
    const th = el("th", null, label);
    if (key) th.title = term(key);
    if (right) th.style.textAlign = "right";
    head.appendChild(th);
  }
  table.appendChild(head);
  for (const r of rows) {
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
    // A leg with no slot says so here rather than leaving the column blank: the
    // stage IS the answer for most of a fan-out's workers. A walking leg adds
    // its cursor in the same cell — a sixth column would be empty on every row
    // that is not paging.
    const stage = el("td", null, r.doing || (r.slotless ? "not on a transfer slot" : "—"));
    if (r.pagingUntil != null) {
      // The reader's own clock, and it only decides the LABEL's precision — a
      // skewed one draws a coarser or finer cursor, never a wrong one.
      const at = el("span", "sy-at", ` back to ${cursorOf(r.pagingUntil, Date.now() / 1000)}`);
      at.title = term("pagingUntil");
      stage.appendChild(at);
    }
    tr.appendChild(stage);
    tr.appendChild(el("td", "n", fmtDur(r.heldForSec)));
    tr.appendChild(el("td", "n", fmt(r.events)));
    tr.appendChild(el("td", "n", fmtDur(r.quietForSec)));
    table.appendChild(tr);
  }
  scroll.appendChild(table);
  box.appendChild(scroll);
  return box;
}

/**
 * WHAT ONE STREAM IS DOING: the phase word, how long it has been in it, and the
 * two numbers a rotation has — the relays it is riding and the tails it holds.
 *
 * There used to be four more marks here (a pass meter, an ETA, how far back the
 * walk had reached, and the pass's own disposition), drawn from the fan-out
 * that walked a stream's relay list in cycles. Every stream rides the visit
 * pool now: there is no pass to be a fraction OF, and where each relay has got
 * to is per relay — the legs panel below, which is the question those marks
 * were standing in for.
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
        tails.title = term("tails");
        line.appendChild(tails);
      }
    }
    box.appendChild(line);
  }
  // UNDER the rotation's two numbers, because "what is it doing right now" is
  // read after "is it getting anywhere" and before anything else on this card.
  const legs = legsPanel(s);
  if (legs) box.appendChild(legs);
  return box;
}

/**
 * THE WORK THAT IS NOT A STREAM — the url round-up, the fold, the stability
 * gate, fitness, the pool, ingest, the healer, the push.
 *
 * One line each, and exactly one number: the one that says whether it is
 * getting anywhere. They are here rather than dropped because a stalled fold is
 * not a side quest — an unfolded list downloads the same relay once per url,
 * which is a sync fault that shows up nowhere else on this card.
 *
 * The ROUND-UP is here for the sharper version of the same complaint: it is not
 * a pass and it decides nothing, but every pass waits on it, it walks the whole
 * store to do its job, and while it did the card said only that the three rows
 * under it were idle. Five minutes per sweep in which the honest reading of
 * this card was "the monitor is between passes" and the truth was "the monitor
 * is working, on the one part of it nothing published".
 *
 * ONE TABLE, TWO CARDS. The rows are split by `splitProcessors` into what the
 * MONITOR decides (which urls may be dialled at all) and what the SYNC does
 * (moving events), because those are two questions and reading one array meant
 * holding both at once. This table is the labels and the tooltips for either
 * side; the split itself is a judgement and lives in `/web/shared/sync.js`
 * where it can be tested.
 *
 * The probe passes' number is `probeProgress`, in `/web/shared/sync.js` — it
 * sums across streams, drops the urls the fold removed from BOTH halves, draws
 * the RISING half of the pair, and withholds the clock while a pass is
 * dialling. All four are decisions that can be wrong silently, which is why
 * none of them is made here.
 */
const BACKGROUND = [
  ["aliasSource", "url round-up", "Walks the store for every relay url the streams' sources and the monitor's own block name, drops what an operator excluded and what a signed record calls dead, and hands the rest to the three passes below. Minutes on a live store, at the head of every sweep — and for all of them those three read `idle`."],
  ["aliasFold", "alias fold", "Finds the urls that are one relay wearing several addresses, so the fan-out stops dialling forty urls of one server. Runs on the alias monitor's own clock, not a stream's."],
  ["consistency", "stability gate", "Refuses a url that answers one filter two different ways. Such a relay cannot hold a cursor still, so every cycle re-serves what the last one already took."],
  ["fitness", "fitness", "The monitor's verdict funnel: one measured certificate per relay, signed onto its NIP-66 record. `prime` is the only admitting value, and it is the whole relay list of every stream that selects on it."],
  ["visits", "rotating pool", "The visit-mode streams' engine: socket-owning workers over every certified relay — catch-up pages, the history audit where due, then a live tail on the open socket. A slot here IS a socket."],
  ["ingest", "ingest", "The queue every downloaded event passes through on its way to the store. Full means downloads are backpressured behind it."],
  ["heal", "healer", "Events this mirror holds that a relay which should have them does not, pushed back to it."],
  ["upstreamPush", "upstream push", "Events published to this relay and forwarded on to the upstreams configured to receive them."],
];

function backgroundPanel(rows, withFunnel = false) {
  if (!rows.length) return null;
  const box = el("div", "sy-bg");
  // THE CORPUS FIRST, above every pass's line.
  //
  // It was drawn under the row that publishes it, which put the widest thing on
  // the card — what became of every url this router knows of — below two lines
  // of clock. Every pass under it works on that same corpus, so it is context
  // for all of them rather than a detail of the one that happens to measure it,
  // and a reader arriving at this card is asking "what is out there" before
  // "when does the next pass run".
  //
  // It is still ONE pass's numbers, which is why it is captioned with that
  // pass's name: `folded onto another url` is the STABILITY GATE's reading of
  // the fold's verdicts, taken when the gate last ran, and unlabelled at the top
  // of a section it would read as the card's own arithmetic.
  //
  // Drawn only where it is asked for: the corpus is what the MONITOR works on,
  // and the same tree above ingest and the healer would be a chart about relay
  // urls at the head of a section that counts events.
  for (const p of withFunnel ? rows : []) {
    const funnel = funnelPanel(p);
    if (!funnel) continue;
    const known = BACKGROUND.find((row) => row[0] === p.name);
    const cap = el("div", "sy-tr-cap", `every relay url this router knows of, as the ${known ? known[1] : p.name} last measured it`);
    if (known) cap.title = known[2];
    box.appendChild(cap);
    box.appendChild(funnel);
    break;
  }
  for (const p of rows) {
    const known = BACKGROUND.find((row) => row[0] === p.name);
    const name = el("b", null, known ? known[1] : p.name);
    if (known) name.title = known[2];
    box.appendChild(name);
    box.appendChild(phaseCell(p));
    box.appendChild(processorFact(p));
    // WHICH URL IT IS HOLDING, on its own row under the pass's line.
    //
    // The counts above never said, and that was the whole of the diagnostic
    // problem: a fitness pass held one url of 12,374 for 74 minutes and the url
    // was not nameable from this card, the progress document, the log, or a
    // thread dump — a suspended coroutine has no frame. The router publishes
    // longest-held first, so the top of its list is the row worth the space.
    const held = heldPanel(p);
    if (held) box.appendChild(held);
  }
  return box;
}

/**
 * THE HELD URLS, longest first — or null when the pass is holding none.
 *
 * Only the leaders are drawn, and only while a pass is running. A pass at the
 * monitor's default dial concurrency is holding five hundred urls, of which 499
 * are ordinary dials a second old; naming them all would bury the one that is
 * not, and the router already sorted them so the answer is at the front.
 *
 * `+N more` rather than silence about the rest, for the reason every other
 * truncated list on this card carries it: a list that does not disclose its cut
 * reads as the whole answer.
 */
function heldPanel(p) {
  const { rows, more } = heldOf(p.inFlight, HELD_SHOWN);
  if (!rows.length) return null;
  const box = el("div", "sy-held");
  for (const r of rows) {
    const line = el("div", null, `holding ${r.short} for ${fmtDur(r.heldForSec)}`);
    if (r.stage) {
      const stage = el("span", "sy-dim", ` · ${r.stage}`);
      stage.title = term("stage");
      line.appendChild(stage);
    }
    box.appendChild(line);
  }
  if (more > 0) box.appendChild(el("div", "sy-dim", `+${fmt(more)} more held`));
  return box;
}

/**
 * THE CLOCK CELL: what the processor is doing, and either where the pass in
 * flight has got to or when the next one is due.
 *
 * The countdown is unset while the SWEEP runs, which is why this cell used to
 * go blank on the passes worth watching: a stability pass runs for hours, and
 * for all of them the line read `measuring` with the one number on it gone.
 *
 * The two are not exclusive, though the sweep makes them look it. A FAST LANE
 * pass runs between sweeps, so the fitness row can carry a live position AND a
 * countdown to the next sweep, both true. The position wins: it is about the
 * work in front of the reader, and the countdown is still one hover away on the
 * row below. `measuringOf` is the decision, in `/web/shared/sync.js`; both
 * branches are appended the same way so a router publishing neither still gets
 * its phase word.
 */
function phaseCell(p) {
  const cell = el("em", null, p.phase || "unknown");
  const run = measuringOf(p);
  if (run) {
    // The position, then the estimate — and the estimate only when the router
    // sent one. It is withheld until a unit has landed and once the last one
    // has, and inventing "~0s left" from its absence is the exact reading the
    // paging ETA was fixed for.
    const done = el("span", null,
      ` · ${fmt(run.attempted)} of ${fmt(run.toProbe)} ${run.unit}(s) this pass` +
      (run.etaSec != null ? `, ~${fmtDur(run.etaSec)} left` : ""));
    done.title = term("measuring");
    cell.appendChild(done);
    // …AND WHEN A UNIT LAST LANDED, which is the half `~0s left` cannot say.
    //
    // Drawn only once the pass has gone quiet for longer than a unit normally
    // takes, because on a healthy pass it is a number between 0 and 2 and the
    // line is already long. Past that it is the finding: `12,373 of 12,374, ~0s
    // left` read as a pass about to finish for 74 minutes, and this is the only
    // thing on the row that would have said otherwise.
    if (run.quietForSec != null && run.quietForSec >= STUCK_PASS_SEC) {
      const quiet = el("span", "sy-hot", ` · nothing finished for ${fmtDur(run.quietForSec)}`);
      quiet.title = term("quietForSec");
      cell.appendChild(quiet);
    }
  } else if (p.nextInSec) {
    const next = el("span", null, ` · next in ${fmtDur(p.nextInSec)}`);
    next.title = term("nextInSec");
    cell.appendChild(next);
  }
  return cell;
}

/**
 * THE CANDIDATE SET, DIVIDED — every url the streams named, once, into what
 * became of it and then into why.
 *
 * Drawn under the stability gate's line because that line is a ratio and a
 * ratio is exactly what cannot be acted on: `595 of 8,172 checked` is the same
 * number whether the other 7,577 are unreachable, auth-walled, thin or waiting
 * on a pass that never ran, and those want four different responses. Every
 * decision behind it — what the width is, where a slice sits, what to do when
 * the arithmetic does not close — is `funnelOf` in `/web/shared/sync.js`, for
 * the reason that module's head gives.
 *
 * The KEYS carry the numbers, and that is not redundancy with the bars: the
 * slice that matters most here is `inconsistent`, which on a live corpus is
 * twelve urls out of seventeen thousand and cannot be drawn at a width anyone
 * can see. The bar shows the shape, the keys show the values, and neither is
 * asked to do the other's job.
 */
function funnelPanel(p) {
  const f = funnelOf(p);
  if (!f) return null;
  const box = el("div", "sy-funnel");
  for (const r of f.rows) {
    const row = el("div", "sy-tr");

    // The guides and the label in one cell, so indentation is the label's own
    // left edge rather than a column that the longest name at any depth would
    // have to be measured against.
    const name = el("div", "sy-tr-name");
    if (r.prefix) name.appendChild(el("i", "sy-tr-guide", r.prefix));
    name.appendChild(el("b", r.tone ? `sy-tr-dot ${r.tone}` : "sy-tr-dot"));
    name.appendChild(el("span", null, r.label));
    // The document's own words where the row is one of ours; a router's
    // free-text reason and a hostname are their own explanation.
    const why = term(r.key);
    if (why) name.title = why;
    // What a reason's urls resolve to as SERVERS, and how much of it the widest
    // one is. Beside the label rather than as child rows: they are a different
    // unit, a unit change inside a tree of url counts reads as a subtotal, and
    // one row per host would be one row per server on a corpus of thousands.
    // These two numbers are what that list was for — spread thin, or a handful
    // of servers — and the names are on the row's title.
    if (r.hosts) {
      // ` — 501 host(s)`, not ` on 501 host(s)`: the label is the ROUTER's own
      // sentence and two of the gate's seven end in a preposition, so the card
      // was rendering `too few events to judge on on 501 host(s)`. A dash joins
      // any of them, and the reasons are free text off the wire — a separator
      // that only works for the words we happen to ship today is not one.
      const spread = r.largest ? ` — ${fmt(r.hosts)} host(s), largest ${fmt(r.largest)}` : ` — ${fmt(r.hosts)} host(s)`;
      name.appendChild(el("em", "sy-tr-hosts", spread));
    }
    row.appendChild(name);

    row.appendChild(el("span", "sy-tr-n", fmt(r.value)));

    // EVERY BAR AGAINST THE ROOT, never against the parent. Against its parent
    // a host with four urls under a reason with five would draw at 80% of the
    // width the whole corpus gets, which is the reading the tree's indentation
    // already carries and the one proportion must not contradict.
    const track = el("span", "sy-tr-track");
    const fill = el("i", r.tone || null);
    fill.style.width = `${Math.min(100, r.share * 100)}%`;
    track.appendChild(fill);
    row.appendChild(track);
    // The names, to a readable handful, and then the count of what is left —
    // see NAMES_IN_TOOLTIP. A title that silently stopped at twelve would be
    // the same "reads as the whole answer" bug the JSON's own `omitted`
    // members exist to prevent, one layer up.
    row.title = `${fmt(r.value)} url(s) — ${(r.share * 100).toFixed(r.share < 0.01 ? 2 : 1)}% of ${fmt(f.total)}` +
      (r.examples && r.examples.length
        ? ` (e.g. ${r.examples.join(", ")}${r.unnamed ? `, and ${fmt(r.unnamed)} more — see the JSON` : ""})`
        : "");
    box.appendChild(row);
  }
  if (f.omitted) {
    box.appendChild(el("div", "sy-tr-note", `${fmt(f.omitted)} more reason(s) not drawn — see the JSON`));
  }
  // The relay's own check on the two identities, drawn LOUD and only when it
  // fails. The tree's `not accounted for` row covers a parent whose children
  // fall short; this covers the other direction, where nothing looks wrong.
  if (f.accountedFor === false) {
    const bad = el("div", "sy-tr-note warn", "these numbers do not add up — see `accountedFor` in the JSON");
    bad.title = term("accountedFor");
    box.appendChild(bad);
  }
  return box;
}

/**
 * What each probe pass is checking FOR. Both publish the same `streams` shape,
 * so one generic sentence made the stability gate report the fold's question.
 *
 * `__proto__: null`, because the key is a name off the wire: a router
 * publishing `constructor` would otherwise draw a function into the cell.
 */
const PROBE_FOR = {
  __proto__: null,
  aliasFold: "checked for aliases",
  consistency: "checked for consistency",
};

/**
 * …and how each says it had NOTHING to check, which is not a rare state: it is
 * the one both passes are working towards, and on a settled corpus they hold it
 * for most of a monthly TTL.
 *
 * Written out per pass rather than assembled from the phrase above, for the
 * reason that map is a map at all — a sentence a reader sees is a sentence
 * somebody wrote. `0 of 0 new relay(s) checked for aliases` is the alternative,
 * and a position with nothing to be a position IN is worse than no position:
 * the two zeroes read as a pass that found nothing to do because it is broken.
 */
const PROBE_NONE = {
  __proto__: null,
  aliasFold: "nothing new to check for aliases",
  consistency: "nothing new to check for consistency",
};

/**
 * WHAT ONE BACKGROUND PASS HAS TO SHOW FOR ITSELF.
 *
 * One line, and only counters that mean something on their own. A loss counter
 * is drawn LOUD and only when non-zero: at zero it belongs in the JSON, above
 * zero it belongs in front of an operator.
 */
function processorFact(p) {
  const cell = el("span");
  /** Append a fact, hanging the document's own words on it where it has some. */
  const add = (text, key, loud) => {
    const part = el(loud ? "s" : "span", null, cell.children.length ? ` · ${text}` : text);
    // Guarded: `term` answers "" for a key this document does not carry, and an
    // EMPTY title still sets the attribute — which paints the help cursor over
    // a tooltip the browser then declines to show.
    const why = key && term(key);
    if (why) part.title = why;
    cell.appendChild(part);
  };
  if (p.capacity) {
    const full = (p.queued || 0) >= p.capacity;
    add(`queue ${fmt(p.queued || 0)} of ${fmt(p.capacity)}${full ? " — FULL, downloads are backpressured behind it" : ""}`,
        "queued", full);
    if (p.accepted != null) add(`${short(p.accepted)} stored`, "accepted");
    // Mostly the same event offered once per relay holding it, which is why it
    // is drawn beside `accepted` and never as a failure on its own.
    if (p.rejected != null) add(`${short(p.rejected)} refused, mostly duplicates`, "rejected");
    if (p.lostToStore) add(`${fmt(p.lostToStore)} LOST TO THE STORE`, "lostToStore", true);
    return cell;
  }
  if (p.prime != null) {
    // The verdict funnel, one line: the certificate count first — it is the
    // number every visit-mode stream's whole relay list equals — then the
    // refusals that are ALIVE, which is the argument for the word `prime`
    // over `live`, then the dead. The glossary hangs per member.
    add(`${fmt(p.prime)} graded prime`, "prime");
    const alive = (p.silent || 0) + (p.alias || 0) + (p.inconsistent || 0) +
                  (p.unpageable || 0) + (p["auth-refused"] || 0) + (p.restricted || 0);
    if (alive) add(`${fmt(alive)} refused while alive`, "silent");
    if (p.dead) add(`${fmt(p.dead)} dead`, "dead");
    return cell;
  }
  if (p.roster != null) {
    // The pool's one line, and the arithmetic it exists to make visible:
    // visiting + tails IS the socket count, because a slot here is a socket.
    add(`${fmt(p.roster)} on the roster`, "roster");
    if (p.awaitingVisit) add(`${fmt(p.awaitingVisit)} awaiting a visit`, "awaitingVisit");
    add(`${fmt(p.visiting || 0)} visiting`, "visiting");
    add(`${fmt(p.tails || 0)} live tail(s)`, "tails");
    if (p.visitsRun) add(`${fmt(p.visitsRun)} visit(s) run`, "visitsRun");
    if (p.auditing) add(`${fmt(p.auditing)} auditing now`, "auditing");
    if (p.auditsRun) add(`${fmt(p.auditsRun)} history audit(s)`, "auditsRun");
    // Not a fault, and not nothing: those relays are re-checked by paging or
    // not at all, and which one is a config question the tooltip names.
    if (p.auditsSkipped) add(`${fmt(p.auditsSkipped)} skipped, no NIP-77`, "auditsSkipped");
    if (p.retracted) add(`${fmt(p.retracted)} RETRACTED upstream`, "retracted", true);
    if (p.abortedVisits) add(`${fmt(p.abortedVisits)} aborted`, "abortedVisits");
    if (p.evictedTails) add(`${fmt(p.evictedTails)} tail(s) rotated out`, "evictedTails");
    if (p.poolReceived) add(`${short(p.poolReceived)} events in`, "poolReceived");
    return cell;
  }
  // THE ROUND-UP's own line, and the only row on this card whose numbers are
  // about the corpus rather than about a verdict over it. Keyed on
  // `candidates`, which no other row publishes at the top level: the passes
  // carry theirs per stream, inside `streams[]`.
  //
  // It states the derivation's arithmetic in the order it happens —
  // `sourced = excluded + heldOutDead + candidates` — with the yield LAST,
  // because that is the number the three rows under it are each a share of and
  // it is what a reader came to this row for. The two drops are drawn only when
  // they are non-zero: a router excluding nothing says so by having nothing to
  // say, where `0 excluded` is a line to read past on every poll forever.
  if (p.candidates != null) {
    add(`${fmt(p.sourced ?? p.candidates)} url(s) named`, "sourced");
    if (p.excluded) add(`${fmt(p.excluded)} excluded`, "excluded");
    if (p.heldOutDead) add(`${fmt(p.heldOutDead)} held out as dead`, "heldOutDead");
    add(`${fmt(p.candidates)} handed to the passes`, "candidates");
    // Not part of the derivation and drawn last for that reason: these are urls
    // no relay list named this round, which the fold still groups against. The
    // tree above spends a whole branch on them, so a row that omitted them here
    // would have the card's mouth and this line disagreeing about the corpus.
    if (p.recordedOnly) add(`${fmt(p.recordedOnly)} more we hold records about`, "recordedOnly");
    return cell;
  }
  const probe = probeProgress(p);
  if (probe) {
    // ONE FACT: how far it got, and how long that took. The verdicts behind it
    // — how many relays are consistent, and the count this router refuses —
    // were briefly on this line too, because nothing anywhere published them;
    // they are now the tree at the head of the section, with the rest of the
    // partition around them. A line that repeats what a chart six rows above it
    // already says is a line a reader has to reconcile.
    // NEW where the row counted what arrived undecided — see `probeProgress`.
    // The word is the difference between "the fold has checked 143 of the
    // 1,754 relays it knows about" and what the number actually means, which is
    // that 1,754 urls turned up with no verdict and 143 of them left with one.
    // The tooltip hangs on whichever member the denominator came from, so the
    // glossary entry a reader lands on is the one describing the number they
    // are pointing at.
    const took = probe.tookSec != null ? ` in ${fmtDur(probe.tookSec)}` : "";
    // A pass whose subject is empty says so in words — see `PROBE_NONE`. Only
    // where the row COUNTED that subject: a router that publishes no `newUrls`
    // has a zero denominator for want of a candidate set, which is a different
    // fact and keeps the reading it has always had.
    add(probe.newOnly && !probe.candidates
        ? `${PROBE_NONE[p.name] || "nothing new to check"}${took}`
        : `${fmt(probe.checked)} of ${fmt(probe.candidates)} ${probe.newOnly ? "new " : ""}relay(s) ` +
          `${PROBE_FOR[p.name] || "checked"}${took}`,
        probe.newOnly ? "newUrls" : "unmeasured");
    return cell;
  }
  if (p.pushed != null) {
    add(`${fmt(p.pushed)} event(s) pushed back to relays missing them`, "pushed");
    // The sweep declining to backpressure itself, which is a choice and not a
    // fault — so it is drawn plainly however large it gets.
    if (p.dropped) add(`${short(p.dropped)} dropped rather than backpressure the sweep`, "dropped");
    return cell;
  }
  return cell;
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
 * One status line (working?), one block per stream (progress), one line per
 * pipeline job (where a slow mirror is actually diagnosed — a full ingest queue
 * means every download is backpressured behind it), and one depth strip (how
 * far back the coverage actually goes).
 *
 * ## What moved OUT of it
 *
 * The alias fold, the stability gate and fitness are the `Relay monitor` card
 * now, with the corpus tree that describes their subject. They were here because they arrive in the same array, and the card
 * asked two questions at once: *is the mirror keeping up* is answered by
 * streams, queue depth and ingest, while *which relays may we dial at all* is
 * answered by a round-up and three passes on a six-hour clock whose unit is a url and whose
 * output is a signed record. An operator arrives with one of those questions,
 * never both. `splitProcessors` is the rule and it is a partition — nothing is
 * dropped by being sorted.
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
  TERMS = d.terms || {};
  if (progress) card.appendChild(statusRow(progress));

  const lanes = (progress && progress.streams) || [];
  if (lanes.length) {
    card.appendChild(el("p", "sy-h", "streams"));
    for (const s of lanes) card.appendChild(streamBlock(s));
  }

  // The half of the background work that moves EVENTS. The half that decides
  // which relays may be dialled at all is its own card — see `monitorCard`.
  const background = backgroundPanel(splitProcessors(progress).pipeline);
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
  const rows = splitProcessors(d.progress).monitor;
  if (!rows.length) return null;
  // The document's own glossary, set again here rather than inherited from
  // whichever card drew first: two cards reading one section must not depend on
  // the order `PANELS` happens to build them in.
  TERMS = d.terms || {};
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

export { syncCard, monitorCard };
