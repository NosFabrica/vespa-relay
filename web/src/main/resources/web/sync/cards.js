// THE MIRROR'S CARDS — what each stream is doing, what the background work is
// doing, and how far the walk has got.
//
// Lifted out of the relay's stats.html unchanged, comments included, when the
// sync process started serving its own page. Every judgement these draw was
// already in `shared/sync.js` — the split moved the RENDERING to the service
// that produces the numbers, and changed neither.

import { ago, cardHead, dayOf, el, fmt, fmtDur, isoOf, short } from "../shared/page.js";
import { backgroundPanel, chip, setTerms, term } from "../shared/processors.js";
import {
  STUCK_LEG_SEC, VIEW_ALL, VIEW_PROBLEMS, VIEW_RECENT, constraintOf, legsOf, rotationOf, visitsOf,
} from "../shared/sync.js";

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


/**
 * WHICH VIEW OF THE RELAY TABLE IS OPEN — module state, exactly like the
 * activity card's `grain`, and for the same reason: the panel is rebuilt on
 * every status tick, so anything held in the card's own closure is gone
 * fifteen seconds after the reader chooses it. The engine already carries the
 * FILTER text across a rebuild (by the input's `name`) and the scroll position
 * (by `.wrap.tall`); a segmented control is neither, so it lives here.
 */
let visitView = VIEW_PROBLEMS;

/** What each outcome word looks like. `live` is the page's good tone; a fault is `warn`. */
const OUTCOME_TONE = {
  // The key is a word off the wire — a router this page has not been taught
  // still gets a neutral chip rather than resolving `constructor` to a
  // function, the same rule `BOTTLENECK` and the funnel tones follow.
  __proto__: null,
  synced: "live",
  refused: "warn",
  quiet: "warn",
  failed: "warn",
  never: null,
};

/**
 * One relay's row.
 *
 * Two clocks side by side and they are NOT the same reading: `last synced` is
 * the last clean visit and `last event` is the last arrival, from a page or
 * from the live tail. A tailed relay delivers between visits, so a table with
 * only the second would call every relay synced; a relay this router asks a
 * narrow filter of may honestly have nothing to send for a week, so a table
 * with only the first would call it dead. Both, with the reason line last.
 */
function visitRow(r) {
  const tr = el("tr", r.why ? "hot" : null);

  const url = el("td", "relay");
  const inner = el("span", null, r.short);
  // The url in its own LTR isolate, exactly as in the legs table: a relay
  // address must not be rewritten by the cell's direction.
  inner.dir = "ltr";
  url.appendChild(inner);
  // WHO WANTS IT and whether a tail is carrying it — on the title rather than
  // in columns of their own, because both are usually the same on every row
  // and a mark that reads the same on every row is not a mark.
  url.title =
    `${r.relay}\n${r.streams.length ? `asked by ${r.streams.join(", ")}` : "no stream currently asks for it"}` +
    `${r.tailed ? "\nholding a live tail" : ""}`;
  tr.appendChild(url);

  const synced = el("td", "n", r.syncedAt == null ? "never" : ago(r.syncedAt));
  if (r.syncedAt != null) synced.title = isoOf(r.syncedAt);
  tr.appendChild(synced);

  // An em dash, not "never": nothing has ever arrived is a different claim
  // from nothing has arrived lately, and on a relay whose filter it holds
  // nothing for, it is not a fault at all. Drawn with `ago`, the same
  // formatter as the column beside it — the two are read against each other,
  // and `6d ago` beside `144h 1m` is a subtraction the reader has to do.
  const heard = el("td", "n", ago(r.lastEventAt));
  if (r.lastEventAt != null) heard.title = isoOf(r.lastEventAt);
  tr.appendChild(heard);

  tr.appendChild(el("td", "n", fmt(r.events)));

  const outcome = el("td");
  const tone = Object.hasOwn(OUTCOME_TONE, r.outcome) ? OUTCOME_TONE[r.outcome] : null;
  // The failure RUN, on the chip: `refused ×14` and `refused` are a relay that
  // has been down for a day and one that missed a turn, and the word alone
  // cannot tell them apart.
  outcome.appendChild(chip(r.failures > 1 ? `${r.outcome} ×${fmt(r.failures)}` : r.outcome, tone, term("outcome")));
  if (!r.onRoster) outcome.appendChild(chip("off the roster", null, term("onRoster")));
  tr.appendChild(outcome);

  // WHAT HAPPENS NEXT, which is what turns "it has not synced in a day" from a
  // complaint into a diagnosis: a relay with a countdown is waiting its turn,
  // and one that is being held right now is not stuck at all.
  const next = el(
    "td",
    "n",
    r.heldForSec != null ? `syncing now (${fmtDur(r.heldForSec)})` : r.nextVisitInSec != null ? `in ${fmtDur(r.nextVisitInSec)}` : "queued",
  );
  next.title = term(r.heldForSec != null ? "heldForSec" : "nextVisitInSec");
  tr.appendChild(next);

  // The ROUTER's own words where there are any — see `whyNotSyncing` for the
  // order they rule each other out in. Full text on the title, because the
  // column is one line and the exception on a `failed` row is the whole
  // message.
  const why = el("td", "why", r.why || "—");
  if (r.why) why.title = r.why;
  tr.appendChild(why);
  return tr;
}

/**
 * WHEN EACH RELAY WAS LAST SYNCED, AND WHY THE REST ARE NOT.
 *
 * ## Why this is its own card and not a panel of the one above
 *
 * The card above answers "is the mirror keeping up" — one status line, one
 * block per stream, the pipeline, the depth strip — and every mark on it is
 * about the POOL. This one is about a relay, and it is the question an
 * operator arrives with when something specific is missing from the store:
 * *this* relay, when did we last get anything from it, and if we did not, what
 * happened. Nothing on the other card can answer it. `inFlight` names the
 * relays a worker is on this instant and forgets them the moment the visit
 * ends; the counters say how many visits ran without saying against what; the
 * depth strip draws a band that finished in March exactly like one that
 * finished a minute ago.
 *
 * ## Three views over one list, and the default is the complaint
 *
 * `not syncing` opens first because that is what brings someone here, and
 * because on a healthy mirror it is empty — which is itself the answer. The
 * counts sit ON the other two buttons so an empty table can never read as an
 * empty roster. `synced lately` is the same rows newest-first, which is how
 * "is anything working at all" is answered without reading any single row.
 *
 * ## What the table refuses to do
 *
 * **It does not merge the two clocks.** See [visitRow].
 *
 * **It does not colour a healthy relay green.** Only rows with a reason line
 * are marked, so the eye lands on the fault rather than on a wall of chips —
 * the same rule the legs table follows with `quietForSec`.
 *
 * **It does not silently cut.** The router's `omitted` and this table's own
 * row cap are added and named together: a truncated list that does not say so
 * reads as the whole answer, and here the whole answer is what is being
 * chased.
 */
function relaySyncCard(section) {
  const d = (section && section.data) || {};
  const visits = d.progress && d.progress.visits;
  const card = el("div", "card");
  cardHead(card, "Relay sync", visits ? null : "No per-relay sync state in this document.", section);
  setTerms(d.terms);
  if (!visits) return card;

  // The whole-roster facts, before any view narrows anything: the denominator
  // every count below is a share of.
  const first = visitsOf(visits, { view: VIEW_ALL, limit: 0 });
  const c = first.counts;
  card.appendChild(el("div", "sy-sub",
    [`${fmt(c.all)} relay(s) this mirror has visited or been asked to visit`,
     `${fmt(c.tailed)} holding a live tail`,
     `${fmt(c.recent)} synced in the last hour`,
     `${fmt(c.never)} never synced`].join(" · ")));

  const bar = el("div", "bar-row");
  const seg = el("div", "seg");
  const filter = el("input");
  const status = el("span", "status");
  const table = el("table", "sy-visits");
  const tbody = el("tbody");

  const draw = () => {
    const { rows, more, cut, omitted, matched } = visitsOf(visits, { q: filter.value, view: visitView });
    tbody.replaceChildren(...rows.map(visitRow));
    if (!rows.length) {
      const tr = el("tr");
      const td = el("td", "why none",
        visitView === VIEW_PROBLEMS && !filter.value.trim()
          ? "Every relay on the roster has synced, and none has failed a visit since."
          : "No relay here matches that.");
      td.colSpan = 7;
      tr.appendChild(td);
      tbody.appendChild(tr);
    }
    // The denominator stays on screen while filtering — a filtered table that
    // only says "5" reads as a mirror with five relays on it. What is not
    // shown is one number beside it and TWO facts on its title: narrowing the
    // filter brings this table's own cut back and can never reach the
    // router's.
    status.textContent = `${fmt(matched)} of ${fmt(c.all)}${more ? ` · ${fmt(more)} not shown` : ""}`;
    status.title = more
      ? [cut ? `${fmt(cut)} more match than this table draws at once — narrow the filter.` : "",
         omitted ? `${fmt(omitted)} relay(s) the router left out of the document; no filter here can reach them.` : ""]
        .filter(Boolean).join("\n")
      : "";
  };

  for (const [key, label, n] of [[VIEW_PROBLEMS, "not syncing", c.problems],
                                 [VIEW_RECENT, "synced lately", c.recent],
                                 [VIEW_ALL, "all", c.all]]) {
    const b = el("button", key === visitView ? "on" : null, `${label} (${fmt(n)})`);
    b.addEventListener("click", () => {
      visitView = key;
      [...seg.children].forEach((x) => x.classList.toggle("on", x === b));
      draw();
    });
    seg.appendChild(b);
  }
  filter.type = "search";
  // Named, so the engine restores what was typed into THIS box across a
  // rebuild and not into the one on Kinds — see `fill` in statspage.js.
  filter.name = "visits";
  filter.placeholder = "filter by host or url";
  filter.setAttribute("aria-label", "filter relays by host or url");
  filter.addEventListener("input", draw);
  bar.append(seg, filter, status);
  card.appendChild(bar);

  const wrap = el("div", "wrap tall");
  const thead = el("thead");
  const hr = el("tr");
  for (const [label, cls, key] of [["Relay", "", null], ["Last synced", "n", "syncedAt"],
                                   ["Last event", "n", "lastEventAt"], ["Events", "n", "events"],
                                   ["Last visit", "", "outcome"], ["Next visit", "n", null],
                                   ["Why not", "", null]]) {
    const th = el("th", cls, label);
    if (key) th.title = term(key);
    hr.appendChild(th);
  }
  thead.appendChild(hr);
  table.append(thead, tbody);
  wrap.appendChild(table);
  card.appendChild(wrap);
  draw();

  // THE ONE CAVEAT WORTH THE WORDS, and it is the reading this card is most
  // likely to be misused for: a clean visit is not a full one. Whether a
  // relay's history is covered is the band's question, one card up.
  card.appendChild(el("p", "card-sub",
    "“Synced” here means the last visit walked every ask and left a tail — not that this relay's history is " +
    "fully covered, which is the coverage card's “settled”. A clean visit that carried nothing is the normal " +
    "state of a tailed relay: the tail already had it."));
  return card;
}


export { relaySyncCard, syncCard };
