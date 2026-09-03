// THE BACKGROUND WORK, drawn the same way on every page that has any.
//
// Both planes register processor rows — the mirror's ingest queue, healer and
// push; the monitor's fold, consistency gate and fitness pass — and both draw
// them with this card. It moved here when the monitor got its own page, which
// is also when `splitProcessors` went away: each document now carries only its
// own rows, so nothing has to sort them by name at render time.
//
// The glossary comes with it. A chip explains itself out of the DOCUMENT's own
// `terms`, never out of a copy in this file, so a page can never describe a
// member in words the service would not use — see `StatusVocabulary`.

import { el, fmt, fmtDur, short, shownOf } from "./page.js";
import { HELD_SHOWN, STUCK_PASS_SEC, funnelOf, heldOf, measuringOf, probeProgress, stageDeltas } from "./sync.js";

/**
 * The glossary the document ships, for the current card's `title` attributes.
 *
 * Set by [syncCard] before anything below draws. The words are the ROUTER's —
 * `SyncVocabulary` publishes them and `SyncVocabularyTest` pins the two
 * directions, no published count without a term and no term without a count —
 * so a mark that explains itself here cannot drift from the member it explains.
 */
let TERMS = {};

/**
 * The last two documents' `health.stages`, so the ingest row can show what
 * moved rather than what has accumulated since boot.
 *
 * Module state on the same terms `TERMS` is, and set the same way — by the card
 * before it draws, never inherited from whichever drew first. The DERIVATION
 * stays pure in `stageDeltas`; only the remembering is here, because only this
 * module knows when a render pass begins.
 */
let stagesNow = null;
let stagesBefore = null;

/** Hand this document's stage totals in, shifting the previous ones back. */
export function setStages(stages) {
  // Guarded on identity, not contents: one document is drawn by more than one
  // card, and shifting per CARD would compare a document against itself and
  // report an idle router.
  if (stages === stagesNow) return;
  stagesBefore = stagesNow;
  stagesNow = stages;
}

/**
 * Point the glossary at THIS document's `terms`.
 *
 * Called by every card that draws a chip, rather than inherited from whichever
 * drew first: two cards reading one section must not depend on the order the
 * panel table happens to build them in.
 */
function setTerms(terms) {
  TERMS = terms || {};
}

/**
 * What the document says a member means, or nothing at all.
 *
 * `hasOwn`, not a plain read: `TERMS` is parsed JSON, so it carries
 * Object.prototype, and several call sites look up a key the DOCUMENT chose —
 * a funnel slice, a phase word. A member named `constructor` or `toString`
 * would come back as a function and be set as an element's `title`. The same
 * rule `funnelOf` in shared/sync.js already holds, and for the same reason.
 */
function term(key) {
  return Object.hasOwn(TERMS, key) ? TERMS[key] || "" : "";
}

/** One labelled pill. `tone` is `live`, `busy`, `warn`, or nothing. */
function chip(text, tone, title) {
  const c = el("span", tone ? `chip ${tone}` : "chip", text);
  if (title) c.title = title;
  return c;
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
 * ONE TABLE, TWO CARDS, and the split is no longer this file's to make. It was
 * `splitProcessors`, a partition of one shared array into what the MONITOR
 * decides (which urls may be dialled at all) and what the SYNC does (moving
 * events) — two questions that reading one array meant holding at once. Each
 * plane keeps its own `Processors` and publishes its own document now, so a row
 * is on a card because that plane registered it. What survives is this table,
 * which is the labels and the tooltips for either side, and the rule it is
 * written to: a name NOT in it is drawn under the router's own word rather than
 * dropped, because dropping a row to keep a card tidy is how a new job runs
 * unwatched.
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
/**
 * The visits row's abort partition — the document's member, and the words the
 * card says it in. Ordered as the engine's own enum is, which is only a
 * tie-break: what is drawn is the largest of them.
 */
const ABORTS = [
  ["abortedAuthRequired", "on a NIP-42 wall"],
  ["abortedClosed", "on a CLOSED"],
  ["abortedQuiet", "on silence"],
  ["abortedUnreachable", "unreachable"],
  ["abortedUnpageable", "unpageable"],
  ["abortedGaveUp", "given up on"],
  ["abortedFailed", "failed"],
];

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
    // THE DEPTH IS A FACT; WHICH KIND OF FULL IT IS, IS A VERDICT. This used to
    // append "downloads are backpressured behind it" to every full queue — the
    // busy-mirror reading — so on a wedge the row carried that beside the
    // health chip saying ingest had stopped. The two FULLs are opposite
    // diagnoses, and `bottleneck` already decides between them ONCE, for the
    // log and this page together. So the row states what it measured and the
    // chip says what it means; re-deciding here would need this file to carry
    // its own copy of the router's threshold, which is precisely how the two
    // drift. `queued`'s glossary entry carries the explanation either way.
    add(`queue ${fmt(p.queued || 0)} of ${fmt(p.capacity)}${full ? " — FULL" : ""}`, "queued", full);
    // The pair that says which full, drawn whenever a worker is in a batch —
    // plainly, because workers working is the healthy state and only their AGE
    // is the finding. It is the number no thread dump can give: a worker
    // suspended in a store call has no frame and its pool thread reads idle.
    if (p.inBatch) {
      add(`${p.inBatch} of ${p.workers} worker(s) in a batch, oldest ${fmt(p.oldestBatchSec || 0)}s`, "inBatch");
    }
    // Loud, and needing no threshold to be sure of: a loop that exited leaves
    // its share of the queue with nothing draining it, and no age can show that.
    if (p.workersRunning != null && p.workers != null && p.workersRunning < p.workers) {
      add(`${p.workers - p.workersRunning} of ${p.workers} worker(s) STOPPED`, "workersRunning", true);
    }
    // WHERE THE TIME WENT, under the queue and workers it explains. Everything
    // above says WHICH constraint; only this says what ingest is spending
    // itself on — `dedup` (store reads), `write` (the feed) and
    // `lock.ingest.wait` (queueing behind another writer) are three faults with
    // three remedies and one appearance from outside. It lives on this row and
    // not up on the status line because a reader diagnosing ingest should not
    // have to look in two places for it.
    const stages = stageDeltas(stagesNow, stagesBefore);
    if (stages.length) {
      add(`time in ingest ${stages.map((r) => `${r.stage} ${Math.round(r.share * 100)}%`).join(" ")}`, "stages");
    } else if (Array.isArray(stagesNow) && stagesNow.length) {
      // Said rather than silently omitted: the totals are cumulative, so the
      // first refresh after opening the page genuinely has nothing to
      // difference — and a row that is simply absent reads as a router that is
      // doing nothing rather than a measurement that has not started.
      add("time in ingest measuring…", "stages");
    }
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
                  (p.unpageable || 0) + (p.noncompliant || 0) +
                  (p["auth-refused"] || 0) + (p.restricted || 0);
    if (alive) add(`${fmt(alive)} refused while alive`, "silent");
    if (p.dead) add(`${fmt(p.dead)} dead`, "dead");
    return cell;
  }
  if (p.roster != null) {
    // THE POOL'S LIFETIME COUNTERS, and not its split.
    //
    // This row used to open with `roster`, `rosterVisits`, `awaitingVisit`,
    // `visiting` and `liveHeld` — the five members the sync card's pool
    // summary partitions, drawn a second time as a sentence a few hundred
    // pixels below the line that partitions them. Five numbers twice, in two
    // vocabularies (`24 visiting` there, `24 with a worker now` here), which is
    // the shape that makes a reader check whether they are the same quantity.
    //
    // They are the same quantity, and the summary is where they belong: it
    // states the arithmetic they are parts of, and it can be drawn per stream.
    // What no other panel counts is everything below — what this pool has DONE
    // since boot — so that is what the row keeps. `roster` is still the key
    // that identifies the row, because it is the member no other processor
    // publishes.
    //
    // UNCONDITIONAL, and it is the only one here that is. Every counter under
    // it is drawn on being non-zero, which is right for each of them and wrong
    // for all of them at once: a pool that has just booted has zero of
    // everything, and a cell guarded all the way down renders EMPTY beside a
    // row that is plainly working. `0 visit(s) run` is a reading — the pool is
    // up and has not finished one yet — where a blank cell is a rendering
    // fault, and this is the row's headline in any case.
    add(`${fmt(p.visitsRun || 0)} visit(s) run`, "visitsRun");
    if (p.negentropyRunning) add(`${fmt(p.negentropyRunning)} negentropy sync(s) now`, "negentropyRunning");
    if (p.negentropyRuns) add(`${fmt(p.negentropyRuns)} negentropy sync(s) of the past`, "negentropyRuns");
    // Not a fault, and not nothing: those relays are re-checked by paging or
    // not at all, and which one is a config question the tooltip names.
    if (p.negentropySkipped) add(`${fmt(p.negentropySkipped)} skipped, no NIP-77`, "negentropySkipped");
    if (p.retracted) add(`${fmt(p.retracted)} RETRACTED upstream`, "retracted", true);
    // THE ABORT, AND THE REASON THAT IS MOST OF IT. A visit that aborts leaves
    // its relay unreconciled, so this number IS whether the resync converges —
    // and on its own it is unactionable, which is the whole complaint the
    // per-reason counters answer. Drawn as the largest single reason rather
    // than as seven chips: the partition is on the document for anyone reading
    // it, and what a card has room to say is which wall the pool is mostly
    // hitting.
    if (p.abortedVisits) {
      add(`${fmt(p.abortedVisits)} aborted`, "abortedVisits");
      const worst = ABORTS.map(([member, word]) => [p[member] || 0, member, word])
        .sort((a, b) => b[0] - a[0])[0];
      if (worst[0]) add(`${fmt(worst[0])} ${worst[2]}`, worst[1]);
    }
    // Not a fault: a relay that has told us its filter width is one we can now
    // finish, where before this it could never complete a single ask.
    if (p.narrowedRelays) add(`${fmt(p.narrowedRelays)} asked in kind chunks`, "narrowedRelays");
    if (p.liveEvicted) add(`${fmt(p.liveEvicted)} live subscription(s) rotated out`, "liveEvicted");
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

export { TERMS, setTerms, term, chip, backgroundPanel, phaseCell, processorFact };
