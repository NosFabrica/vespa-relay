// The background work, drawn the same way on every page that has any: the
// mirror's ingest queue, healer and push, the monitor's fold, stability gate
// and fitness pass. A chip explains itself out of the document's own `terms`,
// never out of a copy in this file; see `StatusVocabulary`.

import { el, fmt, fmtDur, short, shownOf } from "./page.js";
import { HELD_SHOWN, STUCK_PASS_SEC, funnelOf, heldOf, measuringOf, probeProgress, stageDeltas } from "./sync.js";

/**
 * The glossary the document ships, for the current card's `title` attributes.
 * Set by the card before anything below draws; the words are the router's.
 */
let TERMS = {};

/**
 * The last two documents' `health.stages`, so the ingest row can show what
 * moved rather than what has accumulated since boot. The derivation is
 * `stageDeltas`; only the remembering is here.
 */
let stagesNow = null;
let stagesBefore = null;

/** Hand this document's stage totals in, shifting the previous ones back. */
export function setStages(stages) {
  // Guarded on identity, not contents: one document is drawn by more than one
  // card, and shifting per card would compare a document against itself.
  if (stages === stagesNow) return;
  stagesBefore = stagesNow;
  stagesNow = stages;
}

/**
 * Point the glossary at this document's `terms`. Called by every card that
 * draws a chip, so two cards reading one section do not depend on build order.
 */
function setTerms(terms) {
  TERMS = terms || {};
}

/**
 * What the document says a member means, or nothing. `hasOwn`, because `TERMS`
 * is parsed JSON and several callers look up a key the document chose: a
 * member named `constructor` would come back as a function.
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
 * The work that is not a stream, one line each with the one number that says
 * whether it is getting anywhere. The labels and tooltips for either plane's
 * rows; a name not in the table is drawn under the router's own word rather
 * than dropped, because dropping a row to keep a card tidy is how a new job
 * runs unwatched. The probe passes' number is `probeProgress` in sync.js.
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
  // The corpus tree first, above every pass's line: every pass works on it,
  // so it is context for all of them. Captioned with the pass whose numbers
  // they are, and drawn only where asked for, since above ingest and the
  // healer it would be a chart about urls at the head of a section that
  // counts events.
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
    // Which url it is holding, on its own row under the pass's line. The
    // router publishes longest-held first, so the top of its list is the row.
    const held = heldPanel(p);
    if (held) box.appendChild(held);
  }
  return box;
}

/**
 * The held urls, longest first, or null when the pass is holding none. Only
 * the leaders: the rest are ordinary dials a second old, and `+N more` says
 * what the cut left out.
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
 * The clock cell: what the processor is doing, and either where the pass in
 * flight has got to or when the next one is due. The countdown is unset while
 * a sweep runs; a fast-lane pass can carry both, and the position wins.
 */
function phaseCell(p) {
  const cell = el("em", null, p.phase || "unknown");
  const run = measuringOf(p);
  if (run) {
    // The estimate only when the router sent one: it is withheld until a unit
    // has landed and once the last one has, and "~0s left" must not be invented.
    const done = el("span", null,
      ` · ${fmt(run.attempted)} of ${fmt(run.toProbe)} ${run.unit}(s) this pass` +
      (run.etaSec != null ? `, ~${fmtDur(run.etaSec)} left` : ""));
    done.title = term("measuring");
    cell.appendChild(done);
    // When a unit last landed, which `~0s left` cannot say. Drawn only past
    // the threshold; under it the number is between 0 and 2 on a healthy pass.
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
 * The candidate set divided: every url the streams named, once, into what
 * became of it and then why. Every decision behind it is `funnelOf` in
 * sync.js. The keys carry the numbers: `inconsistent` is a dozen urls in
 * thousands and cannot be drawn at a visible width.
 */
function funnelPanel(p) {
  const f = funnelOf(p);
  if (!f) return null;
  const box = el("div", "sy-funnel");
  for (const r of f.rows) {
    const row = el("div", "sy-tr");

    // The guides and the label in one cell, so indentation is the label's own
    // left edge rather than a column measured against the longest name.
    const name = el("div", "sy-tr-name");
    if (r.prefix) name.appendChild(el("i", "sy-tr-guide", r.prefix));
    name.appendChild(el("b", r.tone ? `sy-tr-dot ${r.tone}` : "sy-tr-dot"));
    name.appendChild(el("span", null, r.label));
    // The document's own words where the row is one of ours; a free-text
    // reason and a hostname are their own explanation.
    const why = term(r.key);
    if (why) name.title = why;
    // What a reason's urls resolve to as servers, beside the label rather than
    // as child rows: a unit change inside a tree of url counts reads as a subtotal.
    if (r.hosts) {
      // A dash, not ` on `: the reasons are free text off the wire, and two of
      // the gate's end in a preposition.
      const spread = r.largest ? ` — ${fmt(r.hosts)} host(s), largest ${fmt(r.largest)}` : ` — ${fmt(r.hosts)} host(s)`;
      name.appendChild(el("em", "sy-tr-hosts", spread));
    }
    row.appendChild(name);

    row.appendChild(el("span", "sy-tr-n", fmt(r.value)));

    // Every bar against the root, never the parent: the indentation already
    // carries that reading and the proportion must not contradict it.
    const track = el("span", "sy-tr-track");
    const fill = el("i", r.tone || null);
    fill.style.width = `${Math.min(100, r.share * 100)}%`;
    track.appendChild(fill);
    row.appendChild(track);
    // The names to a readable handful, then the count of what is left; see
    // NAMES_IN_TOOLTIP.
    row.title = `${fmt(r.value)} url(s) — ${(r.share * 100).toFixed(r.share < 0.01 ? 2 : 1)}% of ${fmt(f.total)}` +
      (r.examples && r.examples.length
        ? ` (e.g. ${r.examples.join(", ")}${r.unnamed ? `, and ${fmt(r.unnamed)} more — see the JSON` : ""})`
        : "");
    box.appendChild(row);
  }
  if (f.omitted) {
    box.appendChild(el("div", "sy-tr-note", `${fmt(f.omitted)} more reason(s) not drawn — see the JSON`));
  }
  // The relay's own check on the two identities, loud and only when it fails.
  // The tree's `not accounted for` row covers a parent whose children fall
  // short; this covers the other direction, where nothing looks wrong.
  if (f.accountedFor === false) {
    const bad = el("div", "sy-tr-note warn", "these numbers do not add up — see `accountedFor` in the JSON");
    bad.title = term("accountedFor");
    box.appendChild(bad);
  }
  return box;
}

/**
 * What each probe pass is checking for. Both publish the same `streams` shape,
 * so one generic sentence made the stability gate report the fold's question.
 * `__proto__: null` because the key is a name off the wire.
 */
const PROBE_FOR = {
  __proto__: null,
  aliasFold: "checked for aliases",
  consistency: "checked for consistency",
};

/**
 * How each pass says it had nothing to check, which on a settled corpus is
 * its state for most of a monthly TTL. Written out, because `0 of 0 new
 * relay(s) checked` reads as a pass that is broken.
 */
const PROBE_NONE = {
  __proto__: null,
  aliasFold: "nothing new to check for aliases",
  consistency: "nothing new to check for consistency",
};

/**
 * The visits row's abort partition and the words the card says it in. The
 * engine's own enum order, which is only a tie-break: the largest is drawn.
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

/**
 * What one background pass has to show for itself: one line, and only
 * counters that mean something on their own. A loss counter is drawn loud and
 * only when non-zero.
 */
function processorFact(p) {
  const cell = el("span");
  /** Append a fact, hanging the document's own words on it where it has some. */
  const add = (text, key, loud) => {
    const part = el(loud ? "s" : "span", null, cell.children.length ? ` · ${text}` : text);
    // Guarded: an empty title still sets the attribute, which paints the help
    // cursor over a tooltip the browser then declines to show.
    const why = key && term(key);
    if (why) part.title = why;
    cell.appendChild(part);
  };
  if (p.capacity) {
    const full = (p.queued || 0) >= p.capacity;
    // The depth is a fact; which kind of full it is, is a verdict. `bottleneck`
    // decides that once, for the log and the chip together, so this row states
    // what it measured and does not carry a copy of the router's threshold.
    add(`queue ${fmt(p.queued || 0)} of ${fmt(p.capacity)}${full ? " — FULL" : ""}`, "queued", full);
    // Workers working is the healthy state and only their age is the finding.
    // No thread dump can give it: a worker suspended in a store call has no frame.
    if (p.inBatch) {
      add(`${p.inBatch} of ${p.workers} worker(s) in a batch, oldest ${fmt(p.oldestBatchSec || 0)}s`, "inBatch");
    }
    // Loud, and needing no threshold: a loop that exited leaves its share of
    // the queue with nothing draining it, and no age can show that.
    if (p.workersRunning != null && p.workers != null && p.workersRunning < p.workers) {
      add(`${p.workers - p.workersRunning} of ${p.workers} worker(s) STOPPED`, "workersRunning", true);
    }
    // Where the time went, under the queue and workers it explains: `dedup`,
    // `write` and `lock.ingest.wait` are three faults with three remedies and
    // one appearance from outside.
    const stages = stageDeltas(stagesNow, stagesBefore);
    if (stages.length) {
      add(`time in ingest ${stages.map((r) => `${r.stage} ${Math.round(r.share * 100)}%`).join(" ")}`, "stages");
    } else if (Array.isArray(stagesNow) && stagesNow.length) {
      // Said rather than omitted: the totals are cumulative, so the first
      // refresh has nothing to difference, and an absent row reads as idle.
      add("time in ingest measuring…", "stages");
    }
    if (p.accepted != null) add(`${short(p.accepted)} stored`, "accepted");
    // Mostly the same event offered once per relay holding it, so drawn beside
    // `accepted` and never as a failure on its own.
    if (p.rejected != null) add(`${short(p.rejected)} refused, mostly duplicates`, "rejected");
    if (p.lostToStore) add(`${fmt(p.lostToStore)} LOST TO THE STORE`, "lostToStore", true);
    return cell;
  }
  if (p.prime != null) {
    // The certificate count first, then the refusals that are alive (the
    // argument for the word `prime` over `live`), then the dead.
    add(`${fmt(p.prime)} graded prime`, "prime");
    const alive = (p.silent || 0) + (p.alias || 0) + (p.inconsistent || 0) +
                  (p.unpageable || 0) + (p.noncompliant || 0) +
                  (p["auth-refused"] || 0) + (p.restricted || 0);
    if (alive) add(`${fmt(alive)} refused while alive`, "silent");
    if (p.dead) add(`${fmt(p.dead)} dead`, "dead");
    return cell;
  }
  if (p.roster != null) {
    // The pool's lifetime counters, not its split: the roster members belong
    // to the sync card's pool summary. `visitsRun` is unconditional, because a
    // pool that has just booted has zero of everything and a cell guarded all
    // the way down renders empty beside a row that is plainly working.
    add(`${fmt(p.visitsRun || 0)} visit(s) run`, "visitsRun");
    if (p.negentropyRunning) add(`${fmt(p.negentropyRunning)} negentropy sync(s) now`, "negentropyRunning");
    if (p.negentropyRuns) add(`${fmt(p.negentropyRuns)} negentropy sync(s) of the past`, "negentropyRuns");
    // Not a fault, and not nothing: those relays are re-checked by paging or
    // not at all, which is a config question the tooltip names.
    if (p.negentropySkipped) add(`${fmt(p.negentropySkipped)} skipped, no NIP-77`, "negentropySkipped");
    if (p.retracted) add(`${fmt(p.retracted)} RETRACTED upstream`, "retracted", true);
    // A visit that aborts leaves its relay unreconciled, so this is whether the
    // resync converges. Drawn as the largest single reason rather than seven
    // chips; the partition is on the document.
    if (p.abortedVisits) {
      add(`${fmt(p.abortedVisits)} aborted`, "abortedVisits");
      const worst = ABORTS.map(([member, word]) => [p[member] || 0, member, word])
        .sort((a, b) => b[0] - a[0])[0];
      if (worst[0]) add(`${fmt(worst[0])} ${worst[2]}`, worst[1]);
    }
    // Not a fault: a relay that has told us its filter width is one we can now finish.
    if (p.narrowedRelays) add(`${fmt(p.narrowedRelays)} asked in kind chunks`, "narrowedRelays");
    if (p.liveEvicted) add(`${fmt(p.liveEvicted)} live subscription(s) rotated out`, "liveEvicted");
    if (p.poolReceived) add(`${short(p.poolReceived)} events in`, "poolReceived");
    return cell;
  }
  // The round-up's own line, keyed on `candidates`, which no other row
  // publishes at the top level. The derivation in the order it happens,
  // `sourced = excluded + heldOutDead + candidates`, with the yield last; the
  // two drops only when non-zero.
  if (p.candidates != null) {
    add(`${fmt(p.sourced ?? p.candidates)} url(s) named`, "sourced");
    if (p.excluded) add(`${fmt(p.excluded)} excluded`, "excluded");
    if (p.heldOutDead) add(`${fmt(p.heldOutDead)} held out as dead`, "heldOutDead");
    add(`${fmt(p.candidates)} handed to the passes`, "candidates");
    // Not part of the derivation and drawn last: urls no relay list named this
    // round, which the tree above spends a whole branch on.
    if (p.recordedOnly) add(`${fmt(p.recordedOnly)} more we hold records about`, "recordedOnly");
    return cell;
  }
  const probe = probeProgress(p);
  if (probe) {
    // One fact: how far it got and how long that took. The verdicts are the
    // tree at the head of the section. "new" where the row counted what
    // arrived undecided, and the tooltip hangs on whichever member the
    // denominator came from; see `probeProgress`.
    const took = probe.tookSec != null ? ` in ${fmtDur(probe.tookSec)}` : "";
    // A pass whose subject is empty says so in words, only where the row
    // counted that subject: a router publishing no `newUrls` has a zero
    // denominator for want of a candidate set, a different fact.
    add(probe.newOnly && !probe.candidates
        ? `${PROBE_NONE[p.name] || "nothing new to check"}${took}`
        : `${fmt(probe.checked)} of ${fmt(probe.candidates)} ${probe.newOnly ? "new " : ""}relay(s) ` +
          `${PROBE_FOR[p.name] || "checked"}${took}`,
        probe.newOnly ? "newUrls" : "unmeasured");
    return cell;
  }
  if (p.pushed != null) {
    add(`${fmt(p.pushed)} event(s) pushed back to relays missing them`, "pushed");
    // The sweep declining to backpressure itself, a choice and not a fault, so
    // drawn plainly however large it gets.
    if (p.dropped) add(`${short(p.dropped)} dropped rather than backpressure the sweep`, "dropped");
    return cell;
  }
  return cell;
}

export { TERMS, setTerms, term, chip, backgroundPanel, phaseCell, processorFact };
