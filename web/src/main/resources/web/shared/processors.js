// The background work, drawn the same way on every page that has any: the mirror's ingest
// queue, healer and push, the monitor's fold, stability gate and fitness pass. A chip
// explains itself out of the document's own `terms`, never out of a copy in this file.

import { el, fmt, fmtDur, short, shownOf } from "./page.js";
import { HELD_SHOWN, STUCK_PASS_SEC, funnelOf, heldOf, measuringOf, probeProgress, stageDeltas } from "./sync.js";

/** The glossary the document ships, set by the card before anything below draws. */
let TERMS = {};

/** The last two documents' `health.stages`, so the ingest row can show what moved. */
let stagesNow = null;
let stagesBefore = null;

/** Hand this document's stage totals in, shifting the previous ones back. */
export function setStages(stages) {
  // On identity: one document is drawn by more than one card, and must not shift twice.
  if (stages === stagesNow) return;
  stagesBefore = stagesNow;
  stagesNow = stages;
}

/** Point the glossary at this document's `terms`; every card that draws a chip calls it. */
function setTerms(terms) {
  TERMS = terms || {};
}

/** What the document says a member means, or "". `hasOwn` because the key is the document's. */
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
 * Labels and tooltips for either plane's rows. A name not in the table is drawn under the
 * router's own word rather than dropped, so a new job never runs unwatched.
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
  // The corpus tree first, captioned with the pass whose numbers they are.
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
    // Which urls it is holding, longest first as the router publishes them.
    const held = heldPanel(p);
    if (held) box.appendChild(held);
  }
  return box;
}

/** The held urls, longest first, or null when the pass is holding none. */
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
 * The clock cell: the phase, and either where the pass in flight has got to or when the
 * next one is due. A fast-lane pass can carry both, and the position wins.
 */
function phaseCell(p) {
  const cell = el("em", null, p.phase || "unknown");
  const run = measuringOf(p);
  if (run) {
    // The estimate only when the router sent one; "~0s left" must not be invented.
    const done = el("span", null,
      ` · ${fmt(run.attempted)} of ${fmt(run.toProbe)} ${run.unit}(s) this pass` +
      (run.etaSec != null ? `, ~${fmtDur(run.etaSec)} left` : ""));
    done.title = term("measuring");
    cell.appendChild(done);
    // When a unit last landed; drawn only once it is the finding.
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

/** The candidate set as a tree of what became of each url and why; `funnelOf` decides the rows. */
function funnelPanel(p) {
  const f = funnelOf(p);
  if (!f) return null;
  const box = el("div", "sy-funnel");
  for (const r of f.rows) {
    const row = el("div", "sy-tr");

    // The guides and the label in one cell, so indentation is the label's own left edge.
    const name = el("div", "sy-tr-name");
    if (r.prefix) name.appendChild(el("i", "sy-tr-guide", r.prefix));
    name.appendChild(el("b", r.tone ? `sy-tr-dot ${r.tone}` : "sy-tr-dot"));
    name.appendChild(el("span", null, r.label));
    // The document's own words where the row is one of ours.
    const why = term(r.key);
    if (why) name.title = why;
    // Hosts beside the label, not as child rows: a unit change inside a tree reads as a subtotal.
    if (r.hosts) {
      // A dash, not ` on `: the reasons are free text and may end in a preposition.
      const spread = r.largest ? ` — ${fmt(r.hosts)} host(s), largest ${fmt(r.largest)}` : ` — ${fmt(r.hosts)} host(s)`;
      name.appendChild(el("em", "sy-tr-hosts", spread));
    }
    row.appendChild(name);

    row.appendChild(el("span", "sy-tr-n", fmt(r.value)));

    // Every bar against the root, never the parent, so the proportion agrees with the indentation.
    const track = el("span", "sy-tr-track");
    const fill = el("i", r.tone || null);
    fill.style.width = `${Math.min(100, r.share * 100)}%`;
    track.appendChild(fill);
    row.appendChild(track);
    // A handful of names, then the count of what is left.
    row.title = `${fmt(r.value)} url(s) — ${(r.share * 100).toFixed(r.share < 0.01 ? 2 : 1)}% of ${fmt(f.total)}` +
      (r.examples && r.examples.length
        ? ` (e.g. ${r.examples.join(", ")}${r.unnamed ? `, and ${fmt(r.unnamed)} more — see the JSON` : ""})`
        : "");
    box.appendChild(row);
  }
  if (f.omitted) {
    box.appendChild(el("div", "sy-tr-note", `${fmt(f.omitted)} more reason(s) not drawn — see the JSON`));
  }
  // The relay's own check on the two identities, loud and only when it fails; it covers
  // children exceeding their parent, where the tree itself would look right.
  if (f.accountedFor === false) {
    const bad = el("div", "sy-tr-note warn", "these numbers do not add up — see `accountedFor` in the JSON");
    bad.title = term("accountedFor");
    box.appendChild(bad);
  }
  return box;
}

/** What each probe pass is checking for. `__proto__: null` because the key is a name off the wire. */
const PROBE_FOR = {
  __proto__: null,
  aliasFold: "checked for aliases",
  consistency: "checked for consistency",
};

/** How each pass says it had nothing to check; `0 of 0 checked` reads as a pass that is broken. */
const PROBE_NONE = {
  __proto__: null,
  aliasFold: "nothing new to check for aliases",
  consistency: "nothing new to check for consistency",
};

/** The visits row's abort partition; only the largest is drawn, the order is a tie-break. */
const ABORTS = [
  ["abortedAuthRequired", "on a NIP-42 wall"],
  ["abortedClosed", "on a CLOSED"],
  ["abortedQuiet", "on silence"],
  ["abortedUnreachable", "unreachable"],
  ["abortedUnpageable", "unpageable"],
  ["abortedGaveUp", "given up on"],
  ["abortedFailed", "failed"],
  ["abortedBackpressured", "held by our own ingest"],
];

/**
 * One line per background pass, of counters that mean something on their own. A loss
 * counter is drawn loud and only when non-zero.
 */
function processorFact(p) {
  const cell = el("span");
  const add = (text, key, loud) => {
    const part = el(loud ? "s" : "span", null, cell.children.length ? ` · ${text}` : text);
    // An empty title still sets the attribute and paints the help cursor over nothing.
    const why = key && term(key);
    if (why) part.title = why;
    cell.appendChild(part);
  };
  if (p.capacity) {
    const full = (p.queued || 0) >= p.capacity;
    // The depth is a fact; which kind of full it is, is `bottleneck`'s verdict, not copied here.
    add(`queue ${fmt(p.queued || 0)} of ${fmt(p.capacity)}${full ? " — FULL" : ""}`, "queued", full);
    // Workers working is the healthy state; only their age is the finding.
    if (p.inBatch) {
      add(`${p.inBatch} of ${p.workers} worker(s) in a batch, oldest ${fmt(p.oldestBatchSec || 0)}s`, "inBatch");
    }
    // Loud and needing no threshold: a loop that exited leaves its share of the queue undrained.
    if (p.workersRunning != null && p.workers != null && p.workersRunning < p.workers) {
      add(`${p.workers - p.workersRunning} of ${p.workers} worker(s) STOPPED`, "workersRunning", true);
    }
    // Where the time went: three faults with three remedies and one appearance from outside.
    const stages = stageDeltas(stagesNow, stagesBefore);
    if (stages.length) {
      add(`time in ingest ${stages.map((r) => `${r.stage} ${Math.round(r.share * 100)}%`).join(" ")}`, "stages");
    } else if (Array.isArray(stagesNow) && stagesNow.length) {
      // The first refresh has nothing to difference, and an absent row reads as idle.
      add("time in ingest measuring…", "stages");
    }
    if (p.accepted != null) add(`${short(p.accepted)} stored`, "accepted");
    // Mostly the same event offered once per relay holding it, so never a failure on its own.
    if (p.rejected != null) add(`${short(p.rejected)} refused, mostly duplicates`, "rejected");
    if (p.lostToStore) add(`${fmt(p.lostToStore)} LOST TO THE STORE`, "lostToStore", true);
    return cell;
  }
  if (p.prime != null) {
    // The certificate count, then the refusals that are alive, then the dead.
    add(`${fmt(p.prime)} graded prime`, "prime");
    const alive = (p.silent || 0) + (p.alias || 0) + (p.inconsistent || 0) +
                  (p.unpageable || 0) + (p.noncompliant || 0) +
                  (p["auth-refused"] || 0) + (p.restricted || 0);
    if (alive) add(`${fmt(alive)} refused while alive`, "silent");
    if (p.dead) add(`${fmt(p.dead)} dead`, "dead");
    return cell;
  }
  if (p.roster != null) {
    // Lifetime counters only; the roster split belongs to the sync card. `visitsRun` is
    // unconditional so a freshly booted pool does not render an empty cell.
    add(`${fmt(p.visitsRun || 0)} visit(s) run`, "visitsRun");
    if (p.negentropyRunning) add(`${fmt(p.negentropyRunning)} negentropy sync(s) now`, "negentropyRunning");
    if (p.negentropyRuns) add(`${fmt(p.negentropyRuns)} negentropy sync(s) of the past`, "negentropyRuns");
    // Not a fault, and not nothing: those relays are re-checked by paging or not at all.
    if (p.negentropySkipped) add(`${fmt(p.negentropySkipped)} skipped, no NIP-77`, "negentropySkipped");
    if (p.retracted) add(`${fmt(p.retracted)} RETRACTED upstream`, "retracted", true);
    // Whether the resync converges, as the largest single reason rather than seven chips.
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
  // The round-up's own line, keyed on `candidates`, which no other row publishes at the top
  // level. `sourced = excluded + heldOutDead + candidates`, in that order.
  if (p.candidates != null) {
    add(`${fmt(p.sourced ?? p.candidates)} url(s) named`, "sourced");
    if (p.excluded) add(`${fmt(p.excluded)} excluded`, "excluded");
    if (p.heldOutDead) add(`${fmt(p.heldOutDead)} held out as dead`, "heldOutDead");
    add(`${fmt(p.candidates)} handed to the passes`, "candidates");
    // Not part of the derivation: urls no relay list named this round.
    if (p.recordedOnly) add(`${fmt(p.recordedOnly)} more we hold records about`, "recordedOnly");
    return cell;
  }
  const probe = probeProgress(p);
  if (probe) {
    // How far it got and how long that took; the tooltip hangs on whichever member the
    // denominator came from.
    const took = probe.tookSec != null ? ` in ${fmtDur(probe.tookSec)}` : "";
    // An empty subject is said in words only where the row counted that subject; a router
    // publishing no `newUrls` has a zero denominator for want of a candidate set instead.
    add(probe.newOnly && !probe.candidates
        ? `${PROBE_NONE[p.name] || "nothing new to check"}${took}`
        : `${fmt(probe.checked)} of ${fmt(probe.candidates)} ${probe.newOnly ? "new " : ""}relay(s) ` +
          `${PROBE_FOR[p.name] || "checked"}${took}`,
        probe.newOnly ? "newUrls" : "unmeasured");
    return cell;
  }
  if (p.pushed != null) {
    add(`${fmt(p.pushed)} event(s) pushed back to relays missing them`, "pushed");
    // A choice and not a fault, so drawn plainly however large it gets.
    if (p.dropped) add(`${short(p.dropped)} dropped rather than backpressure the sweep`, "dropped");
    return cell;
  }
  return cell;
}

export { TERMS, setTerms, term, chip, backgroundPanel, phaseCell, processorFact };
