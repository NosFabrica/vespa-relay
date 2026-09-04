// The sync card's judgements over the router's progress document: is the
// router alive, where the constraint is, which legs are worth naming and when
// one is stuck. Functions over plain data; the page formats the numbers and
// hangs the document's own words on them. `web/src/test/js/sync.test.mjs`
// asserts them.

/** Past this, a leg is not slow, it is stuck. The router's own log floor. */
export const STUCK_LEG_SEC = 600;

/**
 * How many held relays the table names. Unbounded: the router already caps its
 * rows (`RelayRotation.DEFAULT_IN_FLIGHT_ROWS`) and `more` discloses the rest.
 */
export const IN_FLIGHT_SHOWN = Infinity;

/** Past this, a probe pass has stopped. Drawn only past it; under it the number is ordinary. */
export const STUCK_PASS_SEC = 300;

/**
 * How many held urls a processor's line names. A probe pass holds hundreds of
 * ordinary dials a second old; the router sorts longest-held first, so the
 * front is the answer and `more` discloses the rest.
 */
export const HELD_SHOWN = 3;

/**
 * How many host names a reason puts in its hover title. A presentation cut,
 * not a data one: the document keeps the router's full inventory, and a native
 * `title` is one unwrapped run of text.
 */
export const NAMES_IN_TOOLTIP = 12;

/**
 * The constraint verdicts, keyed by the router's word. Four different faults
 * with different fixes, so each names what to look at next.
 */
export const BOTTLENECK = {
  // The key is a word off the wire: without this `bottleneck: "constructor"`
  // reaches Object.prototype and the destructure below throws.
  __proto__: null,
  ingest: ["ingest is the limit", "Ingest's queue is full, so every download is backpressured behind it. Look at ingest and the store behind it, not at the relays."],
  // Full and nothing draining it: the workers are held inside a store round
  // trip, so the queue is a stopped pipeline, not backpressure.
  wedged: ["ingest has stopped", "Ingest's queue is full and its workers are stuck in a batch that is not finishing. The store is not answering, nothing queued here is being written, and nothing in the router will end it — the remedy is at the store."],
  downloads: ["relays are the limit", "Ingest drains as fast as it fills. The mirror is going as fast as the upstreams will serve it."],
  upstream: ["nothing arriving", "The queue is empty and no events are reaching it — look at discovery, the guards and the transport, not at ingest."],
  mixed: ["keeping up", "The queue is neither full nor empty: nothing here is the constraint."],
};

/**
 * How much of the socket budget is spent, and whether it is the constraint.
 * `open` near the ceiling is the healthy state of a mirror that stays
 * connected; only `queued` above zero is a fault. Null where the router does
 * not publish the ceiling.
 */
export function socketsOf(health) {
  const ceiling = Number.isFinite(health?.socketCeiling) ? health.socketCeiling : null;
  if (ceiling == null || !Number.isFinite(health?.sockets)) return null;
  // A router too old to publish the queue says nothing, which must not read
  // as "nothing is queued".
  const queued = Number.isFinite(health?.socketsQueued) ? health.socketsQueued : null;
  return {
    open: health.sockets,
    ceiling,
    running: Number.isFinite(health?.socketsRunning) ? health.socketsRunning : null,
    queued,
    share: Math.min(1, health.sockets / Math.max(1, ceiling)),
    // The only reading worth a colour; near the ceiling is not one.
    starved: (queued || 0) > 0,
  };
}

/**
 * The constraint verdict, or null where the document carries no word.
 * Guarded on its own member: the gauges are copied independently on the relay
 * side, so a document can carry numbers with no word or the word with no numbers.
 */
export function constraintOf(health) {
  const word = health?.bottleneck;
  if (!word) return null;
  const [text, why] = BOTTLENECK[word] || [word, ""];
  return { word, text, why, tone: word === "ingest" || word === "wedged" ? "warn" : null };
}

/** Stages drawn on the row. The tail of a long list is noise beside the head of it. */
export const STAGES_SHOWN = 4;

/**
 * Where the ingest time went between two polls, as shares of the interval:
 * the document's milliseconds are cumulative and the poll window varies.
 * A stage that went backwards is a restarted counter and is dropped.
 */
export function stageDeltas(now, before) {
  if (!Array.isArray(now) || !Array.isArray(before)) return [];
  const was = new Map(before.map((r) => [r.stage, r.ms]));
  const all = [];
  for (const r of now) {
    if (!r || typeof r.stage !== "string" || !Number.isFinite(r.ms)) continue;
    const had = was.get(r.stage);
    if (!Number.isFinite(had) || r.ms < had) continue;
    const ms = r.ms - had;
    if (ms > 0) all.push({ stage: r.stage, ms });
  }
  // Of the whole interval, not of the rows that survive the cut.
  const total = all.reduce((sum, r) => sum + r.ms, 0);
  if (!total) return [];
  all.sort((a, b) => b.ms - a.ms);
  const shown = all.slice(0, STAGES_SHOWN);
  // A lock wait never appears alone: its `.hold` and `write` are pulled up
  // beside it, past the cut if need be.
  const waiting = shown.filter((r) => r.stage.endsWith(".wait"));
  for (const w of waiting) {
    for (const name of [w.stage.replace(/\.wait$/, ".hold"), "write"]) {
      if (shown.some((r) => r.stage === name)) continue;
      const companion = all.find((r) => r.stage === name);
      if (companion) shown.push(companion);
    }
  }
  return shown
    .sort((a, b) => b.ms - a.ms)
    .map((r) => ({ ...r, share: r.ms / total }));
}

/**
 * Past this, an outstanding store call has stopped. The fallback only: the
 * router's own `slowAfterSec` wins where published, and 0 there turns the log
 * off without saying what a page should mark.
 */
export const STUCK_CALL_SEC = 60;

/** How many outstanding store calls the row names; the router publishes up to two hundred, longest first. */
export const CALLS_SHOWN = 6;

/**
 * The document's `store` section as the card draws it. Null for a document
 * with no section, which is a router too old to book its calls: nothing drawn,
 * not "0 outstanding".
 */
export function storeOf(store) {
  if (!store || typeof store !== "object") return null;
  // The router's own bound; `STUCK_CALL_SEC` only stands in.
  const stuckSec = store.slowAfterSec > 0 ? store.slowAfterSec : STUCK_CALL_SEC;
  const all = Array.isArray(store.calls) ? store.calls.filter((c) => c && typeof c.caller === "string") : [];
  const rows = all.slice(0, CALLS_SHOWN).map((c) => ({
    caller: c.caller,
    op: typeof c.op === "string" ? c.op : "—",
    // Absent is "this call carries no filter", never a gap in the report.
    asked: typeof c.asked === "string" && c.asked ? c.asked : null,
    elapsedSec: c.elapsedSec || 0,
    // 0 is a reading (nothing was out when it was issued); null is a router
    // that declined to say.
    outstandingAtIssue: c.outstandingAtIssue ?? null,
    hot: (c.elapsedSec || 0) >= stuckSec,
  }));
  const callers = (Array.isArray(store.callers) ? store.callers : [])
    .filter((c) => c && typeof c.caller === "string")
    .map((c) => ({
      caller: c.caller,
      issued: c.issued || 0,
      returned: c.returned || 0,
      failed: c.failed || 0,
      cancelled: c.cancelled || 0,
      outstanding: c.outstanding || 0,
      oldestOutstandingSec: c.oldestOutstandingSec ?? null,
      // A store the schema has drifted under fails in milliseconds; one that
      // has stopped answering shows up as an age. Both are hot.
      hot: (c.failed || 0) > 0 || (c.oldestOutstandingSec ?? 0) >= stuckSec,
    }));
  // Shares of the whole outstanding set, not of the drawn bands: the router
  // publishes empty bands too.
  const banded = (Array.isArray(store.ages) ? store.ages : []).filter((a) => a && Number.isFinite(a.fromSec));
  const inBands = banded.reduce((sum, a) => sum + (a.calls || 0), 0);
  const ages = banded
    .filter((a) => (a.calls || 0) > 0)
    .map((a) => ({
      fromSec: a.fromSec,
      calls: a.calls || 0,
      share: inBands ? (a.calls || 0) / inBands : 0,
      hot: a.fromSec >= stuckSec,
    }));
  return {
    outstanding: store.outstanding || 0,
    // What a row was marked against, so the card can say so.
    stuckSec,
    issued: store.issued || 0,
    returned: store.returned || 0,
    failed: store.failed || 0,
    cancelled: store.cancelled || 0,
    rows,
    // The router's truncation plus this one, so an operator can subtract.
    more: (store.omitted || 0) + (all.length - rows.length),
    callers,
    ages,
    // The router's partition of the outstanding set closing. Drawn as a note
    // when it does not.
    accountedFor: !banded.length || inBands === (store.outstanding || 0),
  };
}

/** The phase word a processor carries while a pass is dialling: `Processors.MEASURING`. */
export const MEASURING = "measuring";

/**
 * How far a probe pass has got: the complement of `unmeasured`, which is what
 * still has no verdict. A folded url is out of both halves, and where the row
 * publishes `newUrls` that is the denominator instead (`newOnly`). `tookSec`
 * is withheld while a pass runs; it belongs to the previous one.
 */
export function probeProgress(p) {
  const streams = p?.streams || [];
  if (!streams.length) return null;
  const sum = (member) => streams.reduce((a, w) => a + (w[member] || 0), 0);
  const folded = sum("foldedAway");
  // Presence, not truthiness: zero new urls is an answer, and `||` would fall
  // back to the whole candidate set exactly when the fold has caught up.
  const fresh = streams.some((w) => w.newUrls != null) ? sum("newUrls") : null;
  const candidates = fresh ?? Math.max(0, sum("candidates") - folded);
  const unmeasured = sum("unmeasured");
  return {
    candidates,
    checked: Math.max(0, candidates - unmeasured),
    newOnly: fresh != null,
    tookSec: p.phase === MEASURING ? null : (p.lastPassSec ?? null),
  };
}

/**
 * Where the pass running right now has got to, the live half of `probeProgress`.
 * Null unless the router published a real denominator; `attempted` is clamped
 * into it because both are read off a live pass.
 */
export function measuringOf(p) {
  const m = p?.measuring;
  if (!m || !(m.toProbe > 0)) return null;
  return {
    unit: m.unit || "url",
    attempted: Math.max(0, Math.min(m.attempted || 0, m.toProbe)),
    toProbe: m.toProbe,
    // Absent until a unit has landed and again once the last one has; both
    // are "no estimate", where a zero would claim the pass is done.
    etaSec: m.etaSec ?? null,
    // `etaSec` reads 0 both one url from done and with the last url wedged;
    // this tells them apart. Absent is a router that predates the member.
    quietForSec: m.quietForSec ?? null,
  };
}

/**
 * The urls a probe pass is holding. Not `legsOf`: a probe leg has no events
 * and is decided by which step it is on, and the router sorts these
 * longest-held first because a long one is the anomaly.
 */
export function heldOf(inFlight, limit = IN_FLIGHT_SHOWN) {
  const all = inFlight?.relays || [];
  const rows = all.slice(0, limit).map((r) => ({
    relay: r.relay,
    // The scheme is dropped and nothing else is: it is the thing being looked up.
    short: String(r.relay || "").replace(/^wss?:\/\//, ""),
    heldForSec: r.heldForSec || 0,
    // Null on a router that predates the member: "not known", never a step.
    stage: r.stage || null,
  }));
  return { rows, more: (inFlight?.omitted || 0) + (all.length - rows.length) };
}

/** The phase word a visit-mode stream carries — `StreamPhases.Phase.Rotating`. */
export const ROTATING = "rotating";

/**
 * The page's own word for a configured stream the router has published no
 * phase for. Not one of `StreamPhases.Phase`, so the glossary has no entry and
 * the mark carries no tooltip.
 */
export const STARTING = "starting";

/**
 * What a rotating stream is riding. `waiting` is a judgement about the roster,
 * made here: before the fitness pass has signed its first `prime`, a stream on
 * discovered relays has an empty world and looks busy from every other member.
 */
export function rotationOf(s) {
  if (s?.phase !== ROTATING || s.roster == null) return null;
  return { roster: s.roster, tails: s.liveHeld ?? null, waiting: s.roster === 0 };
}

/**
 * The tone of each funnel slice, keyed by the router's own words. The
 * `undecided` reasons are free text off the wire, so an unrecognised reason
 * draws neutral and still gets its segment. `__proto__: null` for
 * `BOTTLENECK`'s reason.
 */
const FUNNEL_TONE = {
  __proto__: null,
  // Only one verdict is a fault, and it is the router's to report; see the
  // glossary's `inconsistent`.
  consistent: "good",
  inconsistent: "warn",
  // Neither a fault nor a finding.
  foldedAway: "mute",
  heldOutDead: "mute",
  recordedOnly: "mute",
  // Ours, in both senses: we could not carry it, or our probe broke.
  "declined by our own transport": "ours",
  "the probe failed mid-walk": "ours",
  // The arithmetic not closing must look wrong.
  unattributed: "warn",
};

/**
 * Every discovered url, once, into what became of it, as a tree rooted at
 * everything this router knows of. Every bar is a share of the root, so a host
 * under a reason is visibly a sliver of the corpus. A node whose children fall
 * short gets an `unattributed` child; a pass that publishes no verdict member
 * gets no tree, and nothing is invented from a missing member.
 */
export function funnelOf(p) {
  const streams = p?.streams || [];
  if (!streams.length) return null;
  const sum = (member) => streams.reduce((a, w) => a + (w[member] || 0), 0);
  const candidates = sum("candidates");
  if (!candidates) return null;
  // Absent is not zero: `sum` cannot tell a missing member from a real zero.
  if (!streams.some((w) => w.foldedAway != null || w.consistent != null || w.inconsistent != null)) return null;

  const excluded = Math.max(0, p.excluded || 0);
  const heldOutDead = Math.max(0, p.heldOutDead || 0);
  const dropped = excluded + heldOutDead;
  // Urls that left the relay lists but whose measurements the fold still
  // reads. Rooted at `sourced` alone the tree loses them without a word.
  const recordedOnly = Math.max(0, p.recordedOnly || 0);
  // Both readings of the SAME set, and the max picks whichever this router
  // publishes rather than summing them: `candidates + dropped` is the funnel's
  // own arithmetic, `sourced + recordedOnly` is the mouth the glossary names.
  // Adding `recordedOnly` to `candidates` double counted every url in it —
  // staging drew 40,285 where the router knew 20,152.
  const total = Math.max(candidates + dropped, (p.sourced || 0) + recordedOnly);

  /** One node. `children` is built by the callers below, never inferred. */
  const node = (key, label, value, children = []) => ({
    key, label, value,
    share: total ? value / total : 0,
    tone: FUNNEL_TONE[key] || null,
    children,
  });

  // A reason is a leaf. Its hosts are two numbers, not rows: `hosts` is how
  // many servers the urls resolve to and `largest` the widest one's share,
  // which together tell a dead network spread thin from three servers.
  const asReason = (row) => {
    const value = Math.max(0, row.urls || 0);
    const top = (row.top || []).filter((h) => h && h.host && h.urls > 0);
    const named = row.examples?.length ? row.examples : top.map((h) => h.host);
    return {
      ...node(row.reason, row.reason, value),
      hosts: row.hosts || 0,
      largest: top[0]?.urls || 0,
      // Cut for the tooltip, not the document; the row says how many it did
      // not name.
      examples: named.slice(0, NAMES_IN_TOOLTIP),
      unnamed: Math.max(0, (row.hosts || named.length) - Math.min(named.length, NAMES_IN_TOOLTIP)),
    };
  };

  // Rows that refine another row go under it. The router publishes a flat list
  // summing to `unmeasured`, each row naming its parent; the parent has no
  // urls of its own and is synthesised from its children.
  const all = firstReasons(streams).filter((r) => (r.urls || 0) > 0);
  const children = new Map();
  for (const row of all) {
    if (!row.parent) continue;
    if (!children.has(row.parent)) children.set(row.parent, []);
    children.get(row.parent).push(row);
  }
  const reasons = [];
  const drawn = new Set();
  for (const row of all) {
    const group = row.parent || (children.has(row.reason) ? row.reason : null);
    if (group) {
      // A row whose name is also a parent is consumed as that parent, not
      // drawn beside it: a document carrying both would count every url twice,
      // an overshoot `unattributed` cannot report.
      if (drawn.has(group)) continue;
      drawn.add(group);
      const kids = children.get(group).map(asReason);
      reasons.push(node(group, group, kids.reduce((a, k) => a + k.value, 0), kids));
      continue;
    }
    reasons.push(asReason(row));
  }
  reasons.sort((a, b) => b.value - a.value);

  const kept = [
    node("foldedAway", "folded onto another url", sum("foldedAway")),
    node("consistent", "consistent", sum("consistent")),
    node("inconsistent", "inconsistent — refused", sum("inconsistent")),
    node("unmeasured", "no verdict", sum("unmeasured"), reasons),
  ];
  // Keyed `corpus`, not `sourced`: `sourced` is a published member with an
  // exact meaning and the root is that plus what only our records know.
  const root =
    node("corpus", "every relay url this router knows of", total, [
      node("dropped", "dropped before a pass could see it", dropped, [
        node("excluded", "excluded by config, or our own url", excluded),
        node("heldOutDead", "known dead — a signed unreachability record", heldOutDead),
      ]),
      // `recordedOnly` is NOT a sibling here. The router derives
      // `known = sourced + recordedOnly` and `candidates = known - dead`, so a
      // node beside `candidates` counts every url in it twice. The summary
      // line still reports the number, outside the derivation where it
      // belongs — and without the old label's claim that no relay list names
      // them, which is a fact about the round that was read, not about the
      // urls: a short source read makes it say the opposite of the truth.
      node("candidates", "in reach — the candidate set", candidates, kept),
    ]);
  // The relay's own check of the arithmetic. `unattributed` only reports a
  // parent whose children fall short, so a false here is drawn as a note even
  // when every bar looks whole.
  const claimed = streams.map((w) => w.accountedFor).filter((v) => v != null);
  return {
    total, candidates, root, rows: flatten(root), omitted: firstOmitted(streams),
    accountedFor: claimed.length ? claimed.every(Boolean) : null,
  };
}

/**
 * The tree as rows, depth-first, each carrying its box-drawing prefix. A `│` is
 * drawn at every ancestor that still has a sibling below it, which a depth
 * counter cannot know; the `unattributed` check runs here, once, on the
 * finished tree.
 */
function flatten(root) {
  const rows = [];
  const walk = (n, depth, prefix, last) => {
    rows.push({ ...n, depth, prefix: depth === 0 ? "" : prefix + (last ? "└─ " : "├─ ") });
    const kids = n.children.slice();
    const named = kids.reduce((a, k) => a + k.value, 0);
    if (kids.length && n.value > named) {
      kids.push({ key: "unattributed", label: "not accounted for", value: n.value - named,
                  share: root.value ? (n.value - named) / root.value : 0, tone: "warn", children: [] });
    }
    const below = depth === 0 ? "" : prefix + (last ? "   " : "│  ");
    kids.forEach((k, i) => walk(k, depth + 1, below, i === kids.length - 1));
  };
  walk(root, 0, "", true);
  return rows;
}

/**
 * The `undecided` rows across every stream row, widest first. Concatenated,
 * not merged by reason: the passes publish one row, `all streams`.
 */
function firstReasons(streams) {
  const rows = streams.flatMap((w) => (w.undecided?.reasons || []).filter((r) => r && r.reason));
  return rows.sort((a, b) => (b.urls || 0) - (a.urls || 0));
}

/** Reasons either side dropped, so a truncated breakdown never reads as the whole one. */
function firstOmitted(streams) {
  return streams.reduce((a, w) => a + (w.undecided?.omitted || 0), 0);
}

/**
 * The legs to draw, and how full each quiet bar is. The bar is a share of
 * `STUCK_LEG_SEC`, the threshold its colour keys off, never of the rows; the
 * router publishes them quietest first and this adds what it drops to `omitted`.
 */
export function legsOf(inFlight, limit = IN_FLIGHT_SHOWN) {
  const all = inFlight?.relays || [];
  const rows = all.slice(0, limit).map((r) => {
    const quiet = r.quietForSec || 0;
    return {
      relay: r.relay,
      // The scheme is dropped and nothing else is: it is the thing being looked up.
      short: String(r.relay || "").replace(/^wss?:\/\//, ""),
      pass: r.pass != null ? String(r.pass) : null,
      heldForSec: r.heldForSec || 0,
      events: r.events || 0,
      quietForSec: quiet,
      quietShare: Math.min(1, quiet / STUCK_LEG_SEC),
      hot: quiet >= STUCK_LEG_SEC,
      // Absent means "not on a socket", a different fault from a slow download.
      slotless: r.transferringForSec == null,
      transferringForSec: r.transferringForSec ?? null,
      // The root `live` list names an owner; a stream's own `inFlight` does
      // not, and `poolsOf` supplies it from the row's position.
      stream: r.stream || null,
      // Straight from the router; see `doing` in the glossary. Null on a
      // router that predates the member.
      doing: r.doing || null,
      // The stable word `poolsOf` groups by, never the sentence above. Null is
      // a row in none of the four pools, which is a state and not a gap.
      pool: r.pool || null,
      // `created_at = 0` is a real second relays serve, not a leg with no cursor.
      pagingUntil: r.pagingUntil ?? null,
    };
  });
  return { rows, more: (inFlight?.omitted || 0) + (all.length - rows.length) };
}


/**
 * The four pools as the router names them (`VisitPool.POOL_LIVE` and its
 * neighbours): the `pool` member on every held row, and the word this module
 * groups by. Never derived from `doing`, which is prose that gets reworded.
 */
export const POOL_LIVE = "live";
export const POOL_CATCHING_UP = "catching-up";
export const POOL_REFETCHING = "re-fetching";
export const POOL_NEGENTROPY = "negentropy";

/**
 * The fifth group, which is not a pool: a visit claiming its socket or
 * draining the healer on its way out carries no `pool`. A group rather than a
 * filter, because a relay held for an hour claiming a socket is the row an
 * operator is looking for.
 */
export const POOL_BETWEEN = "between";

/**
 * The fifth budgeted job, which is not a pool either: a stream's dial width.
 * No held row carries it; it appears only in the limits table.
 */
export const JOB_VISITING = "visiting";

/** The order the panel draws them in: the steady state first, then the work. */
export const POOL_ORDER = [POOL_LIVE, POOL_CATCHING_UP, POOL_REFETCHING, POOL_NEGENTROPY];

/** The four pools plus the leftovers: the order the panel walks. */
const GROUP_ORDER = [...POOL_ORDER, POOL_BETWEEN];

/**
 * What each pool is called on the page and what it means. Display strings
 * only: nothing groups by them, so a reword cannot empty a table. The two
 * that re-read the past are labelled as a pair.
 */
export const POOL_LABELS = {
  __proto__: null,
  [POOL_LIVE]: ["live", "Tail subscriptions held open. No worker sits on these — events arrive the moment they exist, and the socket is the whole cost."],
  [POOL_CATCHING_UP]: ["catching up", "Paging forward over what each relay's band does not cover yet — the ordinary sync, newest-first towards the last pass."],
  [POOL_REFETCHING]: ["re-fetching the past", "Paging over history the band ALREADY covers, because the stream's `refetchThePastSeconds` expired it. Same walk as a catch-up and a completely different bill: these relays are re-downloading years."],
  [POOL_NEGENTROPY]: ["negentropy the past", "Reconciling the WHOLE past over NIP-77 and downloading only the difference. It does not stop at what the bands cover, and that is the point: a relay that back-filled behind a catch-up leaves the band saying `walked` and the events missing, so a pass bounded by the bands could never find them. Comparing id sets rather than pages is what makes the whole range affordable. `negentropyConcurrency` is its budget."],
  [JOB_VISITING]: ["visits", "How many relays may be VISITED for this stream at once — its share of the dial width. A visit that cannot get one of these does not dial at all, so this bounds simultaneous TLS handshakes and not merely work."],
  [POOL_BETWEEN]: ["between jobs", "In none of the four: claiming a socket, working out what an ask still owes, or draining the healer's queue on the way out of a visit. Ordinary and usually brief — a row that sits here is one to look at."],
};

/**
 * Every relay this mirror is holding, grouped by what it is asked for. Never
 * drops a row: one whose `pool` the router did not publish lands in
 * `POOL_BETWEEN`, and `omitted` is summed rather than attributed. Empty groups
 * are kept, since "nothing is auditing" is an answer. Null when nothing is
 * held anywhere. `totals` is read off the rotating pool's own row, not summed
 * from the streams, which double-count every relay two streams both want.
 */
export function poolsOf(progress, held = heldRows(progress)) {
  if (!held.rows.length) return null;
  const groups = groupByPool(held.rows);
  return { groups, omitted: held.omitted, totals: poolTotals(progress, groups) };
}

/**
 * Every held relay the document names, in one list where each row knows its
 * stream and its pool. Collected once because the grouping runs twice over
 * the same rows.
 */
export function heldRows(progress) {
  const rows = [];
  let omitted = 0;
  for (const s of progress?.streams || []) {
    const legs = legsOf(s.inFlight);
    omitted += legs.more;
    // A visiting row takes its owner from its position in the document.
    for (const r of legs.rows) rows.push({ ...r, stream: s.name || null });
  }
  const live = legsOf(progress?.live);
  omitted += live.more;
  // A live row names its own stream: one subscription is held per (relay,
  // stream) pair, and `legsOf` has already read it.
  rows.push(...live.rows);
  return { rows, omitted };
}

/**
 * The rows into the panel's groups. `owner` is the stream whose heading the
 * rows are already under, if any; a column repeating a heading is not a column.
 */
function groupByPool(rows, owner = null) {
  const byPool = new Map(GROUP_ORDER.map((key) => [key, []]));
  for (const r of rows) {
    // An unknown word lands with the unpooled: a heading invented from a string
    // off the wire is how a typo becomes a pool.
    byPool.get(POOL_ORDER.includes(r.pool) ? r.pool : POOL_BETWEEN).push(r);
  }

  const groups = [];
  for (const key of GROUP_ORDER) {
    const found = byPool.get(key);
    // The four named pools always appear; an empty `between` is the healthy
    // case and would be a heading that reads the same every tick.
    if (key === POOL_BETWEEN && !found.length) continue;
    // The router's order, re-applied because the merge across streams
    // interleaves lists that were each sorted alone.
    found.sort((a, b) => b.quietForSec - a.quietForSec || b.heldForSec - a.heldForSec || a.relay.localeCompare(b.relay));
    const [label, what] = POOL_LABELS[key] || [key, ""];
    groups.push({
      key,
      label,
      what,
      rows: found,
      // One word for the whole group, lifted into the heading; null where the
      // rows disagree, so the negentropy pool keeps the column.
      doing: found.length && found.every((r) => r.doing === found[0].doing) ? found[0].doing : null,
      // Drawn wherever a row can name its owner, except under a heading that
      // already has. Not "do the rows disagree": a pool holding one row is
      // where the attribution is scarcest.
      streams: !owner && found.some((r) => r.stream),
    });
  }
  return groups;
}

/**
 * The order the jobs are drawn in, the router's own (`PoolLimits.JOBS`):
 * a dial width, a tail budget, then the two walks that spend real bandwidth.
 */
const JOB_ORDER = [JOB_VISITING, POOL_LIVE, POOL_CATCHING_UP, POOL_REFETCHING, POOL_NEGENTROPY];

/**
 * One row per job out of the limits list and the schedule list, both keyed
 * by (stream, job). The union is walked, in `JOB_ORDER` with anything the
 * router has since added on the end, and a row with only one half draws the
 * other as absent.
 */
export function jobsOf(limits, schedule) {
  const by = new Map();
  const seen = [];
  const slot = (job) => {
    if (!by.has(job)) {
      by.set(job, { job, label: POOL_LABELS[job]?.[0] || job || "—", limit: null, schedule: null });
      seen.push(job);
    }
    return by.get(job);
  };
  for (const l of limits) slot(l.job).limit = l;
  for (const r of schedule) slot(r.job).schedule = r;
  // The router's order, then whatever it has since added; never dropped for
  // being a word this page was not taught.
  const order = [...JOB_ORDER.filter((j) => by.has(j)), ...seen.filter((j) => !JOB_ORDER.includes(j))];
  return order.map((j) => by.get(j));
}

/**
 * One section per configured stream, in the document's order, whatever the
 * stream is doing: its phase, roster, four pools, budgets and schedule. Every
 * held, limit and schedule row lands under exactly one section, and whatever
 * no configured stream claims gets a section of its own rather than being
 * dropped. The three are filtered against one `claimed` set on purpose.
 * `held` is taken rather than read so the summary cannot disagree with the
 * sections under it.
 */
export function streamSections(progress, held = heldRows(progress)) {
  const limits = limitsOf(progress);
  const schedule = scheduleOf(progress);
  const out = [];
  const claimed = new Set();
  const mine = (list, name) => list.filter((r) => r.stream === name);
  for (const s of progress?.streams || []) {
    const name = s?.name || null;
    claimed.add(name);
    const groups = groupByPool(mine(held.rows, name), name);
    out.push({
      stream: name,
      // A configured stream always has a phase mark. Defaulted here rather
      // than at the mark, so the unattributed section below carries null.
      phase: s?.phase || STARTING,
      phaseForSec: num(s?.phaseForSec),
      // A judgement rather than a number: a rotating stream with an empty
      // roster is waiting on the fitness pass.
      rotation: rotationOf(s),
      groups,
      totals: streamTotals(s, groups),
      jobs: jobsOf(mine(limits, name), mine(schedule, name)),
      holding: groups.reduce((a, g) => a + g.rows.length, 0),
    });
  }
  // Whatever no configured stream claimed, only when there is something.
  const looseRows = held.rows.filter((r) => !claimed.has(r.stream));
  const looseLimits = limits.filter((r) => !claimed.has(r.stream));
  const looseSchedule = schedule.filter((r) => !claimed.has(r.stream));
  if (looseRows.length || looseLimits.length || looseSchedule.length) {
    // No owner here: the heading says only that nothing claimed these, so a
    // row's own name is worth a column.
    const groups = groupByPool(looseRows);
    out.push({
      stream: null, phase: null, phaseForSec: null, rotation: null,
      groups, totals: streamTotals(null, groups),
      jobs: jobsOf(looseLimits, looseSchedule), holding: looseRows.length,
    });
  }
  return out;
}

/** The rotating pool's own processor row, the only place its size is published. */
const VISITS_PROCESSOR = "visits";

/**
 * How big the pool is and how its relays are split. `units` (`rosterVisits`)
 * is what `working`, `queued` and `waiting` partition, because the pool's unit
 * is a (relay, stream) pair; `relays` is context. `tailed` crosses the three:
 * a live unit keeps its subscription while it is revisited. Null members
 * where the document does not say.
 */
export function poolTotals(progress, groups) {
  const row = (progress?.processors || []).find((p) => p && p.name === VISITS_PROCESSOR);
  return totalsOf(groups, {
    relays: num(row?.roster),
    units: num(row?.rosterVisits),
    queued: num(row?.awaitingVisit),
  });
}

/** A published member, or null where the document does not carry it. */
const num = (v) => (Number.isFinite(v) ? v : null);

/**
 * The arithmetic both cuts share: what the groups hold and what is left of
 * the denominator. One function, because the per-stream lines are read against
 * the pool's.
 */
function totalsOf(groups, { relays, units, queued }) {
  const working = groups.reduce((a, g) => a + (g.key === POOL_LIVE ? 0 : g.rows.length), 0);
  return {
    relays,
    units,
    working,
    queued,
    // A tailed unit keeps its tail while it is revisited, so this crosses the
    // three above rather than joining them.
    tailed: groups.find((g) => g.key === POOL_LIVE)?.rows.length ?? 0,
    // Off units, never relays. Never negative: the counts are read at one tick
    // but not one instant, and "-2 between visits" reads as a router bug.
    waiting: units == null || queued == null ? null : Math.max(0, units - working - queued),
  };
}

/**
 * The same numbers for one stream, off its own row: the pool-wide totals
 * cannot be divided into these. Inside one stream a relay is exactly one
 * unit, so `units` is `relays`, and the pool blocks draw their `n of m` from it.
 */
function streamTotals(s, groups) {
  const relays = num(s?.roster);
  return totalsOf(groups, { relays, units: relays, queued: num(s?.awaitingVisit) });
}

/**
 * One row per (stream, job) the router publishes a limit for, flattened across
 * streams because caps are read as a table. Uncapped rows are dropped unless
 * they have deferred something, which cannot happen without a cap.
 */
export function limitsOf(progress) {
  const rows = [];
  for (const s of progress?.streams || []) {
    for (const l of s.limits || []) {
      const streamCap = Number.isFinite(l.streamCap) ? l.streamCap : null;
      const deferred = l.deferred || 0;
      if (streamCap == null && !deferred) continue;
      rows.push({
        stream: s.name || null,
        job: l.job || null,
        label: POOL_LABELS[l.job]?.[0] || l.job || "—",
        streamCap,
        // Zero permits out is a real reading, not a missing number.
        inUse: Number.isFinite(l.inUse) ? l.inUse : null,
        deferred,
        // At the cap is not a fault; at the cap with work turned away is. Both
        // halves: `deferred` is cumulative since boot. An uncapped row only
        // reaches here by having deferred something.
        biting: deferred > 0 && (streamCap == null || l.inUse >= streamCap),
      });
    }
  }
  return rows;
}

/**
 * When each stream's scheduled re-reads come due. `neverRun` is its own
 * number, not folded into `due`: a fresh deployment is all due, and only a
 * period that has elapsed is the schedule doing something.
 */
export function scheduleOf(progress) {
  const rows = [];
  for (const s of progress?.streams || []) {
    for (const r of s.schedule || []) {
      const due = r.due || 0;
      const neverRun = r.neverRun || 0;
      rows.push({
        stream: s.name || null,
        job: r.job || null,
        label: POOL_LABELS[r.job]?.[0] || r.job || "—",
        everySec: Number.isFinite(r.everySec) ? r.everySec : null,
        due,
        neverRun,
        waiting: r.waiting || 0,
        // Absent means every ask is already due, which is not a zero countdown.
        nextInSec: Number.isFinite(r.nextInSec) ? r.nextInSec : null,
        // Due work with nothing waiting behind it and nothing new: a fresh
        // deployment is all due and healthy, so `due` alone cannot say this.
        backedUp: due > 0 && r.waiting === 0 && neverRun === 0,
      });
    }
  }
  return rows;
}

/**
 * The freshness buckets, freshest first: how old the newest event held from a
 * pair is. The other axis from `SYNC_STATUSES`: `complete` says nothing about
 * the present.
 */
export const FRESHNESS = [
  ["current", "current", "live"],
  ["today", "today", "live"],
  ["thisWeek", "this week", "busy"],
  ["older", "older", "warn"],
  ["nothing", "nothing yet", "warn"],
];

/**
 * The sync statuses a prime (relay, stream) pair can be in, worst first: the
 * document's word, the label and the tone. A contract with `RelayStatusReport`.
 */
export const SYNC_STATUSES = [
  ["refused", "refused", "warn"],
  ["notStarted", "hasn't started", "warn"],
  ["paging", "paging", "busy"],
  ["complete", "complete", "live"],
];

/**
 * The per-relay table. The chips come off `statuses`, never off `rows`, which
 * `RelayStatusReport.MAX_ROWS` cuts; every status is returned even at zero.
 * `visiting` and `tailed` ride beside the status rather than folding into it.
 */
export function relayStatusOf(relays) {
  if (!relays || !relays.pairs) return null;
  const counted = new Map((relays.statuses || []).map((s) => [s.syncStatus, s.pairs || 0]));
  const fresh = new Map((relays.freshness || []).map((f) => [f.behind, f.pairs || 0]));
  // The headline, off the document's own partition, as a share: a count means
  // nothing without its denominator.
  const current = fresh.get("current") || 0;
  return {
    pairs: relays.pairs,
    current,
    currentShare: relays.pairs ? current / relays.pairs : 0,
    chips: SYNC_STATUSES.map(([key, label, tone]) => ({ key, label, tone, pairs: counted.get(key) || 0 })),
    freshness: FRESHNESS.map(([key, label, tone]) => ({ key, label, tone, pairs: fresh.get(key) || 0 })),
    rows: (relays.rows || []).map((r) => ({
      relay: r.relay || "",
      // The scheme is dropped and nothing else is: it is the thing being looked up.
      short: String(r.relay || "").replace(/^wss?:\/\//, ""),
      stream: r.stream || null,
      syncStatus: r.syncStatus || null,
      label: SYNC_STATUSES.find(([k]) => k === r.syncStatus)?.[1] || r.syncStatus || "—",
      // A unit owes one ask per bound provider, so this is what separates
      // 39-of-40 from 1-of-40 under `paging`. Withheld where it would restate
      // the status.
      progress: r.syncStatus !== "complete" && r.asks > 1 ? `${r.settled || 0}/${r.asks}` : null,
      // The router's own verdict, spanning both axes; re-deriving it from
      // `syncStatus` is how the page and the document come to disagree.
      hot: !!r.fault,
      behindSec: Number.isFinite(r.behindSec) ? r.behindSec : null,
      behind: r.behind || null,
      behindLabel: FRESHNESS.find(([k]) => k === r.behind)?.[1] || null,
      // `negentropy` is a tri-state; absent is "not measured", not false.
      negentropy: typeof r.negentropy === "boolean" ? r.negentropy : null,
      kindCap: Number.isFinite(r.kindCap) ? r.kindCap : null,
      // Null rather than 0 for every clock and edge: a 1970 would read as a
      // walk that reached the epoch.
      coveredFrom: Number.isFinite(r.coveredFrom) ? r.coveredFrom : null,
      coveredTo: Number.isFinite(r.coveredTo) ? r.coveredTo : null,
      verifiedAgoSec: Number.isFinite(r.verifiedAgoSec) ? r.verifiedAgoSec : null,
      refusedAgoSec: Number.isFinite(r.refusedAgoSec) ? r.refusedAgoSec : null,
      // The router's reading of which wall and the relay's own sentence, in one cell.
      why: r.refusedFor ? (r.relaySaid ? `${r.refusedFor} — ${r.relaySaid}` : r.refusedFor) : null,
      visiting: !!r.visiting,
      tailed: !!r.tailed,
    })),
    omitted: relays.omitted || 0,
  };
}
