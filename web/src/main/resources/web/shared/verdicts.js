// What the monitor has decided about a relay url, read back off the wire. The
// router signs its fold and stability answers as NIP-66 kind 30166 records in
// this store (`RelayAliasRecord`, Kotlin side), one per url, addressed on `d`.
// The rollup on `/stats.json` cannot say what the store holds about one url or
// when it was measured, so the panel reads the records themselves with a plain
// NIP-01 REQ over the relay's own websocket: the same path a client takes, so
// a verdict unreadable here is unreadable by anyone. This module is the
// parsing half and is pure (no websocket, no DOM); the page supplies the events.

/** NIP-66's relay-discovery record. `d` is the relay url. */
export const MONITOR_KIND = 30166;

/** The fold verdict, in both its forms. Not a NIP-66 tag; every other consumer skips it as unknown. */
export const SAME_AS = "same-as";

/** The stability verdict: did this url answer one filter the same way twice? */
export const SELF_CONSISTENT = "self-consistent";

/**
 * NIP-32's label, where the monitor's fitness grade lives. The same record
 * carries other monitors' country, ISP and ASN labels on `l`, so a reader must
 * match the namespace, never the tag name alone.
 */
export const LABEL = "l";

export const LABEL_NAMESPACE = "L";

export const FITNESS_NAMESPACE = "relay.fitness";

/** The one grade that admits a relay to a stream's roster. */
export const PRIME = "prime";

/** NIP-66's value for a url reached over a circuit, written on `n` by the same pass that writes the grade. */
export const NETWORK_TOR = "tor";

/**
 * Did this url's measurement go over Tor? The record's own `n` answers when
 * present; a `.onion` with no `n` still counts, since an onion address has no
 * other transport.
 */
export function onTor(rec) {
  if (rec.network) return rec.network === NETWORK_TOR;
  return rec.host.endsWith(".onion");
}

/**
 * The NIP-66 tags this reader renders besides the verdicts. Seeing `n` and
 * `rtt-open` beside `same-as` on one row is the live check that
 * `RelayVerdictRecord.edit` still carries forward the tags it does not own.
 * `v`, `g` and `T` are deliberately absent: nothing writes them (see `RelayFacts`),
 * and an always-empty column reads as a relay that declined to answer.
 */
const RENDERED = {
  n: "network",
  R: "requirements",
  N: "supportedNips",
  "rtt-open": "rttOpen",
  "rtt-read": "rttRead",
  "rtt-write": "rttWrite",
  s: "software",
};

/** Tags this panel accounts for. Anything else is counted, never dropped silently. */
const OWNED = new Set([
  "d",
  SAME_AS,
  SELF_CONSISTENT,
  LABEL,
  LABEL_NAMESPACE,
  // The fitness pass's measured facts. A tag that pass writes is never an unknown tag.
  "pageable",
  "nip77",
  "compliant",
  ...Object.keys(RENDERED),
]);

/**
 * How long a verdict stands, matching `RelayAliasRecord.DEFAULT_TTL_SECONDS`.
 * Duplicated rather than fetched, so the panel shows the age next to the
 * verdict and a drifted copy is visible rather than hidden.
 */
export const TTL_SECONDS = 30 * 24 * 60 * 60;

/** Where a verdict tag carries the unix second it was measured. */
const MEASURED_AT_INDEX = 3;

/** Where it carries the version of the rules that measured it. */
const EPOCH_INDEX = 4;

/**
 * The same positions on a NIP-32 label, each one to the right: the spec fixes
 * index 2 as the namespace. Spelled out rather than derived, because the two
 * shapes are set by different specs.
 */
const LABEL_NAMESPACE_INDEX = 2;

const LABEL_EVIDENCE_INDEX = 3;

const LABEL_MEASURED_AT_INDEX = 4;

const LABEL_EPOCH_INDEX = 5;

/** The fitness pass's rules version, `RelayVerdictRecord.FITNESS_EPOCH`. An out-of-epoch grade draws as expired, with its evidence. */
export const FITNESS_EPOCH = "2";

/**
 * The rule versions the router acts on, `RelayAliasRecord.FOLD_EPOCH` and
 * `CONSISTENCY_EPOCH`. A verdict under older rules is discarded and
 * re-measured by the router, so this panel must agree or it would draw a url
 * as folded that the fan-out is dialling.
 */
export const FOLD_EPOCH = "2";

export const CONSISTENCY_EPOCH = "1";

/** Everything after `ws://` or `wss://`, or the string unchanged. */
function afterScheme(url) {
  const lower = url.toLowerCase();
  if (lower.startsWith("wss://")) return url.slice(6);
  if (lower.startsWith("ws://")) return url.slice(5);
  return url;
}

/**
 * The hostname a url reaches: no port, no path, lowercased. Must match
 * `RelayAliases.hostOf`, which decides which urls are one group. An IPv6
 * literal keeps its brackets, so only a colon after the bracket is the port.
 */
export function hostOf(url) {
  const authority = afterScheme(url).split("/")[0];
  const host = authority.startsWith("[") ? authority.split("]")[0] + "]" : authority.split(":")[0];
  return host.toLowerCase();
}

/**
 * Are these two strings the same relay url? Compared after normalising, never
 * by string: a cleared verdict's `same-as` points at the record's own url,
 * possibly spelled with a trailing slash.
 */
export function sameUrl(a, b) {
  const norm = (u) => {
    const rest = afterScheme(u);
    const slash = rest.indexOf("/");
    const authority = (slash < 0 ? rest : rest.slice(0, slash)).toLowerCase();
    // Path case is preserved (a path is opaque), but a lone trailing slash is not a path.
    const path = (slash < 0 ? "" : rest.slice(slash)).replace(/\/+$/, "");
    return authority + path;
  };
  return norm(a) === norm(b);
}

/**
 * One record, read into what it claims, or null when it claims nothing this
 * panel can draw. The three verdicts are read independently: a url may carry
 * any subset of them.
 */
export function readRecord(event) {
  const tags = event.tags || [];
  const tag = (name) => tags.find((t) => t.length > 1 && t[0] === name);
  const d = tag("d");
  if (!d) return null;
  const url = d[1];
  const out = {
    url,
    host: hostOf(url),
    author: event.pubkey,
    // The record's clock, not the verdict's: the monitor rewrites the record
    // on every connect. Kept only to date the record itself.
    recordAt: event.created_at,
    fold: null,
    cleared: false,
    stable: null,
    foldEvidence: null,
    foldMeasuredAt: null,
    foldEpoch: null,
    stableEvidence: null,
    stableMeasuredAt: null,
    stableEpoch: null,
    grade: null,
    gradeEvidence: null,
    gradeMeasuredAt: null,
    gradeEpoch: null,
    // `extra` counts tag names this reader does not know, so a record carrying
    // something new is visible as such.
    requirements: [],
    supportedNips: [],
    extra: 0,
    // The NIP-11-ish document in the content, kept as a flag and not parsed.
    hasDoc: !!(event.content && event.content.length),
  };
  for (const t of tags) {
    // `R` and `N` may appear several times; a relay can be both auth-gated and paid.
    if (t[0] === "R") out.requirements.push(t[1]);
    else if (t[0] === "N") out.supportedNips.push(t[1]);
    else if (RENDERED[t[0]]) out[RENDERED[t[0]]] = t[1];
    else if (!OWNED.has(t[0])) out.extra++;
  }
  // Our namespace's label only: `["l", "CA", "countryCode"]` is not a grade.
  const graded = tags.find(
    (t) => t.length > LABEL_NAMESPACE_INDEX && t[0] === LABEL && t[LABEL_NAMESPACE_INDEX] === FITNESS_NAMESPACE,
  );
  if (graded) {
    out.grade = graded[1] || null;
    out.gradeEvidence = graded[LABEL_EVIDENCE_INDEX] || null;
    out.gradeMeasuredAt = Number(graded[LABEL_MEASURED_AT_INDEX]) || null;
    out.gradeEpoch = graded[LABEL_EPOCH_INDEX] || null;
  }
  const sameAs = tag(SAME_AS);
  if (sameAs) {
    out.foldEvidence = sameAs[2] || null;
    // The verdict's own clock, never the event's as a fallback: null means
    // "this record does not say", which is what `isCurrent` refuses.
    out.foldMeasuredAt = Number(sameAs[MEASURED_AT_INDEX]) || null;
    out.foldEpoch = sameAs[EPOCH_INDEX] || null;
    if (sameUrl(sameAs[1], url)) out.cleared = true;
    else out.fold = sameAs[1];
  }
  const consistent = tag(SELF_CONSISTENT);
  if (consistent) {
    out.stableEvidence = consistent[2] || null;
    out.stableMeasuredAt = Number(consistent[MEASURED_AT_INDEX]) || null;
    out.stableEpoch = consistent[EPOCH_INDEX] || null;
    // A value this reader does not recognise is not a verdict; it is ignored, not guessed at.
    if (consistent[1] === "true") out.stable = true;
    else if (consistent[1] === "false") out.stable = false;
  }
  return out;
}

/**
 * Is a verdict still one the router would act on: measured under the rules it
 * applies today ([want]) and inside the TTL? An aged-out verdict is re-taken
 * on the url's next turn; an out-of-epoch one shows the new build's first
 * pass has not reached this host yet.
 */
export function isCurrent(at, nowSec, epoch, want) {
  return at != null && at >= nowSec - TTL_SECONDS && epoch === want;
}

/**
 * The records grouped the way the fold groups urls: by host, most urls first.
 * A duplicate is a property of a url next to another one, so the host's other
 * urls must be in view. 30166 is addressable and a paged read may serve more
 * than one version of an address; the newest `created_at` per url wins.
 */
export function groupByHost(events, nowSec) {
  const newest = new Map();
  for (const event of events) {
    const rec = readRecord(event);
    if (!rec) continue;
    const held = newest.get(rec.url);
    if (!held || held.recordAt < rec.recordAt) newest.set(rec.url, rec);
  }
  const hosts = new Map();
  for (const rec of newest.values()) {
    if (!hosts.has(rec.host)) {
      hosts.set(rec.host, { host: rec.host, urls: [], folded: 0, cleared: 0, unstable: 0, expired: 0, graded: 0, prime: 0, primeTor: 0 });
    }
    const group = hosts.get(rec.host);
    const current = isCurrent(rec.foldMeasuredAt, nowSec, rec.foldEpoch, FOLD_EPOCH);
    const gradeCurrent = isCurrent(rec.gradeMeasuredAt, nowSec, rec.gradeEpoch, FITNESS_EPOCH);
    const stableCurrent = isCurrent(rec.stableMeasuredAt, nowSec, rec.stableEpoch, CONSISTENCY_EPOCH);
    group.urls.push({ ...rec, foldCurrent: current, gradeCurrent, stableCurrent });
    if (rec.grade && gradeCurrent) group.graded++;
    if (rec.grade === PRIME && gradeCurrent) {
      group.prime++;
      // Counted beside `prime` so the two cannot disagree.
      if (onTor(rec)) group.primeTor++;
    }
    // Every counter is on what the router would act on today; `expired`
    // covers both the fold and the cleared form.
    if (rec.fold && current) group.folded++;
    else if (rec.cleared && current) group.cleared++;
    else if (rec.fold || rec.cleared) group.expired++;
    if (rec.stable === false && stableCurrent) group.unstable++;
  }
  for (const group of hosts.values()) {
    // The survivor usually has no record of its own: `RelayAliases.learn`
    // clears only a leader nothing folded onto, so the url a group collapsed
    // to is the one with no `same-as`. It is synthesised from what the folds
    // point at, and flagged, and only when the target is on this host.
    const known = new Set(group.urls.map((u) => u.url));
    for (const u of [...group.urls]) {
      if (!u.fold || !u.foldCurrent) continue;
      if (known.has(u.fold) || hostOf(u.fold) !== group.host) continue;
      known.add(u.fold);
      group.urls.push({
        url: u.fold,
        host: group.host,
        synthetic: true,
        author: null,
        recordAt: null,
        fold: null,
        cleared: false,
        stable: null,
        foldCurrent: false,
        gradeCurrent: false,
        foldEvidence: null,
        foldMeasuredAt: null,
        foldEpoch: null,
        stableEvidence: null,
        stableMeasuredAt: null,
        stableEpoch: null,
        // Every field a read record has, so the renderer can iterate it.
        requirements: [],
        supportedNips: [],
        extra: 0,
        hasDoc: false,
        grade: null,
        gradeEvidence: null,
        gradeMeasuredAt: null,
        gradeEpoch: null,
      });
      group.inferred = (group.inferred || 0) + 1;
    }
    // Survivors first, then shortest.
    group.urls.sort((a, b) => {
      const survivor = (u) => (!u.fold || !u.foldCurrent ? 0 : 1);
      return survivor(a) - survivor(b) || a.url.length - b.url.length || a.url.localeCompare(b.url);
    });
    group.survivors = group.urls.filter((u) => !u.fold || !u.foldCurrent).length;
  }
  return [...hosts.values()].sort((a, b) => b.urls.length - a.urls.length || a.host.localeCompare(b.host));
}

/**
 * Every record of this kind the relay holds, paged newest-first.
 *
 * A page that is entirely one `created_at` cannot move the cursor, and
 * stepping below it loses the rest of that second, so the page grows while
 * that holds, up to [maxPage] (the relay's NIP-11 `max_limit`), as
 * `RelayDiscovery.scan` does. A run longer than the relay will serve ends the
 * walk with `complete` false. `complete` is true only when an empty page came
 * back on an EOSE, not on our timeout. [ask] is `(limit, until) -> events` so
 * the walk can be tested without a relay.
 */
export async function walkRecords({
  ask,
  pageSize = 500,
  maxPage = 500,
  maxRecords = 40000,
  maxPages = 120,
  onProgress = () => {},
}) {
  const seen = new Map();
  let until = null;
  let stalls = 0;
  let complete = false;
  let grew = 0;
  let pages = 0;
  for (let p = 0; p < maxPages && seen.size < maxRecords; p++) {
    let size = pageSize;
    let events = await ask(size, until);
    pages++;
    // Saturated and all one second: double the ask until the page spans two,
    // or until the relay's own ceiling.
    while (events.length >= size && size < maxPage && oneSecond(events)) {
      size = Math.min(size * 2, maxPage);
      events = await ask(size, until);
      pages++;
      grew++;
    }
    if (!events.length) { complete = events.complete !== false; break; }
    const before = seen.size;
    let oldest = Infinity;
    for (const ev of events) {
      seen.set(ev.id, ev);
      if (ev.created_at < oldest) oldest = ev.created_at;
    }
    onProgress(seen.size);
    if (seen.size > before) {
      until = oldest;
      stalls = 0;
      continue;
    }
    // Nothing new at the relay's ceiling: stepping below would skip the rest
    // of that second, so the read ends partial.
    if (events.length >= size && size >= maxPage && oneSecond(events)) break;
    // A page the timeout cut says nothing about what lies below `oldest`;
    // hold the cursor and ask again. A second short answer ends the walk
    // through the stall break, with `complete` still false.
    if (events.complete !== false) until = oldest - 1;
    if (++stalls >= 2) break;
  }
  return { events: [...seen.values()], complete, pages, grew };
}

/** Does this page cover a single `created_at`? Then its cursor cannot advance. */
function oneSecond(events) {
  if (!events.length) return false;
  let min = Infinity;
  let max = -Infinity;
  for (const e of events) {
    if (e.created_at < min) min = e.created_at;
    if (e.created_at > max) max = e.created_at;
  }
  return min === max;
}

/**
 * The totals a reader needs before any row: how many records answered and how
 * many say anything at all. `silent` is a url no pass has reached, which is
 * not "not folded": the fold's pill wears those words on most rows.
 */
export function summarise(groups, nowSec) {
  let urls = 0;
  let folded = 0;
  let cleared = 0;
  let expired = 0;
  let silent = 0;
  let unstable = 0;
  let stable = 0;
  let inferred = 0;
  let graded = 0;
  let prime = 0;
  // How much of what we admit is reachable over a circuit; `prime` alone
  // cannot tell a clearnet roster from an onion one.
  let primeTor = 0;
  for (const group of groups) {
    urls += group.urls.length;
    folded += group.folded;
    cleared += group.cleared;
    expired += group.expired;
    unstable += group.unstable;
    inferred += group.inferred || 0;
    graded += group.graded;
    prime += group.prime;
    primeTor += group.primeTor;
    for (const u of group.urls) {
      // A synthesised survivor carries no verdict by construction.
      if (u.synthetic) continue;
      // A grade counts as having been looked at.
      if (!u.fold && !u.cleared && u.stable == null && !u.grade) silent++;
      // Only what the router would act on, like `unstable` in the groups.
      if (u.stable === true && u.stableCurrent) stable++;
    }
  }
  return { hosts: groups.length, urls, folded, cleared, expired, silent, stable, unstable, inferred, graded, prime, primeTor };
}
