// What the monitor has decided about a relay url, read back off the wire: the router signs
// its verdicts as NIP-66 kind 30166 records (`RelayAliasRecord` on the Kotlin side), one per
// url, addressed on `d`. This is the parsing half and is pure; the page supplies the events.

/** NIP-66's relay-discovery record. `d` is the relay url. */
export const MONITOR_KIND = 30166;

/** The fold verdict, in both its forms. Not a NIP-66 tag. */
export const SAME_AS = "same-as";

/** The stability verdict: did this url answer one filter the same way twice? */
export const SELF_CONSISTENT = "self-consistent";

/** NIP-32's label, where the fitness grade lives beside other monitors' labels, so match the namespace. */
export const LABEL = "l";

export const LABEL_NAMESPACE = "L";

export const FITNESS_NAMESPACE = "relay.fitness";

/** The one grade that admits a relay to a stream's roster. */
export const PRIME = "prime";

/** NIP-66's value on `n` for a url reached over a circuit. */
export const NETWORK_TOR = "tor";

/** Did this url's measurement go over Tor? A `.onion` with no `n` still counts. */
export function onTor(rec) {
  if (rec.network) return rec.network === NETWORK_TOR;
  return rec.host.endsWith(".onion");
}

/**
 * The NIP-66 tags rendered besides the verdicts. `v`, `g` and `T` are absent because
 * nothing writes them, and an always-empty column reads as a relay that declined to answer.
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
  // The fitness pass's measured facts.
  "pageable",
  "nip77",
  "compliant",
  ...Object.keys(RENDERED),
]);

/** How long a verdict stands; must match `RelayAliasRecord.DEFAULT_TTL_SECONDS`. */
export const TTL_SECONDS = 30 * 24 * 60 * 60;

/** Where a verdict tag carries the unix second it was measured. */
const MEASURED_AT_INDEX = 3;

/** Where it carries the version of the rules that measured it. */
const EPOCH_INDEX = 4;

/** The same positions on a NIP-32 label, one to the right: the spec fixes index 2 as the namespace. */
const LABEL_NAMESPACE_INDEX = 2;

const LABEL_EVIDENCE_INDEX = 3;

const LABEL_MEASURED_AT_INDEX = 4;

const LABEL_EPOCH_INDEX = 5;

/** Must match `RelayVerdictRecord.FITNESS_EPOCH`; an out-of-epoch grade draws as expired. */
export const FITNESS_EPOCH = "2";

/**
 * Must match `RelayAliasRecord.FOLD_EPOCH` and `CONSISTENCY_EPOCH`: the router discards a
 * verdict under older rules, and a panel that disagreed would draw a dialled url as folded.
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
 * The hostname a url reaches: no port, no path, lowercased. Must match `RelayAliases.hostOf`.
 * An IPv6 literal keeps its brackets, so only a colon after the bracket is the port.
 */
export function hostOf(url) {
  const authority = afterScheme(url).split("/")[0];
  const host = authority.startsWith("[") ? authority.split("]")[0] + "]" : authority.split(":")[0];
  return host.toLowerCase();
}

/**
 * Are these two strings the same relay url? A cleared verdict's `same-as` may spell its own
 * url with a trailing slash.
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

/** One record read into what it claims, or null. The three verdicts are read independently. */
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
    // The record's clock, not the verdict's: the monitor rewrites the record on every connect.
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
    // `extra` counts tag names this reader does not know.
    requirements: [],
    supportedNips: [],
    extra: 0,
    hasDoc: !!(event.content && event.content.length),
  };
  for (const t of tags) {
    // `R` and `N` may appear several times.
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
    // The verdict's own clock, never the event's as a fallback; null is what `isCurrent` refuses.
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
    // A value this reader does not recognise is not a verdict.
    if (consistent[1] === "true") out.stable = true;
    else if (consistent[1] === "false") out.stable = false;
  }
  return out;
}

/** Is a verdict still one the router would act on: under today's rules ([want]) and inside the TTL? */
export function isCurrent(at, nowSec, epoch, want) {
  return at != null && at >= nowSec - TTL_SECONDS && epoch === want;
}

/**
 * The records grouped the way the fold groups urls: by host, most urls first. A paged
 * read may serve more than one version of an address; the newest `created_at` wins.
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
    // Every counter is on what the router would act on today.
    if (rec.fold && current) group.folded++;
    else if (rec.cleared && current) group.cleared++;
    else if (rec.fold || rec.cleared) group.expired++;
    if (rec.stable === false && stableCurrent) group.unstable++;
  }
  for (const group of hosts.values()) {
    // The survivor usually has no record of its own, so it is synthesised from what the
    // folds point at, flagged, and only when the target is on this host.
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
        // Every field a read record has.
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
 * Every record the relay holds, paged newest-first on `until`. A page that is all one
 * `created_at` cannot move the cursor, so the page grows up to [maxPage] while that holds;
 * a longer run ends the walk with `complete` false. [ask] is `(limit, until) -> events`.
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
    // Saturated and all one second: double the ask until the page spans two.
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
    // Nothing new at the relay's ceiling: stepping below would skip the rest of that second.
    if (events.length >= size && size >= maxPage && oneSecond(events)) break;
    // A page the timeout cut says nothing about what lies below `oldest`; hold the cursor.
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

/** The totals above the rows. `silent` is a url no pass has reached, which is not "not folded". */
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
  // `prime` alone cannot tell a clearnet roster from an onion one.
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
      if (u.synthetic) continue;
      // A grade counts as having been looked at.
      if (!u.fold && !u.cleared && u.stable == null && !u.grade) silent++;
      // Only what the router would act on.
      if (u.stable === true && u.stableCurrent) stable++;
    }
  }
  return { hosts: groups.length, urls, folded, cleared, expired, silent, stable, unstable, inferred, graded, prime, primeTor };
}
