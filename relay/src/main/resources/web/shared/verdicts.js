// What the monitor has DECIDED about a relay url, read back off the wire.
//
// The router signs its fold and stability answers as NIP-66 kind 30166 records
// in this very store (`RelayAliasRecord`, Kotlin side), one per url, addressed
// on `d`. Everything else on `/stats.json` is a rollup: counts, and the fold's
// own summary of how many urls collapsed onto how many relays. None of it can
// answer the question an operator actually has when a duplicate is still being
// dialled — **what does this store say about THIS url, and when was it
// measured?**
//
// So this reads the records themselves, as a plain NIP-01 REQ over the relay's
// own websocket. That is deliberate on two counts:
//
//  * it is the same path a client takes, so a verdict that cannot be read here
//    cannot be read by anyone, and the panel is a protocol check as much as a
//    view. The kind histogram used to do exactly this with NIP-45 COUNTs and
//    the page lost it when that page was replaced — the note at the top of
//    stats.html said the check "wants to be a test, not a page". That was right
//    about counts, which have a rollup. It is wrong about verdicts, which have
//    no other reader at all.
//  * it needs no new endpoint. The records are already served; the only thing
//    missing was something that asked for them.
//
// This module is the parsing half and is PURE — no websocket, no DOM — because
// the tag semantics are the part that can silently be wrong, and they are the
// part worth testing. The page supplies the events.

/** NIP-66's relay-discovery record. `d` is the relay url. */
export const MONITOR_KIND = 30166;

/**
 * The fold verdict, in both its forms. Not a NIP-66 tag — this monitor defines
 * it — so every other consumer skips it as unknown.
 */
export const SAME_AS = "same-as";

/** The stability verdict: did this url answer one filter the same way twice? */
export const SELF_CONSISTENT = "self-consistent";

/**
 * How long a verdict stands: thirty days, matching
 * `RelayAliasRecord.DEFAULT_TTL_SECONDS`.
 *
 * Duplicated from the Kotlin rather than fetched, and that is a real risk —
 * so the panel reports the AGE next to the verdict and marks what this number
 * says is expired, rather than hiding it. A reader can then see a stale verdict
 * and its age even if this constant has drifted, which is the failure mode a
 * silently-copied threshold usually hides.
 */
export const TTL_SECONDS = 30 * 24 * 60 * 60;

/** Where a verdict tag carries the unix second it was MEASURED. */
const MEASURED_AT_INDEX = 3;

/** Everything after `ws://` or `wss://`, or the string unchanged. */
function afterScheme(url) {
  const lower = url.toLowerCase();
  if (lower.startsWith("wss://")) return url.slice(6);
  if (lower.startsWith("ws://")) return url.slice(5);
  return url;
}

/**
 * The hostname a url reaches — no port, no path, lowercased.
 *
 * Must match `RelayAliases.hostOf` exactly, because that is what decides which
 * urls are one GROUP: grouping differently here would draw a host as unfolded
 * that the router never considered a group at all. An IPv6 literal keeps its
 * brackets, so only a colon AFTER the bracket is the port separator.
 */
export function hostOf(url) {
  const authority = afterScheme(url).split("/")[0];
  const host = authority.startsWith("[") ? authority.split("]")[0] + "]" : authority.split(":")[0];
  return host.toLowerCase();
}

/**
 * Are these two strings the same relay url?
 *
 * Compared after normalising, NEVER by string. `wss://nos.lol` and
 * `wss://nos.lol/` are one url, and telling the two forms apart by `===` is how
 * a CLEARED verdict — whose `same-as` points at the record's own url — reads as
 * a fold of a url onto itself. The Kotlin reader has the same rule for the same
 * reason; getting it wrong here would draw the fold's two answers swapped.
 */
export function sameUrl(a, b) {
  const norm = (u) => {
    const rest = afterScheme(u);
    const slash = rest.indexOf("/");
    const authority = (slash < 0 ? rest : rest.slice(0, slash)).toLowerCase();
    // Path case is preserved — a path is opaque and `/Inbox` need not be
    // `/inbox` — but a lone trailing slash is not a path.
    const path = (slash < 0 ? "" : rest.slice(slash)).replace(/\/+$/, "");
    return authority + path;
  };
  return norm(a) === norm(b);
}

/**
 * One record, read into what it actually claims — or null when it claims
 * nothing this panel can draw.
 *
 * Both verdicts are read INDEPENDENTLY. A url may carry a fold, a stability
 * answer, both or neither, and an early return for a missing `same-as` would
 * hide every stability verdict on a url that was never folded. That exact bug
 * was fixed on the Kotlin side; the reader must not reintroduce it.
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
    // The RECORD's clock, which is not the verdict's — quartz's own monitor
    // rewrites this record every time we connect to the relay, so this tracks
    // the last time we TALKED to it. Kept only to date the record itself.
    recordAt: event.created_at,
    fold: null,
    cleared: false,
    stable: null,
    foldEvidence: null,
    foldMeasuredAt: null,
    stableEvidence: null,
    stableMeasuredAt: null,
  };
  const sameAs = tag(SAME_AS);
  if (sameAs) {
    out.foldEvidence = sameAs[2] || null;
    // The verdict's OWN clock. Absent on records written before it existed, and
    // then the event's is the only honest reading available — the same fallback
    // the Kotlin reader makes.
    out.foldMeasuredAt = Number(sameAs[MEASURED_AT_INDEX]) || event.created_at;
    if (sameUrl(sameAs[1], url)) out.cleared = true;
    else out.fold = sameAs[1];
  }
  const consistent = tag(SELF_CONSISTENT);
  if (consistent) {
    out.stableEvidence = consistent[2] || null;
    out.stableMeasuredAt = Number(consistent[MEASURED_AT_INDEX]) || event.created_at;
    // Anything this reader does not recognise is NOT a verdict. Ignored rather
    // than guessed at — guessing "unstable" would draw a relay as refused on a
    // tag we cannot read.
    if (consistent[1] === "true") out.stable = true;
    else if (consistent[1] === "false") out.stable = false;
  }
  return out;
}

/** Is a verdict measured at [at] still one the router would act on? */
export function isCurrent(at, nowSec) {
  return at != null && at >= nowSec - TTL_SECONDS;
}

/**
 * The records grouped the way the fold groups urls: by HOST.
 *
 * That is the whole point of the panel. A duplicate is not a property of a url,
 * it is a property of a url next to another one, so "why is this still being
 * dialled" is only answerable with the host's other urls in view — which
 * survived, which folded onto it, which were measured and kept, and which carry
 * no verdict at all.
 *
 * A record whose newest copy is the one to read: 30166 is addressable, so a
 * store may serve more than one version of an address across a paged read.
 * Keyed by url with the newest `created_at` winning, or a stale copy could draw
 * over the current verdict.
 *
 * Sorted by how much there is to explain — most urls first — because a host
 * wearing twenty urls is the one an operator opened this for.
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
    if (!hosts.has(rec.host)) hosts.set(rec.host, { host: rec.host, urls: [], folded: 0, cleared: 0, unstable: 0, expired: 0 });
    const group = hosts.get(rec.host);
    const current = isCurrent(rec.foldMeasuredAt, nowSec);
    group.urls.push({ ...rec, foldCurrent: current });
    // Counted on what the router would ACT on. A fold whose verdict has aged
    // out is not folding anything today, and counting it would draw a host as
    // collapsed while every url of it is back in the fan-out.
    if (rec.fold && current) group.folded++;
    else if (rec.cleared && current) group.cleared++;
    if (rec.fold && !current) group.expired++;
    if (rec.stable === false) group.unstable++;
  }
  for (const group of hosts.values()) {
    // THE SURVIVOR USUALLY HAS NO RECORD OF ITS OWN, and leaving it off the
    // list drew the host as having nothing left to dial.
    //
    // A url everything folded ONTO is a canonical, and `RelayAliases.learn`
    // clears — i.e. publishes a verdict about — only a leader that nothing
    // folded onto. So the very url the group collapsed to is the one url with
    // no `same-as` to find, and a group read purely from records showed
    // "23 urls · 0 dialled" for a host that is dialled exactly once. Synthesised
    // from what the folds point AT, and flagged, because "we inferred this from
    // the other records" and "the monitor said this" are different claims.
    //
    // Only when the target really is on this host: a hand-edited or malformed
    // record could point anywhere, and inventing a row for it here would put
    // one host's url inside another host's group.
    const known = new Set(group.urls.map((u) => u.url));
    for (const u of [...group.urls]) {
      if (!u.fold || !u.foldCurrent) continue;
      if (known.has(u.fold) || hostOf(u.fold) !== group.host) continue;
      known.add(u.fold);
      group.urls.push({
        url: u.fold,
        host: group.host,
        synthetic: true,
        fold: null,
        cleared: false,
        stable: null,
        foldCurrent: false,
        foldEvidence: null,
        foldMeasuredAt: null,
        stableEvidence: null,
        stableMeasuredAt: null,
      });
      group.inferred = (group.inferred || 0) + 1;
    }
    // Survivors first, then shortest — the fold's own preference order is the
    // router's opinion and not knowable here, but the url everything points at
    // is, and it belongs at the top of its own group.
    group.urls.sort((a, b) => {
      const survivor = (u) => (!u.fold || !u.foldCurrent ? 0 : 1);
      return survivor(a) - survivor(b) || a.url.length - b.url.length || a.url.localeCompare(b.url);
    });
    group.survivors = group.urls.filter((u) => !u.fold || !u.foldCurrent).length;
  }
  return [...hosts.values()].sort((a, b) => b.urls.length - a.urls.length || a.host.localeCompare(b.host));
}

/**
 * The totals a reader needs before reading any row: how many records answered,
 * and how many of them say anything at all.
 *
 * `silent` is the number worth having on screen. A 30166 record with no verdict
 * tag is one quartz's passive monitor wrote — reachability, rtt — and a store
 * full of those next to zero folds is a completely different diagnosis from a
 * store with no records: the first says the monitor is running and the fold is
 * not, the second says neither is.
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
  for (const group of groups) {
    urls += group.urls.length;
    folded += group.folded;
    cleared += group.cleared;
    expired += group.expired;
    unstable += group.unstable;
    inferred += group.inferred || 0;
    for (const u of group.urls) {
      // A synthesised survivor carries no verdict BY CONSTRUCTION — it is the
      // url the others point at. Counting it as silent would inflate the one
      // number that is supposed to mean "the monitor has not looked at this".
      if (u.synthetic) continue;
      if (!u.fold && !u.cleared && u.stable == null) silent++;
      if (u.stable === true) stable++;
    }
  }
  return { hosts: groups.length, urls, folded, cleared, expired, silent, stable, unstable, inferred };
}
