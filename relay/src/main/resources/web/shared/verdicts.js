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
 * NIP-32's label, which is where the monitor's FITNESS GRADE lives, and the
 * namespace that says the grade is ours to read.
 *
 * It used to be spelled `["s", "prime"]`, and that was a straight collision:
 * `s` is the relay's SOFTWARE to every other monitor on the network — sampled
 * live off `relay.nostr.watch` and `nos.lol`, 12 monitors, and every `s` value
 * is a repository url. So this panel drew our own grade in the software column
 * and had no way to draw the software at all.
 *
 * The namespace is what makes `l` safe to share. The same record carries
 * country, ISP and ASN labels from other monitors, all on `l`, so a reader
 * matching the tag NAME would read a country code as a fitness grade.
 */
export const LABEL = "l";

export const LABEL_NAMESPACE = "L";

export const FITNESS_NAMESPACE = "relay.fitness";

/** The one grade that admits a relay to a stream's roster. */
export const PRIME = "prime";

/**
 * The tags this reader RENDERS besides the verdicts — the NIP-66 payload
 * proper, which the monitor's fitness pass now writes on every record it
 * grades.
 *
 * They are here because they are the other half of a diagnosis. `R: auth` says
 * the relay gates reads behind NIP-42, which is worth checking when a url will
 * not fold — though it is not proof: a NIP-29 host challenges every connection
 * and still serves its group metadata to anyone, and a whole row of `auth` pills
 * beside `not folded` turned out to be the fold's filter ladder rather than the
 * gate (see AGENTS.md). `n: tor` says a fingerprint had to go through a circuit
 * and is given a different idle budget; `rtt-open` says whether a probe's
 * silence was the relay being slow or being absent.
 *
 * Drawing them is also the live check on something the Kotlin side can only
 * test in isolation. A replaceable record has one address and several writers,
 * so `RelayVerdictRecord.edit` must carry forward every tag it does not own — a
 * writer that rebuilds instead silently deletes the others, and the result is
 * still a valid signed record that simply says less. That regression has
 * happened once (`[d, n, rtt-open]` became `[d, same-as]`). Seeing `n` and
 * `rtt-open` beside `same-as` on one row is what says the merge still works in
 * production and not only in a unit test.
 *
 * **`v`, `g` and `T` are NOT here, and their absence is the finding.** This map
 * used to list all three, which made the panel look like it was reporting a
 * version, a geohash and a relay type that nothing has ever written: NIP-66
 * defines no `v` at all, and `g`/`T` come from IP geolocation and a classifier
 * this monitor does not run — see `RelayFacts` for why they stay unwritten
 * rather than guessed. A column that is always empty reads as a relay that
 * declined to answer, which is a different and false claim.
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

/** Tags this panel accounts for. Anything else is COUNTED, never dropped silently. */
const OWNED = new Set([
  "d",
  SAME_AS,
  SELF_CONSISTENT,
  LABEL,
  LABEL_NAMESPACE,
  // The fitness pass's two measured facts. They were in neither list before, so
  // the panel counted the monitor's OWN writes as tags it had never heard of
  // and reported `+2 other tag(s)` on every graded row.
  "pageable",
  "nip77",
  ...Object.keys(RENDERED),
]);

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

/** Where it carries the version of the rules that measured it. */
const EPOCH_INDEX = 4;

/**
 * The same three positions on a NIP-32 label, each one to the right: the spec
 * fixes index 2 as the namespace, so the grade's evidence starts at 3.
 *
 * Spelled out rather than derived from the pair above, because the two shapes
 * are set by different specs and a change to either must not silently move the
 * other.
 */
const LABEL_NAMESPACE_INDEX = 2;

const LABEL_EVIDENCE_INDEX = 3;

const LABEL_MEASURED_AT_INDEX = 4;

const LABEL_EPOCH_INDEX = 5;

/**
 * The fitness pass's rules version — `RelayVerdictRecord.FITNESS_EPOCH`.
 *
 * Same duplication and the same mitigation as the two below: an out-of-epoch
 * grade is drawn as expired WITH its evidence and age, never hidden.
 */
export const FITNESS_EPOCH = "1";

/**
 * The rule versions the router currently acts on — `RelayAliasRecord.FOLD_EPOCH`
 * and `CONSISTENCY_EPOCH`.
 *
 * A verdict measured under an older set of rules is not a stale reading of
 * today's rule, it is a reading of a different one, so the router discards it
 * and re-measures. This panel has to agree, or it would draw a url as folded
 * that the fan-out is dialling — the one thing it exists to make impossible.
 * Duplicated from the Kotlin for the same reason `TTL_SECONDS` is, and the same
 * mitigation: an out-of-epoch verdict is drawn as expired WITH its evidence and
 * age, never hidden.
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
    foldEpoch: null,
    stableEvidence: null,
    stableMeasuredAt: null,
    stableEpoch: null,
    // The fitness grade, which is a THIRD verdict and was never drawn at all —
    // it lived on `s` and this panel rendered it as the relay's software.
    grade: null,
    gradeEvidence: null,
    gradeMeasuredAt: null,
    gradeEpoch: null,
    // Everything the OTHER writers put on this record. `requirements` is a list
    // because a relay can be both auth-gated and paid, and `extra` is a count
    // of tag names this reader does not know — reported rather than dropped, so
    // a record carrying something new is visible as such instead of looking
    // like a record that carries nothing.
    requirements: [],
    supportedNips: [],
    extra: 0,
    // The NIP-11-ish document the monitor carries in the content. Kept as a
    // flag, not parsed: this panel is about verdicts, and it should not grow a
    // second job quietly.
    hasDoc: !!(event.content && event.content.length),
  };
  for (const t of tags) {
    // The two tags a relay may carry SEVERAL of. Everything else in RENDERED is
    // one value, so it is assigned; these are collected, and flattening them
    // would draw a relay that is both auth-gated and paid as only the last one.
    if (t[0] === "R") out.requirements.push(t[1]);
    else if (t[0] === "N") out.supportedNips.push(t[1]);
    else if (RENDERED[t[0]]) out[RENDERED[t[0]]] = t[1];
    else if (!OWNED.has(t[0])) out.extra++;
  }
  // OUR namespace's label, not any label carrying a value we recognise. A
  // monitor labelling this relay `["l", "CA", "countryCode"]` is not grading it.
  const graded = tags.find(
    (t) => t.length > LABEL_NAMESPACE_INDEX && t[0] === LABEL && t[LABEL_NAMESPACE_INDEX] === FITNESS_NAMESPACE,
  );
  if (graded) {
    out.grade = graded[1] || null;
    // NIP-32 spends index 2 on the namespace, so the house shape — evidence,
    // measured-at, epoch — sits one place right of where the fold and the
    // stability tag carry it. Reading it at the fold's offsets would date every
    // grade by its own evidence string, which parses to null and draws as "this
    // record does not say".
    out.gradeEvidence = graded[LABEL_EVIDENCE_INDEX] || null;
    out.gradeMeasuredAt = Number(graded[LABEL_MEASURED_AT_INDEX]) || null;
    out.gradeEpoch = graded[LABEL_EPOCH_INDEX] || null;
  }
  const sameAs = tag(SAME_AS);
  if (sameAs) {
    out.foldEvidence = sameAs[2] || null;
    // The verdict's OWN clock, and NOT the event's when it is missing. The
    // record's clock is bumped every time we connect, so falling back to it
    // dated a pre-stamp verdict as measured minutes ago and drew it as current
    // forever — the same trap the Kotlin reader had, removed on both sides
    // together. Null here means "this record does not say", which is what
    // `isCurrent` refuses.
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
    // Anything this reader does not recognise is NOT a verdict. Ignored rather
    // than guessed at — guessing "unstable" would draw a relay as refused on a
    // tag we cannot read.
    if (consistent[1] === "true") out.stable = true;
    else if (consistent[1] === "false") out.stable = false;
  }
  return out;
}

/**
 * Is a verdict still one the router would act on: measured under the rules it
 * applies TODAY ([want]), and inside the TTL?
 *
 * Both halves, because they fail differently and the panel is where the
 * difference is read. An aged-out verdict will be re-taken when the url's turn
 * comes round; an out-of-epoch one was re-taken the moment the new build's
 * first pass reached it, and if it is still here that pass has not got to this
 * host yet.
 */
export function isCurrent(at, nowSec, epoch, want) {
  return at != null && at >= nowSec - TTL_SECONDS && epoch === want;
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
    if (!hosts.has(rec.host)) hosts.set(rec.host, { host: rec.host, urls: [], folded: 0, cleared: 0, unstable: 0, expired: 0, graded: 0, prime: 0 });
    const group = hosts.get(rec.host);
    const current = isCurrent(rec.foldMeasuredAt, nowSec, rec.foldEpoch, FOLD_EPOCH);
    const gradeCurrent = isCurrent(rec.gradeMeasuredAt, nowSec, rec.gradeEpoch, FITNESS_EPOCH);
    group.urls.push({ ...rec, foldCurrent: current, gradeCurrent });
    if (rec.grade && gradeCurrent) group.graded++;
    if (rec.grade === PRIME && gradeCurrent) group.prime++;
    // Counted on what the router would ACT on. A fold whose verdict has aged
    // out is not folding anything today, and counting it would draw a host as
    // collapsed while every url of it is back in the fan-out.
    //
    // **`expired` counts BOTH forms, which it did not.** It tested `fold` only,
    // so a CLEARED verdict past its TTL fell out of every counter on this page
    // — not folded, not cleared, not expired, and not silent either, since the
    // row does carry a verdict tag. That was survivable while the only way to
    // expire was to wait a month. It stopped being survivable with the rules
    // epoch: bumping it retires every verdict in the store at once, and the
    // cleared half is the majority of them.
    if (rec.fold && current) group.folded++;
    else if (rec.cleared && current) group.cleared++;
    else if (rec.fold || rec.cleared) group.expired++;
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
        // Nobody signed this row — it is inferred from the folds that point at
        // it — so the two fields that come from an event are explicitly empty
        // rather than absent.
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
        // The same SHAPE a read record has, every field of it. A synthesised
        // row that omits the collection fields is one `for…of` away from
        // throwing inside the renderer, which is how a panel that had drawn
        // 4,000 rows correctly died on the 4,001st and left its own filter
        // hidden. The fixture rule again: a stand-in that does not have the
        // shape of the thing it stands in for tests the stand-in.
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
 * Every record of this kind the relay holds, paged newest-first.
 *
 * **A PAGE THAT IS ENTIRELY ONE `created_at` CANNOT MOVE THE CURSOR, and
 * stepping below it loses everything in that second we have not seen.** This is
 * not hypothetical: quartz's monitor flushes its reachability records in
 * batches, so on this store 5 timestamps carry more than 500 records each — the
 * largest 879. Measured against that store, a fixed 500-event page returned
 * **4,595 records and reported the read complete**; the same walk at 1,000
 * returned **5,296**. 701 records missing, silently, with a completeness claim
 * on top.
 *
 * So the page GROWS while it is entirely one timestamp, which is exactly what
 * `RelayDiscovery.scan` does on the Kotlin side and for exactly this reason.
 * The growth is capped by what the relay says it will serve ([maxPage], from
 * its own NIP-11 `limitation.max_limit`) — asking over a relay's cap risks an
 * outright refusal rather than a truncation, which would arrive here as
 * silence.
 *
 * If a run is longer than the relay will serve in one ask, the walk CANNOT be
 * completed and says so rather than stepping over the remainder. `complete` is
 * true only when a page came back empty.
 *
 * [ask] is `(limit, until) -> events` so the walk can be tested without a
 * relay, the same shape `AliasProbe` takes its `fetch` in.
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
    // Saturated AND all one second: the cursor has nowhere to go. Double the
    // ask until the page spans two, or until the relay's own ceiling.
    while (events.length >= size && size < maxPage && oneSecond(events)) {
      size = Math.min(size * 2, maxPage);
      events = await ask(size, until);
      pages++;
      grew++;
    }
    if (!events.length) { complete = true; break; }
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
    // Nothing new, and we are already asking as much as this relay will serve.
    // Stepping below the boundary now would skip whatever is left in that
    // second — so stop, and let the caller say the read is partial rather than
    // draw a number that quietly excludes them.
    if (events.length >= size && size >= maxPage && oneSecond(events)) break;
    until = oldest - 1;
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
  let graded = 0;
  let prime = 0;
  for (const group of groups) {
    urls += group.urls.length;
    folded += group.folded;
    cleared += group.cleared;
    expired += group.expired;
    unstable += group.unstable;
    inferred += group.inferred || 0;
    graded += group.graded;
    prime += group.prime;
    for (const u of group.urls) {
      // A synthesised survivor carries no verdict BY CONSTRUCTION — it is the
      // url the others point at. Counting it as silent would inflate the one
      // number that is supposed to mean "the monitor has not looked at this".
      if (u.synthetic) continue;
      // **A GRADE COUNTS AS HAVING BEEN LOOKED AT, and it did not.** This
      // tested the fold and the stability tag only, so every url the fitness
      // pass had measured — the pass that dials the whole corpus, and the only
      // one most urls ever get — landed in the number that means "nothing has
      // looked at this". A store mid-sweep read as a store with no monitor
      // running, which is the exact confusion the tile exists to end.
      if (!u.fold && !u.cleared && u.stable == null && !u.grade) silent++;
      if (u.stable === true) stable++;
    }
  }
  return { hosts: groups.length, urls, folded, cleared, expired, silent, stable, unstable, inferred, graded, prime };
}
