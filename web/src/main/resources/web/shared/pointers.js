// THE POINTERS A PAGE NEEDS BUT WAS NOT SENT — the follow-up read behind the
// provenance row.
//
// web/provenance.js computes the pills over the array the page already holds,
// and that worked because the relay's search expansion SPLICED the pointer in
// beside the thing it named: a profile arrived behind the Trusted List that
// put it there, and both were in one answer. Once a search for `kinds:[0]`
// returns kind 0 and nothing else — the profiles still found THROUGH those
// lists, labels and assertions, but without them — the page holds the members
// and none of the reasons. Every pill goes quiet, and nothing says why.
//
// So the page asks. This module owns that ask, and it is the same shape the
// score chip has used all along (app.js's paintScores): paint what the answer
// already supports, fire an anonymous read for the rest, repaint when it
// lands. Nothing blocks the render — a card should not wait on the row under
// its byline.
//
// THE GATE MOVES WITH IT, and that is the part to get right. The relay admits
// a declaration only to a reader whose Map delegated its signer for that kind,
// so "it arrived" used to BE the verdict. The reference socket applies no such
// rule: ask it for kind 30392 by member and it answers with every publisher's,
// delegated or not — measured against staging on 2026-09-01, seven lists name
// the two members probed, six of them from the reader's delegated publisher
// and one from a key their Map never mentions. So every declaration filter
// here carries `authors`, from shared/providers.js, and a kind the Map
// delegates to nobody is not asked for at all rather than asked for openly.
//
// LABELS ARE NOT GATED AND MUST NOT BE. NIP-32 is open by construction —
// provenance.js draws a label with a different tone for exactly that reason —
// so there is no author list to narrow one, only a `limit`. Worth knowing what
// that costs: the row's question shifts. It used to be "why is this in this
// page", and a label that never matched the SEARCH never arrived to answer it;
// asked by target, every label the relay holds comes back, including the ones
// that have nothing to do with what was typed. That is the entity page's
// question arriving on the results list. It is the honest consequence of the
// relay no longer sending the matched ones — the client cannot know WHICH
// labels matched — and [LABEL_LIMIT] is the only thing bounding it.
//
// Pure filter-building, so pointers.test.mjs can hold the shapes; only
// [fetchPointers] touches a socket.

import { refConn } from "./conn.js";
import { addrOf } from "./nip19.js";
import { providersFor, publishersOf } from "./providers.js";

/**
 * Which tag a declaration names its SUBJECT with, by kind — and which of the
 * page's three target sets that subject is drawn from.
 *
 * Read off provenance.js rather than re-decided: `MEMBER_TAG` there says a
 * Trusted List holds membership in `p`/`e`/`a` by kind, and an assertion holds
 * its subject in `d` whatever the subject is. Asking with a different tag than
 * the pill reader walks is how the two come to disagree — the page would fetch
 * events that contribute nothing and miss the ones that do.
 *
 * 30395 IS ABSENT ON PURPOSE. Its members are NIP-73 external identifiers,
 * which are not events this page can draw, so there is no target to ask about.
 */
const ASKS = [
  { kind: 30392, tag: "#p", from: "pubkeys" },
  { kind: 30393, tag: "#e", from: "ids" },
  { kind: 30394, tag: "#a", from: "addrs" },
  { kind: 30382, tag: "#d", from: "pubkeys" },
  { kind: 30383, tag: "#d", from: "ids" },
  { kind: 30384, tag: "#d", from: "addrs" },
];

/** NIP-32, asked by every tag that can name something on screen. */
const LABEL_ASKS = [
  { kind: 1985, tag: "#p", from: "pubkeys" },
  { kind: 1985, tag: "#e", from: "ids" },
  { kind: 1985, tag: "#a", from: "addrs" },
];

/**
 * How many values go in one tag filter.
 *
 * 100 because that is what the score chip settled on against this relay, and
 * a results page rarely fills one batch — the point of the constant is the
 * entity page's face strip, which does.
 */
export const BATCH = 100;

/**
 * The ceiling on an UNGATED read. A label filter has no `authors` to narrow
 * it, and a popular pubkey carries thousands; the probe above returned a full
 * page of 20 for two members before it had finished looking. Without this the
 * row's cost is set by the corpus rather than by the page.
 */
export const LABEL_LIMIT = 500;

/**
 * What this page could have a pill drawn ON — the three shapes provenance.js
 * matches a pointer against, and nothing else.
 *
 * `pubkeys` are the authors of the kind-0s HERE, not every author on the page:
 * a pill about a person is drawn on their profile card, so a page with no
 * profile in it has no target for one and asking would be a round trip for
 * pills nobody can see. That is provenanceOf's own rule ("a pill over an
 * absent card is a claim about nothing") applied one step earlier, where it
 * saves the read instead of discarding the answer.
 */
export function targetsOf(events) {
  const pubkeys = new Set(), ids = new Set(), addrs = new Set();
  for (const e of events || []) {
    if (!e || typeof e.id !== "string") continue;
    ids.add(e.id);
    if (e.kind === 0) pubkeys.add(e.pubkey);
    const a = addrOf(e);
    if (a) addrs.add(a);
  }
  return { pubkeys: [...pubkeys], ids: [...ids], addrs: [...addrs] };
}

const chunk = (xs, n) => {
  const out = [];
  for (let i = 0; i < xs.length; i += n) out.push(xs.slice(i, i + n));
  return out;
};

/**
 * The filters that would fetch this page's missing pointers, or `[]` for a
 * page with nothing to ask about.
 *
 * ONE REQ CARRIES THEM ALL: NIP-01 ORs the filters within a subscription, and
 * Relay.req takes the array, so this is one round trip and one EOSE however
 * many kinds the reader delegates. Sending them separately would mean one
 * timeout per kind to reconcile before anything could repaint.
 *
 * [delegations] is shared/providers.js's Map, read through `publishersOf` so
 * a kind delegated under any dimension counts. A kind it names nobody for is
 * SKIPPED — not asked openly. That is the whole gate: an ungated
 * assertion filter would answer with every scorer on the relay and draw them
 * all as though the reader had asked for them.
 */
export function pointerFilters(targets, delegations, { labels = true } = {}) {
  const out = [];
  for (const ask of ASKS) {
    const authors = publishersOf(delegations, ask.kind);
    const values = targets[ask.from] || [];
    if (!authors.length || !values.length) continue;
    for (const batch of chunk(values, BATCH)) {
      out.push({ kinds: [ask.kind], authors, [ask.tag]: batch });
    }
  }
  if (!labels) return out;
  for (const ask of LABEL_ASKS) {
    const values = targets[ask.from] || [];
    if (!values.length) continue;
    for (const batch of chunk(values, BATCH)) {
      out.push({ kinds: [ask.kind], [ask.tag]: batch, limit: LABEL_LIMIT });
    }
  }
  return out;
}

/**
 * The pointer events for [events], read as [observer]'s Map delegates them.
 *
 * Returns the events to hand back to seedProvenance ALONGSIDE the page — never
 * instead of it. A pointer the relay did splice is still in the page and still
 * contributes; provenance.js dedupes per pointer and collapses by value, so an
 * event arriving both ways is one pill, not two.
 *
 * A failed read is `[]` — the pills stay as the page alone supports them,
 * which is the same degradation the names and the score chips take. There is
 * no negative caching here to get wrong: the answer is not stored, it is
 * folded into a row that is rebuilt per search.
 */
export async function fetchPointers(events, observer, opts) {
  const targets = targetsOf(events);
  if (!targets.pubkeys.length && !targets.ids.length && !targets.addrs.length) return [];
  const delegations = await providersFor(observer);
  const filters = pointerFilters(targets, delegations, opts);
  if (!filters.length) return [];
  try {
    const conn = await refConn();
    return await conn.req(filters);
  } catch (e) {
    return [];
  }
}
