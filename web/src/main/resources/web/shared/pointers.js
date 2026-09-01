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
// so there is no author list to narrow one, only a `limit` and the reader's
// own LENS: a label filter carries `observer:<lens>`, where a declaration
// filter deliberately carries none. That split is the relay's, not an
// invention here (see [pointerFilters]). Worth knowing what the rest of it
// costs: the row's question shifts. It used to be "why is this in this
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
import { DECLARATION_KINDS } from "../provenance.js";

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
/** The only thing an `authors` filter or an `observer:` token takes. */
const HEX64 = /^[0-9a-f]{64}$/;

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
 * [trusted] is [trustedSigners]' `kind -> Set(signer)` — the same object
 * provenance.js reads to decide whether a declaration speaks at all. A kind it
 * has nobody for is SKIPPED, not asked openly: an ungated assertion filter
 * would answer with every scorer on the relay and draw them all as though the
 * reader had asked for them.
 *
 * TWO LENSES, ONE REQ, and the split is the relay's own. A declaration filter
 * carries NO lens: it is already narrowed to keys this reader named, and a
 * service key is signed by somebody nobody follows, so the reader's own trust
 * floor would drop their provider's lists on the way in. The store's companion
 * says the same thing in Kotlin — `minRank = INCLUDE_SPAM_MIN_RANK`, "its
 * floor is waived on purpose: a service key nobody follows signs the lists it
 * looks for". Leaving `search` off does it here, since the reference socket
 * stamps `include:spam` on anything that declares nothing (shared/lens.js).
 *
 * A LABEL FILTER CARRIES THE OBSERVER, because it has no `authors` to narrow
 * it and the floor is the only thing standing between this row and every
 * label anyone ever published about these targets. That is also what the relay
 * does with the label half — `q.copy(kinds = labels)` keeps the query's own
 * observer and floor, where the declaration half rewrites the floor. Written
 * as a NIP-50 token rather than left to the socket: `withoutLens` refuses to
 * touch a filter that already declares one, precisely so a lensed read cannot
 * be silently widened to `include:spam`.
 */
export function pointerFilters(targets, trusted, { labels = true, observer = null } = {}) {
  const out = [];
  for (const ask of ASKS) {
    const authors = [...((trusted && trusted.get(ask.kind)) || [])];
    const values = targets[ask.from] || [];
    if (!authors.length || !values.length) continue;
    for (const batch of chunk(values, BATCH)) {
      out.push({ kinds: [ask.kind], authors, [ask.tag]: batch });
    }
  }
  if (!labels) return out;
  // An anonymous reader has no lens to read through; the socket's own
  // `include:spam` is then the honest declaration, and the only one available.
  const lens = observer && HEX64.test(observer) ? { search: `observer:${observer}` } : {};
  for (const ask of LABEL_ASKS) {
    const values = targets[ask.from] || [];
    if (!values.length) continue;
    for (const batch of chunk(values, BATCH)) {
      out.push({ kinds: [ask.kind], [ask.tag]: batch, limit: LABEL_LIMIT, ...lens });
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
  const trusted = trustedSigners(await providersFor(observer), observer);
  const filters = pointerFilters(targets, trusted, { ...opts, observer });
  if (!filters.length) return [];
  try {
    const conn = await refConn();
    return await conn.req(filters);
  } catch (e) {
    return [];
  }
}

/**
 * WHO MAY SPEAK, per declaration kind — the same gate, in the shape the pill
 * reader needs it.
 *
 * [pointerFilters] applies this to the ASK, which covers what this page
 * fetches and nothing else. A declaration also reaches the page a second way:
 * plain recall. The store is explicit that it does — "an explicit
 * `kinds:[30392]` is a NIP-01 ask and serves strangers' lists as plain hits,
 * gate or no gate" — and a search that names no kinds recalls every kind by
 * definition, so a stranger's list whose TITLE matches the search text lands
 * in the answer beside the delegated publisher's.
 *
 * provenance.js drew those as `gated`, because "it arrived" used to mean the
 * expansion admitted it. Two lists titled "Verified Human" then collapsed by
 * value into one pill with a count of 2 — so any stranger could inflate a
 * delegated publisher's corroboration number by signing a list with the same
 * title. That is the count making the exact false claim it exists to avoid.
 *
 * SERVICE KEYS ONLY, AND PER KIND. The key a Map names for 30392 speaks for
 * 30392 and not for 30382; a reader who delegated a list publisher has not
 * thereby asked for that publisher's assertions. Dimensions of ONE kind do
 * collapse — `30382:rank` and `30382:followers` are two things the reader
 * asked one key for, and both are 30382 assertions they wanted — which is
 * `publishersOf`'s union and the same grouping the store's own gate does
 * (`declarations.groupBy(gate::signersOf)`).
 *
 * PLUS THE OBSERVER THEMSELVES, which keeps the page level with the relay
 * rather than a step stricter than it: the store fetches declarations "from
 * their enrolled signers only, PLUS THE READER", so a reader's own self-signed
 * Trusted List unpacks and its members are spliced onto the page. Leaving them
 * out meant the relay put a profile there for a reason the row then declined
 * to give, and a reader's own lists never spoke for themselves.
 *
 * A reader with no Map gets no declaration pill but their own, and an
 * ANONYMOUS reader — who is nobody — gets none at all, which is exactly what
 * the relay serves them ("the label companion alone"). Labels are unaffected
 * either way: NIP-32 is open by construction, and provenance.js draws it in
 * the tone that says so.
 *
 * ONE OBJECT FOR THE ASK AND FOR THE RENDER. [pointerFilters] turns these sets
 * into `authors`, and provenance.js reads the same sets to decide whether a
 * declaration speaks at all — so the fetch and the row cannot come to
 * different views of whom this reader trusts. They briefly did, and it showed
 * as a reader's own list drawing a pill when the relay happened to splice it
 * and none when the page had to fetch it.
 */
export function trustedSigners(delegations, observer) {
  const out = new Map();
  for (const kind of DECLARATION_KINDS) {
    const keys = new Set(publishersOf(delegations, kind));
    if (observer && HEX64.test(observer)) keys.add(observer);
    out.set(kind, keys);
  }
  return out;
}
