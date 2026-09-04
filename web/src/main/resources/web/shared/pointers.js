// The pointers a page needs but was not sent: the follow-up read behind the
// provenance row. A search for `kinds:[0]` returns profiles without the lists,
// labels and assertions they were found through, so the page asks for them the
// way the score chip does: paint what the answer supports, fire an anonymous
// read for the rest, repaint when it lands.
//
// Pure filter-building, so pointers.test.mjs can hold the shapes; only
// [fetchPointers] touches a socket.

import { refConn } from "./conn.js";
import { addrOf } from "./nip19.js";
import { providersFor, publishersOf } from "./providers.js";
import { DECLARATION_KINDS } from "../provenance.js";

/** The only thing an `authors` filter or an `observer:` token takes. */
const HEX64 = /^[0-9a-f]{64}$/;

/**
 * Which tag a declaration names its subject with, by kind, and which of the
 * page's three target sets that subject is drawn from. Read off provenance.js's
 * `MEMBER_TAG`: asking with a different tag than the pill reader walks fetches
 * events that contribute nothing. 30395's members are NIP-73 identifiers, not
 * events, so it has no target here.
 */
const ASKS = [
  { kind: 30392, tag: "#p", from: "pubkeys" },
  { kind: 30393, tag: "#e", from: "ids" },
  { kind: 30394, tag: "#a", from: "addrs" },
  { kind: 30382, tag: "#d", from: "pubkeys" },
  { kind: 30383, tag: "#d", from: "ids" },
  { kind: 30384, tag: "#d", from: "addrs" },
  // The reader's own curation, naming members in `p` like a 30392. Gated on
  // their signer like the rest, so an anonymous reader, who is nobody, never
  // asks for them.
  { kind: 30000, tag: "#p", from: "pubkeys" },
  { kind: 39089, tag: "#p", from: "pubkeys" },
];

/** NIP-32, asked by every tag that can name something on screen. */
const LABEL_ASKS = [
  { kind: 1985, tag: "#p", from: "pubkeys" },
  { kind: 1985, tag: "#e", from: "ids" },
  { kind: 1985, tag: "#a", from: "addrs" },
];

/** How many values go in one tag filter; the entity page's face strip fills one. */
export const BATCH = 100;

/**
 * The ceiling on an ungated read. A label filter has no `authors`, so this is
 * the only bound on the row's cost; pills collapse by value, so it buys
 * distinct values and never volume.
 */
export const LABEL_LIMIT = 100;

/**
 * What this page could have a pill drawn on: the three shapes provenance.js
 * matches a pointer against. `pubkeys` are the authors of the kind-0s here,
 * not every author on the page, since a pill about a person is drawn on their
 * profile card.
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
 * page with nothing to ask about. NIP-01 ORs the filters within a
 * subscription, so one REQ carries them all.
 *
 * [trusted] is [trustedSigners]' `kind -> Set(signer)`; a kind it has nobody
 * for is skipped, not asked openly. `declarations` and `labels` select which
 * half is built, and they go out as separate REQs since one REQ waits for one
 * EOSE and the open half is the slow one. A declaration filter carries no lens
 * (a service key nobody follows would fall under the reader's trust floor); a
 * label filter carries the observer, written as a NIP-50 token because
 * `withoutLens` refuses to touch a filter that already declares one.
 */
export function pointerFilters(targets, trusted, { labels = true, declarations = true, observer = null } = {}) {
  const out = [];
  for (const ask of declarations ? ASKS : []) {
    const authors = [...((trusted && trusted.get(ask.kind)) || [])];
    const values = targets[ask.from] || [];
    if (!authors.length || !values.length) continue;
    for (const batch of chunk(values, BATCH)) {
      out.push({ kinds: [ask.kind], authors, [ask.tag]: batch });
    }
  }
  if (!labels) return out;
  // An anonymous reader has no lens; the socket's own `include:spam` is then
  // the only honest declaration.
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
 * Handed back alongside the page, never instead of it; provenance.js dedupes
 * per pointer, so an event arriving both ways is one pill. A failed read is
 * `[]`, the same degradation the names and the score chips take.
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
 * Who may speak, per declaration kind: the Map's service keys for that kind
 * plus the observer themselves, in the shape provenance.js reads.
 *
 * Per kind, because the key a Map names for 30392 does not speak for 30382;
 * dimensions of one kind collapse, which is `publishersOf`'s union. The same
 * object serves the ask and the render, so the fetch and the row cannot come
 * to different views of whom this reader trusts. An anonymous reader gets an
 * empty set for every kind.
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
