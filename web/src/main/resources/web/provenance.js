// Why an event is in this page: the pills a card draws under its byline, for events the search
// expansion spliced in behind a list or a label. Pure over whatever array it is handed: app.js
// seeds it from the answer and again from shared/pointers.js's follow-up read, and the trust
// gate is applied here from the `trusted` map, never inferred from arrival or adjacency.

import { addrOf } from "./shared/nip19.js";

/**
 * The label namespaces that are metadata rather than provenance: a language on every card, or a
 * trends feed.
 */
export const QUIET_NAMESPACES = new Set(["ISO-639-1", "ISO-639-2", "ISO-3166-1", "ISO-3166-2", "pub.ditto.trends"]);

/** NIP-32. Anyone may publish one about anything, so a label pill is never gated. */
export const LABEL_KIND = 1985;

/**
 * The kinds whose presence in a page is the trust gate's verdict: the relay admits one only
 * for a reader whose kind 10040 delegated its signer for that kind.
 */
export const DECLARATION_KINDS = new Set([30000, 30382, 30383, 30384, 30385, 30392, 30393, 30394, 30395, 39089]);

/**
 * The two NIP-51 kinds the relay splices beside a trust service's output: a reader's own
 * curation, under the same gate, since a reader is always their own signer.
 */
export const PEOPLE_LIST_KINDS = new Set([30000, 39089]);

/** Which tag holds a list's public membership, by kind; 30395's `i` names no event. */
const MEMBER_TAG = { 30000: "p", 30392: "p", 30393: "e", 30394: "a", 39089: "p" };

/** How many pills a preview draws before the rest go behind a count. */
export const PILL_BUDGET = { preview: 4, full: 40 };

const HEX64 = /^[0-9a-f]{64}$/;
const tagsOf = (ev, name) => ((ev && ev.tags) || []).filter((t) => Array.isArray(t) && t[0] === name && t[1]);
const tagOf = (ev, name) => (tagsOf(ev, name)[0] || [])[1] || "";

/**
 * The pills for every event in [events], keyed by the id of the event they go on. [trusted] is
 * `kind -> Set(signer)` from pointers.js's trustedSigners; an absent map delegates nobody, and
 * labels are open regardless.
 */
export function provenanceOf(events, trusted) {
  const byId = new Map();
  const byAddr = new Map();
  const profileOf = new Map(); // pubkey -> the kind 0 in this page
  for (const e of events || []) {
    if (!e || !HEX64.test(e.id || "")) continue;
    byId.set(e.id, e);
    const a = addrOf(e);
    if (a) byAddr.set(a, e);
    if (e.kind === 0) profileOf.set(e.pubkey, e);
  }

  // (target id) -> (pill key) -> pill
  const found = new Map();
  // Deduped per pointer, never globally: one list repeating a member is not two lists.
  const seenHere = new Set();
  const add = (target, pill) => {
    if (!target || target.id === pill.from) return;
    const once = `${target.id} ${pill.key}`;
    if (seenHere.has(once)) return;
    seenHere.add(once);
    let pills = found.get(target.id);
    if (!pills) found.set(target.id, (pills = new Map()));
    const seen = pills.get(pill.key);
    if (!seen) {
      pills.set(pill.key, { ...pill, count: 1, authors: [pill.author] });
      return;
    }
    // Collapsed by value, never by event: two lists titled alike are one pill with a count of 2.
    seen.count++;
    if (!seen.authors.includes(pill.author)) seen.authors.push(pill.author);
  };

  // Each event walked once however many times the array holds it, or a second copy lands on `count`.
  const walked = new Set();
  for (const ev of events || []) {
    if (!ev) continue;
    if (ev.id) {
      if (walked.has(ev.id)) continue;
      walked.add(ev.id);
    }
    seenHere.clear();
    contributionsOf(ev, { byId, byAddr, profileOf, trusted }, add);
  }

  const out = new Map();
  for (const [id, pills] of found) out.set(id, order([...pills.values()]));
  return out;
}

/**
 * Hands every (target, pill) one pointer contributes to [emit]; a callback, so only the
 * members on screen are allocated for.
 */
function contributionsOf(ev, page, emit) {
  if (ev.kind === LABEL_KIND) return labelContributions(ev, page, emit);
  if (!DECLARATION_KINDS.has(ev.kind)) return;
  // Whose word, not only which kind.
  if (!delegated(page.trusted, ev.kind, ev.pubkey)) return;
  if (MEMBER_TAG[ev.kind]) return listContributions(ev, page, emit);
  return assertionContributions(ev, page, emit);
}

/** Did this reader name [pubkey] for [kind]? Nothing is delegated by an absent map. */
const delegated = (trusted, kind, pubkey) => {
  const keys = trusted && trusted.get(kind);
  return !!keys && keys.has(pubkey);
};

/**
 * NIP-32: one pill per label value, on every record the label names; `r` and `t` targets name
 * nothing on this page.
 */
function labelContributions(ev, page, emit) {
  const ns = tagOf(ev, "L");
  const targets = [];
  for (const t of (ev.tags || [])) {
    if (!Array.isArray(t) || !t[1]) continue;
    const target =
      t[0] === "e" ? page.byId.get(t[1])
      : t[0] === "p" ? page.profileOf.get(t[1])
      : t[0] === "a" ? page.byAddr.get(t[1])
      : null;
    if (target) targets.push(target);
  }
  if (!targets.length) return;
  for (const tag of (ev.tags || [])) {
    if (!Array.isArray(tag) || tag[0] !== "l" || !tag[1]) continue;
    // A mark's namespace is its own third element where it has one, else the event's `L`.
    if (QUIET_NAMESPACES.has(tag[2] || ns)) continue;
    const pill = { key: `label:${tag[1]}`, text: tag[1], to: "search", value: tag[1], gated: false, author: ev.pubkey, from: ev.id };
    for (const target of targets) emit(target, pill);
  }
}

/**
 * The `d` values under which a NIP-51 list is the reader saying the opposite of a vouch; some
 * are titled, so the title rule below cannot catch them.
 */
const BLOCK_LIST_D = new Set(["mute", "block", "blocked", "mutelist", "mutelists", "blocklist"]);

/**
 * The words a list is findable by, in the order quartz's `SearchFieldExtractor` indexes them,
 * or "" for a list that draws no pill. `d` is the fallback for a Trusted List only.
 */
const NAME_INDEXED = new Set([30000]);
const listText = (ev) => {
  if (PEOPLE_LIST_KINDS.has(ev.kind)) {
    if (BLOCK_LIST_D.has(tagOf(ev, "d").trim().toLowerCase())) return "";
    return tagOf(ev, "title") || (NAME_INDEXED.has(ev.kind) ? tagOf(ev, "name") : "");
  }
  return tagOf(ev, "title") || tagOf(ev, "d");
};

/**
 * A list, Trusted or NIP-51: one pill named by the list on every member this page holds. One
 * tone for both; where the author matters, [facesNeeded] draws the face.
 */
function listContributions(ev, page, emit) {
  const addr = addrOf(ev);
  if (!addr) return;
  const text = listText(ev);
  if (!text) return;
  const name = MEMBER_TAG[ev.kind];
  const pill = { key: `list:${text}`, text, to: "addr", value: addr, gated: true, author: ev.pubkey, from: ev.id };
  for (const t of (ev.tags || [])) {
    if (!Array.isArray(t) || t[0] !== name || !t[1]) continue;
    const target = name === "p" ? page.profileOf.get(t[1]) : name === "e" ? page.byId.get(t[1]) : page.byAddr.get(t[1]);
    if (target) emit(target, pill);
  }
}

/**
 * A NIP-85 assertion, whose subject is its `d`, read by kind since only the kind says what the
 * string is. Its topics and nothing else: a score is not a reason.
 */
function assertionContributions(ev, page, emit) {
  const subject = tagOf(ev, "d");
  if (!subject) return;
  const target =
    ev.kind === 30382 ? page.profileOf.get(subject)
    : ev.kind === 30383 ? page.byId.get(subject)
    : ev.kind === 30384 ? page.byAddr.get(subject)
    : null; // 30385's subject is a NIP-73 identifier, not an event this page draws.
  if (!target) return;

  // Every `t`: the collapse above folds a repeat into a count.
  for (const t of (ev.tags || [])) {
    if (!Array.isArray(t) || t[0] !== "t" || !t[1]) continue;
    emit(target, { key: `topic:${t[1]}`, text: t[1], to: "topic", value: t[1], gated: true, author: ev.pubkey, from: ev.id });
  }
}

/**
 * Delegated first, then by weight, then alphabetically, so a card never reshuffles its pills
 * between renders.
 */
function order(pills) {
  return pills.sort((a, b) =>
    (b.gated - a.gated) || (b.count - a.count) || a.text.localeCompare(b.text) || a.key.localeCompare(b.key));
}

/**
 * Which pills carry a face: every ungated one, and gated ones only when the page holds more
 * than one delegated publisher.
 */
export function facesNeeded(pillsByTarget) {
  const publishers = new Set();
  for (const pills of pillsByTarget.values()) {
    for (const p of pills) if (p.gated) for (const a of p.authors) publishers.add(a);
  }
  return publishers.size > 1;
}

/** The page's own answer, filled by app.js before it renders and read by cards/base.js. */
export const provenance = new Map();

/**
 * Which page this answer belongs to, bumped by every write including a clear: the second
 * seeding pass lands after an await, and the events alone cannot tell a new search from the
 * same one under a new observer.
 */
let epoch = 0;

/** The current [epoch]. Capture before an await, compare after, drop if it moved. */
export const provenanceEpoch = () => epoch;
/** True when a gated pill should be attributed — see [facesNeeded]. */
export const attribution = { faces: false };

/**
 * Drop what the page knows; the entity page clears on the way in, so a row inherited from the
 * last search cannot mean "how you got here".
 */
export function forgetProvenance() {
  epoch++;
  provenance.clear();
  attribution.faces = false;
}

/** Replace what the page knows with this page's answer. Returns how many cards gained a row. */
export function seedProvenance(events, trusted) {
  epoch++;
  provenance.clear();
  const built = provenanceOf(events, trusted);
  for (const [id, pills] of built) provenance.set(id, pills);
  attribution.faces = facesNeeded(built);
  return built.size;
}
