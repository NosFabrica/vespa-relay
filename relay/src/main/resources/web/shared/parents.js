// Who a reply is answering — the person, not the hash.
//
// A reply's `e` tag names an EVENT, and every card carrying one used to render
// it as `note1qqq…`: an identifier that places nobody. What a reader needs
// above a reply is whose words it answers, so this module answers the two
// questions no renderer can answer on its own — WHICH `e` tag is the parent,
// and WHO wrote it.
//
// WHICH is NIP-10's rule: the tag marked `reply` if there is one, else the one
// marked `root` (a direct reply to a thread's opening post marks only the
// root), else the LAST `e` tag, which is where the deprecated positional form
// puts the parent. A `mention` marker is skipped — a quote is not a parent.
//
// WHO is the harder half. NIP-10 lets the tag carry the parent's author as its
// fifth element and NIP-22 as its fourth, but plenty of clients write neither,
// so an unhinted parent costs one lookup by id. Cached for the page by event
// id, because twenty replies to the same post are one question.

import { refConn } from "./conn.js";

const HEX64 = /^[0-9a-f]{64}$/;
const hex64 = (v) => {
  const s = String(v || "").toLowerCase();
  return HEX64.test(s) ? s : null;
};

/**
 * The kinds whose card leads with "in reply to".
 *
 * ONE list, read by the renderers (through replyTarget) and by cards.js's
 * namedPubkeys alike, so the line and the profile lookup that fills it cannot
 * cover different kinds. A name declared but never rendered is a wasted fetch;
 * one rendered but not declared is an npub where a name belongs, which is the
 * exact bug the render test's enrichment claim exists to catch.
 *
 * 9802 is deliberately absent: a highlight's `e` names what was clipped, and
 * its card already says "source". Reactive kinds are absent for the same
 * reason — "liked <note>" is a relation this line would say twice.
 */
export const REPLY_KINDS = new Set([1, 9, 11, 42, 1311, 1111, 1222, 1244, 1622]);

/**
 * A relay hint as an `e` tag wrote it, or null. Only shape is checked here —
 * entity.js re-gates every hint before dialing one (no private hosts, no ws://
 * from an https page). What this keeps out is the junk that would otherwise be
 * encoded into a link: empty strings, whitespace, and the http(s) urls some
 * clients put in the slot.
 */
const hintOf = (v) => {
  const s = String(v || "").trim();
  return s && s.length <= 120 && !/\s/.test(s) && /^wss?:\/\/./i.test(s) ? s : null;
};

/**
 * The parent's author as the tag itself claims it.
 *
 * NIP-10 writes it as the FIFTH element, after the marker; NIP-22 as the
 * FOURTH, where NIP-10 keeps the marker — so a 64-hex value in either slot is
 * the author and a marker word simply fails the test. A NIP-22 comment also
 * has a required `p` tag naming exactly this person, which is the one case
 * where a `p` tag is an answer rather than a guess: on a kind 1 the `p` tags
 * are everyone in the thread, in an order nobody agreed on.
 */
function authorHint(ev, t) {
  const fromTag = hex64(t[4]) || hex64(t[3]);
  if (fromTag) return fromTag;
  if (ev.kind !== 1111) return null;
  for (const p of (ev.tags || [])) if (Array.isArray(p) && p[0] === "p" && hex64(p[1])) return hex64(p[1]);
  return null;
}

/**
 * What [ev] is a reply to: `{ id, relay, author }`, or null when it is not a
 * reply at all. `author` is only what the event ITSELF carries — the looked-up
 * half is replyAuthor() below, so a caller can tell "nobody said" from "we
 * have not asked yet".
 */
export function replyTarget(ev) {
  if (!ev || typeof ev !== "object") return null;
  // Memoised per EVENT, not per id: one card render asks three times over
  // (the line, the link's author, namedPubkeys), a repaint asks again, and
  // each ask walked and filtered the tag array. Events are immutable here and
  // the same objects survive every re-render of a result list, so a WeakMap
  // keyed on the event turns "parse the tags per question" into "parse once,
  // ever", and drops the entry with the event.
  if (targets.has(ev)) return targets.get(ev);
  const t = findTarget(ev);
  targets.set(ev, t);
  return t;
}

const targets = new WeakMap();

function findTarget(ev) {
  if (!REPLY_KINDS.has(ev.kind)) return null;
  const es = ((ev && ev.tags) || []).filter((t) => Array.isArray(t) && t[0] === "e" && hex64(t[1]));
  if (!es.length) return null;
  const marker = (t) => String(t[3] || "").toLowerCase();
  const marked = (m) => es.find((t) => marker(t) === m);
  const candidates = es.filter((t) => marker(t) !== "mention");
  // A NIP-28 channel message carries the CHANNEL as `["e", <kind 40 id>, …,
  // "root"]` — the one place a root tag names a room rather than somebody's
  // post. Taking it as the parent would render "in reply to <whoever opened
  // the channel>" over every line of chat ever typed there, so on kind 42 a
  // lone `e` tag is the room and only a second one (or an explicit `reply`
  // marker) is a person being answered.
  const roomOnly = ev.kind === 42 && candidates.length < 2;
  const t = marked("reply") || (ev.kind === 42 ? null : marked("root")) || (roomOnly ? null : candidates[candidates.length - 1]);
  if (!t) return null;
  return { id: hex64(t[1]), relay: hintOf(t[2]), author: authorHint(ev, t) };
}

/**
 * The ADDRESSABLE parent of a NIP-22 comment: `{ addr, author }`, or null.
 *
 * A comment on an article, a listing or a live stream has no `e` tag at all —
 * its parent is replaceable and named by an `a`. That address carries the
 * author in the middle field, so this is the one parent whose person is known
 * without hint or lookup.
 */
export function replyAddr(ev) {
  if (!ev || ev.kind !== 1111 || replyTarget(ev)) return null;
  for (const t of (ev.tags || [])) {
    if (!Array.isArray(t) || t[0] !== "a") continue;
    const m = /^(\d+):([0-9a-f]{64}):/.exec(String(t[1] || ""));
    if (m) return { addr: String(t[1]), author: m[2] };
  }
  return null;
}

// event id -> pubkey, or null once this relay has answered without it.
const authors = new Map();

/**
 * Record who wrote the events already in hand.
 *
 * An event IS the answer for its own id, and a result page routinely holds
 * the parent it is asking about — a thread, a search that matches several
 * posts in one conversation, anything sorted by time. Seeding from what
 * arrived costs a walk over a list already in memory and removes those ids
 * from the lookup below entirely.
 */
export function seedParentAuthors(events) {
  for (const ev of events || []) {
    const id = hex64(ev && ev.id), pk = hex64(ev && ev.pubkey);
    if (id && pk && !authors.get(id)) authors.set(id, pk);
  }
}

/** The parent's author if anything knows it: the tag's hint, else the lookup. */
export function replyAuthor(ev) {
  const t = replyTarget(ev);
  if (!t) return null;
  return t.author || authors.get(t.id) || null;
}

/**
 * Whoever the reply line will NAME, by either route — what cards.js's
 * namedPubkeys declares, so the profile is loaded before the line renders.
 */
export const replyPerson = (ev) => replyAuthor(ev) || (replyAddr(ev) || {}).author || null;

/** The reply parents in [events] whose author nothing here knows yet. */
export const unknownParents = (events) =>
  [...new Set(events.map(replyTarget).filter((t) => t && !t.author && !authors.has(t.id)).map((t) => t.id))];

/**
 * Learn who wrote the events named by [ids], and return how many NEW answers
 * that produced — a caller that already rendered needs to know whether
 * repainting would change anything, exactly as enrichProfiles does for names.
 *
 * Anonymous, on the shared reference connection: whose reply this is answering
 * is a fact about the thread, not about the reader, and the authenticated
 * socket is trust-gated — asking there would leave "in reply to note1…"
 * standing on precisely the parents outside your web of trust.
 */
export async function loadParentAuthors(ids) {
  const missing = [...new Set(ids)].filter((id) => id && !authors.has(id));
  if (!missing.length) return 0;
  let learned = 0;
  for (let i = 0; i < missing.length; i += 100) {
    const batch = missing.slice(i, i + 100);
    let evs = [];
    try {
      const conn = await refConn();
      evs = await conn.req({ ids: batch, limit: batch.length }, 5000);
    } catch (e) { continue; }   // leave them unknown rather than wrong
    for (const ev of evs) {
      const id = hex64(ev && ev.id), pk = hex64(ev && ev.pubkey);
      if (id && pk && !authors.get(id)) { authors.set(id, pk); learned++; }
    }
    // "This relay does not hold the parent" is only a fact once the relay has
    // finished answering. EOSE, not merely "resolved": req() hands back
    // whatever arrived when its timeout fired, so caching the gap off a slow
    // read records an absence nobody stated — and the cache is read before
    // every repaint, which makes that null permanent for the session. The same
    // mistake, and the same fix, as profiles.js's missing kind 0.
    if (evs.complete === true) for (const id of batch) if (!authors.has(id)) authors.set(id, null);
  }
  return learned;
}
