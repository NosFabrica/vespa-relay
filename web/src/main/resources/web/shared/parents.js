// Who a reply is answering: which `e` tag is the parent (NIP-10's rule: `reply`, else `root`,
// else the last `e` tag; a `mention` is a quote), and who wrote it, from the tag's author hint
// or one lookup by id cached for the page.

import { refConn } from "./conn.js";

const HEX64 = /^[0-9a-f]{64}$/;
const hex64 = (v) => {
  const s = String(v || "").toLowerCase();
  return HEX64.test(s) ? s : null;
};

/**
 * The kinds whose card leads with "in reply to", read by the renderers and by cards.js's
 * namedPubkeys so the line and its profile lookup cover the same kinds. A highlight's `e` is
 * its source, and a reactive kind already says "liked <note>", so neither is here.
 */
export const REPLY_KINDS = new Set([1, 9, 11, 42, 1311, 1111, 1222, 1244, 1622]);

/**
 * A relay hint as an `e` tag wrote it, or null. Shape only; entity.js re-gates every hint before
 * dialing.
 */
const hintOf = (v) => {
  const s = String(v || "").trim();
  return s && s.length <= 120 && !/\s/.test(s) && /^wss?:\/\/./i.test(s) ? s : null;
};

/**
 * The parent's author as the tag itself claims it: NIP-10's fifth element or NIP-22's fourth.
 * A NIP-22 comment's `p` names exactly this person; a kind 1's `p` tags are the whole thread.
 */
function authorHint(ev, t) {
  const fromTag = hex64(t[4]) || hex64(t[3]);
  if (fromTag) return fromTag;
  if (ev.kind !== 1111) return null;
  for (const p of (ev.tags || [])) if (Array.isArray(p) && p[0] === "p" && hex64(p[1])) return hex64(p[1]);
  return null;
}

/**
 * What [ev] is a reply to as `{ id, relay, author }`, or null. `author` is only what the event
 * itself carries; the looked-up half is replyAuthor().
 */
export function replyTarget(ev) {
  if (!ev || typeof ev !== "object") return null;
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
  // On kind 42 a lone `e` is the room (NIP-28's `root` names the channel), not a parent.
  const roomOnly = ev.kind === 42 && candidates.length < 2;
  const t = marked("reply") || (ev.kind === 42 ? null : marked("root")) || (roomOnly ? null : candidates[candidates.length - 1]);
  if (!t) return null;
  return { id: hex64(t[1]), relay: hintOf(t[2]), author: authorHint(ev, t) };
}

/**
 * The addressable parent of a NIP-22 comment as `{ addr, author }`, or null; the address carries
 * the author.
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

/** Record who wrote the events already in hand; an event is the answer for its own id. */
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

/** Whoever the reply line will name, by either route; what cards.js's namedPubkeys declares. */
export const replyPerson = (ev) => replyAuthor(ev) || (replyAddr(ev) || {}).author || null;

/** The reply parents in [events] whose author nothing here knows yet. */
export const unknownParents = (events) =>
  [...new Set(events.map(replyTarget).filter((t) => t && !t.author && !authors.has(t.id)).map((t) => t.id))];

/**
 * Learn who wrote the events named by [ids], on the anonymous socket; returns how many new
 * answers arrived, so a caller that already rendered knows whether to repaint.
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
    } catch (e) { continue; }   // unknown rather than wrong
    for (const ev of evs) {
      const id = hex64(ev && ev.id), pk = hex64(ev && ev.pubkey);
      if (id && pk && !authors.get(id)) { authors.set(id, pk); learned++; }
    }
    // Only an EOSE says the relay does not hold the parent; a cached null is permanent.
    if (evs.complete === true) for (const id of batch) if (!authors.has(id)) authors.set(id, null);
  }
  return learned;
}
