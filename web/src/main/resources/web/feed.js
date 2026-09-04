// The latest feed: the newest content this relay holds, in time order.
//
// The feed is an empty search: the page's own buildFilters() with no words,
// sort, spam toggle or lens, which leaves `{ kinds, limit }`. A filter with
// no `search` field is a plain NIP-01 read, which the store answers newest
// first. None of the bar's extensions may reach this ask: a search string
// that is nothing but `sort:` is a request for an order, and would rank a
// view labelled "newest first". The kind chips are not extensions; they are
// the `kinds` array of the same plain read. The trust gate rides on the
// connection, so a NIP-42 login makes this a trusted-only feed by itself.
// Three exported rules and no DOM, held by web/src/test/js/feed.test.mjs.

import { replyTarget } from "./shared/parents.js";

/**
 * What counts as content: kinds with a renderer that a person publishes to be
 * read. The diagnostic kinds this index is full of (scores, relay records,
 * reactions, follow lists) are deliberately absent.
 */
export const FEED_KINDS = [1, 11, 20, 21, 22, 1063, 1222, 30023, 34235, 34236];

/**
 * Which kinds this feed asks for. A chip's kinds replace the default rather
 * than narrow it: most chips share no kind with FEED_KINDS, and every kind a
 * chip names has a renderer. Null (the "Everything" chip) is the default.
 */
export const feedKinds = (tabKinds) => (tabKinds && tabKinds.length ? tabKinds : FEED_KINDS);

/** Cards under the hero, and cards on the page "see more" opens. */
export const PREVIEW_CARDS = 3;
export const PAGE_CARDS = 100;

/** How far ahead of now a `created_at` may be and still be believed. */
const FUTURE_SKEW_SECS = 300;

/**
 * How many events to ask for to end up with [want] after shaping. NIP-01 has
 * no "not a reply" filter, so replies come back and are dropped here. A hero
 * that regularly draws one card or none is this ratio being wrong.
 */
export const askFor = (want) => Math.max(want * 3, 24);

/**
 * What arrived, reduced to what is worth showing: newest first, at most
 * [want]. Replies go (parents.js decides what a reply is, so this and the
 * "in reply to" line agree), events dated past the skew go (one event
 * stamped 2050 would pin the top forever; the relay does not reject those by
 * default), and duplicates go. Sorted here rather than trusted from the wire.
 */
export function pickFeed(events, want, nowSecs = Date.now() / 1000) {
  const ceiling = nowSecs + FUTURE_SKEW_SECS;
  const seen = new Set();
  const out = [];
  for (const ev of events || []) {
    if (!ev || !ev.id || seen.has(ev.id)) continue;
    if (!Number.isFinite(Number(ev.created_at)) || Number(ev.created_at) > ceiling) continue;
    if (replyTarget(ev)) continue;
    seen.add(ev.id);
    out.push(ev);
  }
  out.sort((a, b) => b.created_at - a.created_at);
  return out.slice(0, want);
}
