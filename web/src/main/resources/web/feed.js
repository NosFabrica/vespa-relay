// The latest feed: the newest content this relay holds, in time order. It is an empty
// search, `{ kinds, limit }` with no `search` field, so none of the bar's extensions may
// reach the ask; a bare `sort:` would rank a view labelled "newest first". No DOM here.

import { replyTarget } from "./shared/parents.js";

/** Kinds a person publishes to be read; scores, relay records and reactions are left out. */
export const FEED_KINDS = [1, 11, 20, 21, 22, 1063, 1222, 30023, 34235, 34236];

/** Which kinds the feed asks for. A chip's kinds replace the default, never narrow it. */
export const feedKinds = (tabKinds) => (tabKinds && tabKinds.length ? tabKinds : FEED_KINDS);

/** Cards under the hero, and cards on the page "see more" opens. */
export const PREVIEW_CARDS = 3;
export const PAGE_CARDS = 100;

const FUTURE_SKEW_SECS = 300;

/** How many events to ask for to end up with [want] after replies are dropped. */
export const askFor = (want) => Math.max(want * 3, 24);

/**
 * Newest first, at most [want]: replies, duplicates and events dated past the skew go.
 * Sorted here rather than trusted from the wire; one far-future event would pin the top.
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
