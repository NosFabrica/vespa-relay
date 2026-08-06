// The latest feed: the newest CONTENT this relay holds, in time order.
//
// The feed is an EMPTY SEARCH. Not "like" one — literally the page's own
// buildFilters() with no words, no sort, no spam toggle and no lens, which
// leaves `{ kinds, limit }` and nothing else. A filter with no NIP-50 `search`
// field is a plain NIP-01 read, and a plain read is what the store answers
// newest-first. So there is no second query path here to keep in step with the
// first: this module is the two rules that make that answer a FEED, and app.js
// asks the same builder it always did.
//
// Carrying none of the extensions is the deliberate part. A search string —
// even one that is nothing but `sort:` or `observer:` — is a request for an
// ORDER, and "a query that is nothing but extensions becomes unconstrained,
// not match-nothing". Let the bar's sort leak into this ask and the page says
// "newest first" over a list ranked by trust. The bar therefore does not reach
// this view at all, and the view does not show it.
//
// The trust gate rides on the CONNECTION rather than on the query, which is
// what makes an empty search worth showing at all: a NIP-42 login turns even a
// plain NIP-01 filter into a trusted-only feed — newest first, below-floor
// authors dropped (README, "Search"). Signed in, this is your web of trust's
// view of the network with the ask saying nothing about it; signed out it is
// the whole mirror in time order. The hero draws its preview only for the
// first case; the full view serves either, and says which one it is showing.
//
// Two exported rules and no DOM, so tools/webtest/feed.test.mjs can hold them.

import { replyTarget } from "./shared/parents.js";

/**
 * What counts as content.
 *
 * Every kind here has a renderer (cards.js's registry) AND is something a
 * person publishes to be read: notes and threads, pictures, video in both the
 * current NIP-71 kinds and the 34235/34236 pair that preceded them, files,
 * voice, and long-form articles. The kinds this index is full of but nobody
 * browses — scores, relay records, reactions, follow lists — are deliberately
 * absent: a feed of 30382 score cards is a diagnostic, and kind_stats.html is
 * already where diagnostics live.
 */
export const FEED_KINDS = [1, 11, 20, 21, 22, 1063, 1222, 30023, 34235, 34236];

/** Cards under the hero, and cards on the page "see more" opens. */
export const HOME_CARDS = 3;
export const PAGE_CARDS = 100;

/** How far ahead of now a `created_at` may be and still be believed. */
const FUTURE_SKEW_SECS = 300;

/**
 * How many events to ASK for, to end up with [want] after the shaping below.
 *
 * NIP-01 cannot express "a note that is not a reply" — there is no absence
 * filter — so replies come back with everything else and are dropped here.
 * Much of kind 1 is replies, so asking for exactly [want] would routinely
 * render one card where three were promised. The floor matters more than the
 * factor for the home preview: three cards asked as three, times anything, is
 * still a sample small enough to come back entirely replies.
 */
export const askFor = (want) => Math.max(want * 3, 24);

/**
 * What arrived, reduced to what is worth showing: newest first, at most [want].
 *
 * Three rules, each one a thing that would otherwise be visible on screen:
 *
 * - **Replies go.** A reply lifted out of its thread is a fragment, and a feed
 *   of them reads as somebody else's half of a conversation. (Which `e` tag
 *   makes an event a reply is NIP-10's rule and lives in shared/parents.js —
 *   asked here rather than re-derived, so the feed and the "in reply to" line
 *   can never disagree about what a reply is.)
 * - **Future dates go.** A time-ordered feed with no upper bound is pinned by
 *   whoever dates an event furthest ahead — one event stamped 2050 sits at the
 *   top of it forever, and the relay does not reject those by default
 *   (`REJECT_FUTURE_SECONDS` is off unless an operator sets it). A few minutes
 *   of skew is a client with a wrong clock; a year is a claim about the future.
 * - **Duplicates go**, the same belt-and-braces the search does over its own
 *   results: one filter should not repeat an event, but the same note rendering
 *   twice is a visible bug and this is one pass over a list about to be
 *   rendered anyway.
 *
 * Sorted here rather than trusted from the wire. The store answers a plain read
 * newest-first, so this changes nothing today; it costs one pass over a few
 * hundred events and makes the word "latest" on the page true by construction
 * rather than by a property of the store that this page cannot check.
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
