// Pages of results — the one on screen, and the three fetched before anybody
// asks for them.
//
// A search used to be one ask of forty and a list of forty: past the fortieth
// result the index simply ended, with nothing on the page to say whether that
// was the corpus running out or the ask. This module is what makes it a PAGE
// of forty with more behind it. It is all arithmetic — no DOM, no relay client
// and no page state — so web/src/test/js/paging.test.mjs holds every rule in
// it, which is where the off-by-ones in a pager belong.
//
// THE PROTOCOL IS WHY IT LOOKS LIKE THIS. A NIP-01 filter carries `limit` and
// there is no offset — nothing that says "start at 41" — and NIP-50 adds an
// order but no cursor either. `until` paginates a plain chronological read and
// nothing else: a ranked answer is not in `created_at` order, so the last
// card's timestamp says nothing about where the next page begins. So the only
// way to reach page two of a RANKING is to ask for a longer prefix of the same
// answer and cut it locally, which is what askLimit() computes and pageOf()
// cuts. Every ask re-sends the pages already held; that is the cost of the
// missing offset, and it is paid in the background (see preload() in app.js)
// rather than in front of the reader.

/** Cards on one page — what the results view used to show in total. */
export const PAGE_SIZE = 40;

/**
 * How many pages past the one on screen are fetched before anybody asks.
 *
 * Three, so a reader walking forward is never the one waiting: turning a page
 * draws from the buffer and starts the next widening ask behind it, and only
 * somebody who turns four pages faster than one round trip ever sees a
 * skeleton.
 */
export const PRELOAD_PAGES = 3;

/**
 * The deepest prefix this page will ever ask the relay for.
 *
 * A ceiling is not optional here: the ask grows with the page, the relay caps
 * a REQ's `limit` at 5,000 (EnvSettings.maxLimit) and would silently serve a
 * truncated one, and every event this page holds is hydrated — a name, a face,
 * a score, a provenance row — so an unbounded buffer is an unbounded client.
 * Ten pages is where paging a RANKING stops being reading and starts being a
 * corpus dump, and a reader who is 400 results deep in an order they do not
 * trust wants different words rather than an eleventh page.
 *
 * It is a different reason for stopping than the corpus running out, and the
 * pager has to say which — hence canGrow() below, which is asked separately
 * from `exhausted`.
 */
export const MAX_ASK = 400;

/**
 * The same ceiling counted in pages, which is what the URL is clamped against:
 * a hand-made `?page=900` opens the deepest page this view can reach rather
 * than a skeleton no answer will ever fill.
 */
export const MAX_PAGES = Math.floor(MAX_ASK / PAGE_SIZE);

/**
 * The FIRST ask of a search that opens on [page] — enough for that page, and
 * not one event more.
 *
 * Nearly always page 0 and therefore one page: the ask in front of the reader
 * stays exactly what it was before any of this existed, and the three pages
 * ahead are fetched behind the answer rather than in front of it. Bigger only
 * for `?q=cats&page=4` pasted cold, where the page being restored is the one
 * that has to be drawable when the answer lands.
 */
export const firstAsk = (page = 0) => Math.min(MAX_ASK, PAGE_SIZE * (page + 1));

/** The prefix that covers [page] and the [PRELOAD_PAGES] behind it. */
export const askLimit = (page = 0) => Math.min(MAX_ASK, PAGE_SIZE * (page + 1 + PRELOAD_PAGES));

/** The slice of [events] that is page [page]. Out of range is empty, not a throw. */
export const pageOf = (events, page) =>
  (events || []).slice(Math.max(0, page) * PAGE_SIZE, (Math.max(0, page) + 1) * PAGE_SIZE);

/** How many whole and part pages [events] holds. Zero for nothing, not one. */
export const pageCount = (events) => Math.ceil((events || []).length / PAGE_SIZE);

/**
 * Is the buffer already three pages ahead of [page]?
 *
 * Asked in PAGES rather than in events, because a hashtag search is four
 * filters in one REQ and NIP-01's `limit` is per filter — so an ask of forty
 * can come back with a hundred and sixty, and comparing the ask against itself
 * would send a round trip for pages already in hand.
 */
export const covered = (events, page) => pageCount(events) >= page + 1 + PRELOAD_PAGES;

/** May the ask grow at all, or is [asked] already the ceiling? */
export const canGrow = (asked) => asked < MAX_ASK;

/**
 * The furthest page the pager may offer: the last one loaded, plus the one
 * being fetched when there is a fetch left to make.
 *
 * The +1 is what lets a reader outrun the preload — Next into a page whose
 * events are still in flight draws a skeleton and fills in — rather than
 * disabling a control that will be live a moment later.
 */
export const lastPage = (events, { exhausted, asked }) => {
  const loaded = pageCount(events);
  if (!loaded) return 0;
  return exhausted || !canGrow(asked) ? loaded - 1 : loaded;
};

/**
 * The widened answer folded into what is already held — APPENDED, never
 * reordered.
 *
 * The naive fold is to take the new answer whole: it is the same query, longer,
 * so it ought to be the same list with a tail. It is not, quite. Events are
 * published while a reader reads, and one that arrives between the two asks
 * lands wherever the ranking puts it — which for a trust-ranked search can be
 * page one. Taking the new order wholesale would then renumber every result
 * under the reader: the card they were about to click moves down one, and the
 * page they came back from has different cards on it than when they left.
 *
 * That the longer ask agrees with the shorter one about the part they share is
 * the assumption under all of this, and it is the store's to keep, not ours —
 * so it was measured rather than assumed. Against a real Vespa-backed relay
 * over a 305-event corpus, asks of 40 / 160 / 200 / 400 for one search each
 * returned the previous ask as an exact ordered PREFIX, in 183 / 70 / 89 / 65
 * ms: a widened ask is neither a reshuffle nor slower than the first one. This
 * fold is what protects the reader on the day that stops being true.
 *
 * So the pages already held keep their order and their positions, and anything
 * genuinely new is appended in the order the relay ranked it. The cost is that
 * a late arrival deserving position 5 sits at the end of the buffer until the
 * next search re-ranks it — a placement this page can defend as "what the
 * relay said when you asked", against a page whose numbering it cannot defend
 * at all.
 */
export function mergePages(have, arrived) {
  const out = (have || []).slice();
  const seen = new Set(out.map((e) => e && e.id));
  for (const ev of arrived || []) {
    if (!ev || !ev.id || seen.has(ev.id)) continue;
    seen.add(ev.id);
    out.push(ev);
  }
  return out;
}

/**
 * Did that answer prove there is nothing past it?
 *
 * Two ways to know, and both need [complete] — the EOSE flag shared/relay.js
 * hands back. A read that TIMED OUT returned fewer events than it was asked
 * for because we stopped listening, and reading that as "the corpus ends here"
 * would put a full stop on the end of a search over a slow connection.
 *
 * - [got] short of [asked]: the relay had less than the prefix we named. Sound
 *   for a multi-filter REQ too, where `limit` is per filter: if the union is
 *   short of one filter's limit then every filter in it is.
 * - [added] nothing: a longer ask that brought no event the buffer did not
 *   already hold. This is the one that fires on a hashtag search, where the
 *   union can be several times the limit and the first test never trips.
 *
 * That second test is not theoretical: `#pagetag` over a real relay holding
 * 100 tagged notes and 40 NIP-73 comments came back with all 140 on an ask of
 * 40 — the four filters of a hashtag REQ each carry their own limit (query.js's
 * sideLimit) — so `got < asked` was false on every ask, and the pager reached
 * the end of that corpus on `added === 0` alone.
 */
export const drained = ({ complete, got, asked, added }) =>
  !!complete && (added === 0 || got < asked);
