// Pages of results: the one on screen, and the pages fetched before anybody
// asks for them. All arithmetic, no DOM or relay client, held by
// web/src/test/js/paging.test.mjs.
//
// A NIP-01 filter carries `limit` and no offset, and NIP-50 adds an order but
// no cursor. `until` paginates a chronological read only: a ranked answer is
// not in `created_at` order. So page two of a ranking is a longer prefix of
// the same answer, cut locally: askLimit() computes it and pageOf() cuts it.
// Every ask re-sends the pages already held; app.js's preload() pays that in
// the background.

/** Cards on one page. */
export const PAGE_SIZE = 40;

/** Pages fetched past the one on screen, so a reader walking forward never waits on a round trip. */
export const PRELOAD_PAGES = 3;

/**
 * The deepest prefix this page will ever ask for. The relay caps a REQ's
 * `limit` (EnvSettings.maxLimit) and would silently serve a truncated one,
 * and every event held is hydrated. Stopping here is a different reason
 * from the corpus running out, so canGrow() is asked separately from
 * `exhausted`.
 */
export const MAX_ASK = 400;

/** The same ceiling in pages, which the URL's `?page=` is clamped against. */
export const MAX_PAGES = Math.floor(MAX_ASK / PAGE_SIZE);

/**
 * The first ask of a search that opens on [page]: the page and the preload
 * behind it in one ask. The relay's cost is the match set it ranks, not the
 * rows it returns, so one wide ask costs the same as one page and leaves
 * nothing to fetch behind it. The type-ahead asks at this width too, so
 * shared/asks.js can hand Enter the popup's answer.
 */
export const firstAsk = (page = 0) => askLimit(page);

/** The prefix that covers [page] and the [PRELOAD_PAGES] behind it. */
export const askLimit = (page = 0) => Math.min(MAX_ASK, PAGE_SIZE * (page + 1 + PRELOAD_PAGES));

/** The slice of [events] that is page [page]. Out of range is empty, not a throw. */
export const pageOf = (events, page) =>
  (events || []).slice(Math.max(0, page) * PAGE_SIZE, (Math.max(0, page) + 1) * PAGE_SIZE);

/** How many whole and part pages [events] holds. Zero for nothing, not one. */
export const pageCount = (events) => Math.ceil((events || []).length / PAGE_SIZE);

/**
 * Is the buffer already [PRELOAD_PAGES] ahead of [page]? Asked in pages, not
 * events: a hashtag search is several filters in one REQ and `limit` is per
 * filter, so an ask of forty can come back with far more.
 */
export const covered = (events, page) => pageCount(events) >= page + 1 + PRELOAD_PAGES;

/** May the ask grow at all, or is [asked] already the ceiling? */
export const canGrow = (asked) => asked < MAX_ASK;

/**
 * The furthest page the pager may offer: the last one loaded, plus the one
 * being fetched when a fetch is left to make. The +1 lets Next outrun the
 * preload into a skeleton that fills in.
 */
export const lastPage = (events, { exhausted, asked }) => {
  const loaded = pageCount(events);
  if (!loaded) return 0;
  return exhausted || !canGrow(asked) ? loaded - 1 : loaded;
};

/**
 * The widened answer folded into what is already held: appended, never
 * reordered. An event published between two asks lands wherever the ranking
 * puts it, and taking the new order whole would renumber every result under
 * the reader. A late arrival sits at the end until the next search re-ranks it.
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
 * Did that answer prove there is nothing past it? Both tests need [complete],
 * the EOSE flag: a timed-out read is short because we stopped listening.
 * [got] short of [asked] is sound for a multi-filter REQ too; [added] of zero
 * is the test that fires on a hashtag search, where the union can be several
 * times the limit.
 */
export const drained = ({ complete, got, asked, added }) =>
  !!complete && (added === 0 || got < asked);
