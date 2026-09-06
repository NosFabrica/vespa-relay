// Pages of results, all arithmetic and no DOM. A ranked answer has no offset and no
// cursor (`until` only paginates a chronological read), so page two is a longer prefix
// of the same answer cut locally: askLimit() sizes it and pageOf() cuts it.

/** Cards on one page. */
export const PAGE_SIZE = 40;

/** Pages fetched past the one on screen. */
export const PRELOAD_PAGES = 3;

/**
 * The deepest prefix ever asked for; the relay would silently truncate a `limit` past its
 * own cap. Stopping here is not the corpus running out, so canGrow() is separate from `exhausted`.
 */
export const MAX_ASK = 400;

/** The same ceiling in pages, for clamping `?page=`. */
export const MAX_PAGES = Math.floor(MAX_ASK / PAGE_SIZE);

/**
 * The first ask of a search that opens on [page], preload included. The type-ahead asks
 * at this width too, so shared/asks.js can hand Enter the popup's answer.
 */
export const firstAsk = (page = 0) => askLimit(page);

/** The prefix that covers [page] and the [PRELOAD_PAGES] behind it. */
export const askLimit = (page = 0) => Math.min(MAX_ASK, PAGE_SIZE * (page + 1 + PRELOAD_PAGES));

/** The slice of [events] that is page [page]. Out of range is empty, not a throw. */
export const pageOf = (events, page) =>
  (events || []).slice(Math.max(0, page) * PAGE_SIZE, (Math.max(0, page) + 1) * PAGE_SIZE);

/** How many whole and part pages [events] holds. Zero for nothing, not one. */
export const pageCount = (events) => Math.ceil((events || []).length / PAGE_SIZE);

/** Is the buffer already [PRELOAD_PAGES] ahead of [page]? In pages: a multi-filter ask overshoots its limit. */
export const covered = (events, page) => pageCount(events) >= page + 1 + PRELOAD_PAGES;

/** May the ask grow at all, or is [asked] already the ceiling? */
export const canGrow = (asked) => asked < MAX_ASK;

/** The furthest page the pager may offer: the last loaded, plus the one still to fetch. */
export const lastPage = (events, { exhausted, asked }) => {
  const loaded = pageCount(events);
  if (!loaded) return 0;
  return exhausted || !canGrow(asked) ? loaded - 1 : loaded;
};

/**
 * The widened answer folded into what is held: appended, never reordered, so a late
 * arrival cannot renumber the results under the reader.
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
 * Did that answer prove there is nothing past it? Only on EOSE: a timed-out read is short
 * because we stopped listening. [added] of zero is the test a multi-filter ask needs.
 */
export const drained = ({ complete, got, asked, added }) =>
  !!complete && (added === 0 || got < asked);
