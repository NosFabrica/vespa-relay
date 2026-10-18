// The URL is the search. The results view is derived from q, tab, sort, spam and lens, so those
// are the location; sign-in is not among them, since auth belongs to the socket. The hero is
// the bare path, the floor Back lands on; the popup never appears here. Both directions are
// pure, over the view's values; only [pushUrl], [goTo] and [whileRestoring] touch history.

import { npub, pubkeyParam } from "./shared/nip19.js";
import { MAX_PAGES } from "./paging.js";

/** Where "see more" goes: a parameter on the root, not a `/feed` path, so the root serves it from cold. */
export const FEED_URL = "/?feed=1";

/** That url carrying the chip, `/?feed=1&tab=media`; "Everything" writes no parameter. */
export const feedUrl = (tab) => FEED_URL + (tab && tab.kinds ? `&tab=${tab.slug}` : "");

/** Does [href] name the feed, whichever chip it carries? */
export const isFeedHref = (href) => href === FEED_URL || href.startsWith(FEED_URL + "&");

/** A NIP-19 path is the third view. The shape test only routes; entity.js validates. */
export const ENTITY_PATH = /^(npub|nprofile|note|nevent|naddr)1[a-z0-9]+$/i;

/** The identifier a path names, or null when it names anything else. */
export function entitySegOf(pathname) {
  const seg = String(pathname || "").slice(1);
  return ENTITY_PATH.test(seg) ? seg : null;
}

/**
 * `?page=4` as this page counts pages: 0-based, 0 for anything that is not a page, clamped at
 * the ceiling the ask stops at.
 */
export function pageParam(p) {
  const n = Number(p.get("page"));
  return Number.isInteger(n) && n > 1 ? Math.min(n - 1, MAX_PAGES - 1) : 0;
}

/**
 * The url for a results view. [showing] is whether the results are on screen; [tabs][0] is the
 * default chip, which the url leaves out. Anchored at "/": a search launched from /note1…
 * belongs at the root.
 */
export function searchUrl({ text = "", showing = true, page = 0, tab, tabs, sort = "", spam = false, viewingAs = null }) {
  const p = new URLSearchParams();
  if (showing && text) p.set("q", text);
  // One-based, because a person reads it against the pager's buttons.
  if (showing && text && page > 0) p.set("page", String(page + 1));
  if (tab && tab.slug !== tabs[0].slug) p.set("tab", tab.slug);
  if (sort) p.set("sort", sort);
  if (spam) p.set("spam", "1");
  // npub, like everywhere else on the page.
  if (viewingAs) p.set("as", npub(viewingAs));
  const qs = p.toString();
  return "/" + (qs ? "?" + qs : "");
}

/**
 * What a query string asks the page to show. An unknown tab is the first; `sort` comes back as
 * written, for the menu to accept or refuse; `as` is hex or null.
 */
export function readSearch(search, tabs) {
  const p = new URLSearchParams(search);
  return {
    feed: p.get("feed") === "1",
    tab: tabs.find((t) => t.slug === p.get("tab")) || tabs[0],
    text: (p.get("q") || "").trim(),
    sort: p.get("sort") || "",
    spam: p.get("spam") === "1",
    as: pubkeyParam(p.get("as")),
    page: pageParam(p),
  };
}

/**
 * Which view an internal link opens, for the click router: the hero, the feed, a search (a
 * hashtag chip, rebuilt with the reader's filters), or a permalink; null leaves it to the browser.
 */
export function linkTarget(href) {
  if (href === "/") return { view: "home" };
  if (isFeedHref(href)) return { view: "feed" };
  if (/^\/\?q=/.test(href)) return { view: "search", text: new URLSearchParams(href.slice(1)).get("q") || "" };
  if (ENTITY_PATH.test(href.slice(1))) return { view: "entity" };
  return null;
}

// A restore must not re-push what it is restoring, or popstate truncates the forward stack.
let restoring = false;

/** Run [fn], a render of the url already in the bar, with every [pushUrl] inside it a no-op. */
export function whileRestoring(fn) {
  restoring = true;
  try { return fn(); } finally { restoring = false; }
}

/** A new place in the back stack, unless this is a restore or the bar already says it. */
export function pushUrl(url) {
  if (restoring) return;
  // Re-submitting the same search is not a second place.
  if (url === location.pathname + location.search) return;
  history.pushState(null, "", url);
}

/** Push [href] for a navigation the reader asked for, a restore or not. */
export function goTo(href) {
  if (location.pathname + location.search !== href) history.pushState(null, "", href);
}
