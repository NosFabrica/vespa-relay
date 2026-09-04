// Whose eyes a read is read through, as the filter says it. This relay has no
// house observer: under LensRequiredPolicy an unauthenticated REQ or COUNT is
// refused unless every filter carries NIP-50 `observer:<64-hex>` or waives a
// lens with `include:spam`. The authenticated socket is its own lens and a
// search ranked through somebody else carries `observer:` (both app.js); a
// fact about a subject rather than the reader (names, faces, scores, group
// names, reply parents) is waived here, so the dozen anonymous call sites
// share one rule. Pure functions over filter objects, held by lens.test.mjs.

/** The NIP-50 token that waives a lens: everything, unranked. */
export const SPAM_TOKEN = "include:spam";

/**
 * Does this filter already say whose eyes it is read through? Matched as
 * whole whitespace-delimited tokens, the way the relay's parser lexes them:
 * `include:spammy` is a search term, and `observer:npub1…` is not a lens the
 * store can resolve.
 */
export function declaresLens(filter) {
  const search = filter && filter.search;
  if (typeof search !== "string") return false;
  return search.split(/\s+/).some((t) =>
    t === SPAM_TOKEN || /^observer:[0-9a-fA-F]{64}$/.test(t));
}

/**
 * The same filter, waiving a lens. Idempotent, and a filter that already names
 * an `observer:` is returned untouched, because appending `include:spam` would
 * lift the trust floor the lens exists to apply.
 */
export function withoutLens(filter) {
  if (declaresLens(filter)) return filter;
  const search = ((filter && filter.search) || "").trim();
  return { ...filter, search: search ? search + " " + SPAM_TOKEN : SPAM_TOKEN };
}

/** [withoutLens] over one filter or the array NIP-01 ORs inside a subscription. */
export function withoutLensAll(filter) {
  return Array.isArray(filter) ? filter.map(withoutLens) : withoutLens(filter);
}
