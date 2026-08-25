// WHOSE EYES A READ IS READ THROUGH, as the filter says it.
//
// This relay has no house observer. A read that names no lens is not "the same
// answers, unranked" — it is the whole corpus with the trust switched off, and
// since the relay's LensRequiredPolicy that answer has to be ASKED for: an
// unauthenticated REQ or COUNT is refused with `auth-required:` unless every
// filter carries NIP-50 `observer:<64-hex>` or waives a lens with
// `include:spam`.
//
// So every ask this page makes falls into one of three cases, and each one has
// exactly one place that decides it:
//
//   - the authenticated socket, signed in    -> the connection IS the lens,
//                                               nothing is added (app.js)
//   - a search ranked through somebody else  -> `observer:` (app.js's
//                                               "ranking as" control)
//   - a fact about a SUBJECT, not the reader -> `include:spam`, here
//
// The third is the one that needs a module: names, faces, scores, group names,
// reply parents and the observer list are asked down the anonymous reference
// connection precisely so the reader's own trust gate cannot narrow them
// (shared/conn.js says why), and there are a dozen call sites. Stamping them
// one at a time is a rule nobody can keep — the next caller writes a plain
// filter and gets a CLOSED it will read as "no such event".
//
// Pure functions over filter objects, no DOM and no socket, so lens.test.mjs
// can hold them.

/** The NIP-50 token that waives a lens: everything, unranked. */
export const SPAM_TOKEN = "include:spam";

/**
 * Does this filter already say whose eyes it is read through?
 *
 * Matched as whole whitespace-delimited tokens, the way the relay's parser
 * lexes them — `include:spammy` is a search term and `observer:npub1…` is not a
 * lens the store can resolve (it takes 64 hex and ignores anything else, so a
 * bech32 key here would pass a laxer test here and rank nothing there).
 */
export function declaresLens(filter) {
  const search = filter && filter.search;
  if (typeof search !== "string") return false;
  return search.split(/\s+/).some((t) =>
    t === SPAM_TOKEN || /^observer:[0-9a-fA-F]{64}$/.test(t));
}

/**
 * The same filter, waiving a lens — for a read that is about a subject rather
 * than about the reader.
 *
 * Idempotent, and it never overwrites: a filter that already names an
 * `observer:` is returned untouched, because appending `include:spam` to a
 * lensed read would lift the trust floor the lens exists to apply.
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
