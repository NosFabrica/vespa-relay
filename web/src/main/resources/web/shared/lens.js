// Whose eyes a read is read through. An unauthenticated REQ or COUNT is refused unless
// every filter carries `observer:<64-hex>` or waives a lens with `include:spam`; a fact
// about a subject rather than the reader is waived here, so every anonymous call site shares one rule.

/** The NIP-50 token that waives a lens. */
export const SPAM_TOKEN = "include:spam";

/** Does this filter already name a lens? Matched as whole tokens, the way the relay lexes them. */
export function declaresLens(filter) {
  const search = filter && filter.search;
  if (typeof search !== "string") return false;
  return search.split(/\s+/).some((t) =>
    t === SPAM_TOKEN || /^observer:[0-9a-fA-F]{64}$/.test(t));
}

/**
 * The same filter, waiving a lens. A filter that already names an `observer:` is returned
 * untouched, since `include:spam` would lift the floor the lens exists to apply.
 */
export function withoutLens(filter) {
  if (declaresLens(filter)) return filter;
  const search = ((filter && filter.search) || "").trim();
  return { ...filter, search: search ? search + " " + SPAM_TOKEN : SPAM_TOKEN };
}

/** [withoutLens] over one filter or an array of them. */
export function withoutLensAll(filter) {
  return Array.isArray(filter) ? filter.map(withoutLens) : withoutLens(filter);
}
