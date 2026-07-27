/*
 * The profile-search query battery. Each entry is a term a user would type into
 * a "find a person" box: given names, handles, brands, and a couple of
 * multi-word queries. They are deliberately common so both relays return full
 * result pages and the overlap is meaningful rather than noise.
 *
 * Override at runtime with QUERIES="a,b,c" or a --queries flag; this is the
 * default set the report runs when none is given.
 */
export const DEFAULT_QUERIES = [
  "jack",
  "gigi",
  "alice",
  "satoshi",
  "vitor",
  "fiatjaf",
  "odell",
  "lyn",
  "snowden",
  "carla",
  "max",
  "nostr",
  "bitcoin",
  "jb55",
  "hodl",
  "preston",
  "gladstein",
  "amethyst",
];

export function queriesFrom(env, cliList) {
  if (cliList && cliList.length) return cliList;
  const raw = env.QUERIES;
  if (raw && raw.trim()) {
    return raw
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return DEFAULT_QUERIES;
}

/**
 * The relays the routerConfigOverride streams down from — the feed that fills
 * the store. Split by the role each plays in this test: `search` relays answer
 * NIP-50 `search` directly (rare), `feed` relays only answer NIP-01 filters
 * (author / kind lookups), which is how we backfill profiles by pubkey.
 *
 * brainstorm.nostr1.com is intentionally omitted: it currently returns
 * 402 Payment Required ("relay is paused for past-due payment").
 */
export const SOURCES = {
  popular: [
    "wss://relay.primal.net",
    "wss://relay.damus.io",
    "wss://purplepag.es",
    "wss://nos.lol",
    "wss://nostr-pub.wellorder.net",
  ],
  mirrors: [
    "wss://primus.nostr1.com",
    "wss://profiles.nostr1.com",
    "wss://indexer.coracle.social",
    "wss://user.kindpag.es",
    "wss://directory.yabu.me",
    "wss://relay.ditto.pub",
  ],
};

/** All source urls, deduped, in config order. */
export const ALL_SOURCES = [...SOURCES.popular, ...SOURCES.mirrors];

/** brainstorm.world's profile search is a NIP-50 relay (config.js VITE_WOT_SEARCH_RELAY). */
export const BRAINSTORM_SEARCH = "wss://search.brainstorm.world";

/** The kinds the routerConfigOverride mirrors down. */
export const PROFILE_KINDS = [0, 3, 5, 1984, 10000, 30000];
