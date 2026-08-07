// Is this relay ready to rank for the reader who just signed in — and if not,
// which link of the chain is missing?
//
// Signing in switches search to YOUR web of trust, and the store treats that
// lens as a filter: a reader whose trust chain has not been mirrored here gets
// an EMPTY ranked search, not a degraded one. Before this module the page said
// nothing about that, so "search is broken" and "search has not reached you
// yet" looked identical from the outside.
//
// The chain is real, and it is a chain — each link is how the router finds the
// next one:
//
//   your kind 10002  ─ the router discovers write relays out of stored relay
//                      lists, so with none of yours, nothing about you is ever
//                      fetched. This is the only link that cannot fix itself.
//   your kind 10040  ─ read from those write relays; it names the service whose
//                      scores you trust
//   its kind 30382s  ─ the `assertions` stream syncs the named service's cards
//   ranked search    ─ works once those cards are here AND projected
//
// This module is the DECISION only: which state, which numbers, which link
// broke. The words live in readiness.js, so a test can hold the ordering — the
// property that matters — without being rewritten every time a sentence is.
// The one ordering rule: the FIRST unmet link wins, and every link below it
// reports `waiting`, never a second failure. A column of red crosses would say
// four things are wrong when one is.

// The two non-answers a count can come back as. They live with the client
// because they are facts about a CONNECTION — it declined, or it went quiet —
// and re-exported here because this is where the difference gets decided.
export { REFUSED, TIMED_OUT } from "./relay.js";

/**
 * A real count, as opposed to a reason there isn't one.
 *
 * The no-answer sentinels are objects, so they are TRUTHY — the same trap
 * observer_stats.html documents. Every comparison goes through here.
 */
export const counted = (v) => typeof v === "number" && Number.isFinite(v) && v >= 0;

/**
 * here/there as a 0..1 fraction, or null when there is no honest denominator.
 *
 * Null is a supported answer and the caller must draw nothing rather than
 * estimate: NIP-45 COUNT is optional and widely unimplemented — measured,
 * nip85.brainstorm.world answers none of the 45 (relay, service) pairs it
 * serves — and a bar drawn on a guess would put a number on screen that no
 * relay ever stated. Capped at 1: we can hold MORE than an upstream serves
 * (it deleted, we did not), and 118% reads as a bug.
 */
export function fraction(here, there) {
  if (!counted(here) || !counted(there) || there <= 0) return null;
  return Math.min(1, here / there);
}

/**
 * The whole verdict, from everything that could be learned.
 *
 * `facts` carries only answers, never asks:
 *
 *   relayList     {writeRelays: [url]} | null      kind 10002, ours
 *   scoreListSeen boolean                          a kind 10040 exists at all
 *   rankService   {service, relay} | null           its `30382:rank` tag
 *   scores        {here, there}                     counts, or a sentinel each
 *   probe         {authed, anon} | null             rows each socket returned
 *   posts         {here, there, relay, newestHere, newestThere} | null
 *
 * Anything not yet asked is null, which is why `checking` is a state rather
 * than a blank.
 */
export function assess(facts) {
  const f = facts || {};
  const chain = [];
  const link = (key, status, detail) => { chain.push({ key, status, detail }); return status; };

  // --- link 1: do we know where you post? ---------------------------------
  if (f.relayList == null) return checking(chain);
  const writes = f.relayList.writeRelays || [];
  if (!writes.length) {
    // Two different facts, and telling a reader the wrong one is telling them
    // to fix something that is not broken. NO list is the permanent failure —
    // nothing will ever discover them. A list we cannot USE (every write relay
    // in it is `ws://` on an https page, or loopback) is their list being
    // unreachable from a browser, which is a different sentence and the same
    // next step.
    const seen = !!f.relayList.seen;
    link("relayList", "broken", { seen, declared: f.relayList.declared || 0, writeRelays: 0 });
    waitingBelow(chain, ["scoreList", "scores", "ranked"]);
    return { state: seen ? "no-usable-relays" : "no-relay-list", tone: "blocked", percent: null, chain };
  }
  link("relayList", "ok", { writeRelays: writes.length });

  // --- link 2: do you name a service whose scores rank? -------------------
  if (f.scoreListSeen == null) return checking(chain);
  if (!f.scoreListSeen) {
    link("scoreList", "broken", { reason: "absent" });
    waitingBelow(chain, ["scores", "ranked"]);
    return { state: "no-score-list", tone: "blocked", percent: null, chain };
  }
  if (!f.rankService || !f.rankService.service) {
    // A 10040 declaring only `30382:followers` can order a list but cannot
    // rank one, so it is a broken link rather than a missing one — the same
    // distinction observer_stats.html makes by dropping those rows and
    // counting them in its footer instead of showing four dashes.
    link("scoreList", "broken", { reason: "no-rank-dimension" });
    waitingBelow(chain, ["scores", "ranked"]);
    return { state: "no-rank-service", tone: "blocked", percent: null, chain };
  }
  link("scoreList", "ok", { service: f.rankService.service, relay: f.rankService.relay });

  // --- link 3: have the scores arrived? -----------------------------------
  const scores = f.scores || {};
  if (scores.here == null) return checking(chain);
  const pct = fraction(scores.here, scores.there);
  const here = counted(scores.here) ? scores.here : 0;
  if (here === 0) {
    // Zero here IS a claim — this relay answered, and it holds none of that
    // service's cards. Ranked search returns nothing, so this is blocked, not
    // partial, whatever the upstream says.
    link("scores", "broken", { here: 0, there: scores.there });
    waitingBelow(chain, ["ranked"]);
    return { state: "no-scores-yet", tone: "blocked", percent: 0, chain };
  }
  const short = pct != null && pct < 1;
  link("scores", short ? "partial" : "ok", { here, there: scores.there, percent: pct });

  // --- link 4: does a ranked read actually come back? ---------------------
  //
  // The end-to-end check, and the only one that can catch what the three above
  // cannot see: cards can be HERE and not yet projected, because the trust
  // projection is per service and a service new to this relay is derived by a
  // reconcile that runs at startup. Both sockets are asked the same thing, so
  // an empty corpus (both zero) is never read as a broken lens.
  const probe = f.probe;
  if (probe == null) return checking(chain);
  if (probe.anon > 0 && probe.authed === 0) {
    link("ranked", "broken", { authed: 0, anon: probe.anon });
    return { state: "projection-pending", tone: "blocked", percent: pct, chain };
  }
  link("ranked", short ? "partial" : "ok", { authed: probe.authed });

  if (short) {
    return { state: "importing", tone: "partial", percent: pct, chain, counts: scores };
  }
  // Importing with no denominator: we hold cards and cannot say what fraction
  // that is. Still worth saying — "3,197 here" is the reader's own answer to
  // "is anything happening" — but it is not a bar.
  if (pct == null && !counted(scores.there)) {
    return { state: "importing", tone: "partial", percent: null, chain, counts: scores };
  }

  // --- your own posts: downstream, and NOT in the chain -------------------
  //
  // Deliberately last and deliberately separate. It hangs off the relay list
  // like everything else, but nothing above depends on it: ranking is complete
  // without it, and folding it in would tell a reader whose lens is perfectly
  // healthy that their search is broken. The fix for it is nothing at all.
  const posts = f.posts;
  if (posts == null) return { state: "ready", tone: "ok", percent: null, chain };
  const postPct = fraction(posts.here, posts.there);
  const behindByCount = postPct != null && postPct < 1;
  const behindByDate =
    postPct == null &&
    Number.isFinite(posts.newestHere) && Number.isFinite(posts.newestThere) &&
    posts.newestThere > posts.newestHere;
  if (behindByCount || behindByDate) {
    chain.push({ key: "posts", status: "aside", detail: { ...posts, percent: postPct } });
    return { state: "posts-behind", tone: "working", percent: postPct, chain, counts: posts };
  }
  chain.push({ key: "posts", status: "aside", detail: { ...posts, percent: postPct } });
  return { state: "ready", tone: "ok", percent: null, chain };
}

/** Every link below the one that broke — waiting on it, not failing itself. */
function waitingBelow(chain, keys) {
  for (const key of keys) chain.push({ key, status: "waiting", detail: null });
}

/** Nothing is claimed until the answer that decides it has arrived. */
function checking(chain) {
  return { state: "checking", tone: "working", percent: null, chain };
}

/**
 * Does this verdict deserve the reader's attention at all?
 *
 * The failure mode of a status panel is nagging people who are fine, so this
 * is asked before anything is drawn — and `checking` is included, because a
 * spinner for a check that is about to say "ready" is the same nag one beat
 * earlier. readiness.js only reveals the panel once the state is worth it.
 */
export const worthShowing = (v) => !!v && v.state !== "ready" && v.state !== "checking";
