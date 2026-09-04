// Is this relay ready to rank for the reader who just signed in, and if not,
// which link of the chain is missing? The store treats the trust lens as a
// filter, so an unmirrored chain gives an empty ranked search. Each link finds
// the next: kind 10002 names the write relays, kind 10040 (read from those)
// names the scoring service, the `assertions` stream syncs its kind 30382
// cards, and ranked search works once they are here and projected. The
// decision only; the words live in web/readiness.js. The first unmet link
// wins, and every link below it reports `waiting`, never a second failure.

// Facts about a connection (it declined, or it went quiet), re-exported
// because this is where the difference gets decided.
export { REFUSED, TIMED_OUT } from "./relay.js";

/** A real count. The no-answer sentinels are objects and therefore truthy. */
export const counted = (v) => typeof v === "number" && Number.isFinite(v) && v >= 0;

/**
 * here/there as a 0..1 fraction, or null when there is no honest denominator.
 * Null means draw nothing: a bar on a guess puts a number on screen no relay
 * stated. Capped at 1 because we can hold more than an upstream serves.
 */
export function fraction(here, there) {
  if (!counted(here) || !counted(there) || there <= 0) return null;
  return Math.min(1, here / there);
}

/**
 * How much of a provider's scores counts as all of them. The tail of an import
 * is the accounts the service scored lowest, and a panel that nags at 99% is
 * a warning about a search that is already complete for any visible result.
 * Compared against the rounded percentage, so no panel ever prints 90% or more.
 */
const SCORES_ENOUGH_PCT = 90;

/** Short of the bar above, and only when there is an honest denominator. */
const shortOfEnough = (pct) => pct != null && Math.round(pct * 100) < SCORES_ENOUGH_PCT;

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
 *   posts         {here, there, relay, kinds, newestHere, newestThere} | null
 *
 * Anything not yet asked is null, which is why `checking` is a state.
 */
export function assess(facts) {
  const f = facts || {};
  const chain = [];
  const link = (key, status, detail) => { chain.push({ key, status, detail }); return status; };

  // Link 1: do we know where you post?
  if (f.relayList == null) return checking(chain);
  const writes = f.relayList.writeRelays || [];
  if (!writes.length) {
    // No list is the permanent failure; a list we cannot use from a browser
    // (`ws://` on an https page, loopback) is a different sentence.
    const seen = !!f.relayList.seen;
    link("relayList", "broken", { seen, declared: f.relayList.declared || 0, writeRelays: 0 });
    waitingBelow(chain, ["scoreList", "scores", "ranked"]);
    return { state: seen ? "no-usable-relays" : "no-relay-list", tone: "blocked", percent: null, chain };
  }
  link("relayList", "ok", { writeRelays: writes.length });

  // Link 2: do you name a service whose scores rank?
  if (f.scoreListSeen == null) return checking(chain);
  if (!f.scoreListSeen) {
    link("scoreList", "broken", { reason: "absent" });
    waitingBelow(chain, ["scores", "ranked"]);
    return { state: "no-score-list", tone: "blocked", percent: null, chain };
  }
  if (!f.rankService || !f.rankService.service) {
    // A 10040 declaring only `30382:followers` can order a list but cannot
    // rank one: a broken link rather than a missing one.
    link("scoreList", "broken", { reason: "no-rank-dimension" });
    waitingBelow(chain, ["scores", "ranked"]);
    return { state: "no-rank-service", tone: "blocked", percent: null, chain };
  }
  link("scoreList", "ok", { service: f.rankService.service, relay: f.rankService.relay });

  // Link 3: have the scores arrived?
  const scores = f.scores || {};
  if (scores.here == null) return checking(chain);
  // A sentinel in `here` is a count asked and not answered. No answer is no
  // claim, so keep checking rather than fall into the answered-zero branch.
  if (!counted(scores.here)) return checking(chain);
  const pct = fraction(scores.here, scores.there);
  const here = scores.here;
  if (here === 0) {
    // Zero here is a claim: this relay answered and holds none of that
    // service's cards, so ranked search returns nothing whatever upstream says.
    link("scores", "broken", { here: 0, there: scores.there });
    waitingBelow(chain, ["ranked"]);
    return { state: "no-scores-yet", tone: "blocked", percent: 0, chain };
  }
  const short = shortOfEnough(pct);
  link("scores", short ? "partial" : "ok", { here, there: scores.there, percent: pct });

  // Link 4: does a ranked read actually come back? Cards can be here and not
  // yet projected, since the trust projection is derived per service by a
  // reconcile at startup. Both sockets are asked the same thing, so an empty
  // corpus is never read as a broken lens.
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
  // Importing with no denominator: worth saying, but not a bar.
  if (pct == null && !counted(scores.there)) {
    return { state: "importing", tone: "partial", percent: null, chain, counts: scores };
  }

  // Your own posts: downstream, and not in the chain. Ranking is complete
  // without it, and folding it in would tell a reader with a healthy lens
  // that search is broken. Absent is a supported answer: where this relay's
  // mirrored kinds cannot be read the caller asks neither side.
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

/** Every link below the one that broke: waiting on it, not failing itself. */
function waitingBelow(chain, keys) {
  for (const key of keys) chain.push({ key, status: "waiting", detail: null });
}

/** Nothing is claimed until the answer that decides it has arrived. */
function checking(chain) {
  return { state: "checking", tone: "working", percent: null, chain };
}

/**
 * Does this verdict deserve the reader's attention at all? `checking` is
 * excluded too: a spinner for a check about to say "ready" is the same nag.
 */
export const worthShowing = (v) => !!v && v.state !== "ready" && v.state !== "checking";
