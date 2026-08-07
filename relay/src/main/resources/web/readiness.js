// The line under the search box that says whether this relay can rank for you
// yet — and, when it cannot, the one thing that fixes it.
//
// Signing in switches search to YOUR web of trust, and the store treats that
// lens as a filter rather than an ordering: a reader whose trust chain has not
// reached this relay gets an EMPTY ranked search. The page used to say nothing
// at all about that, so "this relay is broken" and "this relay has not got to
// you yet" looked identical — and one of them has a next step.
//
// The DECISION is shared/readiness.js, pure and tested. This module is the two
// halves it cannot be: the asks that produce the facts, and the words.
//
// Asks are the reason this is not eager. A healthy signed-in reader would
// otherwise pay seven round trips per load to be told nothing, so:
//
//   * it runs once per pubkey per page, after a delay, so the first search and
//     the first paint have the sockets to themselves;
//   * it stops at the FIRST broken link — the counts and the probe are never
//     sent for a reader who has no relay list;
//   * a `ready` verdict is remembered for a WEEK, in a cookie the browser
//     expires by itself. A complete chain has nothing left to learn — every
//     link is an event already here — so a returning reader pays nothing at all
//     until the next one. A verdict short of ready is never remembered: that is
//     the one that changes, on the router's six-hour pass.

import { relay, refConn, RELAY_URL } from "./shared/conn.js";
import { Relay } from "./shared/relay.js";
import { esc } from "./shared/format.js";
import { assess, counted, worthShowing, TIMED_OUT } from "./shared/readiness.js";
import { normalizeRelay, whyNotDialable } from "./shared/relayurl.js";

const $panel = document.getElementById("readiness");

/** Where a NIP-85 provider will make you a list, named in state B. */
const PROVIDER_URL = "https://brainstorm.world";

/**
 * The delay before the first ask.
 *
 * Not a guess at how long the page takes — a place in the queue. Everything
 * this module asks is background work, and every socket it uses is one the
 * search, the faces and the score chips are already on.
 */
const START_DELAY_MS = 1500;

/**
 * How long a `ready` verdict is trusted — a week, in a cookie.
 *
 * A reader whose chain is complete has nothing left to learn from this check:
 * every link is an event already mirrored here, and the only way back out of
 * `ready` is their own republished list, which they will have done deliberately
 * and can force a recheck for. The check itself is seven round trips, two of
 * them to somebody else's relay, so the ceiling on how often it is worth paying
 * is set by how often the answer could change, not by the router's six-hour
 * refresh — that interval is what makes a NOT-ready verdict worth re-asking.
 *
 * A cookie rather than localStorage, matching app.js's signed-in preference:
 * same-origin, survives a page switch, and expires by itself, so the week is
 * enforced by the browser rather than by arithmetic we have to get right. It
 * rides along on requests to this relay; the relay reads no cookies at all, and
 * a signed-in reader has already named the same pubkey over NIP-42 on the
 * socket, so it tells the server nothing it was not told directly.
 */
const READY_TTL_DAYS = 7;
const READY_COOKIE = "sot_ready";

/** How long a DISMISSAL is trusted. The router's own refresh interval. */
const DISMISS_TTL_MS = 6 * 60 * 60 * 1000;

const fmt = (n) => Number(n).toLocaleString();
const host = (url) => String(url || "").replace(/^wss?:\/\//, "");
/** "1 write relay", "2 write relays" — never "2 write relay(s)". */
const many = (n, word) => `${fmt(n)} ${word}${n === 1 ? "" : "s"}`;

let checkedFor = null;   // the pubkey this page has already checked

/**
 * Which check is the current one.
 *
 * Bumped by every start and by every clear, and carried by the run itself, so
 * a run whose reason has gone away paints nothing. Everything here is on a
 * timer or a socket, and both outlive the reason they were started for: sign
 * out inside the start delay and the timer still fired, still asked, and still
 * drew a panel about an account that was no longer on screen — a claim about
 * somebody who had left. Switching accounts in the extension is the same race
 * with a worse result, since the panel would be RIGHT about the wrong person.
 */
let generation = 0;

/**
 * Check [pubkey], once, in the background.
 *
 * Safe to call from every path that can change who is signed in — it is the
 * same shape as the login flight above it, and for the same reason: a search,
 * a sign-in click and a back/forward restore can all arrive within a beat of
 * each other and none of them should mean a second round of asks.
 */
export function checkReadiness(pubkey, { force = false } = {}) {
  if (!pubkey) { clearReadiness(); return; }
  if (!force && checkedFor === pubkey) return;
  if (!force && rememberedReady(pubkey)) { checkedFor = pubkey; return; }
  checkedFor = pubkey;
  const gen = ++generation;
  setTimeout(() => { run(pubkey, gen).catch(() => {}); }, force ? 0 : START_DELAY_MS);
}

/** Signing out is a decision: take the panel down and forget the verdict. */
export function clearReadiness() {
  checkedFor = null;
  generation++;
  hide();
}

/**
 * One pass. [gen] is what makes it abandonable.
 *
 * The generation is the ONLY concurrency control here, deliberately: a
 * `running` flag beside it looked like belt and braces and was a hole — press
 * "Check again" while a pass is in flight and the new one returned early on the
 * flag, while the old one then suppressed its own paint for being superseded,
 * so the click did nothing at all and said nothing about it. Letting the newer
 * pass run costs one duplicated round of asks, once, per press; the older one
 * finishes into a paint that is dropped, and closes its sockets on the way out.
 */
async function run(me, gen) {
  if (gen !== generation) return;
  const facts = {};
  // Painted after every stage, so the chain fills in rather than appearing
  // whole — but nothing is REVEALED until the verdict is worth showing, and
  // nothing at all once this pass has been superseded.
  const paint = () => { if (gen === generation) render(assess(facts), me); };

  const anon = await refConn();
  if (!(await readLists(anon, me, facts))) return;
  paint();
  if (!facts.relayList.writeRelays.length || !facts.rankService) return;

  // Together: our own count and the provider relay's answer the same question
  // about two different stores, and neither informs the other. Run in series,
  // the local answer — Vespa, milliseconds — sat waiting on a stranger's relay
  // that observer_stats measured at up to 47.6s for a single COUNT.
  [facts.scores, facts.probe] = await Promise.all([
    scoreCounts(anon, facts.rankService),
    probe(anon),
  ]);
  paint();

  // Only once ranking is complete: a reader still importing scores does not
  // need a second, quieter number about a different thing.
  if (assess(facts).state !== "ready") return;
  facts.posts = await postCounts(anon, me, facts.relayList.writeRelays);
  if (gen !== generation) return;
  const after = assess(facts);
  render(after, me);
  if (after.state === "ready") rememberReady(me);
}

// ---- the asks -------------------------------------------------------------

/**
 * Your kind 0, 10002 and 10040 — one REQ, read ANONYMOUSLY.
 *
 * The authenticated socket is trust-gated to authors the reader has scored,
 * and this whole check exists for readers who have scored nobody: asking there
 * would answer "you do not exist" for exactly the people it is meant to
 * diagnose. Same reason the observer picker and the score chips read here.
 *
 * False when the relay did not FINISH answering. `complete` is EOSE, not
 * merely resolved — a timed-out read hands back whatever arrived, and reading
 * a gap in it as "you have no relay list" would put a red panel in front of
 * somebody whose only problem was a slow moment. The same rule profiles.js and
 * parents.js hold about caching absences.
 */
async function readLists(anon, me, facts) {
  let evs;
  try { evs = await anon.req({ kinds: [0, 10002, 10040], authors: [me], limit: 3 }); }
  catch (e) { return false; }
  if (evs.complete !== true) return false;

  const newest = (kind) =>
    evs.filter((e) => e.kind === kind).sort((a, b) => b.created_at - a.created_at)[0] || null;

  const relayList = newest(10002);
  // NIP-65: an `r` tag with no marker is both read AND write. Only the write
  // ones matter here — they are where this reader publishes, which is where
  // the router would go looking for everything else about them.
  const declared = relayList
    ? (relayList.tags || []).filter((t) => t?.[0] === "r" && t[1] && (t[2] === "write" || t[2] == null))
    : [];
  const writeRelays = [...new Set(declared.map((t) => normalizeRelay(t[1])).filter(Boolean))];
  // `seen` is not the same fact as `writeRelays.length`, and saying so cost a
  // wrong headline. A list naming only `ws://` relays loses every one of them
  // on an https page — the browser refuses those connections outright — and a
  // list naming only loopback loses them too. Both left an empty array, and the
  // panel then told a reader who HAS published a relay list that we had never
  // seen one.
  facts.relayList = { seen: !!relayList, declared: declared.length, writeRelays };

  const scoreList = newest(10040);
  facts.scoreListSeen = !!scoreList;
  const rank = scoreList ? (scoreList.tags || []).find((t) => t?.[0] === "30382:rank") : null;
  facts.rankService = rank?.[1] ? { service: rank[1], relay: normalizeRelay(rank[2]) } : null;
  return true;
}

/**
 * How many of your provider's score cards are here, and how many it serves.
 *
 * The same arithmetic observer_stats.html runs for every observer on the relay
 * — this is one reader's own row. `there` stays null when the 10040 named no
 * relay we can dial, and comes back as a non-answer sentinel when the relay
 * declined or went quiet; all three mean the same thing to the panel, which is
 * that there is no denominator and it must not invent one.
 */
async function scoreCounts(anon, { service, relay: url }) {
  const filter = { kinds: [30382], authors: [service] };
  const [here, there] = await Promise.all([
    anon.count(filter).catch(() => TIMED_OUT),
    url ? askRemote(url, (c) => c.count(filter)) : Promise.resolve(null),
  ]);
  return { here, there };
}

/**
 * Does a read on the authenticated socket actually come back?
 *
 * The end-to-end check, and the only one that sees what the three link checks
 * cannot: score cards can be HERE and not yet PROJECTED — the projection is
 * per service, derived by a reconcile that runs when the relay starts — so a
 * service new to this relay ranks nothing for a while after its cards land.
 *
 * Both sockets are asked the same thing on purpose. An empty answer means
 * something only against a control: on a fresh relay both come back empty, and
 * that is the store having nothing to say rather than a broken lens.
 */
async function probe(anon) {
  if (!relay.authed) return null;
  const filter = { kinds: [1], limit: 1 };
  const [authed, control] = await Promise.all([
    relay.req(filter).catch(() => null),
    anon.req(filter).catch(() => null),
  ]);
  // Either read failing to finish leaves this unanswered rather than zero.
  if (!authed || !control || authed.complete !== true || control.complete !== true) return null;
  return { authed: authed.length, anon: control.length };
}

/**
 * Your own events here, against the write relay that holds most of them.
 *
 * Falls back to the newest `created_at` on each side, because a relay that
 * does not serve NIP-45 can still answer "what is the latest thing they
 * wrote" — and "we have your posts up to 2 July" is a real answer where a
 * missing bar is not.
 */
async function postCounts(anon, me, writeRelays) {
  const filter = { authors: [me] };
  const url = writeRelays[0];
  // All three at once. Our count, our newest and the whole remote exchange are
  // three answers to one question, and none of them reads the others — run in
  // series they were three waits deep, the last of them on somebody else's
  // server.
  const [here, newestHere, answer] = await Promise.all([
    anon.count(filter).catch(() => TIMED_OUT),
    anon.req({ authors: [me], limit: 1 }).catch(() => []),
    url
      ? askRemote(url, async (c) => {
          const [there, newest] = await Promise.all([c.count(filter), c.req({ authors: [me], limit: 1 })]);
          return { there, newest: newest[0]?.created_at ?? null };
        })
      : Promise.resolve(null),
  ]);
  return {
    here,
    there: answer?.there ?? null,
    relay: url || null,
    newestHere: newestHere[0]?.created_at ?? null,
    newestThere: answer?.newest ?? null,
  };
}

/**
 * One question to somebody else's relay, on a socket opened and closed for it.
 *
 * Never pooled and never left open: these are other people's servers, this is
 * a background check running on page load, and a connection we keep is a
 * connection they are holding for us.
 */
async function askRemote(url, ask) {
  let conn = null;
  try {
    conn = new Relay(url);
    await conn.connect();
    return await ask(conn);
  } catch (e) {
    return null;
  } finally {
    try { conn && conn.ws && conn.ws.close(); } catch (e) {}
  }
}

// ---- the words ------------------------------------------------------------

const LINK_LABEL = {
  relayList: "Your relay list",
  scoreList: "Your trusted-scores list",
  scores: "Your provider's scores",
  ranked: "Ranked search",
  posts: "Your own posts",
};

const MARK = { ok: "✓", partial: "•", working: "•", broken: "✕", waiting: "", aside: "•" };

function render(v, me) {
  if (!$panel) return;
  if (!worthShowing(v)) { hide(); return; }
  if (dismissed(me, v.state)) { hide(); return; }
  const view = words(v);
  $panel.innerHTML = `
    <div class="rdy is-${esc(v.tone)}">
      <div class="rdy-top">
        <span class="rdy-dot"></span>
        <p class="rdy-headline">${view.headline}</p>
        <span class="rdy-state">${esc(view.state)}</span>
        <button class="rdy-x" type="button" aria-label="Dismiss">&times;</button>
      </div>
      ${view.meter || ""}
      ${view.body ? `<p class="rdy-body">${view.body}</p>` : ""}
      ${view.actions || ""}
      ${view.hint ? `<p class="rdy-hint">${view.hint}</p>` : ""}
      <button class="rdy-more" type="button" aria-expanded="false">What we checked ▾</button>
      <div class="rdy-chain" hidden>${chainHtml(v.chain)}</div>
    </div>`;
  $panel.hidden = false;
  wire(me, v.state);
}

/** The panel's copy, per state. One place, so the seven states read as a set. */
function words(v) {
  const pct = v.percent == null ? null : Math.round(v.percent * 100);
  const c = v.counts || {};
  switch (v.state) {
    case "no-relay-list":
      return {
        state: "blocked",
        headline: "Search can’t rank for you yet — we’ve never seen your relay list.",
        body:
          "This relay finds people through the relays they post to, and it has no record of yours, " +
          "so nothing about your account has been mirrored here. Name one relay you use and we’ll " +
          "read your profile, relay list and trusted-scores list from it.",
        actions: fetchFormHtml("Read my lists"),
        hint: "We copy the events here exactly as you signed them — nothing is rewritten, and nothing is published on your behalf.",
      };

    case "no-usable-relays":
      return {
        state: "blocked",
        headline: "Search can’t rank for you yet — none of your relays can be reached from here.",
        body:
          "Your relay list is here, and every relay in it is one a browser on an encrypted page " +
          "cannot open: a plain <code>ws://</code> address, or one that points at your own machine. " +
          "Name a relay we can reach and we’ll read your lists from it.",
        actions: fetchFormHtml("Read my lists"),
        hint: "We copy the events here exactly as you signed them — nothing is rewritten, and nothing is published on your behalf.",
      };

    case "no-score-list":
      return {
        state: "blocked",
        headline: "You have no trusted-scores list here.",
        body:
          "Ranking reads a <b>kind 10040</b> — the event that names which service’s scores you trust. " +
          "Without one there is nothing to rank by, which is why a signed-in search comes back empty. " +
          "<b>Brainstorm</b> will make you one: it scores the network and publishes a list naming itself, " +
          "and this relay already mirrors its scores.",
        actions:
          `<div class="rdy-actions">` +
          `<a class="rdy-btn" href="${PROVIDER_URL}" target="_blank" rel="noopener">Get a list from brainstorm.world <span class="rdy-ext" aria-hidden="true">↗</span></a>` +
          `<button class="rdy-btn quiet" type="button" data-act="signout">Search unranked instead</button>` +
          `</div>` +
          `<p class="rdy-hint">Already published one somewhere else? Name that relay and we’ll fetch it:</p>` +
          fetchFormHtml("Look for mine", { quiet: true }),
      };

    case "no-rank-service":
      return {
        state: "blocked",
        headline: "Your list names a follower count, but nothing that ranks.",
        body:
          "Ranking reads the <code>30382:rank</code> dimension of your trusted-scores list. Yours " +
          "declares only <code>30382:followers</code>, which can sort a list but cannot rank one — so " +
          "search has no order to apply. Republish your list naming a rank service.",
        actions:
          `<div class="rdy-actions">` +
          `<a class="rdy-btn" href="${PROVIDER_URL}" target="_blank" rel="noopener">Get one from brainstorm.world <span class="rdy-ext" aria-hidden="true">↗</span></a>` +
          `<button class="rdy-btn quiet" type="button" data-act="signout">Search unranked instead</button>` +
          `</div>`,
      };

    case "no-scores-yet":
      return {
        state: "blocked",
        headline: "None of your provider’s scores have reached this relay yet.",
        body:
          `Your list trusts <b>${esc(host(v.chain.find((l) => l.key === "scoreList")?.detail?.relay) || "a scoring service")}</b>, ` +
          "and this relay holds none of its cards — so there is nothing to rank your results by. The " +
          "router reads new lists on its next pass and starts importing then, usually within six hours. " +
          "Nothing here is waiting on you.",
        actions:
          `<div class="rdy-actions">` +
          `<button class="rdy-btn quiet" type="button" data-act="recheck">Check again</button>` +
          `<button class="rdy-btn quiet" type="button" data-act="signout">Search unranked instead</button>` +
          `</div>`,
      };

    case "projection-pending":
      return {
        state: "blocked",
        headline: "Your provider’s scores are here, but ranking hasn’t picked them up yet.",
        body:
          "This relay derives its ranking from a service’s scores once, when it starts, so a service " +
          "new to it ranks nothing until then. Signing out searches the whole index with no trust " +
          "applied, which works now.",
        actions:
          `<div class="rdy-actions">` +
          `<button class="rdy-btn quiet" type="button" data-act="recheck">Check again</button>` +
          `<button class="rdy-btn quiet" type="button" data-act="signout">Search unranked instead</button>` +
          `</div>`,
      };

    case "importing":
      return pct == null
        ? {
            state: "syncing",
            headline: `Importing your provider’s scores — ${fmt(counted(c.here) ? c.here : 0)} here so far`,
            meter:
              `<p class="rdy-figures"><span><b>${fmt(counted(c.here) ? c.here : 0)}</b> scores here</span>` +
              `<span>${esc(host(scoreRelay(v)) || "that relay")}: no count</span></p>`,
            body:
              "That relay doesn’t answer counts, so we can’t say how many are still to come. The number " +
              "above is what has arrived — it is ours, not theirs.",
          }
        : {
            state: "syncing",
            headline: `Importing your provider’s scores — ${pct}%`,
            meter:
              `<div class="rdy-meter"><div class="rdy-track"><div class="rdy-fill" style="width:${pct}%"></div></div>` +
              `<p class="rdy-figures"><span><b>${fmt(c.here)}</b> here</span>` +
              `<span>of <b>${fmt(c.there)}</b> on ${esc(host(scoreRelay(v)))}</span></p></div>`,
            body:
              "Search works now, but ranks fewer people than it will — anyone your provider has scored " +
              "whose card hasn’t arrived yet is missing from your results. Nothing to do; the router is " +
              "working through them.",
          };

    case "posts-behind": {
      const p = v.counts || {};
      if (pct != null) {
        return {
          state: "mirroring",
          headline: `Ranking is ready. Your own posts are still arriving — ${pct}%`,
          meter:
            `<div class="rdy-meter"><div class="rdy-track"><div class="rdy-fill" style="width:${pct}%"></div></div>` +
            `<p class="rdy-figures"><span><b>${fmt(p.here)}</b> here</span>` +
            `<span>of <b>${fmt(p.there)}</b> on ${esc(host(p.relay))}</span></p></div>`,
          body:
            "This doesn’t change how results are ranked — it only means searching for something you " +
            "wrote may not find it yet.",
        };
      }
      return {
        state: "mirroring",
        headline: `Ranking is ready. We have your posts up to ${day(p.newestHere)}.`,
        body:
          `${esc(host(p.relay))} doesn’t answer counts, so there’s no percentage to show — but it holds ` +
          `posts of yours newer than anything here, the most recent from <b>${day(p.newestThere)}</b>.`,
      };
    }

    default:
      return { state: "checking", headline: "Checking whether search is ready for you…" };
  }
}

const scoreRelay = (v) => v.chain.find((l) => l.key === "scoreList")?.detail?.relay || null;

const day = (secs) =>
  Number.isFinite(secs)
    ? new Date(secs * 1000).toLocaleDateString(undefined, { day: "numeric", month: "long" })
    : "an unknown date";

/** The chain, with the break visible: solid above it, dashed below. */
function chainHtml(chain) {
  return (chain || [])
    .map((l) => {
      const d = l.detail || {};
      let sub = "";
      // A link below the break was never asked, so it has nothing to report.
      // Every case below reads `detail`, and a waiting link carries none — so
      // each one fell through to its own HEALTHY branch: "Ranked search …
      // returns results" and "Your trusted-scores list … names a service for
      // rank", printed under a headline saying search cannot rank for you at
      // all. The ordering rule the whole chain exists for — first break wins,
      // everything below it waits — was right in the verdict column and
      // contradicted one column to its left.
      if (l.status !== "waiting") switch (l.key) {
        case "relayList":
          sub = l.status !== "broken" ? many(d.writeRelays, "write relay")
            : d.seen ? `${many(d.declared, "write relay")} named, none reachable from a browser`
              : "no relays of yours are known here";
          break;
        case "scoreList":
          if (d.reason === "absent") sub = "none published, or none mirrored here";
          else if (d.reason === "no-rank-dimension") sub = "names a followers service, no rank service";
          else sub = `names ${esc(host(d.relay) || "a service")} for rank`;
          break;
        case "scores":
          sub = counted(d.there)
            ? `${fmt(d.here)} of ${fmt(d.there)} mirrored`
            : counted(d.here) ? `${fmt(d.here)} mirrored, no total available` : "";
          break;
        case "ranked":
          sub = l.status === "broken"
            ? "the index answers, your lens returns nothing"
            : l.status === "partial" ? "ranked by what has arrived so far" : "returns results";
          break;
        case "posts":
          sub = counted(d.there)
            ? `${fmt(d.here)} here · ${esc(host(d.relay))} has ${fmt(d.there)} — doesn’t affect ranking`
            : "doesn’t affect ranking";
          break;
      }
      const verdict =
        l.status === "waiting" ? "waiting"
          : l.status === "broken" ? "missing"
            : l.key === "scores" && d.percent != null ? `${Math.round(d.percent * 100)}%`
              : l.key === "posts" && d.percent != null ? `${Math.round(d.percent * 100)}%`
                : l.status === "partial" ? "partial" : "here";
      return `
        <div class="rdy-link ${esc(l.status)}">
          <div class="rdy-rail"><span class="rdy-mark">${MARK[l.status] || ""}</span></div>
          <div class="rdy-what">${esc(LINK_LABEL[l.key] || l.key)}${sub ? `<small>${sub}</small>` : ""}</div>
          <div class="rdy-verdict">${esc(verdict)}</div>
        </div>`;
    })
    .join("");
}

/** The relay field, which is the whole of state A and half of state B. */
function fetchFormHtml(label, { quiet = false } = {}) {
  return `
    <div class="rdy-actions">
      <input class="rdy-field" type="url" spellcheck="false" autocapitalize="off" autocomplete="off"
             placeholder="wss://relay.example.com" aria-label="A relay you use" />
      <button class="rdy-btn${quiet ? " quiet" : ""}" type="button" data-act="fetch">${esc(label)}</button>
    </div>`;
}

// ---- pulling your lists off a relay you name ------------------------------

/**
 * Read [url] for this reader's kind 0, 10002 and 10040 and copy them here.
 *
 * This is the one path out of state A, and it does something no automatic
 * enrolment could: it supplies the EVENTS, not just a pubkey. A relay that has
 * never seen your kind 10002 has nowhere to go looking for the rest of you, so
 * "we noticed you signed in" cannot help — but three signed events, stored,
 * are what every stream downstream keys off.
 *
 * Published verbatim. Nothing is re-signed and nothing is edited: an event is
 * its signature, and the smallest change to one makes it a forgery this relay
 * would (correctly) refuse.
 */
async function fetchFrom(url, me, say) {
  const dial = normalizeRelay(url);
  // ESCAPED: every other headline on this panel is a literal with escaped
  // inserts, and this one is the exception — whyNotDialable() quotes the url
  // back so the reader can see what was wrong with it, and that string came
  // out of a text field. Somebody's own typing can only attack themselves, but
  // that is the argument observer_stats.html had already accepted once before
  // a stranger's display name went into innerHTML raw.
  if (!dial) { say({ tone: "blocked", state: "refused", headline: esc(whyNotDialable(url)), form: true, value: url }); return; }
  // The one relay this must never be pointed at is this one. Asking ourselves
  // what we are missing is a loop, and it would answer "nothing" every time.
  if (dial === normalizeRelay(RELAY_URL)) {
    say({ tone: "blocked", state: "refused",
          headline: "That is this relay — the one that is missing your lists.",
          body: "Name a relay you post to instead, so we have somewhere to read them from.",
          form: true, value: "" });
    return;
  }

  say({ tone: "working", state: "dialling", headline: `Connecting to ${esc(host(dial))}…`, spin: true });
  const found = new Map();               // event id -> event
  // Bounded, though all three kinds are replaceable and a well-behaved relay
  // holds one of each: this is a stranger's server answering a question about
  // us, and "it will only send three" is an assumption about somebody else's
  // implementation rather than something we know.
  const first = await askRemote(dial, (c) => c.req({ kinds: [0, 10002, 10040], authors: [me], limit: 10 }));
  if (first == null) {
    say({ tone: "blocked", state: "refused", headline: `${esc(host(dial))} didn’t answer.`,
          body: "It may be down, or it may not accept connections from a browser. Try another relay you use.",
          form: true, value: url });
    return;
  }
  for (const ev of first) found.set(ev.id, ev);

  // One hop, and only one: if their relay list names write relays, the 10040
  // usually lives on those rather than where the list itself was found. This
  // is exactly the walk the router does — outbox lists are how it discovers
  // where to read — done once, by hand, for one person.
  const list = [...found.values()].filter((e) => e.kind === 10002).sort((a, b) => b.created_at - a.created_at)[0];
  const writes = list
    ? [...new Set((list.tags || [])
        .filter((t) => t?.[0] === "r" && t[1] && (t[2] === "write" || t[2] == null))
        .map((t) => normalizeRelay(t[1]))
        .filter((u) => u && u !== dial))]
    : [];
  if (writes.length) {
    say({ tone: "working", state: "reading", spin: true,
          headline: `Checking the ${many(writes.length, "relay")} your list names…`,
          foundList: foundHtml(found, dial, true) });
    const hops = await Promise.all(
      writes.slice(0, 4).map((u) => askRemote(u, (c) => c.req({ kinds: [0, 10040], authors: [me] }))),
    );
    for (const evs of hops) for (const ev of evs || []) if (!found.has(ev.id)) found.set(ev.id, ev);
  }

  if (!found.size) {
    say({ tone: "blocked", state: "empty",
          headline: `Nothing of yours on ${esc(host(dial))}.`,
          body: "It answered, and it holds no profile, relay list or trusted-scores list for your key. " +
                "If that is the relay you post to, the lists have not been published yet.",
          form: true, value: "" });
    return;
  }

  // Newest per kind only. A relay may hand back several of a replaceable kind,
  // and copying the superseded ones here would store events this relay would
  // only have to replace again.
  const newest = new Map();
  for (const ev of found.values()) {
    const prev = newest.get(ev.kind);
    if (!prev || prev.created_at < ev.created_at) newest.set(ev.kind, ev);
  }
  say({ tone: "working", state: "copying", spin: true, headline: "Copying them to this relay…" });
  // Together, on one socket. Three events published in series is three full
  // round trips for three independent OKs, and NIP-01 has never required a
  // client to wait for one before sending the next — the client already keys
  // its OK waiters by event id.
  const results = new Map(
    await Promise.all([...newest].map(([kind, ev]) =>
      relay.publish(ev).then(() => [kind, null], (e) => [kind, e.message || "refused"]))),
  );

  const failed = [...results.values()].filter(Boolean);
  const copied = results.size - failed.length;
  say({
    tone: failed.length && !copied ? "blocked" : "ok",
    state: failed.length && !copied ? "refused" : "forwarded",
    headline: copied
      ? `Copied ${many(copied, "event")} to this relay.`
      : "This relay refused them.",
    foundList: publishedHtml(newest, results),
    body: copied
      ? "The router reads new lists on its next pass and starts importing your provider’s scores then, " +
        "usually within six hours. You can close the page; nothing here is waiting on you."
      : "The relay gave a reason for each above. Nothing was stored.",
  });
  if (copied) rememberRelay(dial);
}

const KIND_NAME = { 0: "Profile", 10002: "Relay list", 10040: "Trusted-scores list" };

function foundHtml(found, dial, pending) {
  const seen = new Set([...found.values()].map((e) => e.kind));
  return `<ul class="rdy-found">${[0, 10002, 10040].map((k) => {
    if (seen.has(k)) {
      return `<li><span class="tick">✓</span><b>${esc(KIND_NAME[k])}</b> <span class="kind">kind ${k} · ${esc(host(dial))}</span></li>`;
    }
    return pending
      ? `<li><span class="dash">·</span>${esc(KIND_NAME[k])} — still looking <span class="kind">kind ${k}</span></li>`
      : `<li><span class="cross">✕</span>${esc(KIND_NAME[k])} — not there <span class="kind">kind ${k}</span></li>`;
  }).join("")}</ul>`;
}

function publishedHtml(newest, results) {
  return `<ul class="rdy-found">${[...newest.keys()].map((k) => {
    const err = results.get(k);
    return err
      ? `<li><span class="cross">✕</span><b>${esc(KIND_NAME[k] || `kind ${k}`)}</b> <span class="kind">${esc(err)}</span></li>`
      : `<li><span class="tick">✓</span><b>${esc(KIND_NAME[k] || `kind ${k}`)}</b> <span class="kind">stored here</span></li>`;
  }).join("")}</ul>`;
}

/** The panel during the fetch — the same card, different contents. */
function sayFetch(me, s) {
  if (!$panel) return;
  $panel.innerHTML = `
    <div class="rdy is-${esc(s.tone)}">
      <div class="rdy-top">
        ${s.spin ? '<span class="rdy-spinner"></span>' : '<span class="rdy-dot"></span>'}
        <p class="rdy-headline">${s.headline}</p>
        <span class="rdy-state">${esc(s.state)}</span>
        <button class="rdy-x" type="button" aria-label="Dismiss">&times;</button>
      </div>
      ${s.foundList || ""}
      ${s.body ? `<p class="rdy-body">${s.body}</p>` : ""}
      ${s.form ? fetchFormHtml("Try again") : ""}
    </div>`;
  $panel.hidden = false;
  if (s.form) {
    const field = $panel.querySelector(".rdy-field");
    if (field) field.value = s.value || "";
  }
  wire(me);
}

// ---- wiring ---------------------------------------------------------------

function wire(me, state) {
  const el = $panel.querySelector(".rdy");
  if (!el) return;
  // A fetch panel has no verdict to dismiss — the x just puts it away.
  el.querySelector(".rdy-x")?.addEventListener("click", () => { if (state) dismiss(me, state); hide(); });

  const more = el.querySelector(".rdy-more");
  const chain = el.querySelector(".rdy-chain");
  more?.addEventListener("click", () => {
    const open = chain.hidden;
    chain.hidden = !open;
    more.setAttribute("aria-expanded", String(open));
    more.textContent = open ? "What we checked ▴" : "What we checked ▾";
  });

  const field = el.querySelector(".rdy-field");
  const go = () => fetchFrom(field.value, me, (s) => sayFetch(me, s)).catch(() => {});
  el.querySelectorAll("[data-act]").forEach((b) => {
    b.addEventListener("click", () => {
      const act = b.dataset.act;
      if (act === "fetch") go();
      else if (act === "recheck") checkReadiness(me, { force: true });
      else if (act === "signout") document.getElementById("me")?.click();
    });
  });
  // Enter in the field is the same press as the button beside it — a url is a
  // one-field form, and reaching for the mouse to submit one is a papercut.
  field?.addEventListener("keydown", (e) => { if (e.key === "Enter") { e.preventDefault(); go(); } });
  if (field) field.value = field.value || lastRelay();
}

function hide() {
  if (!$panel) return;
  $panel.hidden = true;
  $panel.innerHTML = "";
}

// ---- what the page remembers ----------------------------------------------
//
// The verdict and the dismissal are per pubkey, because both are facts about
// one account, and a shared key would carry one reader's dismissal to the next
// person to sign in on the same browser. They keep different homes for
// different reasons: the verdict is a cookie so the browser enforces its week,
// the dismissal is localStorage because it is compared against a STATE as well
// as a clock and a cookie holds one string.

const readJson = (key) => { try { return JSON.parse(localStorage.getItem(key) || "null"); } catch (e) { return null; } };
const writeJson = (key, v) => { try { localStorage.setItem(key, JSON.stringify(v)); } catch (e) {} };

/**
 * The one that is a cookie, so the browser expires it for us.
 *
 * Still per pubkey: the value is the account the verdict was about, and a
 * different reader signing in on the same browser matches nothing and pays for
 * their own check. Signing out does NOT clear it — the verdict stays true about
 * the person it names, and they should not re-pay for it on their way back in.
 */
function rememberedReady(pk) {
  // Built from the constant rather than written out, so renaming the cookie
  // cannot leave a reader silently re-checking every load against a name that
  // is never set any more.
  const m = document.cookie.match(new RegExp(`(?:^|;\\s*)${READY_COOKIE}=([^;]*)`));
  return !!m && decodeURIComponent(m[1]) === pk;
}
function rememberReady(pk) {
  // `secure` only on https: setting it on an http page — a local relay, or the
  // dev server — makes the browser drop the cookie silently, so the check would
  // run again on every load with nothing to show for it.
  const secure = location.protocol === "https:" ? "; Secure" : "";
  document.cookie =
    `${READY_COOKIE}=${encodeURIComponent(pk)}; path=/; max-age=${READY_TTL_DAYS * 24 * 60 * 60}; SameSite=Lax${secure}`;
}

// Dismissal is a decision about a state, not about the feature: it is cleared
// the moment the state changes, so a reader who dismissed "importing — 43%"
// still hears about it when their scores stop arriving altogether.
const DISMISS_KEY = "sot_ready_dismissed";
function dismiss(pk, state) { writeJson(DISMISS_KEY, { pubkey: pk, state, at: Date.now() }); }
function dismissed(pk, state) {
  const v = readJson(DISMISS_KEY);
  // The STATE is part of the key, which is what makes the comment above true.
  // It was not, and the comment was the only thing saying it: one click on
  // "importing — 43%" silenced the panel for six hours, including for the
  // reader whose import then stopped dead or whose provider list went away —
  // the two things they would most want to hear about, hidden by a dismissal
  // that meant "yes, I know it is downloading".
  return !!v && v.pubkey === pk && v.state === state && Date.now() - v.at < DISMISS_TTL_MS;
}

// The relay a previous fetch worked from, offered back as the field's default.
// Somebody who had to name their relay once should not have to remember it a
// second time on the next device or the next state.
const RELAY_KEY = "sot_last_relay";
const rememberRelay = (url) => { try { localStorage.setItem(RELAY_KEY, url); } catch (e) {} };
const lastRelay = () => { try { return localStorage.getItem(RELAY_KEY) || ""; } catch (e) { return ""; } };
