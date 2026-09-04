// The line under the search box that says whether this relay can rank for
// this reader yet and, when it cannot, the one thing that fixes it. The
// verdict is shared/readiness.js's; this module makes the asks that feed it
// and writes the words. The check is lazy: once per pubkey per page, after a
// delay, stopping at the first broken link, with a `ready` verdict kept in a
// week-long cookie and the posts count sent only when this relay says which
// kinds it mirrors (shared/mirrors.js).

import { relay, refConn, RELAY_URL } from "./shared/conn.js";
import { Relay } from "./shared/relay.js";
import { esc } from "./shared/format.js";
import { readMirrorScope, scopedTo } from "./shared/mirrors.js";
import { assess, counted, worthShowing, TIMED_OUT } from "./shared/readiness.js";
import { normalizeRelay, whyNotDialable } from "./shared/relayurl.js";
import { seedProviders } from "./shared/providers.js";

const $panel = document.getElementById("readiness");

/** Where a NIP-85 provider will make the reader a list. */
const PROVIDER_URL = "https://brainstorm.world";

/** The first ask waits for the search, the faces and the score chips, which share its sockets. */
const START_DELAY_MS = 1500;

/**
 * How long a `ready` verdict is trusted, in a cookie so the browser enforces
 * the expiry. A complete chain has nothing left to learn; only a verdict
 * short of ready is worth re-asking on the router's next pass.
 */
const READY_TTL_DAYS = 7;
const READY_COOKIE = "sot_ready";

/** How long a dismissal is trusted: the router's own refresh interval. */
const DISMISS_TTL_MS = 6 * 60 * 60 * 1000;

const fmt = (n) => Number(n).toLocaleString();
const host = (url) => String(url || "").replace(/^wss?:\/\//, "");
const many = (n, word) => `${fmt(n)} ${word}${n === 1 ? "" : "s"}`;

let checkedFor = null;   // the pubkey this page has already checked

/**
 * Which check is current. Bumped by every start and clear and carried by the
 * run, so a run whose reason has gone (sign-out, an account switch in the
 * extension) paints nothing.
 */
let generation = 0;

/**
 * Check [pubkey] once, in the background. Safe to call from every path that
 * can change who is signed in; a repeat for the same pubkey costs nothing.
 */
export function checkReadiness(pubkey, { force = false } = {}) {
  if (!pubkey) { clearReadiness(); return; }
  if (!force && checkedFor === pubkey) return;
  if (!force && rememberedReady(pubkey)) { checkedFor = pubkey; return; }
  checkedFor = pubkey;
  const gen = ++generation;
  setTimeout(() => { run(pubkey, gen).catch(() => {}); }, force ? 0 : START_DELAY_MS);
}

/** Take the panel down and forget the verdict. */
export function clearReadiness() {
  checkedFor = null;
  generation++;
  hide();
}

/**
 * One pass. [gen] is the only concurrency control: a newer pass runs, and
 * the older one finishes into a paint that is dropped.
 */
async function run(me, gen) {
  if (gen !== generation) return;
  const facts = {};
  // Painted after every stage so the chain fills in, but nothing is shown
  // until the verdict is worth showing, and nothing once superseded.
  const paint = () => { if (gen === generation) render(assess(facts), me); };

  const anon = await refConn();
  if (!(await readLists(anon, me, facts))) return;
  paint();
  if (!facts.relayList.writeRelays.length || !facts.rankService) return;

  // Our count and the provider relay's answer the same question about two
  // stores; run in series, the local answer waited on a stranger's relay.
  [facts.scores, facts.probe] = await Promise.all([
    scoreCounts(anon, facts.rankService),
    probe(anon),
  ]);
  paint();

  // Only once ranking is complete: a reader still importing scores does not
  // need a second, quieter number about a different thing.
  if (assess(facts).state !== "ready") return;
  // Without the mirror's kind list the two post counts are not the same
  // question (shared/mirrors.js), so no scope means no posts stage at all.
  const scope = await readMirrorScope();
  // Sign-out across the await above must stop the asks, not only the paint.
  if (gen !== generation) return;
  if (scope) facts.posts = await postCounts(anon, me, facts.relayList.writeRelays, scope);
  if (gen !== generation) return;
  const after = assess(facts);
  render(after, me);
  if (after.state === "ready") rememberReady(me);
}

// ---- the asks -------------------------------------------------------------

/**
 * The reader's kind 0, 10002 and 10040, one REQ read anonymously: the
 * authenticated socket is trust-gated, and this check exists for readers who
 * have scored nobody. False unless the relay reached EOSE; a timed-out read
 * must not become "you have no relay list".
 */
async function readLists(anon, me, facts) {
  let evs;
  try { evs = await anon.req({ kinds: [0, 10002, 10040], authors: [me], limit: 3 }); }
  catch (e) { return false; }
  if (evs.complete !== true) return false;

  const newest = (kind) =>
    evs.filter((e) => e.kind === kind).sort((a, b) => b.created_at - a.created_at)[0] || null;

  const relayList = newest(10002);
  // NIP-65: an `r` tag with no marker is read and write. Only write relays
  // matter here, being where the router would look for the rest of the reader.
  const declared = relayList
    ? (relayList.tags || []).filter((t) => t?.[0] === "r" && t[1] && (t[2] === "write" || t[2] == null))
    : [];
  const writeRelays = [...new Set(declared.map((t) => normalizeRelay(t[1])).filter(Boolean))];
  // `seen` is not `writeRelays.length`: a list naming only ws:// or loopback
  // relays is a list the reader published and a browser cannot use.
  facts.relayList = { seen: !!relayList, declared: declared.length, writeRelays };

  const scoreList = newest(10040);
  // The same event shared/providers.js would fetch again for the chips and
  // the provenance row; this read is complete-gated, which seeding requires.
  seedProviders(me, scoreList, true);
  facts.scoreListSeen = !!scoreList;
  const rank = scoreList ? (scoreList.tags || []).find((t) => t?.[0] === "30382:rank") : null;
  facts.rankService = rank?.[1] ? { service: rank[1], relay: normalizeRelay(rank[2]) } : null;
  return true;
}

/**
 * How many of the provider's score cards are here, and how many it serves.
 * `there` is null or a sentinel when there is no denominator to be had, and
 * the panel must not invent one.
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
 * Does a read on the authenticated socket come back? The one check that sees
 * cards that are here but not yet projected. The anonymous read is the
 * control: on a fresh relay both are empty, and that is not a broken lens.
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
 * The reader's own events here, against the write relay holding most of
 * them. One filter, scoped to the kinds this relay mirrors, sent to both
 * sides; `scopedTo` refuses to build one without a scope. The newest-event
 * reads carry the same bound and stand in where a relay answers no COUNT.
 */
async function postCounts(anon, me, writeRelays, scope) {
  const filter = scopedTo({ authors: [me] }, scope);
  const newest = { ...filter, limit: 1 };
  const url = writeRelays[0];
  // Three answers to one question, none of which reads the others.
  const [here, newestHere, answer] = await Promise.all([
    anon.count(filter).catch(() => TIMED_OUT),
    anon.req(newest).catch(() => []),
    url
      ? askRemote(url, async (c) => {
          const [there, latest] = await Promise.all([c.count(filter), c.req(newest)]);
          return { there, newest: latest[0]?.created_at ?? null };
        })
      : Promise.resolve(null),
  ]);
  return {
    here,
    there: answer?.there ?? null,
    relay: url || null,
    // What both sides were asked for, so the words can say so. Null where
    // the mirror asks for everything.
    kinds: filter.kinds || null,
    newestHere: newestHere[0]?.created_at ?? null,
    newestThere: answer?.newest ?? null,
  };
}

/**
 * One question to somebody else's relay, on a socket opened and closed for
 * it: a connection we keep is one they are holding for us.
 */
async function askRemote(url, ask) {
  let conn = null;
  try {
    // Not `lensless`: `include:spam` is this relay's extension, and a foreign
    // NIP-50 relay would search for the token as a word.
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
          // "About": the denominator is the other relay's NIP-45 COUNT, which
          // need not agree with itself across filters.
          headline: `Ranking is ready. Your own posts are still arriving — about ${pct}%`,
          meter:
            `<div class="rdy-meter"><div class="rdy-track"><div class="rdy-fill" style="width:${pct}%"></div></div>` +
            `<p class="rdy-figures"><span><b>${fmt(p.here)}</b> here</span>` +
            `<span>of <b>${fmt(p.there)}</b> on ${esc(host(p.relay))}</span></p></div>`,
          body:
            "This doesn’t change how results are ranked — it only means searching for something you " +
            "wrote may not find it yet.",
          hint: scopeNote(p),
        };
      }
      return {
        state: "mirroring",
        headline: `Ranking is ready. We have your posts up to ${day(p.newestHere)}.`,
        body:
          `${esc(host(p.relay))} doesn’t answer counts, so there’s no percentage to show — but it holds ` +
          `posts of yours newer than anything here, the most recent from <b>${day(p.newestThere)}</b>.`,
        hint: scopeNote(p),
      };
    }

    default:
      return { state: "checking", headline: "Checking whether search is ready for you…" };
  }
}

const scoreRelay = (v) => v.chain.find((l) => l.key === "scoreList")?.detail?.relay || null;

/**
 * What both sides of the posts figure were asked for. Said because the
 * number is smaller than the one the reader's own client shows. Empty where
 * the mirror asks for every kind, since there is no narrowing to explain.
 */
const scopeNote = (p) =>
  p.kinds
    ? `Both sides of that comparison cover the ${fmt(p.kinds.length)} kinds this relay mirrors, so it weighs ` +
      `like against like — anything you post outside them is on neither side of it.`
    : "";

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
      // A link below the break was never asked and has nothing to report;
      // every case below reads `detail`, and a waiting link carries none.
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

/** The relay field, for the states where naming a relay is the way out. */
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
 * Read [url] for the reader's kind 0, 10002 and 10040 and copy them here,
 * verbatim: an event is its signature, and the smallest edit is a forgery
 * this relay would refuse. The one path out of "no relay list".
 */
async function fetchFrom(url, me, say) {
  const dial = normalizeRelay(url);
  // Escaped: whyNotDialable() quotes the field's own text back.
  if (!dial) { say({ tone: "blocked", state: "refused", headline: esc(whyNotDialable(url)), form: true, value: url }); return; }
  // Asking this relay what it is missing would answer "nothing" every time.
  if (dial === normalizeRelay(RELAY_URL)) {
    say({ tone: "blocked", state: "refused",
          headline: "That is this relay — the one that is missing your lists.",
          body: "Name a relay you post to instead, so we have somewhere to read them from.",
          form: true, value: "" });
    return;
  }

  say({ tone: "working", state: "dialling", headline: `Connecting to ${esc(host(dial))}…`, spin: true });
  const found = new Map();               // event id -> event
  // Bounded: a stranger's relay may send more than one of each replaceable kind.
  const first = await askRemote(dial, (c) => c.req({ kinds: [0, 10002, 10040], authors: [me], limit: 10 }));
  if (first == null) {
    say({ tone: "blocked", state: "refused", headline: `${esc(host(dial))} didn’t answer.`,
          body: "It may be down, or it may not accept connections from a browser. Try another relay you use.",
          form: true, value: url });
    return;
  }
  for (const ev of first) found.set(ev.id, ev);

  // One hop only: the 10040 usually lives on the write relays the list
  // names, which is the walk the router does, done once by hand.
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

  // Newest per kind only; a superseded replaceable copied here would only be
  // replaced again.
  const newest = new Map();
  for (const ev of found.values()) {
    const prev = newest.get(ev.kind);
    if (!prev || prev.created_at < ev.created_at) newest.set(ev.kind, ev);
  }
  say({ tone: "working", state: "copying", spin: true, headline: "Copying them to this relay…" });
  // Together, on one socket: NIP-01 never required a client to wait for one
  // OK before sending the next event.
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

/** The panel during the fetch: the same card, different contents. */
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
  // A fetch panel has no verdict to dismiss; the x only puts it away.
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
  // Enter in the field is the button beside it.
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
// Both are per pubkey, so one reader's dismissal never carries to the next
// to sign in on the same browser. The verdict is a cookie so the browser
// enforces its week; the dismissal is localStorage because it is keyed on a
// state as well as a clock.

const readJson = (key) => { try { return JSON.parse(localStorage.getItem(key) || "null"); } catch (e) { return null; } };
const writeJson = (key, v) => { try { localStorage.setItem(key, JSON.stringify(v)); } catch (e) {} };

/**
 * The cookie half. Signing out does not clear it: the verdict stays true
 * about the person it names.
 */
function rememberedReady(pk) {
  const m = document.cookie.match(new RegExp(`(?:^|;\\s*)${READY_COOKIE}=([^;]*)`));
  return !!m && decodeURIComponent(m[1]) === pk;
}
function rememberReady(pk) {
  // `Secure` on an http page makes the browser drop the cookie silently.
  const secure = location.protocol === "https:" ? "; Secure" : "";
  document.cookie =
    `${READY_COOKIE}=${encodeURIComponent(pk)}; path=/; max-age=${READY_TTL_DAYS * 24 * 60 * 60}; SameSite=Lax${secure}`;
}

// A dismissal is about a state, not about the feature: the moment the state
// changes the panel speaks again.
const DISMISS_KEY = "sot_ready_dismissed";
function dismiss(pk, state) { writeJson(DISMISS_KEY, { pubkey: pk, state, at: Date.now() }); }
function dismissed(pk, state) {
  const v = readJson(DISMISS_KEY);
  return !!v && v.pubkey === pk && v.state === state && Date.now() - v.at < DISMISS_TTL_MS;
}

// The relay a previous fetch worked from, offered back as the field's default.
const RELAY_KEY = "sot_last_relay";
const rememberRelay = (url) => { try { localStorage.setItem(RELAY_KEY, url); } catch (e) {} };
const lastRelay = () => { try { return localStorage.getItem(RELAY_KEY) || ""; } catch (e) { return ""; } };
