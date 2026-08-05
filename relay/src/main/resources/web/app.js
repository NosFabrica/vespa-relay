// The SearchOverTrust page itself: sign-in, the search views (hero, type-ahead
// popup, full results), the "ranking as" lens, and the URL/backstack wiring.
// Everything stateful lives here; the shared/ modules underneath are the
// stateless client, codec and caches, and cards.js is the rendering.

import { RELAY_URL, relay, refConn } from "./shared/conn.js";
import { npub, noteId, shortNpub, pubkeyParam } from "./shared/nip19.js";
import { esc } from "./shared/format.js";
import { profiles, displayName, seedProfiles, enrichProfiles } from "./shared/profiles.js";
import { watchNip05 } from "./shared/nip05.js";
import { parseQuery } from "./shared/query.js";
import { card, popupRow, namedPubkeys } from "./cards.js";
import { showEntity, cancelEntity } from "./entity.js";
import { mountSearchField } from "./searchfield.js";

const POPUP_LIMIT = 8;
const FULL_LIMIT = 40;
const DEBOUNCE_MS = 150;

// ---- the filter chips are literal NIP-01 `kinds` filters ----------------
// `slug` is the tab's name in the URL (`?tab=notes`). A slug, not the label
// ("Code & git" percent-encodes into line noise) and not the kinds list
// (which this page is free to tune without breaking every bookmarked URL).
//
// A tab's kinds must be the ones the matching FAMILY renders, and were not:
// Media asked for 31922 — a NIP-52 date-based calendar event, which renders
// under Live — while leaving out 1986 audio, so the audio tab kind was
// unreachable from any chip and every "Media" result set could contain a
// conference date. Kept in sync with shared/kinds.js by hand; the tone table
// there is the reference for which family a kind belongs to.
const KIND_TABS = [
  { label: "Everything", slug: "all", kinds: null },
  { label: "People", slug: "people", kinds: [0] },
  { label: "Notes", slug: "notes", kinds: [1, 11, 1111] },
  { label: "Articles", slug: "articles", kinds: [30023, 30024, 30818, 30040, 30041] },
  { label: "Media", slug: "media", kinds: [20, 21, 22, 1063, 1986, 1222, 34235, 34236] },
  { label: "Code & git", slug: "code", kinds: [1337, 1617, 1618, 1621, 30617] },
  { label: "Live", slug: "live", kinds: [30311, 30312, 30313, 31922, 31923, 31924] },
  { label: "Lists", slug: "lists", kinds: [10003, 10015, 30001, 30003, 30015, 30267, 39701] },
];

// ---- signed-in preference, shared across the relay's pages ----------------
// A cookie rather than localStorage: same-origin, survives a page switch, and
// the pages are served by the relay itself so there is nothing cross-site about
// it. Default is SIGNED IN — an unset cookie means "not chosen yet", and the
// useful default is the one that makes ranking work.
const AUTH_COOKIE = "sot_signedin";
function wantsSignIn() {
  const m = document.cookie.match(/(?:^|;\s*)sot_signedin=([01])/);
  return m ? m[1] === "1" : true;
}
function rememberSignIn(yes) {
  document.cookie = `${AUTH_COOKIE}=${yes ? 1 : 0}; path=/; max-age=31536000; SameSite=Lax`;
}

// ---- NIP-07 login -> NIP-42 auth -----------------------------------------
// Signing the challenge switches the CONNECTION's ranking observer to you
// (and enrolls you: the relay starts syncing your trust chain).
let me = null; // authenticated hex pubkey

// A dropped socket loses the AUTH, not the identity. Keeping `me` means the
// page still knows who you are and re-authenticates on the next request,
// instead of silently dropping you to UNRANKED results with nothing on screen
// to say the results changed meaning. That downgrade got sharper when the
// relay stopped applying a default observer: signed out is now the whole
// corpus with no trust applied, not someone else's ranking of it.
relay.onclose = () => { renderWhoami(); };

// The restore half of the back/forward cache dance (the pagehide half lives
// with the sockets in shared/conn.js). Coming back from the cache runs no
// load code, so `me` was still set and the field still showed a face, while
// the socket was closed and nothing was authenticated: a page that looked
// signed in and ranked nothing.
window.addEventListener("pageshow", (ev) => {
  // Only a restore FROM the cache needs this; a normal load already connects
  // on its own path and re-running it would open a second socket.
  if (!ev.persisted) return;
  // Re-authenticate rather than merely reconnect: NIP-42 belongs to the
  // connection, so the identity survived the freeze but the proof did not.
  // The settled flight is stale for the same reason — cleared, so the next
  // ensureLogin() runs a real sign-in instead of awaiting old success.
  loginTried = false;
  loginFlight = null;
  scores.clear();
  scoreLensKey = null;
  renderWhoami();
  ensureLogin().then(paintScores).catch(() => {});
});

async function waitForChallenge(ms) {
  const t0 = Date.now();
  while (!relay.challenge && Date.now() - t0 < ms) await new Promise(r => setTimeout(r, 100));
  return relay.challenge;
}

// Sign the CURRENT challenge and send it, retrying if the connection changed
// underneath us.
//
// NIP-07 signing waits on a human clicking approve in an extension popup,
// which is seconds — and a challenge belongs to one connection. If the socket
// is replaced in that window (an idle drop, or the relay restarting), the
// signature we are about to send is for a challenge the server has never
// heard of, and it answers "invalid: challenge does not match". Re-reading
// the challenge after signing and retrying against the new one is the whole
// fix; without it the only recovery is for the user to click again.
async function signAndAuth(attempt = 0, waitMs = 3000) {
  const challenge = await waitForChallenge(waitMs);
  if (!challenge) throw new Error("The relay sent no NIP-42 challenge");
  const signed = await window.nostr.signEvent({
    kind: 22242,
    created_at: Math.floor(Date.now() / 1000),
    content: "",
    tags: [["relay", RELAY_URL], ["challenge", challenge]],
  });
  if (relay.challenge !== challenge) {
    // Two tries only: a socket flapping faster than a person can click is a
    // broken relay, and looping would just spam the signer.
    if (attempt >= 1) throw new Error("The connection kept resetting while signing — try again");
    await relay.connect();
    return signAndAuth(attempt + 1);
  }
  await relay.auth(signed);
  return signed.pubkey;
}

async function login() {
  if (!(window.nostr && window.nostr.signEvent)) throw new Error("No Nostr extension found (window.nostr / NIP-07)");
  await relay.connect();
  // A CLICK gets a longer wait than a background attempt. The socket may have
  // only just opened — the page does not connect at all until something needs
  // it — and the AUTH challenge is a message that arrives after the handshake,
  // so a 3s budget was being spent on connecting rather than waiting, and the
  // first press simply failed. That is why it took a few presses.
  me = await signAndAuth(0, 10000);
  renderWhoami();
  // Our own face, fetched fresh and AWAITED. enrichProfiles skips a pubkey it
  // has already seen — including one recorded as `null` by a lookup that ran
  // before this account was on screen — so a cached miss survived signing in
  // and the avatar kept the old face until a reload. Deleting first forces
  // the re-read; awaiting means the caller can rely on it being done.
  profiles.delete(me);
  try { await enrichProfiles([me]); } catch (e) {}
  renderWhoami();
  // The fetch above races page load: the reference socket is opening at the
  // same time as the main one, and when it loses, `me` is signed in with no
  // profile and NOTHING retries — the avatar sat on a placeholder until the
  // user clicked twice, which signed them out and back in again. Retry a
  // couple of times, quietly, and stop as soon as a face arrives.
  for (let i = 0; i < 3 && me && !profiles.get(me); i++) {
    await new Promise((r) => setTimeout(r, 600 * (i + 1)));
    if (!me) break;
    profiles.delete(me);
    try { await enrichProfiles([me]); } catch (e) {}
    renderWhoami();
  }
}

// Signed in automatically, once, best effort.
//
// There used to be an "as me" toggle next to this that meant "authenticate",
// and a separate picker that meant "rank as somebody". Off + somebody was a
// legal combination that did NOTHING: the lens needs an authenticated reader,
// so the picker silently had no effect. One thing you choose (whose trust) and
// one thing that just happens (being signed in) is the same feature without
// the inert corner.
//
// A relay read is still fine without an extension — it is the whole corpus,
// unranked — so a failure here downgrades rather than blocks, and says so.
//
// ONE sign-in, shared as an in-flight promise that every caller awaits. The
// previous shape used a bare `loginTried` flag, and it raced: while the first
// caller sat inside login() waiting on the extension popup, the type-ahead
// fired more searches, each saw the flag already set with `me` still null,
// returned early, and sent its REQ UNAUTHENTICATED. Those results render from
// the whole corpus, unranked — and nothing ever corrects them, because a
// relay does not re-run a subscription it answered under the old auth state;
// AUTH only changes what later REQs see. Waiting on the shared flight means
// no REQ is ever sent on this socket before its auth question is settled —
// stronger than resending after the fact, because nothing wrong renders.
let loginTried = false;   // the first attempt has SETTLED (labels key on this)
let loginFlight = null;   // the first attempt itself, awaited by every search
async function ensureLogin() {
  await relay.connect();
  if (!loginFlight) {
    loginFlight = (async () => {
      // A remembered "signed out" is a decision, not an absence — respect it
      // rather than prompting the extension on every page load.
      if (!wantsSignIn() || !(window.nostr && window.nostr.signEvent)) {
        $obsBox.classList.add("anon");
        renderWhoami();
        return;
      }
      try { await login(); } catch (e) {
        me = null;
        $obsBox.classList.add("anon");
        $whoami.innerHTML = `<span class="err">${esc(e.message || String(e))} — showing the whole corpus, unranked</span>`;
      }
    })().finally(() => { loginTried = true; });
  }
  await loginFlight;
  if (!me) return;
  // Keyed on the CONNECTION's auth state, not on whether we remember a
  // pubkey: a reconnect leaves `me` set but the new socket unauthenticated,
  // and searching then really would rank by nobody — there is no default
  // observer behind it any more.
  if (relay.authed) return;
  me = await signAndAuth();
  renderWhoami();
}

// The resend half of NIP-42, wired to the client: if the store ever answers
// a REQ with CLOSED "auth-required:" — a reconnect gap, a stricter policy —
// authenticate through the same shared flow and the client resends that REQ
// itself. The anonymous reference connection gets no such hook: it must
// never authenticate, so for it auth-required is a real answer.
relay.onAuthRequired = () => ensureLogin();

// ---- search over NIP-50 ---------------------------------------------------
let tab = KIND_TABS[0];

function searchString(text) {
  const sort = $sort.value;
  let s = text;
  if (sort) s += " sort:" + sort;
  if ($spam.checked) s += " include:spam";
  // Whose web of trust ranks this. Absent, the store uses the connection's
  // authenticated pubkey — you. Present, it uses theirs, which is how you
  // read the index through somebody else's eyes without holding their key.
  //
  // Only sent while signed in. search() awaits ensureLogin() first, so this
  // holds in practice; asserting it here means a future caller that skips
  // that step degrades to your own ranking rather than sending a lens the
  // store has no authenticated reader to apply it for.
  if (viewingAs && me) s += " observer:" + viewingAs;
  return s;
}

/**
 * The typed string as the REQ this page sends.
 *
 * `from:npub…` and `to:npub…` leave the NIP-50 search entirely and become the
 * NIP-01 filter fields they are — `authors` and `#p`. Those are indexed on
 * every relay and compose with the ranking rather than competing with it,
 * whereas leaving them in `search` would have the full-text index hunting for
 * the literal string "from:npub1…" and a narrowed search would look like an
 * empty one.
 *
 * With a person filter and no words left, `search` is omitted rather than sent
 * empty: a filter carrying only the sort/spam/observer extensions is a text
 * query for nothing, and what the reader asked for — everything this person
 * wrote — is an ordinary NIP-01 read. The trade is real and worth saying out
 * loud: no `search` means no NIP-50, so the trust ranking and the extensions
 * do not apply to that one shape. Add a word and they are back.
 *
 * Shared with exportText() so the filter a reader is shown is the filter that
 * was sent, byte for byte, rather than a second construction of it.
 */
function buildFilter(text, limit) {
  const q = parseQuery(text);
  const filter = {};
  if (q.terms || !(q.authors.length || q.mentions.length)) filter.search = searchString(q.terms);
  if (tab.kinds) filter.kinds = tab.kinds;
  if (q.authors.length) filter.authors = q.authors;
  if (q.mentions.length) filter["#p"] = q.mentions;
  filter.limit = limit;
  return filter;
}

async function search(text, limit) {
  await ensureLogin();
  const filter = buildFilter(text, limit);
  const events = await relay.req(filter);
  seedProfiles(events);
  // Authors, plus everyone the cards will NAME — a 30382's d subject, a
  // 10040's service column, a zap's sender. The names rule holds in the
  // results list, not only on permalinks. This used to be a tag scan written
  // here, which meant it could only cover the slots that existed when it was
  // written; namedPubkeys lives with the renderers and is held to them by a
  // test, so a new family that names somebody cannot silently stop being
  // enriched. Faces are excluded on purpose: list previews draw them without
  // names, and a follow list can carry thousands.
  //
  // NOT awaited. This used to block the return, so the results existed and
  // the page showed a skeleton until a SECOND round trip finished — up to the
  // 5s enrichProfiles timeout of nothing, over a list the relay had already
  // sent. base.js says it plainly about the score chip: "the score is a
  // second round trip, and a face should not wait on it." A name is the same
  // round trip; it was just on the other side of the render.
  const mentioned = events.flatMap(namedPubkeys);
  const names = enrichProfiles([...events.filter(e => e.kind !== 0).map(e => e.pubkey), ...mentioned]);
  return { events, names };
}

// ---- viewing as somebody else -------------------------------------------
//
// Only a pubkey that has published a kind 10040 is a usable observer: that
// event is what names the services whose scores the store projects. Anyone
// else ranks nothing, so offering them would be offering an empty feed.
//
// The whole set is small enough to hold — 271 of them on this relay against
// 12.28M profiles — so it is fetched once and filtered by name in the page.
// A server-side name search would have to join kind 0 against kind 10040,
// which NIP-01 filters cannot express in one REQ.
let viewingAs = null;         // hex pubkey, or null for "as me"
let observers = null;         // [{pubkey, name}] once loaded

async function loadObservers() {
  if (observers) return observers;
  // Read on an ANONYMOUS connection, deliberately.
  //
  // The shared `relay` socket is authenticated, and the store gates an
  // authenticated reader to authors that reader has scored — so asking there
  // returned only the observers YOU already trust. The one list that must not
  // be personalised is the list of lenses you could switch to; a picker that
  // hides everyone you have not met is a picker that can never introduce you
  // to anyone.
  // On the shared reference connection, which is already anonymous for this
  // exact reason — this used to open a THIRD socket of its own to say the
  // same thing.
  const anon = await refConn();
  let lists, profileEvents = [];
  lists = await anon.req({ kinds: [10040], limit: 2000 });
  const ks = [...new Set(lists.map((e) => e.pubkey))];
  for (let i = 0; i < ks.length; i += 200) {
    profileEvents = profileEvents.concat(await anon.req({ kinds: [0], authors: ks.slice(i, i + 200), limit: 200 }));
  }
  seedProfiles(profileEvents);
  observers = ks.map(pubkey => {
    const p = profiles.get(pubkey);
    return { pubkey, name: displayName(p), nip05: (p?.nip05 || "").trim() };
  }).sort((a, b) => (a.name || "\uffff").localeCompare(b.name || "\uffff"));
  return observers;
}

function renderObserverOptions(filterText) {
  const q = filterText.trim().toLowerCase();
  const hits = (observers || []).filter(o =>
    !q || o.name.toLowerCase().includes(q) || (o.nip05 || "").toLowerCase().includes(q) ||
    npub(o.pubkey).startsWith(q) || o.pubkey.startsWith(q));
  $obsList.innerHTML = "";
  for (const o of hits.slice(0, 50)) {
    const li = document.createElement("li");
    // Name, else nip05, else a short npub. Never the hex.
    li.textContent = o.name || o.nip05 || shortNpub(o.pubkey);
    li.title = npub(o.pubkey);
    li.onclick = () => { setViewingAs(o.pubkey, o.name || o.nip05 || shortNpub(o.pubkey)); };
    $obsList.appendChild(li);
  }
  if (!hits.length) {
    const li = document.createElement("li");
    li.className = "muted";
    li.textContent = observers ? "no observer matches" : "loading observers…";
    $obsList.appendChild(li);
  }
}

/** The lens state and its widget, with no search behind it — what a history
    restore needs, since the restore runs its own search exactly once. */
function applyViewingAs(pubkey, name) {
  viewingAs = pubkey;
  $obsBox.classList.toggle("active", !!pubkey);
  $obsCurrent.textContent = pubkey ? (name || shortNpub(pubkey)) : "me";
  $obsList.innerHTML = "";
  $obsFilter.value = "";
}

function setViewingAs(pubkey, name) {
  applyViewingAs(pubkey, name);
  rerun();
}

// ---- trust scores, as the ACTIVE LENS sees them -------------------------
//
// The chip on a face answers "what does this ranking think of them", which is
// the question the whole relay is organised around and was nowhere on screen.
// The lens is whoever the page is reading as: you, or the observer picked in
// "ranking as" — the same key that ranks the results, so the chip and the
// order can never disagree.
//
// Read ANONYMOUSLY. A score is a fact about a subject, not about the reader,
// and asking on the authenticated socket would gate the lookup to authors the
// reader already scored — which is circular: you would only see scores for
// people you had already scored.
const scores = new Map();          // pubkey -> number | null (null = no score)
let scoreLensKey = null;           // whose lens `scores` was built for
const rankServices = new Map();    // observer pubkey -> rank service pubkey | null

/**
 * The `30382:rank` service an observer trusts, from their kind 10040.
 *
 * Cached only when the relay ANSWERED. A dropped or timed-out lookup used to
 * be cached as "this lens ranks nothing", and since that is the early return
 * below, one slow read meant no score chip anywhere for the rest of the
 * session — with nothing on screen to say the chips were missing rather than
 * empty. Same mistake profiles.js documents at length; it lived here too.
 */
async function rankServiceOf(observer) {
  if (rankServices.has(observer)) return rankServices.get(observer);
  let svc = null, answered = false;
  try {
    const conn = await refConn();
    const evs = await conn.req({ kinds: [10040], authors: [observer], limit: 1 });
    answered = evs.complete === true;
    svc = (evs[0]?.tags || []).find(t => t?.[0] === "30382:rank")?.[1] || null;
  } catch (e) { answered = false; }
  if (answered) rankServices.set(observer, svc);
  return svc;
}

/**
 * Fill in any score chips currently on the page.
 *
 * Every exit paints, including the ones that have nothing to say. Most chips
 * live inside HTML that is rebuilt per search, so a lens with no scores used
 * to clear itself simply by being re-rendered — but the SEARCH FIELD's chips
 * outlive every search, and returning early there left one lens's numbers
 * sitting on a face under the next lens, or after signing out.
 */
async function paintScores() {
  const lens = viewingAs || me;
  if (scoreLensKey !== lens) { scores.clear(); scoreLensKey = lens; }
  const chips = [...document.querySelectorAll(".score-chip[data-pk]")];
  if (!chips.length) return;
  const svc = lens ? await rankServiceOf(lens) : null;
  // Nobody to rank by, or a lens that ranks nothing: the chips are not stale,
  // they are ANSWERED — with no number.
  if (!svc) { paintChips(chips); return; }
  const need = [...new Set(chips.map(c => c.dataset.pk))].filter(pk => !scores.has(pk));
  for (let i = 0; i < need.length; i += 100) {
    const batch = need.slice(i, i + 100);
    let evs = [];
    try {
      const conn = await refConn();
      evs = await conn.req({ kinds: [30382], authors: [svc], "#d": batch, limit: batch.length });
    } catch (e) { /* leave them unknown rather than wrong */ }
    // The lens can change WHILE this is in flight — the reader picks another
    // observer, or signs out — and `scores` was cleared and re-keyed to the
    // new one by the paint that followed them. Writing this batch into it then
    // files one lens's numbers under another's name, and the `null`s below are
    // worse: they are cached as "this lens gives them no score", which nothing
    // re-asks. The answer is simply stale; drop it.
    if (scoreLensKey !== lens) return;
    const seen = new Set();
    for (const ev of evs) {
      const d = (ev.tags || []).find(t => t?.[0] === "d")?.[1];
      const rank = (ev.tags || []).find(t => t?.[0] === "rank")?.[1];
      if (!d) continue;
      seen.add(d);
      scores.set(d, rank == null ? null : Number(rank));
    }
    // "The service returned no card for this pubkey" is only a fact once the
    // service finished answering. EOSE, not merely "resolved": req() hands
    // back whatever arrived when its timeout fired, so caching the gap off a
    // slow read records a `null` the relay never stated — and `scores` is
    // consulted before every repaint, so that null is permanent for the lens.
    if (evs.complete === true) for (const pk of batch) if (!seen.has(pk)) scores.set(pk, null);
  }
  paintChips(chips);
}

/** The chips themselves, from whatever `scores` now knows. */
function paintChips(chips) {
  for (const c of chips) {
    const v = scores.get(c.dataset.pk);
    if (v == null || Number.isNaN(v)) { c.textContent = ""; c.classList.remove("on"); c.removeAttribute("title"); continue; }
    c.textContent = v;
    c.classList.add("on");
    c.title = `trust ${v} — as ${viewingAs ? "the observer you are viewing as" : "you"} rank them`;
  }
}

/**
 * The whole search, written out so somebody else can argue with it.
 *
 * The point is not a backup — it is handing a ranked list to a reader who was
 * not here and letting them judge the ORDER. That takes more than the titles:
 * what was asked, whose web of trust was applied, what each author scores
 * under that lens, and how old each result is. Rank without the inputs is
 * just a list.
 *
 * Text, not JSON: the intended reader is a person or a model reasoning about
 * whether position 7 deserves to be above position 3, and prose beats a
 * structure they have to re-derive. The raw event stays one click away.
 */
function exportText() {
  const lens = viewingAs || me;
  const nameOf = (k) => displayName(profiles.get(k)) || "";
  const L = [];
  L.push("SEARCH-OVER-TRUST EXPORT");
  L.push(`taken           ${new Date().toISOString()}`);
  L.push(`relay           ${RELAY_URL}`);
  L.push("");
  const typed = $q.value.trim();
  const q = parseQuery(typed);
  const full = buildFilter(typed, FULL_LIMIT);
  const people = (keys) => keys.map((k) => `${npub(k)}${nameOf(k) ? `  (${nameOf(k)})` : ""}`).join(", ");
  L.push("QUERY AS CONFIGURED");
  L.push(`  typed         ${JSON.stringify(typed)}`);
  L.push(`  terms         ${JSON.stringify(q.terms)}`);
  // The person filters are the reason an order can look wrong and be right:
  // a reader judging position 7 has to know the list was narrowed to two
  // authors before it was ranked at all.
  if (q.authors.length) L.push(`  from          ${people(q.authors)}`);
  if (q.mentions.length) L.push(`  to            ${people(q.mentions)}`);
  L.push(`  tab           ${tab.label}${tab.kinds ? ` (kinds ${tab.kinds.join(", ")})` : " (all kinds)"}`);
  L.push(`  sort          ${$sort.value || "(relevance — NIP-50 default)"}`);
  L.push(`  include spam  ${$spam.checked ? "yes — unranked authors included" : "no — trust floor applied"}`);
  L.push(`  signed in as  ${me ? `${nameOf(me) || "(no name)"}  ${npub(me)}` : "(anonymous — no web of trust applied)"}`);
  L.push(`  ranking as    ${lens ? `${nameOf(lens) || "(no name)"}  ${npub(lens)}` : "(nobody)"}`);
  L.push(`  search string ${full.search == null ? "(none — a person filter with no words is a plain NIP-01 read)" : JSON.stringify(full.search)}`);
  L.push(`  full filter   ${JSON.stringify(full)}`);
  L.push("");
  // Whose scores produced this order, listed once rather than repeated per
  // result — and kept OUT of the events themselves, which are reproduced
  // exactly as the relay sent them. Annotating an event with a number the
  // relay never put there would be inventing evidence.
  const authors = [...new Set(s.hits.map((e) => e.pubkey))];
  if (lens) {
    L.push("AUTHOR SCORES UNDER THIS LENS");
    for (const a of authors) {
      const sc = scores.get(a);
      L.push(`  ${npub(a)}  ${sc == null ? "(no score)" : sc}  ${nameOf(a) || ""}`.trimEnd());
    }
    L.push("");
  }
  L.push(`RESULTS  ${s.hits.length} event(s) in ${s.lastMs ?? "?"} ms, in the order the relay returned them.`);
  L.push("Verbatim, unmodified — this is the array the page is rendering.");
  L.push("");
  L.push(JSON.stringify(s.hits, null, 2));
  L.push("");
  L.push("QUESTION FOR THE READER");
  L.push("  Is this order defensible? The relay ranks under the lens named above,");
  L.push("  by the author scores listed above — not by recency or text relevance");
  L.push("  alone. A result placed above another whose author scores higher, or a");
  L.push("  low-scoring author near the top, is worth challenging.");
  L.push("  The events are verbatim: nothing has been trimmed or annotated.");
  return L.join("\n");
}

// ---- page state + wiring ---------------------------------------------------
const $q = document.getElementById("q");
const $clear = document.getElementById("clear");
const $popup = document.getElementById("popup");
const $mentions = document.getElementById("mentions");
const $results = document.getElementById("results");
const $chips = document.getElementById("chips");
const $sort = document.getElementById("sort");
const $spam = document.getElementById("spam");
const $whoami = document.getElementById("whoami");
const $me = document.getElementById("me");

/** The avatar in the field: your picture signed in, a neutral mark signed out. */
function renderMe() {
  const p = me ? profiles.get(me) : null;
  const nm = displayName(p);
  $me.classList.toggle("in", !!me);
  if (me && p && p.picture) {
    $me.innerHTML = `<img src="${esc(p.picture)}" alt="" onerror="this.remove()"/>`;
  } else if (me && nm) {
    $me.textContent = nm.slice(0, 2).toUpperCase();
  } else if (me) {
    // Signed in, profile not (yet) known. Initials off an npub spell "NP" for
    // every account on earth, which reads as a broken avatar rather than as a
    // missing profile.
    $me.innerHTML =
      `<svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor"` +
      ` stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">` +
      `<circle cx="12" cy="12" r="9"/><path d="M12 8v4M12 16h.01"/></svg>`;
  } else {
    // A person outline, not two dots: "no account" should look like an empty
    // seat, and a dot pair looked like something still loading.
    $me.innerHTML =
      `<svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor"` +
      ` stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">` +
      `<path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>`;
  }
  $me.title = me
    ? `Signed in as ${nm || shortNpub(me)} — click to sign out (reconnects)`
    : loginTried
      ? "Signed out — click to sign in with your Nostr extension"
      : "Signing in…";
  $me.setAttribute("aria-label", me ? "Sign out" : "Sign in");
}

// NIP-42 auth belongs to the SOCKET, so signing out is a reconnect, not a
// flag. Anything less would leave the relay still treating the connection as
// authenticated while the page claimed otherwise.
$me.addEventListener("click", async () => {
  $me.classList.add("busy");
  try {
    if (me) {
      rememberSignIn(false);
      me = null;
      // The render-only half: the finally below reruns the search once for
      // the whole click, and setViewingAs here meant every sign-out searched
      // twice — two REQs for one action, with the first result thrown away.
      applyViewingAs(null, null);
      if (relay.ws) relay.ws.close();
      await relay.connect();
    } else {
      rememberSignIn(true);
      loginTried = true;
      await login();
    }
  } catch (e) {
    me = null;
    $whoami.innerHTML = `<span class="err">${esc(e.message || String(e))}</span>`;
  } finally {
    $me.classList.remove("busy");
    renderMe();
    renderWhoami();
    rerun();
  }
});
const $obsBox = document.getElementById("obsbox");
const $obsCurrent = document.getElementById("obscurrent");
const $obsFilter = document.getElementById("obsfilter");
const $obsList = document.getElementById("obslist");
const $obsReset = document.getElementById("obsreset");

// Loaded lazily: the picker is a power feature, and 271 REQs' worth of
// profiles should not be on the path of somebody who just wants to search.
$obsFilter.addEventListener("focus", async () => {
  renderObserverOptions($obsFilter.value);
  await loadObservers();
  renderObserverOptions($obsFilter.value);
});
$obsFilter.addEventListener("input", () => renderObserverOptions($obsFilter.value));
$obsReset.addEventListener("click", () => setViewingAs(null, null));
// Dismissed like any other popup. It is absolutely positioned over the page,
// so without this it sat open over whatever you clicked next.
document.addEventListener("click", (e) => {
  if (!e.target.closest("#obsbox")) $obsList.innerHTML = "";
});
$obsFilter.addEventListener("keydown", (e) => {
  if (e.key === "Escape") { $obsList.innerHTML = ""; $obsFilter.blur(); }
});

// ---- the field renders its own contents ----------------------------------
//
// `$q` stops being an <input> here and becomes a field that draws the people
// in it as faces (searchfield.js says why an input could not). Everything
// below still reads and writes `$q.value`, and the URL still carries the
// plain `from:npub1…` text — the rendering is a view of that string.
//
// Mounted before applyUrl() runs at the bottom of this file: the restore path
// assigns `$q.value`, and until this call that property is an ordinary DOM
// attribute with nothing rendering behind it.

/**
 * Who the picker offers for a half-typed `from:`/`to:`.
 *
 * On the AUTHENTICATED socket, unlike the "ranking as" list beside it. That
 * one must never be personalised — it is the list of lenses you could switch
 * to, and hiding everyone you have not met would make it useless. This one is
 * the opposite question: "who do I mean by ali", and the right answer is the
 * one your own web of trust puts first. So it is a plain NIP-50 profile
 * search down the same connection every other search uses, ranked by you.
 *
 * A pasted HEX key resolves to itself rather than being searched for: it
 * already names one person, and the full-text index has never heard of it.
 * (A pasted npub never reaches here — query.js calls that token finished, so
 * it becomes a face without a round trip at all. Hex does not, precisely so
 * that picking it here is what rewrites it as an npub.)
 */
async function lookupAuthors(partial) {
  const direct = pubkeyParam(partial);
  if (direct) { await enrichProfiles([direct]).catch(() => {}); return [direct]; }
  await ensureLogin();
  const events = await relay.req({ kinds: [0], search: partial, limit: 12 });
  seedProfiles(events);
  return [...new Set(events.map((e) => e.pubkey))];
}

/** Every human edit of the field, whatever made it — typing, paste, a pick. */
function onQueryEdit() {
  const text = $q.value.trim();
  document.body.classList.toggle("has-query", text.length > 0);
  clearTimeout(debounceTimer);
  // A half-written `from:` is not a search for "from:". The two popups share
  // one square of screen and one set of arrow keys, so while the picker owns
  // them the results preview stays shut.
  if (!text || field.mentioning) { closePopup(); return; }
  debounceTimer = setTimeout(() => runPopup(text), DEBOUNCE_MS);
}

// paintScores goes in for the same reason the entity page takes it: the faces
// the field and its picker draw carry the same score chip a card's does, and
// which lens fills it in is app state.
const field = mountSearchField($q, $mentions, { lookup: lookupAuthors, onEdit: onQueryEdit, paintScores });

// `hitsFor` is the text `hits` actually answers. They outlive each other:
// results stay on screen while the box is edited, and a debounce can be
// abandoned before it ever runs — so "there are hits" is not "these hits are
// about what the box says", and reopening the popup on that assumption showed
// one query's answers under another query's words.
const s = { requestId: 0, hits: [], hitsFor: null, lastMs: null, loading: false, error: null };
let debounceTimer = null;
let activeKey = null;

/**
 * The avatar says who you are; the "ranking as" control says whose trust is
 * applied. A sentence repeating both was noise, so this slot now carries only
 * what neither widget can show: an error, or the fact that you are signed out
 * BY CHOICE rather than because signing in failed.
 */
function renderWhoami() {
  renderMe();
  $whoami.textContent = loginTried && !me && !wantsSignIn() ? "signed out — the whole corpus, unranked" : "";
}

function renderChips() {
  $chips.innerHTML = KIND_TABS.map((t, i) =>
    `<button class="chip${t === tab ? " on" : ""}" data-tab="${i}" role="tab" aria-selected="${t === tab}">${esc(t.label)}</button>`
  ).join("");
}

const skelCards = (n) => Array.from({ length: n }, () =>
  `<div class="skel-card">
     <div class="skel-line" style="width:34%"></div>
     <div class="skel-line" style="width:92%"></div>
     <div class="skel-line" style="width:66%"></div>
   </div>`).join("");

const skelRows = (n) => Array.from({ length: n }, () =>
  `<div class="skel-row">
     <div class="skel-dot"></div>
     <div style="flex:1">
       <div class="skel-line" style="width:42%"></div>
       <div class="skel-line" style="width:74%;margin-top:8px"></div>
     </div>
   </div>`).join("");

function statusBody(placeholder) {
  if (s.error) return `<div class="error">${esc(s.error)}</div>`;
  if (s.loading && !s.hits.length) return placeholder;
  if (!s.hits.length) return `<div class="empty"><b>No results</b>Try a different term, or widen the filter above.</div>`;
  return null;
}

function renderPopup() {
  const busy = s.loading || s.error;
  const meta = busy ? "" : [`${s.hits.length}`, s.lastMs != null ? `${s.lastMs} ms` : ""].filter(Boolean).join(" · ");
  const head = `<div class="popup-head"><span>Results</span><span class="timing">${meta}</span></div>`;
  const body = statusBody(skelRows(3)) ?? s.hits.slice(0, POPUP_LIMIT).map(popupRow).join("");
  $popup.innerHTML = head + body;
  paintScores();
}

function renderResults() {
  const stats = s.error ? "" : `${s.hits.length} result${s.hits.length === 1 ? "" : "s"} · ${s.lastMs ?? "?"} ms`;
  const head = `<div class="list-head"><div class="list-title">${esc(tab.label)}</div>` +
    `<div class="list-right"><span class="list-stats">${stats}</span>` +
    `<button type="button" id="export" class="export" title="Download this search and its results as text">Export</button></div></div>`;
  // Not `.map(card)`: map hands the index as card's second argument, where a
  // renderer would read it as the opts object.
  const body = statusBody(skelCards(4)) ?? s.hits.map((ev) => card(ev)).join("");
  $results.innerHTML = head + body;
  paintScores();
  watchNip05();
}

async function run(text, mode, render) {
  const myId = ++s.requestId;
  s.loading = true; s.error = null;
  if (mode === "full") { s.hits = []; s.hitsFor = null; }
  render();
  const t0 = performance.now();
  let names = null;
  try {
    const found = await search(text, mode === "popup" ? POPUP_LIMIT : FULL_LIMIT);
    if (myId !== s.requestId) return;
    s.hits = found.events; s.hitsFor = text; names = found.names;
  } catch (e) {
    if (myId !== s.requestId) return;
    s.error = e.message || String(e); s.hits = []; s.hitsFor = text;
  }
  s.lastMs = Math.round(performance.now() - t0); s.loading = false;
  render();
  // The names land after the list does, so paint them when they arrive —
  // once, and only if the lookup actually learned something. Skipped while a
  // raw event is expanded: a re-render would collapse a panel the reader
  // opened, and a name appearing is not worth taking that away.
  if (names) {
    names.then((learned) => {
      if (!learned || myId !== s.requestId) return;
      // The field's own chips are named from the same cache, and a `from:`
      // whose profile arrived on THIS lookup would otherwise sit on a short
      // npub until the next edit.
      field.repaint();
      if (document.querySelector(".raw-body:not([hidden])")) return;
      render();
    }).catch(() => {});
  }
}

function openPopup() {
  $popup.classList.add("open");
  $q.setAttribute("aria-expanded", "true");
  $q.setAttribute("aria-controls", "popup");
}
function closePopup() {
  $popup.classList.remove("open");
  // Two listboxes hang off one combobox, and only one is ever up. When the
  // PICKER is the one showing, these attributes are describing it — lowering
  // them here told a screen reader the list had closed while the people list
  // was on screen and being arrowed through.
  if (!field.pickerOpen) {
    $q.setAttribute("aria-expanded", "false");
    $q.removeAttribute("aria-activedescendant");
  }
  activeKey = null;
}
const popupItems = () => Array.from($popup.querySelectorAll(".popup-item"));
function setActive(idx) {
  const items = popupItems();
  if (!items.length) return;
  if (idx < 0) idx = items.length - 1;
  if (idx >= items.length) idx = 0;
  items.forEach(el => el.classList.remove("active"));
  items[idx].classList.add("active");
  items[idx].scrollIntoView({ block: "nearest" });
  activeKey = idx;
}

// Never over the people picker: a debounced type-ahead from before the `from:`
// was started can still land after it, and the two popups occupy the same box.
function runPopup(text) { if (field.mentioning) return; openPopup(); run(text, "popup", renderPopup); }

function runFull(text) {
  clearTimeout(debounceTimer); // else a type-ahead still in flight re-opens the popup over the results
  cancelEntity(); // a search launched FROM an entity page must not be painted over by its slow fetch
  document.title = "SearchOverTrust";
  closePopup();
  field.close(); // Enter with nothing highlighted searches; the picker is done
  $results.hidden = false;
  document.body.classList.add("searching");
  // Every way into the full view converges here — Enter, a chip/sort/spam/
  // lens change via rerun() — so this is the one place the URL learns about
  // it. Pushed, not replaced: each of those is a state the user chose and
  // Back should be able to undo. A restore FROM history is the exception,
  // and syncUrl() itself knows to stand down there.
  syncUrl();
  run(text, "full", renderResults);
}

/**
 * A picked popup result opens as its own page — the record itself, full
 * screen, not the full results list scrolled to it (which is what this used
 * to do, and readers took as landing "near" the thing instead of ON it). A
 * profile is named by its key, everything else by its id; the event's KIND
 * still decides the card, over in entity.js. Routed through the same
 * pushState + applyUrl() pair as any internal link, so the entity page is a
 * real history entry and Back undoes the click.
 */
function openPicked(ev) {
  if (!ev) return;
  const href = ev.kind === 0 ? `/${npub(ev.pubkey)}` : `/${noteId(ev.id)}`;
  if (location.pathname + location.search !== href) history.pushState(null, "", href);
  applyUrl();
}
function rerun() {
  const text = $q.value.trim();
  if (!text) return;
  if (!$results.hidden) runFull(text);
  else if ($popup.classList.contains("open")) runPopup(text);
}

/** Back to the landing hero: no query, no results, nothing sticky. The view
    change only — history restore uses this too, and must not push. */
function showHero() {
  clearTimeout(debounceTimer);
  cancelEntity(); // same for "← Search" out of an entity page mid-fetch
  document.title = "SearchOverTrust";
  s.requestId++; s.hits = []; s.hitsFor = null; s.error = null; s.loading = false; s.lastMs = null;
  $q.value = "";
  $results.hidden = true;
  $results.innerHTML = "";
  document.body.classList.remove("searching", "has-query");
  closePopup();
}

/** The hero as a NAVIGATION — clear button, brand click. Pushed, so Back
    returns to the search that was just cleared instead of losing it. */
function reset() {
  showHero();
  syncUrl();
  $q.focus();
}

$q.addEventListener("keydown", (e) => {
  // The picker gets first refusal on the arrows, Enter, Tab and Escape while
  // it is open — asked outright rather than by racing this listener in the
  // capture phase, because which of two listeners on one element runs first
  // is registration order and that is not a thing to depend on.
  if (field.handleKey(e)) return;
  if (e.key === "Enter") {
    e.preventDefault();
    // An arrowed-to row is a selection, same as a click on it: Enter opens
    // that record. A plain Enter with nothing highlighted stays the full
    // search — the popup is a preview of it, not a gate in front of it.
    if ($popup.classList.contains("open") && activeKey != null) { openPicked(s.hits[activeKey]); return; }
    const text = $q.value.trim();
    if (text) runFull(text);
  } else if (e.key === "ArrowDown") {
    if ($popup.classList.contains("open")) { e.preventDefault(); setActive((activeKey ?? -1) + 1); }
  } else if (e.key === "ArrowUp") {
    if ($popup.classList.contains("open")) { e.preventDefault(); setActive((activeKey ?? popupItems().length) - 1); }
  } else if (e.key === "Escape") {
    closePopup();
  }
});

$clear.addEventListener("click", reset);

// "/" anywhere focuses the search box, the way every search page does it.
document.addEventListener("keydown", (e) => {
  if (e.key !== "/" || e.metaKey || e.ctrlKey || e.altKey) return;
  // `isContentEditable`, not just the tag list: the search box IS a div now,
  // so a tag test alone swallowed every "/" typed INTO the field and focused
  // the thing that already had focus.
  const el = document.activeElement;
  if (!el || el.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName)) return;
  e.preventDefault();
  $q.focus();
  $q.select();
});

$popup.addEventListener("mousedown", (e) => {
  const item = e.target.closest(".popup-item");
  if (!item) return;
  e.preventDefault();
  openPicked(s.hits[Number(item.dataset.idx)]);
});

document.addEventListener("click", (e) => { if (!e.target.closest(".search-wrap")) closePopup(); });
$q.addEventListener("focus", () => {
  // Never over a half-written `from:`/`to:` — the picker owns that box, and
  // it may be shut simply because the blur that took focus away closed it.
  if (field.mentioning) return;
  const text = $q.value.trim();
  if (s.hits.length && s.hitsFor === text && text && $results.hidden) openPopup();
});

$results.addEventListener("click", (e) => {
  if (e.target.closest("#export")) {
    const blob = new Blob([exportText()], { type: "text/plain;charset=utf-8" });
    const a = document.createElement("a");
    const slug = ($q.value.trim().replace(/[^a-z0-9]+/gi, "-").replace(/^-|-$/g, "") || "search").slice(0, 40);
    a.href = URL.createObjectURL(blob);
    a.download = `sot-${slug}-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "")}.txt`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 10000);
    return;
  }
  const btn = e.target.closest(".raw-toggle");
  if (!btn) return;
  const box = btn.parentElement.querySelector(".raw-body");
  if (!box.hidden) { box.hidden = true; btn.textContent = "json"; return; }
  const ev = s.hits.find((h) => h.id === btn.dataset.id);
  // Serialised only when asked for, and only once.
  if (!box.textContent) box.textContent = ev ? JSON.stringify(ev, null, 2) : "(no longer in the current results)";
  box.hidden = false;
  btn.textContent = "hide json";
});

$chips.addEventListener("click", (e) => {
  const chip = e.target.closest(".chip");
  if (!chip) return;
  tab = KIND_TABS[Number(chip.dataset.tab)];
  renderChips();
  rerun();
});

$sort.addEventListener("change", rerun);
$spam.addEventListener("change", rerun);

// ---- the URL is the search ------------------------------------------------
//
// Everything the results view shows is derived from five inputs — q, tab,
// sort, spam, lens — so those five ARE the location: `/?q=…&tab=notes&
// sort=rank&spam=1&as=npub…`. Sign-in state is deliberately not among them;
// auth belongs to the socket and to the person holding the extension, and a
// shared URL must not try to carry it.
//
// The hero is the clean path with no params, which gives Back a floor to
// land on. The popup preview never appears here: it is keystroke feedback,
// and pushing an entry per keystroke is how Back buttons become useless.

// A restore must not re-push what it is restoring — popstate would otherwise
// truncate the forward stack it is trying to walk.
let navRestoring = false;

function currentUrl() {
  const p = new URLSearchParams();
  const text = $q.value.trim();
  if (!$results.hidden && text) p.set("q", text);
  if (tab.slug !== KIND_TABS[0].slug) p.set("tab", tab.slug);
  if ($sort.value) p.set("sort", $sort.value);
  if ($spam.checked) p.set("spam", "1");
  // npub, like everywhere else on the page — this string gets pasted into
  // chats, and a 64-char hash is not how anyone names a person.
  if (viewingAs) p.set("as", npub(viewingAs));
  const qs = p.toString();
  // Anchored at "/", not location.pathname: with entity pages the pathname
  // can be /note1…, and a search launched from one belongs at the root — a
  // URL like /note1…?q=cats would name two places at once.
  return "/" + (qs ? "?" + qs : "");
}

function syncUrl() {
  if (navRestoring) return;
  const url = $results.hidden ? "/" : currentUrl();
  // Re-submitting the same search re-queries the relay but is not a second
  // place; pushing it would make Back appear to do nothing.
  if (url === location.pathname + location.search) return;
  history.pushState(null, "", url);
}

/**
 * URL -> page, the single restore path: initial load, Back, and Forward all
 * come through here. Returns whether it started a search, so the boot path
 * knows whether sign-in is already being driven by that search or needs its
 * own kick.
 */
function applyUrl() {
  navRestoring = true;
  try {
    // A NIP-19 path is the third view: not the hero, not the results — the
    // one thing the identifier names. The shape test only routes; whether
    // the identifier VALIDATES is entity.js's question, so a bad checksum
    // still lands on a page that explains itself.
    const seg = location.pathname.slice(1);
    if (/^(npub|nprofile|note|nevent|naddr)1[a-z0-9]+$/i.test(seg)) {
      clearTimeout(debounceTimer);
      s.requestId++; // cancel any in-flight search render
      s.hits = []; s.hitsFor = null; s.error = null; s.loading = false;
      $q.value = "";
      document.body.classList.remove("has-query");
      document.body.classList.add("searching");
      closePopup();
      $results.hidden = false;
      showEntity(seg, { paintScores, ensureLogin });
      return false; // no search running — boot still signs in eagerly
    }
    cancelEntity(); // leaving the entity view invalidates its in-flight fetch
    document.title = "SearchOverTrust";
    const p = new URLSearchParams(location.search);
    const text = (p.get("q") || "").trim();
    tab = KIND_TABS.find((t) => t.slug === p.get("tab")) || KIND_TABS[0];
    renderChips();
    $sort.value = p.get("sort") || "";
    if ($sort.selectedIndex < 0) $sort.value = ""; // an unknown sort is no sort
    $spam.checked = p.get("spam") === "1";
    const as = pubkeyParam(p.get("as"));
    applyViewingAs(as, null);
    if (as) {
      // The URL carries the key; the label wants the name. Filled in after
      // the fact so the restore itself never waits on a profile lookup.
      enrichProfiles([as]).then(() => {
        const nm = displayName(profiles.get(as));
        if (nm && viewingAs === as) $obsCurrent.textContent = nm;
      }).catch(() => {});
    }
    $q.value = text;
    document.body.classList.toggle("has-query", !!text);
    if (text) runFull(text); else showHero();
    return !!text;
  } finally {
    navRestoring = false;
  }
}

window.addEventListener("popstate", applyUrl);

// Internal navigation: every link this app can render itself — the brand's
// and entity head's "/", and any /npub1…, /note1… a card emits — stays a real
// <a> (middle-click and copy-link work) but a plain left click becomes a
// pushState render instead of a full reload that would tear down the socket
// and the NIP-42 auth on it. One document-level listener rather than wiring
// per card: cards are HTML strings re-rendered wholesale, so per-element
// handlers would need re-attachment on every render.
document.addEventListener("click", (e) => {
  if (e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
  const a = e.target.closest("a[href^='/']");
  if (!a) return;
  const href = a.getAttribute("href");
  if (href === "/") { e.preventDefault(); reset(); return; }
  if (!/^\/(npub|nprofile|note|nevent|naddr)1[a-z0-9]+$/i.test(href)) return;
  e.preventDefault();
  if (location.pathname + location.search !== href) history.pushState(null, "", href);
  applyUrl();
});

// Chips render at boot, not only inside applyUrl's search branch: the entity
// branch returns before that code, so a direct load of /npub1… used to show
// an empty chip row until the first Back into a search view.
renderChips();
renderWhoami();
// applyUrl() either restores a deep-linked search, shows an entity page, or
// shows the hero. When it starts a search, that search's own ensureLogin()
// is already driving sign-in, so the extra call is skipped as redundant —
// every caller shares the one login flight now, so a second call would
// merely be noise rather than the race it used to be.
// On an idle load, sign in eagerly rather than on the first keystroke: the
// lens picker is only meaningful once there is an authenticated reader, and
// an idle page should not sit on "signing in…" until somebody types.
if (!applyUrl()) ensureLogin().then(renderWhoami).catch(() => renderWhoami());
// What `autofocus` did while the field was an <input>. A contenteditable takes
// the attribute inconsistently across browsers, and the condition is the one
// that was always meant: focus the box when the page IS the box — never on an
// entity permalink, which has its own content to read.
if ($results.hidden) $q.focus();
