// The SearchOverTrust page itself: sign-in, the search views (hero, type-ahead
// popup, full results), the "ranking as" lens, and the URL/backstack wiring.
// Everything stateful lives here; the shared/ modules underneath are the
// stateless client, codec and caches, and cards.js is the rendering.

import { RELAY_URL, relay, refConn } from "./shared/conn.js";
import { npub, shortNpub, pubkeyParam } from "./shared/nip19.js";
import { esc } from "./shared/format.js";
import { profiles, displayName, seedProfiles, enrichProfiles } from "./shared/profiles.js";
import { watchNip05 } from "./shared/nip05.js";
import { card, popupRow } from "./cards.js";
import { showEntity, cancelEntity } from "./entity.js";

const POPUP_LIMIT = 8;
const FULL_LIMIT = 40;
const DEBOUNCE_MS = 150;

// ---- the filter chips are literal NIP-01 `kinds` filters ----------------
// `slug` is the tab's name in the URL (`?tab=notes`). A slug, not the label
// ("Code & git" percent-encodes into line noise) and not the kinds list
// (which this page is free to tune without breaking every bookmarked URL).
const KIND_TABS = [
  { label: "Everything", slug: "all", kinds: null },
  { label: "People", slug: "people", kinds: [0] },
  { label: "Notes", slug: "notes", kinds: [1, 11] },
  { label: "Articles", slug: "articles", kinds: [30023, 30024, 30818] },
  { label: "Media", slug: "media", kinds: [20, 21, 22, 1063, 31922, 34235, 34236] },
  { label: "Code & git", slug: "code", kinds: [1337, 1617, 1621, 30617] },
  { label: "Live", slug: "live", kinds: [30311] },
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

async function search(text, limit) {
  await ensureLogin();
  const filter = { search: searchString(text), limit };
  if (tab.kinds) filter.kinds = tab.kinds;
  const events = await relay.req(filter);
  seedProfiles(events);
  await enrichProfiles(events.filter(e => e.kind !== 0).map(e => e.pubkey));
  return events;
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
  const keys = [...new Set(lists.map((e) => e.pubkey))];
  observers = keys.map(pubkey => {
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

/** The `30382:rank` service an observer trusts, from their kind 10040. */
async function rankServiceOf(observer) {
  if (rankServices.has(observer)) return rankServices.get(observer);
  let svc = null;
  try {
    const conn = await refConn();
    const [ev] = await conn.req({ kinds: [10040], authors: [observer], limit: 1 });
    svc = (ev?.tags || []).find(t => t[0] === "30382:rank")?.[1] || null;
  } catch (e) { svc = null; }
  rankServices.set(observer, svc);
  return svc;
}

/** Fill in any score chips currently on the page. */
async function paintScores() {
  const lens = viewingAs || me;
  if (scoreLensKey !== lens) { scores.clear(); scoreLensKey = lens; }
  const chips = [...document.querySelectorAll(".score-chip[data-pk]")];
  if (!lens || !chips.length) return;
  const svc = await rankServiceOf(lens);
  if (!svc) return;                     // this lens ranks nothing; no chips
  const need = [...new Set(chips.map(c => c.dataset.pk))].filter(pk => !scores.has(pk));
  for (let i = 0; i < need.length; i += 100) {
    const batch = need.slice(i, i + 100);
    let evs = [];
    try {
      const conn = await refConn();
      evs = await conn.req({ kinds: [30382], authors: [svc], "#d": batch, limit: batch.length });
    } catch (e) { /* leave them unknown rather than wrong */ }
    const seen = new Set();
    for (const ev of evs) {
      const d = (ev.tags || []).find(t => t[0] === "d")?.[1];
      const rank = (ev.tags || []).find(t => t[0] === "rank")?.[1];
      if (!d) continue;
      seen.add(d);
      scores.set(d, rank == null ? null : Number(rank));
    }
    for (const pk of batch) if (!seen.has(pk)) scores.set(pk, null);
  }
  for (const c of chips) {
    const v = scores.get(c.dataset.pk);
    if (v == null || Number.isNaN(v)) { c.textContent = ""; c.classList.remove("on"); continue; }
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
  L.push("QUERY AS CONFIGURED");
  L.push(`  terms         ${JSON.stringify($q.value.trim())}`);
  L.push(`  tab           ${tab.label}${tab.kinds ? ` (kinds ${tab.kinds.join(", ")})` : " (all kinds)"}`);
  L.push(`  sort          ${$sort.value || "(relevance — NIP-50 default)"}`);
  L.push(`  include spam  ${$spam.checked ? "yes — unranked authors included" : "no — trust floor applied"}`);
  L.push(`  signed in as  ${me ? `${nameOf(me) || "(no name)"}  ${npub(me)}` : "(anonymous — no web of trust applied)"}`);
  L.push(`  ranking as    ${lens ? `${nameOf(lens) || "(no name)"}  ${npub(lens)}` : "(nobody)"}`);
  L.push(`  search string ${JSON.stringify(searchString($q.value.trim()))}`);
  L.push(`  full filter   ${JSON.stringify({ search: searchString($q.value.trim()), limit: FULL_LIMIT, ...(tab.kinds ? { kinds: tab.kinds } : {}) })}`);
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
      viewingAs = null;
      setViewingAs(null, null);
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

const s = { requestId: 0, hits: [], lastMs: null, loading: false, error: null };
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
  // Only once there is something to reveal. run() renders TWICE — a skeleton
  // the instant the search starts, then the results — and the first pass was
  // consuming the pending id against a list of placeholders, so the real
  // render had nothing left to scroll to.
  if (!s.loading && !s.error) revealPending();
}

async function run(text, mode, render) {
  const myId = ++s.requestId;
  s.loading = true; s.error = null;
  if (mode === "full") s.hits = [];
  render();
  const t0 = performance.now();
  try {
    const hits = await search(text, mode === "popup" ? POPUP_LIMIT : FULL_LIMIT);
    if (myId !== s.requestId) return;
    s.hits = hits;
  } catch (e) {
    if (myId !== s.requestId) return;
    s.error = e.message || String(e); s.hits = [];
  }
  s.lastMs = Math.round(performance.now() - t0); s.loading = false;
  render();
}

function openPopup() { $popup.classList.add("open"); $q.setAttribute("aria-expanded", "true"); }
function closePopup() { $popup.classList.remove("open"); $q.setAttribute("aria-expanded", "false"); activeKey = null; }
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

function runPopup(text) { openPopup(); run(text, "popup", renderPopup); }
// The id of a result clicked in the quick popup, waiting for the full view to
// render so it can be scrolled to.
let pendingReveal = null;

function runFull(text, revealId) {
  clearTimeout(debounceTimer); // else a type-ahead still in flight re-opens the popup over the results
  cancelEntity(); // a search launched FROM an entity page must not be painted over by its slow fetch
  document.title = "SearchOverTrust";
  closePopup();
  $results.hidden = false;
  document.body.classList.add("searching");
  // Every way into the full view converges here — Enter, a popup click, a
  // chip/sort/spam/lens change via rerun() — so this is the one place the
  // URL learns about it. Pushed, not replaced: each of those is a state the
  // user chose and Back should be able to undo. A restore FROM history is
  // the exception, and syncUrl() itself knows to stand down there.
  syncUrl();
  pendingReveal = revealId || null;
  run(text, "full", renderResults);
}

/** Scroll a just-rendered result into view and flag it for a moment. */
function revealPending() {
  if (!pendingReveal) return;
  const el = $results.querySelector(`.result[data-id="${CSS.escape(pendingReveal)}"]`);
  // Cleared either way: the results ARE rendered by now, so if the clicked
  // event is not among them (the full query can return a different set than
  // the popup's preview) there is nothing to wait for.
  pendingReveal = null;
  if (!el) return;
  el.scrollIntoView({ behavior: "smooth", block: "center" });
  el.classList.add("revealed");
  // Removed rather than left on: a highlight that never fades stops meaning
  // "this is the one you clicked" and starts meaning nothing.
  setTimeout(() => el.classList.remove("revealed"), 5000);
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
  s.requestId++; s.hits = []; s.error = null; s.loading = false; s.lastMs = null;
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

$q.addEventListener("input", () => {
  const text = $q.value.trim();
  document.body.classList.toggle("has-query", text.length > 0);
  clearTimeout(debounceTimer);
  if (!text) { closePopup(); return; }
  debounceTimer = setTimeout(() => runPopup(text), DEBOUNCE_MS);
});

$q.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    e.preventDefault();
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
  if (/^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement.tagName)) return;
  e.preventDefault();
  $q.focus();
  $q.select();
});

$popup.addEventListener("mousedown", (e) => {
  const item = e.target.closest(".popup-item");
  if (!item) return;
  e.preventDefault();
  // Clicking a quick result should land you ON that result, not merely in a
  // list that happens to contain it. The popup shows the first few of what
  // the full view will render, so the id survives the switch — carry it over,
  // scroll to it, and mark it briefly so the eye finds it without hunting.
  const picked = s.hits[Number(item.dataset.idx)];
  runFull($q.value.trim(), picked && picked.id);
});

document.addEventListener("click", (e) => { if (!e.target.closest(".search-wrap")) closePopup(); });
$q.addEventListener("focus", () => { if (s.hits.length && $q.value.trim() && $results.hidden) openPopup(); });

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
      s.hits = []; s.error = null; s.loading = false;
      $q.value = "";
      document.body.classList.remove("has-query");
      document.body.classList.add("searching");
      closePopup();
      $results.hidden = false;
      showEntity(seg, { paintScores });
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

renderWhoami();
// applyUrl() renders the chips and either restores a deep-linked search or
// shows the hero. When it restores one, that search's own ensureLogin() is
// already driving sign-in, so the extra call is skipped as redundant — every
// caller shares the one login flight now, so a second call would merely be
// noise rather than the race it used to be.
// On an idle load, sign in eagerly rather than on the first keystroke: the
// lens picker is only meaningful once there is an authenticated reader, and
// an idle page should not sit on "signing in…" until somebody types.
if (!applyUrl()) ensureLogin().then(renderWhoami).catch(() => renderWhoami());
