// The SearchOverTrust page itself: sign-in, the search views (hero, type-ahead popup, full
// results), the "ranking as" lens, and the URL/backstack wiring. Everything stateful lives
// here; the shared/ modules underneath are the stateless client, codec and caches.

import { RELAY_URL, relay, refConn } from "./shared/conn.js";
import { npub, shortNpub, pubkeyParam } from "./shared/nip19.js";
import { esc } from "./shared/format.js";
import { avatarHtml } from "./shared/avatar.js";
import { profiles, displayName, seedProfiles, enrichProfiles } from "./shared/profiles.js";
import { watchNip05 } from "./shared/nip05.js";
import { parseQuery, buildFilters as filtersFor, effectiveSort } from "./shared/query.js";
import { SPAM_TOKEN } from "./shared/lens.js";
import { ownGroups, metaGroup, postedTo, rank as rankGroups, sealed as sealedGroups, privateGroups } from "./shared/groups.js";
import { seedGroupNames, seedGroupEvents, enrichGroupNames, forgetPrivateGroupNames } from "./shared/groupnames.js";
import { isTyping, navKey, stepIndex } from "./shared/keynav.js";
import { replyPerson, seedParentAuthors, unknownParents, loadParentAuthors } from "./shared/parents.js";
import { selfHref } from "./cards/base.js";
import { seedProvenance, forgetProvenance, provenance, provenanceEpoch } from "./provenance.js";
import { fetchPointers, trustedSigners } from "./shared/pointers.js";
import { providersFor, knownProviders } from "./shared/providers.js";
import { card, popupRow, namedPubkeys } from "./cards.js";
import { showEntity, cancelEntity } from "./entity.js";
import { feedKinds, PREVIEW_CARDS, PAGE_CARDS, askFor, pickFeed } from "./feed.js";
import { PAGE_SIZE, MAX_ASK, MAX_PAGES, firstAsk, askLimit, pageOf, pageCount, covered, canGrow, lastPage, mergePages, drained } from "./paging.js";
import { mountSearchField, softKeyboard } from "./searchfield.js";
import { AskCache } from "./shared/asks.js";
import { checkReadiness, clearReadiness } from "./readiness.js";

// Rows the popup draws; the ask itself is one results page wide, so Enter can reuse it.
const POPUP_LIMIT = 8;
// A keystroke gap long enough to count as a pause.
const DEBOUNCE_MS = 250;

// ---- the filter chips are literal NIP-01 `kinds` filters ----------------
// `slug` names the tab in the URL, so the kinds can change without breaking bookmarks. A tab's
// kinds must be the ones the matching family in shared/kinds.js renders.
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
// A cookie so every page the relay serves reads it. Unset defaults to signed in.
const AUTH_COOKIE = "sot_signedin";
function wantsSignIn() {
  const m = document.cookie.match(/(?:^|;\s*)sot_signedin=([01])/);
  return m ? m[1] === "1" : true;
}
function rememberSignIn(yes) {
  document.cookie = `${AUTH_COOKIE}=${yes ? 1 : 0}; path=/; max-age=31536000; SameSite=Lax`;
}

// ---- the face you ended on, kept across loads ----------------------------
// The last signed-in account's picture and name, so the field can draw them before the
// socket, the challenge and the kind 0 read. A cache of what was on screen; it authorises nothing.
const FACE_KEY = "sot_face";
let meFace = readFace();
function readFace() {
  try {
    const f = JSON.parse(localStorage.getItem(FACE_KEY) || "null");
    return f && /^[0-9a-f]{64}$/.test(f.pubkey || "") ? f : null;
  } catch (e) { return null; }
}
/**
 * Keep what the relay said about [pk]'s face, but only once it answered (`has`); a dropped
 * read is not "no picture".
 */
function rememberFace(pk) {
  if (!pk || !profiles.has(pk)) return;
  const p = profiles.get(pk);
  meFace = p ? { pubkey: pk, picture: p.picture || "", name: displayName(p) } : null;
  writeFace();
}
/** Signing out is a decision: the next load must not flash the old face. */
function forgetFace() {
  meFace = null;
  writeFace();
}
function writeFace() {
  try {
    if (meFace) localStorage.setItem(FACE_KEY, JSON.stringify(meFace));
    else localStorage.removeItem(FACE_KEY);
  } catch (e) {}
}

// ---- NIP-07 login -> NIP-42 auth -----------------------------------------
// Signing the challenge switches the connection's ranking observer to you. It enrols nothing.
let me = null;        // the pubkey the relay accepted a NIP-42 AUTH for
let mePending = null; // the pubkey the extension named, before that proof

// A dropped socket loses the AUTH, not the identity: the next request re-authenticates.
relay.onclose = () => { renderWhoami(); };

// The restore half of the back/forward cache (pagehide is in shared/conn.js): `me` is still
// set, the socket is closed and nothing is authenticated.
window.addEventListener("pageshow", (ev) => {
  // A normal load connects on its own path.
  if (!ev.persisted) return;
  // NIP-42 belongs to the connection, so the proof and the settled flight are both stale.
  loginTried = false;
  loginFlight = null;
  scores.clear();
  scoreLensKey = null;
  renderWhoami();
  ensureLogin().then(paintScores).catch(() => {});
});

// Sign the current challenge and send it. A challenge belongs to one connection: if the socket
// was replaced while the human signed, re-read it and sign again.
async function signAndAuth(attempt = 0, waitMs = 3000) {
  const challenge = await relay.waitForChallenge(waitMs);
  if (!challenge) throw new Error("The relay sent no NIP-42 challenge");
  const signed = await window.nostr.signEvent({
    kind: 22242,
    created_at: Math.floor(Date.now() / 1000),
    content: "",
    tags: [["relay", RELAY_URL], ["challenge", challenge]],
  });
  if (relay.challenge !== challenge) {
    // Two tries only: looping would spam the signer.
    if (attempt >= 1) throw new Error("The connection kept resetting while signing — try again");
    await relay.connect();
    return signAndAuth(attempt + 1);
  }
  await relay.auth(signed);
  return signed.pubkey;
}

/**
 * Who the extension says you are, and your face, both before the proof. `getPublicKey()` is
 * local, so this runs beside the handshake. The pubkey draws and prefetches; it never authorises.
 */
async function prefetchFace() {
  let pk = null;
  try { pk = await window.nostr.getPublicKey(); } catch (e) { pk = null; }
  if (!/^[0-9a-f]{64}$/.test(pk || "")) {
    // The extension would not say, so the boot's assumed face comes down.
    mePending = null;
    renderMe();
    return null;
  }
  mePending = pk;
  renderMe();                 // the remembered face, if this is the same account
  // enrichProfiles skips a pubkey it has seen, including a cached `null`, so delete first.
  profiles.delete(pk);
  try { await enrichProfiles([pk]); } catch (e) {}
  renderMe();
  return pk;
}

async function login() {
  if (!(window.nostr && window.nostr.signEvent)) throw new Error("No Nostr extension found (window.nostr / NIP-07)");
  // The socket and the face start together; only the AUTH needs both.
  const connecting = relay.connect();
  const face = prefetchFace();
  await connecting;
  // A click gets a longer wait than a background attempt: the challenge arrives after the handshake.
  me = await signAndAuth(0, 10000);
  mePending = null;
  renderWhoami();
  // Awaited so callers can rely on the face being fetched when login() returns.
  const named = await face;
  if (named !== me) {
    // The extension never answered getPublicKey, or signed as another account.
    profiles.delete(me);
    try { await enrichProfiles([me]); } catch (e) {}
  }
  rememberFace(me);
  renderWhoami();
  // The read above races the reference socket's opening. Retry on `has` only: a cached `null` is an answer.
  for (let i = 0; i < 3 && me && !profiles.has(me); i++) {
    await new Promise((r) => setTimeout(r, 600 * (i + 1)));
    if (!me) break;
    profiles.delete(me);
    try { await enrichProfiles([me]); } catch (e) {}
    rememberFace(me);
    renderWhoami();
  }
}

// Signed in automatically, once, best effort; a failure downgrades to the unranked corpus and
// says so. One in-flight promise that every read awaits, so no REQ goes out on this socket
// before its auth question is settled.
let loginTried = false;   // the first attempt has settled (labels key on this)
let loginFlight = null;   // the first attempt itself, awaited by every search
async function ensureLogin() {
  // Started, not awaited: only the AUTH needs the socket, and it connects itself.
  relay.connect().catch(() => {});
  if (!loginFlight) {
    loginFlight = (async () => {
      // A remembered "signed out" is a decision; do not prompt the extension.
      if (!wantsSignIn() || !(window.nostr && window.nostr.signEvent)) {
        mePending = null;
        // The picker stays live: it is a signed-out reader's only way to a ranked answer.
        applyViewingAs(viewingAs, $obsCurrent.textContent);
        renderWhoami();
        return;
      }
      try { await login(); } catch (e) {
        me = null;
        mePending = null;   // the extension named somebody it could not prove
        applyViewingAs(viewingAs, $obsCurrent.textContent);
        $whoami.innerHTML = `<span class="err">${esc(e.message || String(e))} — showing the whole corpus, unranked</span>`;
      }
    })().finally(() => { loginTried = true; });
  }
  await loginFlight;
  if (!me) { clearReadiness(); return; }
  // Keyed on the connection's auth state: a reconnect leaves `me` set but the socket unauthenticated.
  if (relay.authed) { checkReadiness(me); return; }
  me = await signAndAuth();
  renderWhoami();
  // Both paths to an authenticated socket meet here, and the check is about the connection's lens.
  checkReadiness(me);
}

// The resend half of NIP-42: on CLOSED "auth-required:" the client authenticates and resends.
// The anonymous connection gets no such hook; for it auth-required is a real answer.
relay.onAuthRequired = () => ensureLogin();

// ---- search over NIP-50 ---------------------------------------------------
let tab = KIND_TABS[0];

/**
 * Whether this page has a web of trust to read through at all. `me`, not `mePending`: an unproved
 * pubkey is not a lens.
 */
const lensless = () => !me && !viewingAs;

/** Whether this read waives a lens: the reader's switch, or having none. */
const spamOn = () => $spam.checked || lensless();

/**
 * The NIP-50 string a one-off ask needs: the words, plus this page's lens where the connection
 * does not already carry one. Separate from [searchString]: no sort menu, no spam switch.
 */
function askString(words) {
  if (me) return words || "";
  const parts = words ? [words] : [];
  parts.push(viewingAs ? "observer:" + viewingAs : SPAM_TOKEN);
  return parts.join(" ");
}

function searchString(text) {
  const sort = $sort.value;
  const parts = text ? [text] : [];
  if (sort) parts.push("sort:" + sort);
  // Whose web of trust ranks this. Sent signed out too: the store resolves `observer:` anonymously.
  if (viewingAs) parts.push("observer:" + viewingAs);
  // Last, so the exported string reads lens then waiver. Never both.
  if (spamOn()) parts.push("include:spam");
  return parts.join(" ");
}

/**
 * The filters this page's REQ carries: shared/query.js's construction plus the page state.
 * Shared with exportText() so the filters shown are the filters sent.
 */
function buildFilters(text, limit) {
  return filtersFor(text, { kinds: tab.kinds, limit, searchString });
}

/**
 * The feed's ask: the same builder with nothing to say but its kinds. The sort menu and the
 * spam switch do not ride along, since either would put an order under a heading that says "latest".
 */
const feedFilters = (limit) =>
  filtersFor("", { kinds: feedKinds(tab.kinds), limit, searchString: feedSearchString });

/**
 * The feed's whole NIP-50 string: a lens declaration or nothing; a lensless read has to say so or
 * the relay refuses it.
 */
const feedSearchString = () => askString("");

/** One event, one card, however many filters it answered; arrival order is kept. */
function uniqueById(events) {
  const seen = new Set();
  const out = [];
  for (const e of events) {
    if (!e || seen.has(e.id)) continue;
    seen.add(e.id);
    out.push(e);
  }
  return out;
}

/**
 * [deep] is the full results view: it fetches reply lines and the provenance row, which the
 * type-ahead does not pay for per keystroke.
 */
async function search(text, limit, deep, signal) {
  await ensureLogin();
  const filters = buildFilters(text, limit);
  // Through the cache, so Enter reuses the popup's answer. [signal] is the popup's, so an
  // overtaken type-ahead is closed at the relay.
  const answer = await asks.take(filters, () => relay.req(filters, undefined, { signal }));
  // `complete` is EOSE, not the timeout. shared/relay.js marks the array and uniqueById
  // returns a new one, so read it first.
  return {
    ...hydrate(uniqueById(answer), deep, { row: deep ? "own" : "keep" }),
    text,
    complete: answer.complete !== false,
    asked: limit,
    // The raw count: a relay that repeats an event would otherwise end the pager early.
    got: answer.length,
  };
}

/**
 * The newest content, for the hero's preview and the feed page. The ask is over-sized and cut
 * back because NIP-01 cannot say "not a reply"; feed.js owns that arithmetic.
 */
async function fetchFeed(want) {
  await ensureLogin();
  // pickFeed dedupes by id itself.
  return hydrate(pickFeed(await relay.req(feedFilters(askFor(want))), want), false, { row: "clear" });
}

/** The lookups a list of events needs once it has arrived, shared by the search and the feed. */
function hydrate(events, deep, { row = "own" } = {}) {
  seedProfiles(events);
  // The provenance row is handed back as [row], not seeded here: seedProvenance replaces, and
  // a superseded read must not overwrite the row of the view that won.
  if (seedGroupEvents(events)) field.repaint();
  // Authors plus everyone the cards will name, not awaited. One argument on purpose:
  // namedPubkeys takes the render depth as its second, and flatMap would pass the index.
  const mentioned = events.flatMap((e) => namedPubkeys(e));
  const names = enrichProfiles([...events.filter(e => e.kind !== 0).map(e => e.pubkey), ...mentioned]);
  // The rooms those events were said in, on the same terms as the names.
  const groups = enrichGroupNames(events.map(postedTo).filter(Boolean));
  // A thread carries its own parents, and an event is ground truth about who wrote it.
  seedParentAuthors(events);
  return { events, names, groups, row: rowSeed(events, row), parents: deep ? replyParents(events) : null };
}

/**
 * The reply lines' own lookups, as a separate promise from the names so the author names do
 * not wait two more round trips.
 */
async function replyParents(events) {
  const learned = await loadParentAuthors(unknownParents(events));
  return learned + await enrichProfiles(events.map(replyPerson).filter(Boolean));
}

// ---- viewing as somebody else -------------------------------------------
// Only a pubkey with a kind 10040 is a usable observer. The set is small, so it is fetched
// once and filtered by name here.
let viewingAs = null;         // hex pubkey, or null for "as me"
let observers = null;         // [{pubkey, name}] once loaded

async function loadObservers() {
  if (observers) return observers;
  // Anonymous on purpose: the list of lenses you could switch to must not be personalised.
  const anon = await refConn();
  const lists = await anon.req({ kinds: [10040], limit: 2000 });
  const ks = [...new Set(lists.map((e) => e.pubkey))];
  // The chunks are one question, so they go out together.
  const chunks = [];
  for (let i = 0; i < ks.length; i += 200) chunks.push(ks.slice(i, i + 200));
  const answers = await Promise.all(chunks.map((c) => anon.req({ kinds: [0], authors: c, limit: c.length })));
  seedProfiles(answers.flat());
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

/** The lens state and its widget, with no search behind it; a history restore runs its own. */
function applyViewingAs(pubkey, name) {
  viewingAs = pubkey;
  $obsBox.classList.toggle("active", !!pubkey);
  // "me" only when there is a me; signed out with nobody picked says "nobody".
  $obsCurrent.textContent = pubkey ? (name || shortNpub(pubkey)) : me || mePending ? "me" : "nobody";
  $obsList.innerHTML = "";
  $obsFilter.value = "";
  renderAdvCount();
}

function setViewingAs(pubkey, name) {
  applyViewingAs(pubkey, name);
  rerun();
}

// ---- the provenance row's own lookup ------------------------------------
// A search for `kinds:[0]` is answered with kind 0 and nothing else, so the lists, labels and
// assertions a profile was found through have to be asked for: paint what is known, ask
// anonymously, repaint when it lands, never block a card.

/**
 * Whose word this lens took, from the Map already in hand. Synchronous: the first seed runs before
 * the render.
 */
const trustedNow = () => {
  const lens = viewingAs || me;
  return trustedSigners(knownProviders(lens), lens);
};

/** What the row currently says, as one string; the second seed can remove a pill as well as add one. */
function pillPrint() {
  const out = [];
  for (const [id, pills] of provenance) out.push(id, ...pills.map((p) => `${p.key}:${p.count}`));
  return out.join("|");
}

/**
 * Who owns the provenance row, per view, as a thunk [run] fires once the answer is on screen:
 * `own` is the results list, `clear` the feed and the hero's preview (a plain NIP-01 read has
 * no row), `keep` the popup, which cannot draw a pill and floats over the results list.
 */
function rowSeed(events, mode) {
  if (mode === "keep") return null;
  if (mode === "clear") return () => { forgetProvenance(); return []; };
  return () => {
    seedProvenance(events, trustedNow());
    return enrichProvenance(events);
  };
}

/**
 * Fetch the pointers this page was not sent and fold them into the row; returns how many pills
 * the page gained. Seeded from `[...events, ...pointers]`, since a spliced pointer still counts.
 */
function enrichProvenance(events) {
  const lens = viewingAs || me;
  // The epoch this row owns, moved by each half's own write so the second folds on top of the first.
  let mine = provenanceEpoch();
  const carried = [];   // every pointer folded so far, both halves

  const fold = async (opts) => {
    const got = await fetchPointers(events, lens, opts).catch(() => []);
    // Both awaits before the guard, or the await reopens the window it closes.
    const trusted = trustedSigners(await providersFor(lens).catch(() => new Map()), lens);
    if (mine !== provenanceEpoch()) return 0;
    const before = pillPrint();
    carried.push(...got);
    // Re-seeded from everything: the Map may have landed on this read.
    seedProvenance([...events, ...carried], trusted);
    mine = provenanceEpoch();
    return pillPrint() === before ? 0 : 1;
  };

  // Two asks, two repaints: the open half is far larger than the gated one.
  return [fold({ labels: false }), fold({ declarations: false })];
}

// ---- trust scores, as the active lens sees them -------------------------
// The chip on a face reads through the same key that ranks the results. Read anonymously: a
// score is a fact about a subject, and the authenticated socket is gated.
const scores = new Map();          // pubkey -> number | null (null = no score)
let scoreLensKey = null;           // whose lens `scores` was built for

/**
 * The `30382:rank` services an observer trusts, all of them in the reader's order; a `followers`
 * service cannot rank.
 */
async function rankServicesOf(observer) {
  return (await providersFor(observer)).get("30382:rank") || [];
}

/**
 * Fill in any score chips currently on the page. The provenance row makes the same kind 30382
 * read; docs/decisions/web-app.md says why the two stay apart. Every exit paints, even with
 * nothing to say: the search field's chips outlive every search.
 */
async function paintScores() {
  const lens = viewingAs || me;
  if (scoreLensKey !== lens) { scores.clear(); scoreLensKey = lens; }
  const chips = [...document.querySelectorAll(".score-chip[data-pk]")];
  if (!chips.length) return;
  const svc = lens ? await rankServicesOf(lens) : [];
  // Nobody to rank by, or a lens that ranks nothing: answered, with no number.
  if (!svc.length) { paintChips(chips); return; }
  const need = [...new Set(chips.map(c => c.dataset.pk))].filter(pk => !scores.has(pk));
  const batches = [];
  for (let i = 0; i < need.length; i += 100) batches.push(need.slice(i, i + 100));
  const conn = batches.length ? await refConn().catch(() => null) : null;
  const reads = await Promise.all(batches.map((batch) =>
    // A failed read leaves this batch unknown: an empty array is not complete.
    (conn ? conn.req({ kinds: [30382], authors: svc, "#d": batch, limit: batch.length * svc.length }) : Promise.resolve([]))
      .catch(() => [])));
  // The lens changed while these were in flight; filing them under the new lens would be permanent.
  if (scoreLensKey !== lens) return;
  // By service priority, not arrival: `authors` is an OR, so two services' cards interleave.
  const priority = new Map(svc.map((pk, i) => [pk, i]));
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i], evs = reads[i];
    const seen = new Set();
    const from = new Map();                       // d -> the rank of the service that supplied it
    for (const ev of evs) {
      const d = (ev.tags || []).find(t => t?.[0] === "d")?.[1];
      const rank = (ev.tags || []).find(t => t?.[0] === "rank")?.[1];
      if (!d) continue;
      seen.add(d);
      const pri = priority.has(ev.pubkey) ? priority.get(ev.pubkey) : Number.MAX_SAFE_INTEGER;
      if (from.has(d) && from.get(d) <= pri) continue;
      from.set(d, pri);
      scores.set(d, rank == null ? null : Number(rank));
    }
    // "No card for this pubkey" is a fact only after EOSE; a null cached here is permanent for the lens.
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
 * The whole search, written out so somebody else can argue with the order. Text rather than
 * JSON, because the reader is a person or a model.
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
  const full = buildFilters(typed, s.asked || PAGE_SIZE);
  // The order the store applies, which is not always the menu's: a typed `sort:` rides through untouched.
  const sort = effectiveSort(full[0].search ?? "");
  const people = (keys) => keys.map((k) => `${npub(k)}${nameOf(k) ? `  (${nameOf(k)})` : ""}`).join(", ");
  L.push("QUERY AS CONFIGURED");
  L.push(`  typed         ${JSON.stringify(typed)}`);
  L.push(`  terms         ${JSON.stringify(q.terms)}`);
  if (q.authors.length) L.push(`  from          ${people(q.authors)}`);
  if (q.mentions.length) L.push(`  to            ${people(q.mentions)}`);
  // A hashtag search is a union of three claims: a `t` tag, a NIP-22 comment, a NIP-32 label.
  if (q.hashtags.length) L.push(`  hashtags      ${q.hashtags.map((t) => `#${t}`).join(", ")}`);
  // As typed; the ids the filter carries are in the full filter lines below.
  if (q.scopes.length) L.push(`  scopes        ${q.scopes.map((s) => `${s.field}:${s.value}`).join(", ")}`);
  // An `h` tag holds only the id, so the host is not recoverable from the events.
  if (q.groups.length) L.push(`  groups        ${q.groups.map((g) => `group:${g}`).join(", ")}  (matched by id alone — any host's group with this id)`);
  const when = (at) => `${at}  (${new Date(at * 1000).toISOString()})`;
  if (q.since != null) L.push(`  since         ${when(q.since)}`);
  if (q.until != null) L.push(`  until         ${when(q.until)}`);
  L.push(`  tab           ${tab.label}${tab.kinds ? ` (kinds ${tab.kinds.join(", ")})` : " (all kinds)"}`);
  L.push(`  sort          ${sort || "(relevance — NIP-50 default)"}${sort && sort !== $sort.value ? "  (from the search box, not the Filters menu)" : ""}`);
  L.push(
    `  include spam  ${
      $spam.checked
        ? "yes — unranked authors included"
        : lensless()
          ? "yes — nothing signed in and nobody picked, so this read waives a lens (the relay answers no other kind)"
          : "no — trust floor applied"
    }`,
  );
  L.push(`  signed in as  ${me ? `${nameOf(me) || "(no name)"}  ${npub(me)}` : "(anonymous — no web of trust applied)"}`);
  L.push(`  ranking as    ${lens ? `${nameOf(lens) || "(no name)"}  ${npub(lens)}` : "(nobody)"}`);
  L.push(`  search string ${full[0].search == null ? "(none — no words and no sort/spam/lens to carry, so this is a plain NIP-01 read)" : JSON.stringify(full[0].search)}`);
  // Every filter of the REQ, one per line; the label stays singular for one.
  L.push(`  full filter${full.length > 1 ? "s " : "  "} ${JSON.stringify(full[0])}`);
  for (const f of full.slice(1)) L.push(`                ${JSON.stringify(f)}`);
  L.push("");
  // Scores listed once, and kept out of the events, which stay verbatim.
  const authors = [...new Set(s.hits.map((e) => e.pubkey))];
  if (lens) {
    L.push("AUTHOR SCORES UNDER THIS LENS");
    for (const a of authors) {
      const sc = scores.get(a);
      L.push(`  ${npub(a)}  ${sc == null ? "(no score)" : sc}  ${nameOf(a) || ""}`.trimEnd());
    }
    L.push("");
  }
  // Every page fetched, not the page on screen: an order is judged whole.
  L.push(`RESULTS  ${s.hits.length} event(s) over ${Math.max(1, pageCount(s.hits))} page(s) of ${PAGE_SIZE}, in the order the relay returned them.`);
  L.push(`On screen: page ${s.page + 1}.  ${s.exhausted ? "This is the whole answer — the relay had nothing past it." : "There may be more behind it; the pager had not reached the end."}`);
  L.push(`The first ${Math.min(PAGE_SIZE, s.hits.length)} came back in ${s.lastMs ?? "?"} ms; the pages after them were fetched separately.`);
  L.push("Verbatim, unmodified — these are the events the page is drawing from.");
  L.push("");
  L.push(JSON.stringify(s.hits, null, 2));
  L.push("");
  L.push("QUESTION FOR THE READER");
  // Under `sort:recent` the lens decides membership but not order, so the question changes.
  if (sort === "recent") {
    L.push("  This order is not a ranking: `sort:recent` asked for the search's");
    L.push("  own match set in time order, newest first, so a weak match sitting");
    L.push("  above a strong one is the answer working. What is worth challenging");
    L.push("  is MEMBERSHIP. The lens and the trust floor named above still decide");
    L.push("  who is IN this list — they have only stopped ordering it — and");
    L.push("  nothing on this page can show you who was dropped. Ask whether a");
    L.push("  timestamp is out of sequence, and whether somebody who belongs in a");
    L.push("  chronological list of these words is missing from it.");
  } else {
    L.push("  Is this order defensible? The relay ranks under the lens named above,");
    L.push("  by the author scores listed above — not by recency or text relevance");
    L.push("  alone. A result placed above another whose author scores higher, or a");
    L.push("  low-scoring author near the top, is worth challenging.");
  }
  L.push("  The events are verbatim: nothing has been trimmed or annotated.");
  return L.join("\n");
}

// ---- page state + wiring ---------------------------------------------------
const $q = document.getElementById("q");
const $clear = document.getElementById("clear");
const $popup = document.getElementById("popup");
const $mentions = document.getElementById("mentions");
const $results = document.getElementById("results");
const $feedPreview = document.getElementById("feed-preview");
const $chips = document.getElementById("chips");
const $sort = document.getElementById("sort");
const $spam = document.getElementById("spam");
const $whoami = document.getElementById("whoami");
const $me = document.getElementById("me");
// By class: `.hero` is what `body.searching .hero` makes a sticky toolbar.
const $hero = document.querySelector(".hero");

/** The avatar in the field: your picture signed in, a neutral mark signed out. */
function renderMe() {
  // `me` is proven; `mePending` is only what the extension named, drawn faded.
  const who = me || mePending;
  const p = who ? profiles.get(who) : null;
  // The remembered face stands in until the relay has answered (`has`).
  const kept = who && !profiles.has(who) && meFace && meFace.pubkey === who ? meFace : null;
  const pic = (p && p.picture) || (kept && kept.picture) || "";
  const nm = displayName(p) || (kept ? kept.name : "");
  $me.classList.toggle("in", !!me);
  $me.classList.toggle("pending", !!who && !me);
  if (who && pic) {
    // Rewritten only when the url changed: replacing an identical <img> blinks.
    const img = $me.querySelector("img.avatar");
    if (!img || img.getAttribute("src") !== pic) $me.innerHTML = avatarHtml(pic, who, "fill");
  } else if (who && nm) {
    $me.textContent = nm.slice(0, 2).toUpperCase();
  } else if (who) {
    // Signed in, profile not (yet) known; initials off an npub would spell "NP".
    $me.innerHTML =
      `<svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor"` +
      ` stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">` +
      `<circle cx="12" cy="12" r="9"/><path d="M12 8v4M12 16h.01"/></svg>`;
  } else {
    // "No account" is an empty seat, not something still loading.
    $me.innerHTML =
      `<svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor"` +
      ` stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">` +
      `<path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>`;
  }
  $me.title = me
    ? `Signed in as ${nm || shortNpub(me)} — click to sign out (reconnects)`
    : who
      ? `Signing in as ${nm || shortNpub(who)}…`
      : loginTried
        ? "Signed out — click to sign in with your Nostr extension"
        : "Signing in…";
  $me.setAttribute("aria-label", me ? "Sign out" : "Sign in");
}

// NIP-42 auth belongs to the socket, so signing out is a reconnect, not a flag.
$me.addEventListener("click", async () => {
  $me.classList.add("busy");
  try {
    if (me) {
      rememberSignIn(false);
      me = null;
      mePending = null;
      // Forgotten, not merely unrendered, or it flashes back on the next load.
      forgetFace();
      // A private group's label must not stay on a pill for whoever uses this tab next.
      forgetPrivateGroupNames();
      // The field's pills repaint only on a lookup that learned something, so repaint by hand.
      field.repaint();
      // The render-only half; the finally below reruns the search once.
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
    mePending = null;
    $whoami.innerHTML = `<span class="err">${esc(e.message || String(e))}</span>`;
  } finally {
    $me.classList.remove("busy");
    renderMe();
    renderWhoami();
    // The readiness panel is about one account's trust chain.
    me ? checkReadiness(me) : clearReadiness();
    rerun();
    // The hero's feed is a different list per reader, and none signed out.
    showFeedPreview();
  }
});
const $obsBox = document.getElementById("obsbox");
const $obsCurrent = document.getElementById("obscurrent");
const $obsFilter = document.getElementById("obsfilter");
const $obsList = document.getElementById("obslist");
const $obsReset = document.getElementById("obsreset");

// Loaded lazily: the observers' profiles are not on the path of a plain search.
$obsFilter.addEventListener("focus", async () => {
  renderObserverOptions($obsFilter.value);
  await loadObservers();
  renderObserverOptions($obsFilter.value);
});
$obsFilter.addEventListener("input", () => renderObserverOptions($obsFilter.value));
$obsReset.addEventListener("click", () => setViewingAs(null, null));
// Dismissed like any other popup: it is absolutely positioned over the page.
document.addEventListener("click", (e) => {
  if (!e.target.closest("#obsbox")) $obsList.innerHTML = "";
});

// ---- the advanced filters -------------------------------------------------
// The <details> element owns the disclosure; this file owns the badge, the one thing that
// admits a filter is on while the panel is shut.
const $adv = document.getElementById("adv");
const $advBtn = document.getElementById("advbtn");
const $advCount = document.getElementById("advcount");
// The idle tooltip, read off the markup rather than repeated here.
const advIdleTitle = $advBtn.title;

/** What is on, on the button: a count while shut, and the list as its title. */
function renderAdvCount() {
  const on = [];
  if ($sort.value) on.push("Sort: " + $sort.options[$sort.selectedIndex].text);
  if (viewingAs) on.push("Ranking as: " + $obsCurrent.textContent);
  // The reader's own switch only; a waiver forced by having no lens is not a filter they set.
  if ($spam.checked) on.push("Spam included");
  $advCount.textContent = String(on.length);
  $advCount.hidden = !on.length;
  $advBtn.title = on.length ? on.join(" · ") : advIdleTitle;
}

// Dismissed like the popups beside it. A click in the observer list is inside #adv.
document.addEventListener("click", (e) => {
  if ($adv.open && !e.target.closest("#adv")) $adv.open = false;
});

// One Escape handler for the things stacked here, innermost first.
document.addEventListener("keydown", (e) => {
  if (e.key !== "Escape") return;
  // The <dialog> closes itself on this press (after dispatch, so `open` still reads true here).
  if ($help.open) return;
  // The list is drawn inside the panel; one key closes one thing.
  if ($obsList.childElementCount) { $obsList.innerHTML = ""; $obsFilter.blur(); return; }
  if (!$adv.open) return;
  // Escape also closes the search popup, and must not yank the caret from the field.
  const inside = $adv.contains(document.activeElement);
  $adv.open = false;
  if (inside) $advBtn.focus();
});

// ---- the syntax sheet -----------------------------------------------------
// A modal <dialog> already closes on Escape, traps Tab and returns focus; what is left is the
// close button, the backdrop click and the shortcut in.
const $help = document.getElementById("help");
const $helpBtn = document.getElementById("helpbtn");

/** Open it, or shut it if it is already up: the `?` press is a toggle. */
function toggleHelp() {
  if ($help.open) $help.close();
  else $help.showModal();
}

$helpBtn.addEventListener("click", () => $help.showModal());
document.getElementById("helpclose").addEventListener("click", () => $help.close());
// A backdrop click is dispatched at the <dialog> itself; one on the sheet hits a child.
$help.addEventListener("click", (e) => { if (e.target === $help) $help.close(); });

// Back closes it, with no history entry of its own; close() on a shut dialog is a no-op.
window.addEventListener("popstate", () => $help.close());

// `?` opens it, guarded like `/`. Shift is how the key is reached, so it is not refused.
document.addEventListener("keydown", (e) => {
  if (e.key !== "?" || e.metaKey || e.ctrlKey || e.altKey) return;
  const el = document.activeElement;
  if (isTyping(el)) return;
  e.preventDefault();
  toggleHelp();
});

// ---- the field renders its own contents ----------------------------------
// `$q` becomes a field that draws the people in it as faces (searchfield.js); everything below
// still reads and writes `$q.value`. Mounted before applyUrl() assigns `$q.value`.

/**
 * Who the picker offers for a half-typed `from:`/`to:`: a NIP-50 profile search on the
 * authenticated socket, ranked by the reader. A pasted hex key resolves to itself.
 */
async function lookupAuthors(partial) {
  const direct = pubkeyParam(partial);
  if (direct) { await enrichProfiles([direct]).catch(() => {}); return [direct]; }
  await ensureLogin();
  const events = await relay.req({ kinds: [0], search: askString(partial), limit: 12 });
  seedProfiles(events);
  return [...new Set(events.map((e) => e.pubkey))];
}

// The reader's own kind 10009, held for the session. Read on the authenticated socket, so it
// comes back empty for a reader with no scores mirrored here (readiness.js explains instead).
// Cached only when the relay answered.
let ownGroupList = null;   // the parsed candidates, from the public tags
let ownGroupsFor = null;   // whose they are, so signing out drops them
let ownGroupLock = null;   // sealed(): the encrypted half, or null if there is none
let ownGroupSecret = null; // the rows behind that lock, once it has been opened
let unlockAsk = null;      // the in-flight decrypt, so N keystrokes are one prompt
let unlockDenied = false;  // the extension said no; do not ask again unasked

/** Every cached thing about the reader's own list, dropped when the reader changes. */
function forgetOwnGroups() {
  ownGroupList = null;
  ownGroupsFor = null;
  ownGroupLock = null;
  ownGroupSecret = null;
  unlockAsk = null;
  unlockDenied = false;
}

async function ownGroupCandidates() {
  const who = me;
  if (!who) { forgetOwnGroups(); return []; }
  if (ownGroupsFor !== who) forgetOwnGroups();
  if (ownGroupsFor === who && ownGroupList) return ownGroupList;
  let evs = [];
  try {
    evs = await relay.req({ kinds: [10009], authors: [who], limit: 1 });
  } catch (e) { return ownGroupList && ownGroupsFor === who ? ownGroupList : []; }
  if (evs.complete !== true) return [];
  const rows = ownGroups(evs[0]);
  ownGroupList = rows;
  ownGroupLock = sealedGroups(evs[0]);
  ownGroupsFor = who;
  return rows;
}

/**
 * Whether the extension can open a payload of this scheme; "unsupported" and "denied" want
 * different words.
 */
const canDecrypt = (scheme) => {
  const api = window.nostr && window.nostr[scheme];
  return !!(api && typeof api.decrypt === "function");
};

/**
 * Open the private half of the reader's own group list: one prompt, ever, and a refusal is
 * final until the reader clicks the retry row. The peer key is the reader's own pubkey.
 */
async function unlockOwnGroups() {
  if (ownGroupSecret) return ownGroupSecret;
  if (!ownGroupLock || !me) return [];
  if (unlockAsk) return unlockAsk;
  const lock = ownGroupLock;
  const who = me;
  unlockAsk = (async () => {
    const plain = await window.nostr[lock.scheme].decrypt(who, lock.content);
    return privateGroups(plain);
  })()
    .then((rows) => {
      // Cached even when empty, so the reader is not re-prompted to learn that.
      ownGroupSecret = rows;
      ownGroupLock = null;
      unlockDenied = false;
      return rows;
    })
    .catch(() => {
      // Refused, dismissed or failed: the lock stays, and only a click asks again.
      unlockDenied = true;
      return [];
    })
    .finally(() => {
      unlockAsk = null;
      // The picker drew before this landed; `field` is declared below, and the catch covers the TDZ.
      try { field.refreshGroups(); } catch (e) { /* nothing mounted, so nothing is showing */ }
    });
  return unlockAsk;
}

/** The picker's "unlock" row, clicked: the reader asking for the dialog back. */
async function retryUnlockGroups() {
  unlockDenied = false;
  unlockAsk = null;
  await unlockOwnGroups();
}

/** What the picker should say about the locked half, or null; only `denied` a click can fix. */
function groupLockState() {
  if (!ownGroupLock || !me) return null;
  if (!canDecrypt(ownGroupLock.scheme)) return { state: "unsupported", scheme: ownGroupLock.scheme };
  if (unlockDenied) return { state: "denied" };
  return { state: "asking" };
}

/**
 * Which groups the picker offers for a half-typed `group:`: the reader's own kind 10009 and a
 * NIP-50 search over kind 39000, never folded into one row (shared/groups.js). The decrypt
 * prompt is raised here, on first use of `group:`, not on page load.
 */
async function lookupGroups(partial) {
  await ensureLogin().catch(() => {});
  const own = await ownGroupCandidates().catch(() => []);
  // Started, not awaited: the public rows return now, and the picker re-asks when the dialog is answered.
  if (ownGroupLock && canDecrypt(ownGroupLock.scheme) && !unlockDenied) unlockOwnGroups();
  const secret = ownGroupSecret || [];
  let found = [];
  try {
    // `group:` alone is "show me my groups", not a match-all over every 39000.
    if (partial) found = await relay.req({ kinds: [39000], search: askString(partial), limit: 12 });
  } catch (e) { found = []; }
  const meta = found.map(metaGroup).filter(Boolean);
  const hosts = [...new Set(meta.map((g) => g.host).filter(Boolean))];
  if (hosts.length) await enrichProfiles(hosts).catch(() => {});
  const rows = rankGroups(partial, { own: [...own, ...secret], meta });
  // Rows go in as candidates, so groupnames.js still tells a list name from a corpus name.
  seedGroupNames(rows);
  return { rows, lock: groupLockState() };
}

/** Every human edit of the field, whatever made it — typing, paste, a pick. */
function onQueryEdit() {
  const text = $q.value.trim();
  document.body.classList.toggle("has-query", text.length > 0);
  clearTimeout(debounceTimer);
  // The pickers and the preview share one square of screen and the arrow keys.
  if (!text || field.picking) { closePopup(); return; }
  armPopup(text);
}

// paintScores goes in because which lens fills a chip is app state.
const field = mountSearchField($q, $mentions, {
  lookup: lookupAuthors, lookupGroup: lookupGroups, unlockGroups: retryUnlockGroups,
  onEdit: onQueryEdit, onSubmit: submitField, paintScores,
});

// The last ranked ask, so Enter after a type-ahead is one search, not two.
const asks = new AskCache();

// `hitsFor` is the text `hits` answers; the box may say something else by now.
const s = {
  requestId: 0, hits: [], hitsFor: null, lastMs: null, loading: false, error: null, complete: true,
  // ---- the pager, five more facts about that same array ----------
  page: 0,           // the slice of it on screen, 0-based
  asked: 0,          // the prefix the last ask named; what canGrow() measures against
  exhausted: false,  // the relay proved there is nothing past what we hold (paging.js's drained)
  more: null,        // this view's query, re-askable at a longer limit; null on a view that cannot page
  preloading: false, // one widening ask at a time
};

/**
 * The type-ahead's answers, kept apart from the results view's, so a keystroke over a page of
 * results does not replace `hits` under the pager.
 */
const pop = {
  requestId: 0, hits: [], hitsFor: null, lastMs: null, loading: false, error: null, complete: true,
  // ---- the ask in flight, which the results view may take over or close ----
  inFlightFor: null, // the text the in-flight ask is for; null when none is
  abort: null,       // its AbortController — see runFull()
};

/** Forget an answer without drawing anything: a view that is being left. */
function forget(st) {
  st.requestId++;
  st.hits = []; st.hitsFor = null; st.error = null; st.loading = false;
}

let debounceTimer = null;
let activeKey = null;

/** The text waiting for the popup while an ask is in flight, latest only; one type-ahead ask at a time. */
let popupQueued = null;

/**
 * Only what the avatar and the "ranking as" control cannot show: an error, or being signed out by
 * choice.
 */
function renderWhoami() {
  renderMe();
  $whoami.textContent =
    loginTried && !me && !wantsSignIn()
      ? viewingAs
        ? "signed out — ranked through the observer you picked"
        : "signed out — the whole corpus, unranked (include:spam)"
      : "";
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

/** "Nothing matched" against "nothing could match": an `until` before `since` excludes everything. */
function emptyWindow(text) {
  const q = parseQuery(text || "");
  return q.since != null && q.until != null && q.since > q.until;
}

// `empty` is what a view that was asked no question says; the feed has none.
function statusBody(st, placeholder, empty) {
  if (st.error) return `<div class="error">${esc(st.error)}</div>`;
  if (st.loading && !st.hits.length) return placeholder;
  if (!st.hits.length) {
    if (empty) return empty;
    const why = emptyWindow(st.hitsFor)
      ? "The window is empty — until: is before since:, so nothing can fall inside it."
      : "Try a different term, or widen the filter above.";
    return `<div class="empty"><b>No results</b>${esc(why)}</div>`;
  }
  return null;
}

function renderPopup() {
  const busy = pop.loading || pop.error;
  const meta = busy ? "" : [`${pop.hits.length}`, pop.lastMs != null ? `${pop.lastMs} ms` : ""].filter(Boolean).join(" · ");
  const head = `<div class="popup-head"><span>Results</span><span class="timing">${meta}</span></div>`;
  const body = statusBody(pop, skelRows(3)) ?? pop.hits.slice(0, POPUP_LIMIT).map(popupRow).join("");
  $popup.innerHTML = head + body;
  paintScores();
}

/**
 * Which corpus this feed is, in one line; the title answers why the "ranking as" lens does not
 * apply here.
 */
const lensNote = () =>
  `<div class="prov" title="The feed is a plain NIP-01 read, so the &quot;ranking as&quot; lens — a NIP-50 observer: extension — applies to searches, not here.">` +
  (me
    ? "newest first, through your own web of trust — the relay drops authors below your trust floor"
    : "newest first, the whole corpus — sign in (the avatar in the field) to read it through your web of trust") +
  `</div>`;

/** "Latest", and what it was narrowed to: the lit chip can be off screen on a phone. */
const feedTitle = () => (tab.kinds ? `Latest &middot; ${esc(tab.label)}` : "Latest");

/** The list heading every card list wears: a name, and whatever goes right. */
const listHead = (title, right) =>
  `<div class="list-head"><div class="list-title">${title}</div><div class="list-right">${right}</div></div>`;

/** The cards, and the repaints that must follow them: score chips, the cursor and nip05 marks. */
function paintList($el, html) {
  $el.innerHTML = html;
  paintScores();
  paintCursor();
  watchNip05();
}

/** The feed page: the same cards as a search, over a list nobody searched for. */
function renderFeed() {
  const stats = s.error ? "" : `${s.hits.length} event${s.hits.length === 1 ? "" : "s"} · ${s.lastMs ?? "?"} ms`;
  // An empty feed is about the index or the trust floor, never a query.
  const empty = `<div class="empty"><b>Nothing here yet</b>` +
    (me
      ? "This relay holds no recent posts from anyone your web of trust reaches."
      : "This relay's index holds no recent posts of these kinds.") +
    (tab.kinds ? ` The ${esc(tab.label)} filter is on — pick Everything for the whole feed.` : "") +
    `</div>`;
  // No Export button: a time-ordered list has no ranking to defend.
  const body = statusBody(s, skelCards(4), empty) ?? s.hits.map((ev) => card(ev)).join("");
  paintList($results, listHead(feedTitle(), `<span class="list-stats">${stats}</span>`) + lensNote() + body);
}

// ---- the same feed, three cards, under the hero ---------------------------
// Only for a signed-in reader: signed out the same read is the firehose, which the hero does
// not stand behind. Its own state of the same shape as `s`, driven by the same run().
const feedPreview = { requestId: 0, hits: [], hitsFor: null, lastMs: null, loading: false, error: null, reader: null, tab: null };

function hideFeedPreview() {
  feedPreview.requestId++;   // whatever is in flight must not reveal this again
  $feedPreview.hidden = true;
  $feedPreview.innerHTML = "";
  document.body.classList.remove("has-feed-preview");
}

/** Where "see more" goes: a parameter on the root, not a `/feed` path, so the root serves it from cold. */
const FEED_URL = "/?feed=1";

/** That url carrying the chip, `/?feed=1&tab=media`; "Everything" writes no parameter. */
const feedUrl = (t = tab) => FEED_URL + (t.kinds ? `&tab=${t.slug}` : "");

/** Does [href] name the feed, whichever chip it carries? */
const isFeedHref = (href) => href === FEED_URL || href.startsWith(FEED_URL + "&");

function renderFeedPreview() {
  // Nothing and nothing coming, including the error path: the hero stays the hero.
  if (!feedPreview.loading && !feedPreview.hits.length) { hideFeedPreview(); return; }
  const body = feedPreview.hits.length
    ? feedPreview.hits.map((ev) => card(ev)).join("")
    : skelCards(PREVIEW_CARDS);
  // The link carries the chip: "see more" of these three.
  paintList($feedPreview, listHead(feedTitle(), `<a class="feed-more" href="${feedUrl()}">See more &rarr;</a>`) + body);
}

/**
 * Draw the preview if this is the hero and there is a reader. It keeps the cards through the
 * wait, except across a change of reader or of chip.
 */
async function showFeedPreview() {
  const my = feedPreview.requestId;
  try { await ensureLogin(); } catch (e) {}
  // Anything that left the hero, and any second call, moved the counter on.
  if (my !== feedPreview.requestId) return;
  if (!me || !$results.hidden) { hideFeedPreview(); return; }
  if (feedPreview.reader !== me || feedPreview.tab !== tab) {
    feedPreview.hits = [];
    feedPreview.reader = me;
    feedPreview.tab = tab;
  }
  $feedPreview.hidden = false;
  document.body.classList.add("has-feed-preview");
  run(feedPreview, () => fetchFeed(PREVIEW_CARDS), KEEP, renderFeedPreview);
}

/** Where the page on screen sits in the answer. The denominator appears only once `exhausted` says so. */
function rangeLabel(shown) {
  const total = s.hits.length;
  // The reader outran the preload; the page they asked for is in flight.
  if (!shown.length) return `page ${s.page + 1}`;
  // A plain count beside a live Next button would claim to hold the whole answer.
  if (!lastPage(s.hits, s)) return `${total} result${total === 1 ? "" : "s"}`;
  const from = s.page * PAGE_SIZE + 1;
  return `${from}–${from + shown.length - 1}${s.exhausted ? ` of ${total}` : ""}`;
}

/**
 * The pager, redrawn without touching the cards: an innerHTML rewrite of the list re-arms the
 * lazy-media observers and takes any selection with it.
 */
function repaintPager() {
  const foot = $results.querySelector(".pager-foot");
  if (!foot) { renderResults(); return; }
  foot.innerHTML = pagerHtml();
}

/**
 * Back, the pages held, and forward. Numbers rather than infinite scroll so Back can return to
 * a place (`?page=`). The page past the buffer is "…", one we can ask for but do not hold.
 */
function pagerHtml() {
  const loaded = pageCount(s.hits);
  const last = lastPage(s.hits, s);
  const btn = (page, label, cls, on, extra = "") =>
    `<button type="button" class="pg${cls}"${on ? ` data-page="${page}"` : " disabled"}${extra}>${label}</button>`;
  const note = !s.exhausted && !canGrow(s.asked)
    ? `<div class="pg-note">${MAX_ASK} results deep is as far as this page follows a ranking. ` +
      `Narrow the search — a word, a <code>from:</code>, a date — to see past it.</div>`
    : "";
  // One page and nothing behind it is furniture; the note cannot be up yet.
  if (last <= 0) return "";
  const cells = [];
  for (let i = 0; i <= last; i++) {
    const here = i === s.page;
    cells.push(btn(i, i < loaded ? String(i + 1) : "…", here ? " on" : "",
                   true, here ? ' aria-current="page"' : ""));
  }
  return `<nav class="pager" aria-label="Result pages">` +
    btn(s.page - 1, "‹ Prev", " pg-nav", s.page > 0) +
    `<div class="pg-nums">${cells.join("")}</div>` +
    btn(s.page + 1, "Next ›", " pg-nav", s.page < last) +
    `</nav>` + note;
}

/**
 * One page of the results and the pager; the cut happens here because a NIP-01 filter has a `limit`
 * and no offset.
 */
function renderResults() {
  const shown = pageOf(s.hits, s.page);
  const stats = s.error ? "" : `${rangeLabel(shown)} · ${s.lastMs ?? "?"} ms`;
  const right = `<span class="list-stats">${stats}</span>` +
    `<button type="button" id="export" class="export" title="Download this search and its results as text">Export</button>`;
  // Not `.map(card)`: map would hand the index as card's opts argument. A page with no cards
  // under a buffer that has some is the reader having outrun the preload.
  const body = statusBody(s, skelCards(4)) ??
    (shown.length ? shown.map((ev) => card(ev)).join("") : skelCards(3)) +
    `<div class="pager-foot">${pagerHtml()}</div>`;
  paintList($results, listHead(esc(tab.label), right) + body);
}

/** Turn to page [n], from the pager's clicks; a URL restore comes in through runFull(). */
function goPage(n) {
  if (!Number.isInteger(n)) return;   // a disabled button carries no data-page
  const want = Math.max(0, Math.min(n, lastPage(s.hits, s)));
  if (want === s.page) return;
  s.page = want;
  // A page turn is a place: Back undoes it. Pushed here rather than through syncUrl(), which reads the box.
  const url = pageUrl();
  if (url !== location.pathname + location.search) history.pushState(null, "", url);
  renderResults();
  // Back to the top of the list, clear of the sticky toolbar.
  $results.style.scrollMarginTop = Math.ceil($hero.getBoundingClientRect().height) + 12 + "px";
  $results.scrollIntoView({ block: "start" });
  preload();
}

/**
 * Land the reader on the last page there is when the one they asked for stopped existing.
 * Returns whether it moved, because moving is a repaint the caller owes. replaceState:
 * pushing would leave a phantom entry at the same page.
 */
function settlePage() {
  const end = lastPage(s.hits, s);
  if (s.page <= end) return false;
  s.page = end;
  history.replaceState(null, "", pageUrl());
  return true;
}

/**
 * The url of this page of these results, named from `hitsFor` and never from the search box,
 * which may hold the next query half-typed.
 */
const pageUrl = () => currentUrl(s.hitsFor || $q.value.trim(), true, s.page);

/**
 * The three pages ahead of the one on screen, fetched before anybody asks: the ask that reaches
 * page four is the one that reached page one, only longer. It does not bump requestId: the
 * first ask's late lookups must still land.
 */
async function preload() {
  if (!s.more || s.preloading || s.exhausted || s.error) return;
  if (covered(s.hits, s.page) || !canGrow(s.asked)) return;
  const want = askLimit(s.page);
  if (want <= s.asked) return;
  const myId = s.requestId;
  const ask = s.more;
  // Only the success path asks again; an error that re-kicked would loop.
  let again = false;
  s.preloading = true;
  try {
    const found = await ask(want);
    if (myId !== s.requestId) return;
    const grown = mergePages(s.hits, found.events);
    // Did the reader outrun us? Asked of the old buffer.
    const waiting = !pageOf(s.hits, s.page).length;
    // What the reader is looking at before the fold; a part page can fill up.
    const wasOnScreen = pageOf(s.hits, s.page).map((e) => e.id).join();
    let moved = false;
    s.exhausted = drained({
      complete: found.complete, got: found.got ?? found.events.length, asked: want, added: grown.length - s.hits.length,
    });
    s.asked = want;
    s.hits = grown;
    moved = settlePage();
    // A preload lands under the reader, so an open json panel is kept unless the page on screen changed.
    const sameCards = wasOnScreen === pageOf(s.hits, s.page).map((e) => e.id).join();
    if (moved || waiting || !sameCards) {
      if (moved || waiting || !document.querySelector(".raw-body:not([hidden])")) renderResults();
      else repaintPager();
    } else repaintPager();
    paintLate(s, myId, [found.names, found.groups, ...(found.row ? found.row() : []), found.parents].filter(Boolean), renderResults);
    again = true;
  } catch (e) {
    // A failed widening is not a failed search; nothing is marked exhausted, so Next tries again.
  } finally {
    s.preloading = false;
  }
  // Once more, in case the reader moved during the round trip; each pass asks for strictly more, so
  // this terminates.
  if (again && myId === s.requestId) preload();
}

/** The pager, back to a view that has no pages: the hero, the feed, an entity. */
function resetPages() {
  s.page = 0; s.asked = 0; s.exhausted = false; s.more = null; s.preloading = false;
}

/** Whether the list already on screen survives the wait for the next answer. */
const REPLACE = false, KEEP = true;

// One ask, rendered. `st` is the state it drives; `fetch` is a thunk. The stale-answer guard,
// the timing, the skeleton and the late repaints are the same for every caller.
async function run(st, fetch, keep, render) {
  const myId = ++st.requestId;
  st.loading = true; st.error = null;
  if (!keep) { st.hits = []; st.hitsFor = null; }
  render();
  const t0 = performance.now();
  let late = [];
  try {
    const found = await fetch();
    if (myId !== st.requestId) return;
    // The feed's `hitsFor` is null, so focus can never reopen the popup on it.
    st.hits = found.events; st.hitsFor = found.text ?? null;
    // What the pager needs: how far this ask reached, and whether the relay ran out first (EOSE,
    // not our own timeout). Only an ask that named its limit gets the second half.
    st.complete = found.complete !== false;
    if (found.asked != null) {
      st.asked = found.asked;
      st.exhausted = drained({ complete: st.complete, got: found.got ?? found.events.length, asked: found.asked, added: found.events.length });
    }
    // After the guard: the row seed replaces rather than adds. See rowSeed.
    late = [found.names, found.groups, ...(found.row ? found.row() : []), found.parents].filter(Boolean);
  } catch (e) {
    if (myId !== st.requestId) return;
    st.error = e.message || String(e); st.hits = []; st.hitsFor = null;
  }
  st.lastMs = Math.round(performance.now() - t0); st.loading = false;
  render();
  paintLate(st, myId, late, render);
  // Whether this answer is the one on screen; a superseded search must not page.
  return myId === st.requestId;
}

/**
 * The lookups that land after the list, each painting when it arrives and only if it learned
 * something. Skipped while a raw event is expanded, since a re-render would collapse it.
 */
function paintLate(st, myId, late, render) {
  for (const lookup of late) {
    lookup.then((learned) => {
      if (!learned || myId !== st.requestId) return;
      // The field's chips are named from the same cache.
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
  popupQueued = null; // a text that queued for the popup is not run into a popup that closed
  $popup.classList.remove("open");
  // Two listboxes hang off one combobox; when the picker is up these describe it.
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

// Never over the people picker: a debounced type-ahead can land after the `from:` was started.
function runPopup(text) {
  if (field.picking) return;
  if (pop.loading) { popupQueued = text; return; }
  popupQueued = null;
  openPopup();
  const abort = new AbortController();
  pop.inFlightFor = text;
  pop.abort = abort;
  // askLimit(0), the results view's first ask, so Enter reuses this answer.
  run(pop, () => search(text, askLimit(0), false, abort.signal), KEEP, renderPopup).then((live) => {
    if (pop.abort === abort) { pop.inFlightFor = null; pop.abort = null; }
    const next = popupQueued;
    popupQueued = null;
    // Only while the popup is still the view and the box still says the text, and through the
    // debounce again.
    if (live && next != null && next !== text && $q.value.trim() === next) armPopup(next);
  });
}

/** Run the popup for [text] once the box has held it for a debounce. */
function armPopup(text) {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => { if ($q.value.trim() === text) runPopup(text); }, DEBOUNCE_MS);
}

/**
 * The full results view, opening on [page]: 0 for every way in but a restore. The first ask is
 * firstAsk(page) so a deep-linked `?page=4` is drawable the moment the answer lands.
 */
function runFull(text, page = 0) {
  clearTimeout(debounceTimer); // a type-ahead in flight would re-open the popup over the results
  pop.requestId++;             // and its answer must not repaint a popup this view has closed
  // A type-ahead for this text is this view's answer (shared/asks.js). One for any other text
  // holds the connection's ranked-read lane at the relay, so it is closed and the submit takes it.
  if (pop.abort && pop.inFlightFor !== text) pop.abort.abort();
  cancelEntity(); // a search launched from an entity page must not be painted over by its fetch
  hideFeedPreview();   // the hero's preview is not part of a results page
  document.title = "SearchOverTrust";
  closePopup();
  field.close(); // Enter with nothing highlighted searches; the picker is done
  // j/k are letters while a caret is in a text field; on a phone this also puts the keyboard away.
  $q.blur();
  $results.hidden = false;
  document.body.classList.add("searching");
  document.body.classList.remove("feed"); // searching from the feed leaves it
  // A new answer, so the pager starts over. Before syncUrl(), which writes the page number.
  resetPages();
  s.page = page;
  const ask = firstAsk(page);
  // The pager's handle on this view. A thunk, so every control change re-runs from page one.
  s.more = (limit) => search(text, limit, true);
  // Every way into the full view converges here, so this is where the URL learns about it.
  syncUrl();
  run(s, () => search(text, ask, true), REPLACE, renderResults).then((live) => {
    if (!live || s.error) return;
    // If the page run() drew does not exist, draw the one that does.
    if (settlePage()) renderResults();
    preload();
  });
}

/** The feed page: the newest [PAGE_CARDS] content events, full cards. */
function runFeed() {
  clearTimeout(debounceTimer);
  pop.requestId++;
  // No pager: the feed's only cursor would be `until`, a different mechanism.
  resetPages();
  cancelEntity();
  hideFeedPreview();   // the preview and the page are the same feed; one at a time
  document.title = `SearchOverTrust — latest${tab.kinds ? ` ${tab.label.toLowerCase()}` : ""}`;
  closePopup();
  field.close();
  $q.blur();   // a full-page list takes the keyboard, exactly as a search does
  $results.hidden = false;
  // `feed` hides the Filters half of the bar, whose extensions this view does not send, and keeps
  // the chips.
  document.body.classList.add("searching", "feed");
  syncUrl();
  run(s, () => fetchFeed(PAGE_CARDS), REPLACE, renderFeed);
}

/**
 * A picked popup result opens as its own page, through the same pushState + applyUrl() pair as any
 * internal link.
 */
function openPicked(ev) {
  // selfHref, not a second spelling of the same rule; it also guards a missing id.
  const href = ev && selfHref(ev);
  if (href) navigate(href);
}
/** Is the feed page the view on screen? One spelling, three readers. */
const onFeed = () => document.body.classList.contains("feed");

function rerun() {
  // The entity view gates on the reader's web of trust, and `$q` is empty on it.
  const seg = entitySeg();
  if (seg) { openEntity(seg); return; }
  // The feed's answer changes with who is asking and which chip is on.
  if (onFeed()) { runFeed(); return; }
  const text = $q.value.trim();
  // The hero's preview is the same feed, so it answers the same chip. Nothing goes to the URL.
  if (!text) { if ($results.hidden) showFeedPreview(); return; }
  if (!$results.hidden) runFull(text);
  else if ($popup.classList.contains("open")) runPopup(text);
}

/** Back to the landing hero. The view change only; history restore uses this too, and must not push. */
function showHero() {
  clearTimeout(debounceTimer);
  cancelEntity(); // same for "← Search" out of an entity page mid-fetch
  document.title = "SearchOverTrust";
  forget(s); forget(pop);
  s.lastMs = null;
  resetPages();
  $q.value = "";
  $results.hidden = true;
  $results.innerHTML = "";
  document.body.classList.remove("searching", "has-query", "feed");
  closePopup();
  // Every route to the hero goes through here, so the preview is drawn here.
  showFeedPreview();
}

/** The hero as a navigation (clear button, brand click), pushed so Back returns to the search. */
function reset() {
  showHero();
  syncUrl();
  $q.focus();
}

/**
 * Enter in the search box, however it arrived; searchfield.js also calls this off `beforeinput` for
 * a phone's action key.
 */
function submitField() {
  // An arrowed-to row is a selection; a plain Enter is the full search.
  if ($popup.classList.contains("open") && activeKey != null) { openPicked(pop.hits[activeKey]); return; }
  const text = $q.value.trim();
  if (text) runFull(text);
}

$q.addEventListener("keydown", (e) => {
  // The picker gets first refusal while open.
  if (field.handleKey(e)) return;
  if (e.key === "Enter") {
    e.preventDefault();
    submitField();
  } else if (e.key === "ArrowDown") {
    if ($popup.classList.contains("open")) { e.preventDefault(); setActive((activeKey ?? -1) + 1); }
  } else if (e.key === "ArrowUp") {
    if ($popup.classList.contains("open")) { e.preventDefault(); setActive((activeKey ?? popupItems().length) - 1); }
  } else if (e.key === "Escape") {
    closePopup();
  }
});

$clear.addEventListener("click", reset);

// "/" focuses the search box, except while typing and while the syntax sheet is up: a
// document-level keydown still arrives under a modal <dialog>.
document.addEventListener("keydown", (e) => {
  if (e.key !== "/" || e.metaKey || e.ctrlKey || e.altKey) return;
  const el = document.activeElement;
  if (!el || isTyping(el) || $help.open) return;
  e.preventDefault();
  $q.focus();
  $q.select();
});

// ---- j and k walk the results --------------------------------------------
// The cursor is an event id, not a row number: names and reply parents repaint the list after
// the cards land.
let cursorId = null;

// Only cards that open something: a permalink card carries no `data-href`.
const cursorCards = () => [...$results.querySelectorAll(".result[data-href]")];
const cursorAt = (cards) => cards.findIndex((el) => el.dataset.id === cursorId);

/**
 * Draw the cursor after every render. An id the list no longer holds is dropped, but only
 * against a list that has cards: every search paints a skeleton first.
 */
function paintCursor() {
  const cards = cursorCards();
  const at = cursorAt(cards);
  if (at < 0 && cards.length) cursorId = null;
  cards.forEach((el, i) => el.classList.toggle("cursor", i === at));
}

function moveCursor(cards, delta) {
  const el = cards[stepIndex(cursorAt(cards), cards.length, delta)];
  cursorId = el.dataset.id;
  paintCursor();
  // `nearest` knows nothing about the sticky toolbar.
  el.style.scrollMarginTop = Math.ceil($hero.getBoundingClientRect().height) + 12 + "px";
  el.style.scrollMarginBottom = "12px";
  el.scrollIntoView({ block: "nearest" });
}

document.addEventListener("keydown", (e) => {
  const move = navKey(e, document.activeElement);
  if (!move || $results.hidden || $help.open) return;
  const cards = cursorCards();
  // Nothing to walk, so the key is not ours; decided before preventDefault().
  if (!cards.length) return;
  if (move === "open") {
    // Enter is ours only while the cursor is on something.
    const el = cards[cursorAt(cards)];
    if (!el) return;
    e.preventDefault();
    navigate(el.dataset.href);
    return;
  }
  e.preventDefault();
  // A type-ahead preview left open over the list is furniture now.
  closePopup();
  moveCursor(cards, move === "next" ? 1 : -1);
});

$popup.addEventListener("mousedown", (e) => {
  const item = e.target.closest(".popup-item");
  if (!item) return;
  e.preventDefault();
  openPicked(pop.hits[Number(item.dataset.idx)]);
});

document.addEventListener("click", (e) => { if (!e.target.closest(".search-wrap")) closePopup(); });
$q.addEventListener("focus", () => {
  // Never over a half-written `from:`/`to:`; the picker owns that box.
  if (field.picking) return;
  const text = $q.value.trim();
  if (pop.hits.length && pop.hitsFor === text && text && $results.hidden) openPopup();
});

// Clicks inside a list of cards, for the results view and the hero's preview.
// `hitsOf` is a thunk because each answer replaces the preview's array.
const cardClicks = (hitsOf) => (e) => {
  // The pager's buttons are delegated here, before the `button` bail-out below.
  const pg = e.target.closest(".pg");
  if (pg) { goPage(Number(pg.dataset.page)); return; }
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
  // The provenance row's overflow: the pills are already in the DOM behind `hidden`.
  const more = e.target.closest(".prov-more");
  if (more) {
    const open = more.getAttribute("aria-expanded") === "true";
    for (const pill of more.parentElement.querySelectorAll(".prov-pill.extra")) pill.hidden = open;
    more.setAttribute("aria-expanded", String(!open));
    more.textContent = open ? more.dataset.label : "show fewer";
    return;
  }
  const btn = e.target.closest(".raw-toggle");
  if (btn) {
    const box = btn.parentElement.querySelector(".raw-body");
    if (!box.hidden) { box.hidden = true; btn.textContent = "json"; return; }
    const ev = hitsOf().find((h) => h.id === btn.dataset.id);
    // Serialised only when asked for, and only once.
    if (!box.textContent) box.textContent = ev ? JSON.stringify(ev, null, 2) : "(no longer in the current results)";
    box.hidden = false;
    btn.textContent = "hide json";
    return;
  }
  // The card itself opens its own page. Not over a real control, a text selection, or the json
  // block a reader just opened.
  if (e.target.closest("a, button, input, textarea, select, label, audio, video, summary, .raw")) return;
  // `getSelection()` may return null, and String(null) is truthy.
  const sel = window.getSelection && window.getSelection();
  if (sel && String(sel).trim()) return;
  const art = e.target.closest(".result[data-href]");
  if (art) { e.preventDefault(); navigate(art.dataset.href); }
};
$results.addEventListener("click", cardClicks(() => s.hits));
$feedPreview.addEventListener("click", cardClicks(() => feedPreview.hits));

$chips.addEventListener("click", (e) => {
  const chip = e.target.closest(".chip");
  if (!chip) return;
  tab = KIND_TABS[Number(chip.dataset.tab)];
  renderChips();
  rerun();
});

$sort.addEventListener("change", () => { renderAdvCount(); rerun(); });
$spam.addEventListener("change", () => { renderAdvCount(); rerun(); });

// ---- the URL is the search ------------------------------------------------
// The results view is derived from q, tab, sort, spam and lens, so those are the location.
// Sign-in state is not among them: auth belongs to the socket. The hero is the bare path, the
// floor Back lands on; the popup never appears here.

// A restore must not re-push what it is restoring, or popstate truncates the forward stack.
let navRestoring = false;

/**
 * `?page=4` as this page counts pages: 0-based, 0 for anything that is not a page, clamped at
 * the ceiling the ask stops at.
 */
function pageParam(p) {
  const n = Number(p.get("page"));
  return Number.isInteger(n) && n > 1 ? Math.min(n - 1, MAX_PAGES - 1) : 0;
}

/**
 * The url for the page as it stands, optionally with a different query, which is what a
 * hashtag chip navigates to. Knows nothing about the feed; "where am I now" is syncUrl's question.
 */
function currentUrl(text = $q.value.trim(), showing = !$results.hidden, page = s.page) {
  const p = new URLSearchParams();
  if (showing && text) p.set("q", text);
  // One-based, because a person reads it against the pager's buttons.
  if (showing && text && page > 0) p.set("page", String(page + 1));
  if (tab.slug !== KIND_TABS[0].slug) p.set("tab", tab.slug);
  if ($sort.value) p.set("sort", $sort.value);
  if ($spam.checked) p.set("spam", "1");
  // npub, like everywhere else on the page.
  if (viewingAs) p.set("as", npub(viewingAs));
  const qs = p.toString();
  // Anchored at "/": a search launched from /note1… belongs at the root.
  return "/" + (qs ? "?" + qs : "");
}

function syncUrl() {
  if (navRestoring) return;
  // The feed url is the flag and the chip, since the view reads nothing else.
  const url = onFeed() ? feedUrl() : $results.hidden ? "/" : currentUrl();
  // Re-submitting the same search is not a second place.
  if (url === location.pathname + location.search) return;
  history.pushState(null, "", url);
}

// A NIP-19 path is the third view. The shape test only routes; entity.js validates.
const ENTITY_PATH = /^(npub|nprofile|note|nevent|naddr)1[a-z0-9]+$/i;

/** The identifier this page is showing, or null when it is showing anything else. */
function entitySeg() {
  const seg = location.pathname.slice(1);
  return ENTITY_PATH.test(seg) ? seg : null;
}

/** Draw the entity view for [seg]: the one call, so the router and [rerun] cannot open it two ways. */
function openEntity(seg) {
  // seedRow is the same thunk the results list runs, so a permalink and a card agree on who vouched.
  return showEntity(seg, {
    paintScores,
    ensureLogin,
    setHits: (evs) => { s.hits = evs; },
    seedRow: (events) => rowSeed(events, "own")(),
  });
}

/**
 * URL to page, the single restore path: initial load, Back and Forward. Returns whether it
 * started a search, so boot knows whether sign-in is already being driven.
 */
function applyUrl() {
  navRestoring = true;
  try {
    const seg = entitySeg();
    if (seg) {
      clearTimeout(debounceTimer);
      forget(s); forget(pop); // cancel any in-flight search or type-ahead render
      resetPages();
      // The lens is part of a permalink only when the URL carries one: a click keeps the session's
      // observer.
      const asHere = pubkeyParam(new URLSearchParams(location.search).get("as"));
      if (asHere) {
        applyViewingAs(asHere, null);
        // The name is filled in after the fact so the restore never waits on it.
        enrichProfiles([asHere]).then(() => {
          const nm = displayName(profiles.get(asHere));
          if (nm && viewingAs === asHere) { $obsCurrent.textContent = nm; renderAdvCount(); }
        }).catch(() => {});
      }
      $q.value = "";
      hideFeedPreview();
      document.body.classList.remove("has-query", "feed");
      document.body.classList.add("searching");
      closePopup();
      $results.hidden = false;
      openEntity(seg);
      return false; // no search running; boot still signs in eagerly
    }
    cancelEntity(); // leaving the entity view invalidates its in-flight fetch
    document.title = "SearchOverTrust";
    const p = new URLSearchParams(location.search);
    // The feed takes no q and none of the bar's extensions, so those go back to their defaults;
    // the tab is read, not cleared.
    if (p.get("feed") === "1") {
      tab = KIND_TABS.find((t) => t.slug === p.get("tab")) || KIND_TABS[0];
      // A url naming two views, or an unknown tab, is corrected to what is on screen; replaceState,
      // the same place spelled properly.
      if (location.search !== feedUrl().slice(1)) history.replaceState(null, "", feedUrl());
      renderChips();
      $sort.value = "";
      $spam.checked = false;
      applyViewingAs(null, null); // last: it recounts the badge, and the two
                                  // above fire no `change` of their own
      $q.value = "";
      document.body.classList.remove("has-query");
      runFeed();
      return true;
    }
    const text = (p.get("q") || "").trim();
    tab = KIND_TABS.find((t) => t.slug === p.get("tab")) || KIND_TABS[0];
    renderChips();
    $sort.value = p.get("sort") || "";
    if ($sort.selectedIndex < 0) $sort.value = ""; // an unknown sort is no sort
    $spam.checked = p.get("spam") === "1";
    const as = pubkeyParam(p.get("as"));
    applyViewingAs(as, null); // recounts the badge for all three: setting the
                              // two above fires no `change` of its own
    if (as) {
      // The name is filled in after the fact so the restore never waits on it.
      enrichProfiles([as]).then(() => {
        const nm = displayName(profiles.get(as));
        if (nm && viewingAs === as) { $obsCurrent.textContent = nm; renderAdvCount(); }
      }).catch(() => {});
    }
    $q.value = text;
    document.body.classList.toggle("has-query", !!text);
    if (text) runFull(text, pageParam(p)); else showHero();
    return !!text;
  } finally {
    navRestoring = false;
  }
}

window.addEventListener("popstate", applyUrl);

// Internal navigation: every link stays a real <a>, but a plain left click becomes a pushState
// render instead of a reload that would tear down the socket and its NIP-42 auth.
document.addEventListener("click", (e) => {
  if (e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
  const a = e.target.closest("a[href^='/']");
  if (!a) return;
  const href = a.getAttribute("href");
  if (href === "/") { e.preventDefault(); reset(); return; }
  if (isFeedHref(href)) { e.preventDefault(); navigate(href); return; }
  // A hashtag chip is a search. Its url is rebuilt through currentUrl() so the filters it does not
  // name are kept.
  if (/^\/\?q=/.test(href)) {
    e.preventDefault();
    // Page one: a hashtag clicked on page four is a different search.
    navigate(currentUrl(new URLSearchParams(href.slice(1)).get("q") || "", true, 0));
    return;
  }
  if (!/^\/(npub|nprofile|note|nevent|naddr)1[a-z0-9]+$/i.test(href)) return;
  e.preventDefault();
  navigate(href);
});

// ---- media that loads when it is nearly on screen -------------------------
// A video's url waits in `data-src` until the card is a screen away. Armed from a
// MutationObserver because cards are inserted from more than one place.
const loadMedia = (v) => { v.src = v.dataset.src; delete v.dataset.src; };
const nearViewport = "IntersectionObserver" in window
  ? new IntersectionObserver((entries, obs) => {
      for (const e of entries) if (e.isIntersecting) { loadMedia(e.target); obs.unobserve(e.target); }
    }, { rootMargin: "500px 0px" })
  : null;
function armMedia(root) {
  if (!root || root.nodeType !== 1) return;
  const vids = [...root.querySelectorAll("video[data-src]")];
  if (root.matches("video[data-src]")) vids.push(root);
  // No IntersectionObserver is no lazy loading, not a page of dead players.
  for (const v of vids) nearViewport ? nearViewport.observe(v) : loadMedia(v);
}
new MutationObserver((records) => {
  for (const r of records) for (const n of r.addedNodes) armMedia(n);
}).observe(document.body, { childList: true, subtree: true });

/** An internal path as a pushState render: the one place a href becomes a view. */
function navigate(href) {
  if (location.pathname + location.search !== href) history.pushState(null, "", href);
  applyUrl();
}

// The face from the last load, drawn on the first paint as an assumption prefetchFace()
// corrects. Chips render here too: the entity branch of applyUrl() returns before drawing them.
if (wantsSignIn() && meFace) mePending = meFace.pubkey;
renderChips();
renderWhoami();
// The anonymous reference socket, warmed at boot.
refConn().catch(() => {});
// A restored search drives sign-in itself; an idle load signs in eagerly.
if (!applyUrl()) ensureLogin().then(renderWhoami).catch(() => renderWhoami());
// Focus the box when the page is the box, and never on a phone, where a scripted focus()
// cannot raise the keyboard (softKeyboard() is that distinction).
if ($results.hidden && !softKeyboard()) $q.focus();
