// The SearchOverTrust page itself: sign-in, the search views (hero, type-ahead
// popup, full results), the "ranking as" lens, and the URL/backstack wiring.
// Everything stateful lives here; the shared/ modules underneath are the
// stateless client, codec and caches, and cards.js is the rendering.

import { RELAY_URL, relay, refConn } from "./shared/conn.js";
import { npub, shortNpub, pubkeyParam } from "./shared/nip19.js";
import { esc } from "./shared/format.js";
import { avatarHtml } from "./shared/avatar.js";
import { profiles, displayName, seedProfiles, enrichProfiles } from "./shared/profiles.js";
import { watchNip05 } from "./shared/nip05.js";
import { parseQuery, buildFilters as filtersFor, effectiveSort } from "./shared/query.js";
import { ownGroups, metaGroup, postedTo, rank as rankGroups, sealed as sealedGroups, privateGroups } from "./shared/groups.js";
import { seedGroupNames, seedGroupEvents, enrichGroupNames, forgetPrivateGroupNames } from "./shared/groupnames.js";
import { isTyping, navKey, stepIndex } from "./shared/keynav.js";
import { replyPerson, seedParentAuthors, unknownParents, loadParentAuthors } from "./shared/parents.js";
import { selfHref } from "./cards/base.js";
import { card, popupRow, namedPubkeys } from "./cards.js";
import { showEntity, cancelEntity } from "./entity.js";
import { feedKinds, PREVIEW_CARDS, PAGE_CARDS, askFor, pickFeed } from "./feed.js";
import { mountSearchField, softKeyboard } from "./searchfield.js";
import { checkReadiness, clearReadiness } from "./readiness.js";

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

// ---- the face you ended on, kept across loads ----------------------------
//
// Every full load — a reload, a pasted /npub1… link, a click in from anywhere
// — is a new socket, a new challenge, a new signature, and only THEN a REQ for
// your kind 0. The field therefore sat on a placeholder for that entire chain
// on every single load, redrawing an answer that had not changed since the
// last one. The picture url and name of the account you signed in as are kept
// here so the field can draw them at once.
//
// Keyed BY PUBKEY, and dropped the moment the key it is keyed to stops being
// the one on screen — see the boot note at the foot of this file for the one
// window where it is drawn on an assumption rather than on an answer. This is a
// cache of what was already on your own screen, not a session: it authorises
// nothing, the proof still has to happen, and until it does the field says so
// rather than claiming you are signed in.
const FACE_KEY = "sot_face";
let meFace = readFace();
function readFace() {
  try {
    const f = JSON.parse(localStorage.getItem(FACE_KEY) || "null");
    return f && /^[0-9a-f]{64}$/.test(f.pubkey || "") ? f : null;
  } catch (e) { return null; }
}
/**
 * Keep what the relay just said about [pk]'s face.
 *
 * Only when it ANSWERED — `has` is true once it has, profile or no profile.
 * A dropped or timed-out read is not "you have no picture", and recording it
 * as one would throw away a good face AND cost the next load the head start
 * this whole cache exists for. Same rule, and the same reason, as the one
 * profiles.js spells out about caching absences off incomplete reads.
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
// Signing the challenge switches the CONNECTION's ranking observer to you.
//
// It does NOT start anything syncing on your behalf — this comment used to say
// it enrolled you, and index.html's header said the same, while the relay's
// enrolment hook (NostrRelayServer's `onObserver`) was never wired to anything.
// The router reaches you through the streams it already runs, on their own six-
// hourly cycle, or not at all; readiness.js is what says which, and what to do
// about it.
let me = null;        // the pubkey the relay ACCEPTED a NIP-42 AUTH for
let mePending = null; // the pubkey the extension named, before that proof

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
  const challenge = await relay.waitForChallenge(waitMs);
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

/**
 * Who the extension says you are, and your face — both BEFORE the proof.
 *
 * Nothing about a public profile depends on NIP-42. `getPublicKey()` is a local
 * call: no network, no signature, no challenge. So naming the account and
 * reading its kind 0 can happen BESIDE the handshake, the challenge and the
 * signing popup instead of behind all three — and the read is what the picture
 * actually waits on.
 *
 * The pubkey it returns draws and prefetches; it never authorises. `me` is
 * still set from the signature the relay accepted, and until that lands the
 * field renders the face faded and titled "signing in as".
 */
async function prefetchFace() {
  let pk = null;
  try { pk = await window.nostr.getPublicKey(); } catch (e) { pk = null; }
  if (!/^[0-9a-f]{64}$/.test(pk || "")) {
    // The extension would not say. Whatever the boot assumed is unsupported —
    // take it down rather than leave a face nothing is going to confirm.
    mePending = null;
    renderMe();
    return null;
  }
  mePending = pk;
  renderMe();                 // the remembered face, if this is the same account
  // enrichProfiles skips a pubkey it has already seen — including one recorded
  // as `null` by a lookup that ran before this account was on screen — so a
  // cached miss survived signing in and the avatar kept the old face until a
  // reload. Deleting first forces the re-read.
  profiles.delete(pk);
  try { await enrichProfiles([pk]); } catch (e) {}
  renderMe();
  return pk;
}

async function login() {
  if (!(window.nostr && window.nostr.signEvent)) throw new Error("No Nostr extension found (window.nostr / NIP-07)");
  // The socket and the face start TOGETHER, because neither needs the other.
  // This was strictly serial — connect, challenge, sign, AUTH, and only then
  // open a second socket from cold and ask for one kind 0 — so the picture
  // landed a full connect-plus-round-trip after the relay had already accepted
  // the login, with the extension's popup sitting in the middle of the chain.
  // Now the only step that needs both halves is the AUTH itself.
  const connecting = relay.connect();
  const face = prefetchFace();
  await connecting;
  // A CLICK gets a longer wait than a background attempt. The socket may have
  // only just opened — the page does not connect at all until something needs
  // it — and the AUTH challenge is a message that arrives after the handshake,
  // so a 3s budget was being spent on connecting rather than waiting, and the
  // first press simply failed. That is why it took a few presses.
  me = await signAndAuth(0, 10000);
  mePending = null;
  renderWhoami();
  // Usually already resolved during the signing popup. AWAITED all the same,
  // so a caller can still rely on the face being fetched by the time login()
  // returns — that is what the retry below and the sign-in click both assume.
  const named = await face;
  if (named !== me) {
    // The extension never answered getPublicKey, or signed as an account other
    // than the one it named. Fall back to the read login() always did.
    profiles.delete(me);
    try { await enrichProfiles([me]); } catch (e) {}
  }
  rememberFace(me);
  renderWhoami();
  // The fetch above races page load: the reference socket is opening at the
  // same time as the main one, and when it loses, `me` is signed in with no
  // profile and NOTHING retries — the avatar sat on a placeholder until the
  // user clicked twice, which signed them out and back in again. Retry a
  // couple of times, quietly, and stop as soon as a face arrives.
  //
  // `has`, not `get`: the cache records `null` for a pubkey the relay ANSWERED
  // about and has no kind 0 for. Retrying on that spent 3.6s of sleeps and
  // three more REQs re-asking a question already answered, for every account
  // that simply has no profile. Only a read that came back with nothing at all
  // is worth repeating.
  for (let i = 0; i < 3 && me && !profiles.has(me); i++) {
    await new Promise((r) => setTimeout(r, 600 * (i + 1)));
    if (!me) break;
    profiles.delete(me);
    try { await enrichProfiles([me]); } catch (e) {}
    rememberFace(me);
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
  // STARTED, not awaited. This used to gate the whole login flight on the
  // handshake, which put the three independent waits — the socket, the
  // extension, the profile read — back in series behind the slowest thing
  // nobody was waiting for yet. Only the AUTH itself needs both halves, and it
  // does its own connect. Everything else that sends anything connects on its
  // own too (Relay.req does), so a failure here surfaces at the ask.
  relay.connect().catch(() => {});
  if (!loginFlight) {
    loginFlight = (async () => {
      // A remembered "signed out" is a decision, not an absence — respect it
      // rather than prompting the extension on every page load.
      if (!wantsSignIn() || !(window.nostr && window.nostr.signEvent)) {
        // Nobody is going to prove the assumed face below, so take it down.
        mePending = null;
        $obsBox.classList.add("anon");
        renderWhoami();
        return;
      }
      try { await login(); } catch (e) {
        me = null;
        mePending = null;   // the extension named somebody it could not prove
        $obsBox.classList.add("anon");
        $whoami.innerHTML = `<span class="err">${esc(e.message || String(e))} — showing the whole corpus, unranked</span>`;
      }
    })().finally(() => { loginTried = true; });
  }
  await loginFlight;
  if (!me) { clearReadiness(); return; }
  // Keyed on the CONNECTION's auth state, not on whether we remember a
  // pubkey: a reconnect leaves `me` set but the new socket unauthenticated,
  // and searching then really would rank by nobody — there is no default
  // observer behind it any more.
  if (relay.authed) { checkReadiness(me); return; }
  me = await signAndAuth();
  renderWhoami();
  // Whether this relay can actually rank for the account that just proved
  // itself. Asked from HERE rather than from login(), because this is the one
  // place both paths to an authenticated socket meet — the first sign-in and
  // the re-auth after a reconnect — and the check is about the connection's
  // lens, not about the sign-in. It is idempotent per pubkey and it waits;
  // see readiness.js on why nothing here is on the critical path.
  checkReadiness(me);
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
 * The filters this page's REQ carries, from what the person typed.
 *
 * The construction itself lives in shared/query.js — pure, and tested there
 * against the whole of the box's language. What is HERE is the page state it
 * needs: which tab is on, how many rows this view wants, and the NIP-50
 * extension string the sort menu, the spam toggle and the "ranking as" lens
 * build between them.
 *
 * Shared with exportText() so the filters a reader is shown are the filters
 * that were sent, byte for byte, rather than a second construction of them.
 */
function buildFilters(text, limit) {
  return filtersFor(text, { kinds: tab.kinds, limit, searchString });
}

/**
 * The feed's ask: the SAME builder with nothing to say but its kinds.
 *
 * No words, and — pointedly — no `searchString`, so the sort menu, the spam
 * toggle and the "ranking as" lens do not ride along. What is left is
 * `{ kinds, limit }`: a plain NIP-01 read, which is what the store answers
 * newest-first. Any of those three would make it a NIP-50 query for an ORDER
 * instead, and the page would be saying "latest" over a ranked list. That is
 * why the feed view hides the Filters disclosure rather than leaving three
 * controls on screen that this ask cannot carry.
 *
 * `kinds` is the exception, and the reason the chip row stays: it is a field
 * of this filter, not an extension on a search string, so the tab narrows the
 * feed exactly as it narrows a search. feed.js's feedKinds() owns which list
 * that is.
 */
const feedFilters = (limit) => filtersFor("", { kinds: feedKinds(tab.kinds), limit });

/**
 * One event, one card, however many filters it answered.
 *
 * A hashtag search is four filters in one REQ, and one event can answer several
 * of them: a top-level comment on the topic carries it in both `i` and `I`, and
 * a note that tags `t` and also labels itself answers two.
 *
 * THIS relay's store already dedupes across the filters of a subscription
 * (NostrSemanticsStore.recallOrdered — `distinctBy(idOf)` whenever there is
 * more than one query), so this is belt and braces rather than the fix for a
 * known duplicate. It stays because NIP-01 does not require that of a relay and
 * the same note rendering twice is a visible bug; it is a Set and one pass over
 * a list the page is about to render anyway. Arrival order is kept, and since
 * store 8a45e4d1a2 that order is one ranking over all four filters rather than
 * one run per filter, so keeping the first copy of a duplicate keeps it at the
 * best position it earned.
 */
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

async function search(text, limit, deep) {
  await ensureLogin();
  const filters = buildFilters(text, limit);
  return { ...hydrate(uniqueById(await relay.req(filters)), deep), text };
}

/**
 * The newest content, for the hero's preview and for the feed page.
 *
 * The ask is over-sized and then cut back, because NIP-01 has no way to say
 * "not a reply" — feed.js owns both halves of that arithmetic. `deep` is
 * false for the same reason: once the replies are gone there is no "in reply
 * to" line to fill in, so the two round trips replyParents() costs would buy
 * nothing.
 */
async function fetchFeed(want) {
  await ensureLogin();
  // No uniqueById on the way in: pickFeed dedupes by id itself, in the same
  // pass that drops the replies and the future dates.
  return hydrate(pickFeed(await relay.req(feedFilters(askFor(want))), want), false);
}

/**
 * The lookups a list of events needs once it has arrived — everything between
 * "the relay answered" and "the cards can be dressed".
 *
 * Split out of search() when the feed appeared: the feed asks a completely
 * different question (a plain NIP-01 read, no NIP-50 anything) and needs
 * exactly the same names, faces and reply lines afterwards. Two copies of this
 * would drift the first time a renderer started naming somebody new.
 */
function hydrate(events, deep) {
  seedProfiles(events);
  // The same for the search box's `group:` pill, and it is nearly free: a
  // `group:` query already asks for the group's own kind 39000 beside its
  // posts (query.js's buildFilters sends the `#d` with the `#h`), so the name
  // the pill wants is usually in the answer the reader just got, with no
  // second round trip to pay for it. Repainted on the same "only if it learned
  // something" terms the chips are, and reaching `field` from here is safe for
  // the ordinary reason: hydrate runs on a search, which cannot happen before
  // the box that search was typed into exists.
  if (seedGroupEvents(events)) field.repaint();
  // Authors, plus everyone the cards will NAME — a 30382's d subject, a
  // 10040's service column, a zap's sender. The names rule holds in the
  // results list, not only on permalinks. This used to be a tag scan written
  // here, which meant it could only cover the slots that existed when it was
  // written; namedPubkeys lives with the renderers and is held to them by a
  // test, so a new family that names somebody cannot silently stop being
  // enriched — including the people a list's grid names, which arrive here
  // capped at what one card can draw rather than at what the list carries.
  //
  // NOT awaited. This used to block the return, so the results existed and
  // the page showed a skeleton until a SECOND round trip finished — up to the
  // 5s enrichProfiles timeout of nothing, over a list the relay had already
  // sent. base.js says it plainly about the score chip: "the score is a
  // second round trip, and a face should not wait on it." A name is the same
  // round trip; it was just on the other side of the render.
  // Called with ONE argument on purpose. namedPubkeys takes the render depth
  // as its second, and `flatMap(namedPubkeys)` hands it the array INDEX —
  // harmless while a number has no `.full`, and a silent depth switch the
  // moment that argument grows a second field somebody reads.
  const mentioned = events.flatMap((e) => namedPubkeys(e));
  const names = enrichProfiles([...events.filter(e => e.kind !== 0).map(e => e.pubkey), ...mentioned]);
  // The rooms those events were said in, on exactly the same terms as the
  // names: a NIP-29 chat card draws the group beside its badge, an `h` tag
  // carries nothing but the id, and the seed above has already taken whatever
  // 39000s came back with the results — so this asks only for the ids nothing
  // on the page can name yet, and reports how many it learned so a lookup that
  // learned nothing costs no repaint. Not awaited, for the reason the names
  // are not: a card should not wait on the label above it.
  const groups = enrichGroupNames(events.map(postedTo).filter(Boolean));
  // Free, and it removes most of the asks below: a thread in the results
  // carries its own parents, and an event is ground truth about who wrote it.
  seedParentAuthors(events);
  return { events, names, groups, parents: deep ? replyParents(events) : null };
}

/**
 * The reply lines' own lookups, as a SEPARATE promise from the names.
 *
 * "In reply to <person>" needs a person, and most `e` tags name only an event
 * — so an unhinted parent is a lookup by id to learn its author, and then that
 * author's profile. Two more round trips behind the one the names already
 * cost, which is why they are not chained onto it: this began as `await names`
 * followed by the rest, and that made every author name in the list wait for
 * the parent lookup to finish — a repaint that used to land in one round trip
 * arrived in three, or after the 5s timeout when a parent was missing. The two
 * are independent facts and now repaint independently.
 *
 * Only the parent AUTHORS are enriched here, not namedPubkeys again: the names
 * promise is asking for that set concurrently, and enrichProfiles dedupes
 * against the cache rather than against what is in flight, so the overlap
 * would be a second REQ for pubkeys already being fetched.
 *
 * Full renders only (the `deep` flag): the type-ahead popup draws a name and a
 * line of text per row, no reply lines, so a debounced keystroke has nothing
 * to spend two round trips on.
 */
async function replyParents(events) {
  const learned = await loadParentAuthors(unknownParents(events));
  return learned + await enrichProfiles(events.map(replyPerson).filter(Boolean));
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
  const lists = await anon.req({ kinds: [10040], limit: 2000 });
  const ks = [...new Set(lists.map((e) => e.pubkey))];
  // The batches are chunks of ONE question, so they go out together rather
  // than one round trip after another — 271 observers is two REQs, and asking
  // them serially made the picker's first open cost two full waits for no
  // reason. NIP-01 subscriptions are concurrent by design and this client
  // already keys replies by subscription id; only the `await` was serialising.
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

/** The lens state and its widget, with no search behind it — what a history
    restore needs, since the restore runs its own search exactly once. */
function applyViewingAs(pubkey, name) {
  viewingAs = pubkey;
  $obsBox.classList.toggle("active", !!pubkey);
  $obsCurrent.textContent = pubkey ? (name || shortNpub(pubkey)) : "me";
  $obsList.innerHTML = "";
  $obsFilter.value = "";
  renderAdvCount();
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
  const batches = [];
  for (let i = 0; i < need.length; i += 100) batches.push(need.slice(i, i + 100));
  // In flight together: the chips are already on screen waiting to be filled,
  // and one batch's answer never informs the next one's ask. An entity page
  // with a long face strip was paying a full round trip per hundred faces.
  const conn = batches.length ? await refConn().catch(() => null) : null;
  const reads = await Promise.all(batches.map((batch) =>
    // A failed read leaves this batch unknown rather than wrong — an empty
    // array is NOT marked complete, so nothing gets cached as "no score".
    (conn ? conn.req({ kinds: [30382], authors: [svc], "#d": batch, limit: batch.length }) : Promise.resolve([]))
      .catch(() => [])));
  // The lens can change WHILE these are in flight — the reader picks another
  // observer, or signs out — and `scores` was cleared and re-keyed to the new
  // one by the paint that followed them. Writing these answers into it now
  // files one lens's numbers under another's name, and the `null`s below are
  // worse: they are cached as "this lens gives them no score", which nothing
  // re-asks. The answers are simply stale; drop them.
  if (scoreLensKey !== lens) return;
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i], evs = reads[i];
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
  const full = buildFilters(typed, FULL_LIMIT);
  // The order the STORE will apply, which is not always the one the menu shows:
  // a `sort:` typed into the box rides through parseQuery untouched, so
  // `/?q=cats sort:recent` is a chronological search with the menu on "Best
  // match". Every line below that reasons about the ORDER reads this rather
  // than $sort — an audit of an order has to name the order that was served.
  const sort = effectiveSort(full[0].search ?? "");
  const people = (keys) => keys.map((k) => `${npub(k)}${nameOf(k) ? `  (${nameOf(k)})` : ""}`).join(", ");
  L.push("QUERY AS CONFIGURED");
  L.push(`  typed         ${JSON.stringify(typed)}`);
  L.push(`  terms         ${JSON.stringify(q.terms)}`);
  // The person filters are the reason an order can look wrong and be right:
  // a reader judging position 7 has to know the list was narrowed to two
  // authors before it was ranked at all.
  if (q.authors.length) L.push(`  from          ${people(q.authors)}`);
  if (q.mentions.length) L.push(`  to            ${people(q.mentions)}`);
  // Same reason, and one more: a hashtag search is a union, so a reader
  // comparing two results has to know one may have arrived as a `t` tag, the
  // next as a NIP-22 comment on the topic, and the next as a NIP-32 label —
  // three different claims, ranked into one list.
  if (q.hashtags.length) L.push(`  hashtags      ${q.hashtags.map((t) => `#${t}`).join(", ")}`);
  // As typed, not as asked: the id spellings the filter actually carries are in
  // the full filter lines below, and a reader auditing the order needs to see
  // both — what the person meant, and what the relay was asked for it.
  if (q.scopes.length) L.push(`  scopes        ${q.scopes.map((s) => `${s.field}:${s.value}`).join(", ")}`);
  // The id and nothing else, because the id is all the filter carries. A group
  // is the pair (id, host relay) and an `h` tag holds only the id, so a reader
  // auditing this order has to be told that the rows could have come from more
  // than one relay's group of that name — it is not recoverable from the events.
  if (q.groups.length) L.push(`  groups        ${q.groups.map((g) => `group:${g}`).join(", ")}  (matched by id alone — any host's group with this id)`);
  // The window, as both the second the filter carries and the moment it stands
  // for. A reader auditing an order has to be able to tell an empty page from a
  // window that excluded everything, and a bare epoch second cannot say which.
  const when = (at) => `${at}  (${new Date(at * 1000).toISOString()})`;
  if (q.since != null) L.push(`  since         ${when(q.since)}`);
  if (q.until != null) L.push(`  until         ${when(q.until)}`);
  L.push(`  tab           ${tab.label}${tab.kinds ? ` (kinds ${tab.kinds.join(", ")})` : " (all kinds)"}`);
  // Named from the string, and told apart from the menu when the two differ —
  // a reader auditing the order needs to know a token in the box produced it.
  L.push(`  sort          ${sort || "(relevance — NIP-50 default)"}${sort && sort !== $sort.value ? "  (from the search box, not the Filters menu)" : ""}`);
  L.push(`  include spam  ${$spam.checked ? "yes — unranked authors included" : "no — trust floor applied"}`);
  L.push(`  signed in as  ${me ? `${nameOf(me) || "(no name)"}  ${npub(me)}` : "(anonymous — no web of trust applied)"}`);
  L.push(`  ranking as    ${lens ? `${nameOf(lens) || "(no name)"}  ${npub(lens)}` : "(nobody)"}`);
  L.push(`  search string ${full[0].search == null ? "(none — no words and no sort/spam/lens to carry, so this is a plain NIP-01 read)" : JSON.stringify(full[0].search)}`);
  // Every filter of the REQ, one per line: they are ORed in one subscription,
  // and a reader shown only the first would think the comments came from
  // nowhere. The label stays singular for the ordinary one-filter search.
  L.push(`  full filter${full.length > 1 ? "s " : "  "} ${JSON.stringify(full[0])}`);
  for (const f of full.slice(1)) L.push(`                ${JSON.stringify(f)}`);
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
  // Which question, because `sort:recent` changes what a wrong answer looks
  // like. The trust question below asks whether the ORDER earned itself; under
  // "Newest" the relay was told not to order by trust at all, so asking it
  // would send the reader hunting for a misranking in a list that is supposed
  // to be chronological — and would say nothing about the one thing the lens
  // still does there, which is decide who is IN the list.
  //
  // Keyed on the SENT string (see `sort` above), not on $sort: a shared
  // `/?q=cats sort:recent` link is a chronological search with the menu
  // untouched, and it was getting the trust question this branch exists to
  // avoid.
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
  // No union caveat any more. This block used to warn that a multi-filter REQ
  // came back as each filter's ranked run end to end, so a jump back up the
  // trust scale was a seam and not a misranking — true of the store until
  // vespaEventStore 8a45e4d1a2, which merges the filters of one REQ on the
  // engine's scores when they share a rank profile (which this page's four
  // hashtag filters always do: they carry the same search string). The order is
  // now one ranking of the union, so a jump back up the scale IS worth
  // challenging, and the question above stands unqualified.
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
// By class, because the class is the thing being asked about: `.hero` is what
// `body.searching .hero` makes a sticky toolbar, and its height is what the
// keyboard cursor has to scroll clear of.
const $hero = document.querySelector(".hero");

/** The avatar in the field: your picture signed in, a neutral mark signed out. */
function renderMe() {
  // Two states, deliberately distinct. `me` is PROVEN — an AUTH this relay
  // accepted. `mePending` is only what the extension answered when the page
  // opened: enough to draw a face, not enough to claim a session, so the field
  // shows it faded and says "signing in as" until the proof lands.
  const who = me || mePending;
  const p = who ? profiles.get(who) : null;
  // The remembered face stands in only until the relay has ANSWERED about this
  // pubkey — `has` is true once it has, profile or no profile. So a kept
  // picture can outlive the truth by at most one round trip, and there is no
  // flash of placeholder in between while the real read is in flight.
  const kept = who && !profiles.has(who) && meFace && meFace.pubkey === who ? meFace : null;
  const pic = (p && p.picture) || (kept && kept.picture) || "";
  const nm = displayName(p) || (kept ? kept.name : "");
  $me.classList.toggle("in", !!me);
  $me.classList.toggle("pending", !!who && !me);
  if (who && pic) {
    // The page's one face renderer, at whatever size this button happens to
    // be — it is pinned to the field's box, so "fill" is the honest answer.
    // Its own hand-rolled <img> removed itself when the picture failed to
    // load, which left the button empty; the shared face falls back to the
    // generated one, the same as every other picture of the same person.
    //
    // Rewritten only when the url actually changed: the kept face and the
    // fetched one are usually the same picture, and replacing the <img> with
    // an identical one makes it blink for a frame.
    const img = $me.querySelector("img.avatar");
    if (!img || img.getAttribute("src") !== pic) $me.innerHTML = avatarHtml(pic, who, "fill");
  } else if (who && nm) {
    $me.textContent = nm.slice(0, 2).toUpperCase();
  } else if (who) {
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
    : who
      ? `Signing in as ${nm || shortNpub(who)}…`
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
      mePending = null;
      // Forgotten here, not merely unrendered: the kept face is drawn from the
      // pubkey the extension names, and it would otherwise flash back on the
      // next load of a page the reader had deliberately signed out of.
      forgetFace();
      // And the names that came out of the ENCRYPTED half of this reader's
      // group list, for the same reason: the public ones are the network's to
      // read, but a label somebody gave a group in private must not still be
      // on a pill for whoever uses this tab next. forgetOwnGroups() covers the
      // rows themselves on the next lookup; this is the copy the search box
      // draws from.
      forgetPrivateGroupNames();
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
    mePending = null;
    $whoami.innerHTML = `<span class="err">${esc(e.message || String(e))}</span>`;
  } finally {
    $me.classList.remove("busy");
    renderMe();
    renderWhoami();
    // Signing OUT takes the panel down immediately: it is about one account's
    // trust chain, and leaving it up over a signed-out page would be a claim
    // about somebody who is no longer here.
    me ? checkReadiness(me) : clearReadiness();
    rerun();
    // The hero's feed is a different list for a signed-in reader than for
    // anyone else — and for a signed-OUT one it is not drawn at all — so it
    // follows this click the way the results do.
    showFeedPreview();
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

// ---- the advanced filters -------------------------------------------------
//
// The disclosure itself is the <details> element's job — open/closed, the
// keyboard, the ARIA. What is left for this file is the part hiding a control
// costs: the badge, which is the only thing on screen that admits a filter is
// on while the panel is shut. tools/webtest/filters.test.mjs holds the three
// of them — panel control, badge fact, URL param — in step.
const $adv = document.getElementById("adv");
const $advBtn = document.getElementById("advbtn");
const $advCount = document.getElementById("advcount");
// The idle tooltip, read off the markup rather than repeated here: two copies
// of one sentence is one sentence that gets edited and one that does not.
const advIdleTitle = $advBtn.title;

/** What is on, on the button: a count while shut, and the list as its title. */
function renderAdvCount() {
  const on = [];
  if ($sort.value) on.push("Sort: " + $sort.options[$sort.selectedIndex].text);
  if (viewingAs) on.push("Ranking as: " + $obsCurrent.textContent);
  if ($spam.checked) on.push("Spam included");
  $advCount.textContent = String(on.length);
  $advCount.hidden = !on.length;
  $advBtn.title = on.length ? on.join(" · ") : advIdleTitle;
}

// Dismissed like the popups it sits next to, and for the same reason: it is
// absolutely positioned over the page. The observer list inside it is a click
// within #adv, so picking somebody does not close the panel out from under the
// list they picked from.
document.addEventListener("click", (e) => {
  if ($adv.open && !e.target.closest("#adv")) $adv.open = false;
});

// ONE Escape handler for the two things stacked here, asked in order rather
// than each stopping the key on its way past: a stopPropagation() in the
// observer field would swallow Escape for every other listener on the page,
// including ones written later that have no idea this field exists.
document.addEventListener("keydown", (e) => {
  if (e.key !== "Escape") return;
  // Innermost first — the list is drawn INSIDE the panel, so closing the panel
  // on the same press would dismiss two things for one key.
  if ($obsList.childElementCount) { $obsList.innerHTML = ""; $obsFilter.blur(); return; }
  if (!$adv.open) return;
  // Focus returns to the button only if it was inside the panel: Escape is also
  // how the search popup closes, and that press must not yank the caret out of
  // the field somebody is typing in.
  const inside = $adv.contains(document.activeElement);
  $adv.open = false;
  if (inside) $advBtn.focus();
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

// The reader's own kind 10009, held for the session. One event, and it is the
// only place the protocol writes an id, its host relay and a name down
// together — so it is worth a round trip and not worth a second one.
//
// READ ON THE AUTHENTICATED SOCKET, so this list is behind the same lens as
// everything else the relay serves a signed-in reader — and comes back EMPTY,
// on purpose, for a reader with no scores and no 10040 mirrored here.
//
// That is worth stating because it looks like a bug from the outside and is
// not. The store applies the observer as a FILTER (its "observer gate"), so a
// reader whose trust chain has not reached this relay reads back nothing at
// all — including their own events. Measured against a real Vespa: signed in
// on a store with no scores, `{kinds:[10009], authors:[me]}` returned 0 of the
// reader's OWN event, and returned it the moment a provider they trust scored
// them. Routing this one read around that — down the anonymous connection,
// which does answer — would make the group picker the single place on the page
// that shows a reader content this relay has otherwise decided it cannot rank
// for them. So it does not: no chain here, no personal groups, and
// readiness.js is the panel that explains why rather than a special case here.
//
// (Whether an observer should be gated by their own trust AT ALL is a separate
// question, and one for the store: the reputation tensor is derived only from
// 30382s about a subject, so there is no self-edge and you score 0 under your
// own lens. That is being fixed where it lives. Nothing here should anticipate
// it — when the store stops gating a reader out of their own events, this read
// starts answering, with no change on this side.)
//
// The 39000 name search below is on the same socket for the ordinary reason:
// which groups exist and are worth showing first IS a ranked question, exactly
// as the people picker's is.
//
// Cached only when the relay ANSWERED, the rule rankServiceOf() and profiles.js
// both state at length: a dropped read cached as "you have no groups" would
// leave `group:` opening on an empty list for the rest of the session, with
// nothing on screen to say the list was missing rather than empty.
let ownGroupList = null;   // the parsed candidates, from the PUBLIC tags
let ownGroupsFor = null;   // whose they are, so signing out drops them
let ownGroupLock = null;   // sealed(): the encrypted half, or null if there is none
let ownGroupSecret = null; // the rows behind that lock, once it has been opened
let unlockAsk = null;      // the in-flight decrypt, so N keystrokes are ONE prompt
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
 * Whether the extension in front of us can open a payload of this scheme.
 *
 * Both halves of NIP-07's encryption API are OPTIONAL, and the two are
 * advertised separately — an extension may implement `nip04` and not `nip44`,
 * which for a 10009 is the difference between an openable list and one that
 * simply cannot be read here. Asked before the call rather than discovered
 * from the exception, because "your extension does not do this" and "you said
 * no" want different words and a different offer.
 */
const canDecrypt = (scheme) => {
  const api = window.nostr && window.nostr[scheme];
  return !!(api && typeof api.decrypt === "function");
};

/**
 * Open the private half of the reader's own group list — ONE prompt, ever.
 *
 * This is the only place on the page that asks the signer for anything beyond
 * a signature, so what it costs is worth being explicit about: a NIP-07
 * extension answers a decrypt request by putting a permission dialog in front
 * of the reader. Three rules follow, and all three exist to keep that dialog
 * from becoming noise:
 *
 *  - **Only when there is a payload.** No `.content`, no ask. `sealed()` is
 *    that test, and its doc explains why a payload is not proof there is
 *    anything IN it — an empty private list encrypts the empty string, so a
 *    reader who removed their last private group still carries a ciphertext.
 *    Asking is the only way to find out, and finding out that the answer is
 *    "nothing" is a fine outcome: it is cached like any other.
 *  - **Once per reader.** `unlockAsk` holds the in-flight promise, so the eight
 *    keystrokes of `group:chachi` produce one dialog and not eight. It is
 *    cleared only when the answer is known or when a retry is asked for by
 *    hand.
 *  - **A refusal is final until the reader changes their mind.** A denied
 *    prompt sets `unlockDenied` and NOTHING re-asks on its own — not the next
 *    keystroke, not the next search. The picker offers a row to try again, and
 *    a click on it is the reader asking, which is the only thing that should
 *    reopen a dialog they just dismissed.
 *
 * The peer key is the reader's OWN pubkey. NIP-51 private items are
 * self-encrypted (quartz's `PrivateTagsInContent`), which reads oddly the
 * first time — you are the sender and the recipient — and is what makes the
 * list readable on a new device with nothing but the key.
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
      // Cached even when it is EMPTY, which is the case worth naming: the
      // payload decrypted to nothing, so there was never anything to unlock,
      // and re-prompting a reader to be told that again would be the exact
      // noise the rules above exist to prevent.
      ownGroupSecret = rows;
      ownGroupLock = null;
      unlockDenied = false;
      return rows;
    })
    .catch(() => {
      // Refused, dismissed, or the extension failed. All three are "not
      // opened", and none of them is evidence about what is inside — so the
      // lock STAYS, and only a click asks again.
      unlockDenied = true;
      return [];
    })
    .finally(() => {
      unlockAsk = null;
      // The answer arrived after the picker had already drawn what it had, so
      // the picker is told to ask again. Guarded on the token still being
      // there, inside the field: an extension dialog takes focus out of the
      // page, and the reader may be somewhere else entirely by now. The catch
      // is for `field` itself — it is declared below this function and only
      // reachable through it, so it is always initialised by the time this
      // runs, and a bare reference would be a TDZ throw rather than a null
      // check if that ever stopped being true.
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

/**
 * What the picker should SAY about the locked half, or null when there is
 * nothing to say — which is the common case and has to stay silent.
 *
 * `unsupported` and `denied` are deliberately different states rather than one
 * "could not unlock": one of them is a thing the reader can fix by clicking,
 * and the other is a thing they can only fix by changing extensions. Offering
 * a retry for the second would be a button that cannot work.
 */
function groupLockState() {
  if (!ownGroupLock || !me) return null;
  if (!canDecrypt(ownGroupLock.scheme)) return { state: "unsupported", scheme: ownGroupLock.scheme };
  if (unlockDenied) return { state: "denied" };
  return { state: "asking" };
}

/**
 * Which groups the picker offers for a half-typed `group:`.
 *
 * Two asks, and they answer different questions — see shared/groups.js for why
 * they are never folded into one row. Your own kind 10009 says which groups are
 * YOURS, with the host relay's url written down; a NIP-50 search over kind
 * 39000 says which groups EXIST, ranked by the relay against the corpus.
 *
 * The 39000 half is an ordinary search of the same shape the people picker
 * makes: a group's `name` lands in the store's primary search tier and its
 * `about` in the secondary, and the primary carries the prefix/fuzzy `near`
 * column — so a half-typed name reaches "Alice's Club" while it is still being
 * typed, exactly as a half-typed person does.
 *
 * The hosts are enriched as PEOPLE, because for this purpose they are: a NIP-29
 * relay signs its own groups' metadata, and a relay that also publishes a kind 0
 * for that key (this one does — see RelayProfile) gives the row a name to show
 * instead of a hex prefix. It stays a claim the key made about itself, and
 * groups.js draws it differently for that reason.
 *
 * A failed 39000 read leaves the reader's own groups standing rather than
 * throwing the lot away: half an answer is the honest amount here, and the
 * half that survives is the one they are most likely to have meant.
 *
 * THE PERMISSION PROMPT LIVES HERE, and its trigger is the whole feature: a
 * reader whose 10009 carries an encrypted payload is asked to open it the
 * first time they use `group:` at all — not on page load, where the dialog
 * would arrive with no question attached to it, and not never, which is what
 * shipping the public half alone amounted to. See [unlockOwnGroups] for the
 * three rules that keep one prompt from becoming eight.
 *
 * The private rows are folded in as `own`, not kept beside it. rank() dedupes
 * a reader's rows on (id, host), so a group that is in BOTH halves collapses
 * to one — and to the public one, which is correct: it is not a secret if the
 * tag is in the clear.
 */
async function lookupGroups(partial) {
  await ensureLogin().catch(() => {});
  const own = await ownGroupCandidates().catch(() => []);
  // Started, NOT awaited, and that is the difference between a picker and a
  // hostage. A permission dialog is answered by a human on their own schedule
  // — or ignored entirely, with the tab still sitting there — so awaiting it
  // would leave the list on "Finding groups…" for as long as the reader
  // wanted to think about it, with their PUBLIC groups already in hand and
  // not being shown. So the ask is fired, the rows we have are returned with
  // a notice saying what is still pending, and `onUnlocked` re-asks when the
  // answer lands. The prompt is not raised again while one is open, nor after
  // a refusal — unlockOwnGroups()'s rules, which this call site deliberately
  // does not repeat.
  if (ownGroupLock && canDecrypt(ownGroupLock.scheme) && !unlockDenied) unlockOwnGroups();
  const secret = ownGroupSecret || [];
  let found = [];
  try {
    // Empty partial asks nothing of the relay: `group:` alone is "show me my
    // groups", and a match-all over every 39000 in the corpus is neither that
    // question nor a useful answer to it.
    if (partial) found = await relay.req({ kinds: [39000], search: partial, limit: 12 });
  } catch (e) { found = []; }
  const meta = found.map(metaGroup).filter(Boolean);
  const hosts = [...new Set(meta.map((g) => g.host).filter(Boolean))];
  if (hosts.length) await enrichProfiles(hosts).catch(() => {});
  const rows = rankGroups(partial, { own: [...own, ...secret], meta });
  // Every row here is a group and a name, which is the one thing the pill
  // cannot work out for itself — so a pick draws the name the reader picked BY
  // without a second round trip, and without the pill flashing the id first.
  // What it must not become is a shortcut past groupnames.js's own rule: rows
  // go in as candidates, so a name from your list and a name from the corpus
  // stay told apart there, and two hosts disagreeing about an id still leaves
  // the id on the pill.
  seedGroupNames(rows);
  return { rows, lock: groupLockState() };
}

/** Every human edit of the field, whatever made it — typing, paste, a pick. */
function onQueryEdit() {
  const text = $q.value.trim();
  document.body.classList.toggle("has-query", text.length > 0);
  clearTimeout(debounceTimer);
  // A half-written `from:` is not a search for "from:", and neither is a
  // `since:` with a calendar under it. All three popups share one square of
  // screen and one set of arrow keys, so while a picker owns them the results
  // preview stays shut.
  if (!text || field.picking) { closePopup(); return; }
  debounceTimer = setTimeout(() => runPopup(text), DEBOUNCE_MS);
}

// paintScores goes in for the same reason the entity page takes it: the faces
// the field and its picker draw carry the same score chip a card's does, and
// which lens fills it in is app state.
const field = mountSearchField($q, $mentions, {
  lookup: lookupAuthors, lookupGroup: lookupGroups, unlockGroups: retryUnlockGroups,
  onEdit: onQueryEdit, onSubmit: submitField, paintScores,
});

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

/**
 * Nothing came back — but "nothing matched" and "nothing COULD match" are two
 * different answers, and only one of them is worth trying a different term for.
 *
 * A window whose `until` is before its `since` is the second: it excludes every
 * event that has ever existed, and the relay returning zero is not evidence
 * about the search. It is easy to build without noticing — pick a `since`, then
 * pick an `until` from the same calendar a month too early — and easier still
 * to inherit from a shared URL, so the check is on the query rather than on the
 * picker that usually makes it.
 */
function emptyWindow(text) {
  const q = parseQuery(text || "");
  return q.since != null && q.until != null && q.since > q.until;
}

// `empty` is a view's own way of saying it has nothing, for a view that was
// asked no question: both sentences below answer "no results" by talking about
// the QUERY, and the feed has none — telling a reader of an empty feed that the
// window is inverted, or to try a different term, names things they never typed.
function statusBody(placeholder, empty) {
  if (s.error) return `<div class="error">${esc(s.error)}</div>`;
  if (s.loading && !s.hits.length) return placeholder;
  if (!s.hits.length) {
    if (empty) return empty;
    const why = emptyWindow(s.hitsFor)
      ? "The window is empty — until: is before since:, so nothing can fall inside it."
      : "Try a different term, or widen the filter above.";
    return `<div class="empty"><b>No results</b>${esc(why)}</div>`;
  }
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

/**
 * Which corpus this feed is, in one line, wherever it is drawn.
 *
 * It is not decoration. Signed in, the relay applies the reader's trust floor
 * to a plain NIP-01 read as well as to a search, so the feed IS a web-of-trust
 * feed and nothing in the ask says so. Signed out, exactly the same request is
 * the whole mirror in time order — a completely different list under the same
 * heading, and the reader is entitled to know which one they are looking at.
 *
 * The title attribute answers the question the "ranking as" control raises by
 * sitting a few pixels above this: it is a NIP-50 extension, it rides on a
 * search string, and this view sends none.
 */
const lensNote = () =>
  `<div class="prov" title="The feed is a plain NIP-01 read, so the &quot;ranking as&quot; lens — a NIP-50 observer: extension — applies to searches, not here.">` +
  (me
    ? "newest first, through your own web of trust — the relay drops authors below your trust floor"
    : "newest first, the whole corpus — sign in (the avatar in the field) to read it through your web of trust") +
  `</div>`;

/**
 * What the feed's heading says: "Latest", and what it was narrowed to.
 *
 * The chip that narrowed it is lit, but it is one of eight in a row that
 * scrolls sideways on a phone — where the lit one can be off screen entirely —
 * and it lives up in the header while the list is what the reader is looking
 * at. A hundred pictures under a bare "Latest" reads as the whole feed having
 * changed rather than as a filter being on, so the heading names the tab.
 */
const feedTitle = () => (tab.kinds ? `Latest &middot; ${esc(tab.label)}` : "Latest");

/** The list heading every card list wears: a name, and whatever goes right. */
const listHead = (title, right) =>
  `<div class="list-head"><div class="list-title">${title}</div><div class="list-right">${right}</div></div>`;

/**
 * The cards, and the two repaints that MUST follow them.
 *
 * A score chip is filled in by paintScores and a nip05 is verified by
 * watchNip05, both after the fact — so every list of cards ends in the same
 * three lines, and a list that forgets one draws chips that never fill or
 * verification marks that never resolve. Nothing about it announces itself, so
 * it is a function rather than a convention repeated at three call sites.
 */
function paintList($el, html) {
  $el.innerHTML = html;
  paintScores();
  paintCursor();
  watchNip05();
}

/** The feed page: the same cards as a search, over a list nobody searched for. */
function renderFeed() {
  const stats = s.error ? "" : `${s.hits.length} event${s.hits.length === 1 ? "" : "s"} · ${s.lastMs ?? "?"} ms`;
  // An empty feed is a fact about the INDEX (or about the reader's trust
  // floor), never about a query — there is no query. Both readings are named,
  // because "nothing here" means two different things depending on which of
  // them is looking. A third reading arrived with the chips: an empty MEDIA
  // feed says nothing about the relay's notes, and a reader who does not
  // notice which chip is lit would read it as one.
  const empty = `<div class="empty"><b>Nothing here yet</b>` +
    (me
      ? "This relay holds no recent posts from anyone your web of trust reaches."
      : "This relay's index holds no recent posts of these kinds.") +
    (tab.kinds ? ` The ${esc(tab.label)} filter is on — pick Everything for the whole feed.` : "") +
    `</div>`;
  // No Export button, unlike a search: that download exists to let somebody
  // argue with a RANKING — the query, the lens, the scores behind the order.
  // A time-ordered list has no order to defend.
  const body = statusBody(skelCards(4), empty) ?? s.hits.map((ev) => card(ev)).join("");
  paintList($results, listHead(feedTitle(), `<span class="list-stats">${stats}</span>`) + lensNote() + body);
}

// ---- the same feed, three cards, under the hero ---------------------------
//
// The landing page was a search box over an empty page: nothing to read, and
// no evidence that the index behind it holds anything at all. Three of the
// newest cards fix both, and "see more" opens the full page above.
//
// Only for a SIGNED-IN reader, which is the one condition this preview has.
// Signed out the same read is the whole mirror in time order — the firehose,
// including everything nobody's web of trust vouches for — and putting that
// under the search box would be the house showing you a feed it does not
// stand behind. The feed page still serves it to anyone who asks, labelled;
// the hero stays the clean page it is.
//
// Its own state of the same SHAPE as the results view's, driven by the same
// run(): it is on screen exactly when the results are not, so sharing `s`
// would have the two taking turns clearing each other — but everything after
// the ask (the stale-answer guard, the timing, the skeleton, the repaint when
// the names land) is identical, and writing that dance out a second time is
// how the two drift. It already had: the hand-written copy repainted over an
// expanded json panel, which run() has known not to do since the day a reader
// lost one mid-read.
const feedPreview = { requestId: 0, hits: [], hitsFor: null, lastMs: null, loading: false, error: null, reader: null, tab: null };

function hideFeedPreview() {
  feedPreview.requestId++;   // whatever is in flight must not reveal this again
  $feedPreview.hidden = true;
  $feedPreview.innerHTML = "";
  document.body.classList.remove("has-feed-preview");
}

/**
 * Where "see more" goes.
 *
 * A parameter on the ROOT, not a path of its own. The feed is the results view
 * with an empty query, and `/` is already the page — so `/?feed=1` is served
 * from cold by the route that serves the landing page, while `/feed` would be
 * a 404 until somebody added a route for it. The only thing the prettier url
 * would buy is being prettier, and the server has no business knowing this
 * view exists. `=1` is the page's existing idiom for a flag, as `spam=1`.
 *
 * One spelling, read by the link and by the click interceptor that turns it
 * into a render — two literals could drift into a link that reloads the page.
 */
const FEED_URL = "/?feed=1";

/**
 * That url, carrying the chip — `/?feed=1&tab=media`.
 *
 * The feed used to be one flag and nothing else, which was true while its
 * kinds were its own. Now the chips narrow it, and a narrowed feed that cannot
 * be linked to is a view whose state dies on reload — the same lie as a
 * control that does nothing, one step later. "Everything" writes no parameter,
 * so the plain `/?feed=1` still means what it always did and nothing that
 * already links to it changes.
 *
 * `tab` is spelled the same here as in a search url on purpose: it is the same
 * chip, and one slug means one thing wherever it appears.
 */
const feedUrl = (t = tab) => FEED_URL + (t.kinds ? `&tab=${t.slug}` : "");

/** Does [href] name the feed, whichever chip it carries? */
const isFeedHref = (href) => href === FEED_URL || href.startsWith(FEED_URL + "&");

function renderFeedPreview() {
  // Nothing, and nothing still coming: the hero goes back to being the hero
  // rather than standing a heading and a "see more" over an empty box. This is
  // also the error path — a feed that could not be read is not worth putting
  // an error where the landing page's content goes.
  if (!feedPreview.loading && !feedPreview.hits.length) { hideFeedPreview(); return; }
  const body = feedPreview.hits.length
    ? feedPreview.hits.map((ev) => card(ev)).join("")
    : skelCards(PREVIEW_CARDS);
  // The link carries the chip: "see more" of THESE three, not of a feed the
  // reader has just narrowed away from.
  paintList($feedPreview, listHead(feedTitle(), `<a class="feed-more" href="${feedUrl()}">See more &rarr;</a>`) + body);
}

/**
 * Draw the preview, if this is the hero and there is a reader to draw it for.
 *
 * Safe to call from anywhere that might have changed either of those — the
 * hero being shown, a sign-in, a sign-out — because it re-checks both rather
 * than trusting the caller to know.
 *
 * It re-asks every time, like every other view on this page, rather than
 * holding an answer for a while: a feed that says "latest" and hands back a
 * two-minute-old list on the way home is wrong in the one way this view is not
 * allowed to be wrong. What it does keep is the CARDS, so the wait redraws
 * nothing — the skeleton is for the first ask only. Two things it must not
 * keep across: a change of reader, because those cards were gated by the
 * previous account's web of trust and flashing them under the next one's name
 * for a round trip would be showing somebody another person's feed; and a
 * change of CHIP, because the heading and the "see more" link repaint from the
 * new tab immediately while the cards would still be the old one's — "Latest ·
 * Media" over three notes, which reads as the filter having answered.
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

function renderResults() {
  const stats = s.error ? "" : `${s.hits.length} result${s.hits.length === 1 ? "" : "s"} · ${s.lastMs ?? "?"} ms`;
  const right = `<span class="list-stats">${stats}</span>` +
    `<button type="button" id="export" class="export" title="Download this search and its results as text">Export</button>`;
  // Not `.map(card)`: map hands the index as card's second argument, where a
  // renderer would read it as the opts object.
  const body = statusBody(skelCards(4)) ?? s.hits.map((ev) => card(ev)).join("");
  paintList($results, listHead(esc(tab.label), right) + body);
}

/** Whether the list already on screen survives the wait for the next answer. */
const REPLACE = false, KEEP = true;

// One ask, rendered. Two parameters carry what used to be assumed: `st` is the
// state it drives, because the hero's preview is a second list on a page that
// only ever had one; `fetch` is a thunk rather than a query string, because
// the results view now answers two different questions — a NIP-50 search, and
// the feed's plain NIP-01 read. Everything after the ask is the same for all
// three callers: the stale-answer guard, the timing, the skeleton, and the
// repaint when the names land — which knows not to fire over a json panel the
// reader has opened.
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
    // `hitsFor` is what the SEARCH BOX would have to say for these hits to be
    // about it, and the feed's answer is "nothing does" — null, so reopening
    // the popup on focus can never show the feed under a typed query.
    st.hits = found.events; st.hitsFor = found.text ?? null; late = [found.names, found.groups, found.parents].filter(Boolean);
  } catch (e) {
    if (myId !== st.requestId) return;
    st.error = e.message || String(e); st.hits = []; st.hitsFor = null;
  }
  st.lastMs = Math.round(performance.now() - t0); st.loading = false;
  render();
  // The names land after the list does, the group names beside them and the
  // reply parents after both, so paint each when it arrives — and only if that
  // lookup actually learned something. Independently, because they are:
  // chaining them meant the names could not repaint until the parents had also
  // answered. Skipped while a raw event is expanded: a re-render would collapse
  // a panel the reader opened, and a name appearing is not worth taking that
  // away.
  for (const lookup of late) {
    lookup.then((learned) => {
      if (!learned || myId !== st.requestId) return;
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
function runPopup(text) { if (field.picking) return; openPopup(); run(s, () => search(text, POPUP_LIMIT, false), KEEP, renderPopup); }

function runFull(text) {
  clearTimeout(debounceTimer); // else a type-ahead still in flight re-opens the popup over the results
  cancelEntity(); // a search launched FROM an entity page must not be painted over by its slow fetch
  hideFeedPreview();   // …and the hero's preview is not part of a results page
  document.title = "SearchOverTrust";
  closePopup();
  field.close(); // Enter with nothing highlighted searches; the picker is done
  // The field lets go of the keyboard on submit, so the results are what it is
  // aimed at. j/k are the LETTERS j and k while a caret is in a text field —
  // keeping focus here would have made the whole shortcut unreachable without
  // first clicking somewhere else — and "/" brings the box back in one key.
  // On a phone it also puts the on-screen keyboard away, which was covering
  // the answer it had just been used to ask for.
  $q.blur();
  $results.hidden = false;
  document.body.classList.add("searching");
  document.body.classList.remove("feed"); // searching FROM the feed leaves it
  // Every way into the full view converges here — Enter, a chip/sort/spam/
  // lens change via rerun() — so this is the one place the URL learns about
  // it. Pushed, not replaced: each of those is a state the user chose and
  // Back should be able to undo. A restore FROM history is the exception,
  // and syncUrl() itself knows to stand down there.
  syncUrl();
  run(s, () => search(text, FULL_LIMIT, true), REPLACE, renderResults);
}

/**
 * The feed page: the newest [PAGE_CARDS] content events, full cards.
 *
 * Reached from the hero's "see more", from a pasted link, and from Back — all
 * three through applyUrl(), which holds navRestoring for exactly this reason,
 * so the syncUrl() below is a no-op on every one of them. It is here for the
 * fourth way in: a chip picked while already on this page, which is a place
 * the reader chose and Back should be able to undo, the same as a chip picked
 * during a search. `feed=1` plus that chip IS the whole state; the other four
 * parameters this view still takes none of.
 */
function runFeed() {
  clearTimeout(debounceTimer);
  cancelEntity();
  hideFeedPreview();   // the preview and the page are the same feed; one at a time
  // The chip belongs in the title for the same reason it belongs in the
  // heading: two tabs open on two narrowings of this feed are otherwise the
  // same word twice.
  document.title = `SearchOverTrust — latest${tab.kinds ? ` ${tab.label.toLowerCase()}` : ""}`;
  closePopup();
  field.close();
  $q.blur();   // a full-page list takes the keyboard, exactly as a search does
  $results.hidden = false;
  // `feed` hides the Filters half of the bar and keeps the chips. The three
  // behind Filters are NIP-50 extensions riding on a search string this view
  // does not send; the chips are the `kinds` of the read it does send, so they
  // are the half that works here. Taking a working control off the screen is
  // the same waste as leaving a dead one on it.
  document.body.classList.add("searching", "feed");
  syncUrl();
  run(s, () => fetchFeed(PAGE_CARDS), REPLACE, renderFeed);
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
  // selfHref, not a second spelling of it. This used to build the path here —
  // `kind 0 ? npub : note` — which is the same rule the cards apply, written
  // twice: a type-ahead row and the card for the same event could disagree
  // about where that event lives, and the copy here had no guard, so an event
  // with no id navigated to "/" and looked like the picker had reset the page.
  const href = ev && selfHref(ev);
  if (href) navigate(href);
}
/** Is the feed page the view on screen? One spelling, three readers. */
const onFeed = () => document.body.classList.contains("feed");

function rerun() {
  // The feed has no query to re-run, but it does have an answer that changes
  // with who is asking and with which chip is on: signing in applies the trust
  // floor, signing out lifts it, and the line under the head says which. Same
  // reason a search re-runs.
  if (onFeed()) { runFeed(); return; }
  const text = $q.value.trim();
  // The hero, with the preview under it, is the third thing a chip can change.
  // Nothing re-ran here at all before the preview existed — there was nothing
  // on the page to re-run — so a chip picked on the landing page lit up and
  // left the same three cards sitting underneath it, which is a filter that
  // says it is on and is not. The preview is the same feed as the page, so it
  // answers the same chip. Nothing goes to the URL: `/` is the floor Back
  // lands on, three cards under a hero are not a destination, and the "see
  // more" link is what carries the chip on to one.
  if (!text) { if ($results.hidden) showFeedPreview(); return; }
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
  document.body.classList.remove("searching", "has-query", "feed");
  closePopup();
  // The hero's own three cards, if there is a reader to draw them for. Here
  // rather than at each call site: every route to the hero — boot, Back, the
  // clear button, the brand — goes through this function.
  showFeedPreview();
}

/** The hero as a NAVIGATION — clear button, brand click. Pushed, so Back
    returns to the search that was just cleared instead of losing it. */
function reset() {
  showHero();
  syncUrl();
  $q.focus();
}

/**
 * Enter in the search box, however it arrived.
 *
 * A function rather than the body of the keydown branch below, because a
 * phone's action key is not reliably a keydown at all — searchfield.js takes
 * it off the `beforeinput` a soft keyboard DOES produce and calls this. Both
 * doors, one behaviour.
 */
function submitField() {
  // An arrowed-to row is a selection, same as a click on it: Enter opens
  // that record. A plain Enter with nothing highlighted stays the full
  // search — the popup is a preview of it, not a gate in front of it.
  if ($popup.classList.contains("open") && activeKey != null) { openPicked(s.hits[activeKey]); return; }
  const text = $q.value.trim();
  if (text) runFull(text);
}

$q.addEventListener("keydown", (e) => {
  // The picker gets first refusal on the arrows, Enter, Tab and Escape while
  // it is open — asked outright rather than by racing this listener in the
  // capture phase, because which of two listeners on one element runs first
  // is registration order and that is not a thing to depend on.
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

// "/" anywhere focuses the search box, the way every search page does it.
// Not while something is being typed into — see keynav.js's isTyping(), which
// is that rule, written once for the two shortcuts that need it.
document.addEventListener("keydown", (e) => {
  if (e.key !== "/" || e.metaKey || e.ctrlKey || e.altKey) return;
  const el = document.activeElement;
  if (!el || isTyping(el)) return;
  e.preventDefault();
  $q.focus();
  $q.select();
});

// ---- j and k walk the results --------------------------------------------
//
// The cursor is an EVENT, not a row number. The list re-renders under it more
// often than it looks: names and reply parents land after the cards do and
// repaint them, and a filter change replaces the list wholesale. Holding a row
// number through that would leave the cursor on whatever slid into position 3;
// holding the id keeps it on the thing the reader was reading, and a card that
// is no longer in the list is a cursor that no longer points at anything.
let cursorId = null;

// The cards a cursor can sit on are the ones that OPEN something. A permalink
// card carries no `data-href` — it is already the page it would open — so on
// an entity view this is empty and j/k do nothing at all, rather than lighting
// up a card with nowhere to go.
const cursorCards = () => [...$results.querySelectorAll(".result[data-href]")];
const cursorAt = (cards) => cards.findIndex((el) => el.dataset.id === cursorId);

/**
 * Draw the cursor where it is — after every render, which replaced the very
 * elements that were carrying the class.
 *
 * An id the list no longer holds is a cursor pointing at nothing, so it goes —
 * but only against a list that HAS cards. Every search paints a skeleton
 * first, and forgetting the id there cost the cursor everything that re-runs
 * the query: Back out of a card landed on the list with nothing selected, and
 * a chip change dropped the cursor off an event that was still in the results.
 * Keying this to the event instead of the row is only worth anything if the id
 * outlives the moment the list is empty.
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
  // `nearest` scrolls the least, which is what walking a list wants — but the
  // toolbar is sticky once results are up, and "nearest" knows nothing about
  // what is floating over the top of the page. Measured rather than guessed:
  // that bar is a different height on a phone, and grows again when the chip
  // row wraps.
  el.style.scrollMarginTop = Math.ceil($hero.getBoundingClientRect().height) + 12 + "px";
  el.style.scrollMarginBottom = "12px";
  el.scrollIntoView({ block: "nearest" });
}

document.addEventListener("keydown", (e) => {
  const move = navKey(e, document.activeElement);
  if (!move || $results.hidden) return;
  const cards = cursorCards();
  // Nothing to walk — a permalink, an empty result set, the error card — so
  // the key is not ours. This has to be decided BEFORE preventDefault(): a
  // shortcut that takes a key and then does nothing with it is worse than no
  // shortcut, and `j` is a key Firefox's find-as-you-type is otherwise about
  // to use.
  if (!cards.length) return;
  if (move === "open") {
    // Enter is only ours while the cursor is actually on something. Every
    // other Enter on this page belongs to whatever has focus — a chip, the
    // json button, a link — and swallowing those to do nothing would be worse
    // than not having the shortcut.
    const el = cards[cursorAt(cards)];
    if (!el) return;
    e.preventDefault();
    navigate(el.dataset.href);
    return;
  }
  e.preventDefault();
  // A type-ahead preview left open over the list is furniture at this point:
  // the reader is walking the results themselves.
  closePopup();
  moveCursor(cards, move === "next" ? 1 : -1);
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
  if (field.picking) return;
  const text = $q.value.trim();
  if (s.hits.length && s.hitsFor === text && text && $results.hidden) openPopup();
});

// Clicks inside a LIST OF CARDS. Two lists exist now — the results view and
// the hero's feed preview — and they behave identically down to the json
// panel, so the behaviour is a factory over "which events is this list
// showing" rather than a second copy under the preview. (`#export` can only
// ever appear in the results head; the preview never renders one.)
//
// Note the thunk: each answer REPLACES the preview's array, so a handler closed
// over the array itself would keep serving the first answer's events to the
// json panel forever.
const cardClicks = (hitsOf) => (e) => {
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
  // The card itself opens its own page. The hover lift has always said it
  // does — border, shadow and a 1px rise, the page's own vocabulary for
  // "this is a thing you click" — while only the links inside it navigated,
  // so a note card promised a destination and delivered nothing.
  //
  // Three things it must NOT swallow, in order of how easily they are lost:
  // a real control (a link, the json button, an audio scrubber), which owns
  // its own click and usually goes somewhere else entirely; a text SELECTION,
  // because dragging across a body to copy it ends in a mouseup that is not a
  // navigation; and anything at permalink depth, where cards.js sets no
  // data-href because the card IS the page.
  //
  // Keyboard and middle-click are served by the byline date, which is a real
  // anchor to the same place — that is what makes this safe to add rather
  // than a div pretending to be a link.
  // `.raw` covers the whole json block, not just its button: a click inside
  // an expanded raw event is somebody reading it, and navigating away would
  // close the panel they just opened.
  if (e.target.closest("a, button, input, textarea, select, label, audio, video, summary, .raw")) return;
  // `getSelection()` may return null, and String(null) is "null" — truthy,
  // which would have made every card unclickable wherever that happens.
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
//
// `feed=1` is the sixth and last thing this url can say, and it is exclusive
// with four of the five: the feed is the results view with an empty query and
// none of the bar's extensions. `tab` is the exception, because the chips are
// the one control that acts on this view — so `/?feed=1&tab=media` is the
// whole of the feed's state, and the pair is written and read together. A
// parameter rather than a `/feed` path deliberately — the root already serves
// this page, and a new path would need a route on the server before a reload
// of it could work at all.

// A restore must not re-push what it is restoring — popstate would otherwise
// truncate the forward stack it is trying to walk.
let navRestoring = false;

/**
 * The url for the page as it stands — optionally with a DIFFERENT query, which
 * is what a hashtag chip navigates to. applyUrl() reads the url as the whole
 * of the page's state, so anything a chip's url omits is a setting the chip
 * silently clears: the kind tab, the sort, include:spam, and the ranking lens
 * — three of which the Filters badge is at that moment counting. A chip
 * changes the query and nothing else.
 *
 * Deliberately knows nothing about the feed, even though the feed is a url this
 * page can be at. A chip on a feed CARD is the reason: it asks this function
 * for a search, from a page that is not one, and an early return of `/?feed=1`
 * here would answer a question nobody asked. "Where am I now" is syncUrl's
 * question, and that is where the feed is answered.
 */
function currentUrl(text = $q.value.trim(), showing = !$results.hidden) {
  const p = new URLSearchParams();
  if (showing && text) p.set("q", text);
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
  // The feed is the flag and the chip, and nothing else, because it reads
  // nothing else: a url carrying a sort this view ignores would be state that
  // cannot be restored, which is the same lie as a control that does nothing.
  // Every path here today leaves the feed before syncing, so this is the guard
  // for the next caller rather than for any current one.
  const url = onFeed() ? feedUrl() : $results.hidden ? "/" : currentUrl();
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
      hideFeedPreview();
      document.body.classList.remove("has-query", "feed");
      document.body.classList.add("searching");
      closePopup();
      $results.hidden = false;
      // The entity view hands its drawn events BACK: the `json` toggle looks an
      // event up by id among the page's current results, and a permalink used
      // to leave that empty — so the one card on the page answered "no longer
      // in the current results" about itself.
      showEntity(seg, { paintScores, ensureLogin, setHits: (evs) => { s.hits = evs; } });
      return false; // no search running — boot still signs in eagerly
    }
    cancelEntity(); // leaving the entity view invalidates its in-flight fetch
    document.title = "SearchOverTrust";
    const p = new URLSearchParams(location.search);
    // The fourth view, and the only one that is a whole page from one flag and
    // one chip: the feed takes no q and none of the bar's three extensions. So
    // those controls go back to their DEFAULTS with the url that carries none
    // of them — the Filters panel is hidden here, and a `sort:rank` left over
    // from the previous search would otherwise be applied, invisibly, to the
    // next search typed from this page. The tab is read instead of cleared,
    // because on this view it is not a leftover: it is what the feed asked
    // for. runFeed() awaits sign-in itself, hence `true`: boot does not need
    // to kick it a second time.
    if (p.get("feed") === "1") {
      tab = KIND_TABS.find((t) => t.slug === p.get("tab")) || KIND_TABS[0];
      // A hand-made `/?feed=1&q=cats&sort=rank` is a url naming two views.
      // The feed wins — it is checked first — so the address bar is corrected
      // to what is actually on screen rather than left describing a search
      // that is not running. Compared against the url this view WOULD write,
      // so an unknown `tab=nonsense` is corrected too rather than left naming
      // a chip that is not lit. replaceState, not push: this is the same
      // place, spelled properly, and it leaves the forward stack alone.
      if (location.search !== feedUrl().slice(1)) history.replaceState(null, "", feedUrl());
      renderChips();
      $sort.value = "";
      $spam.checked = false;
      applyViewingAs(null, null); // last of the three on purpose: it recounts the
                                  // badge, and the two above fire no `change` of
                                  // their own — the same order the search branch
                                  // below relies on
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
      // The URL carries the key; the label wants the name. Filled in after
      // the fact so the restore itself never waits on a profile lookup.
      enrichProfiles([as]).then(() => {
        const nm = displayName(profiles.get(as));
        if (nm && viewingAs === as) { $obsCurrent.textContent = nm; renderAdvCount(); }
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
  // "See more" is a real anchor to a real url — middle-click, copy-link and a
  // cold load all work, because it is the root with a parameter and the root
  // is the page — but a plain click renders it in place like every other
  // internal link, socket and NIP-42 auth intact. Tested before the `?q=` arm
  // below only because it is the cheaper question; a feed url does not match
  // that pattern either way.
  if (isFeedHref(href)) { e.preventDefault(); navigate(href); return; }
  // `/?q=…` is the third shape a card can link to, after "/" and the NIP-19
  // paths: a hashtag chip is a SEARCH, and applyUrl() already restores one
  // from exactly this url. Without this arm the chip still worked — as a full
  // page load, which tears down the socket and re-does the NIP-42 handshake
  // to arrive at the same results.
  //
  // The chip's url is rebuilt through currentUrl() rather than followed as
  // written: it names a query and knows nothing about the page it was clicked
  // on, and every filter it does not name is one applyUrl() would clear. From
  // a FEED card that rebuild carries the kind chip and nothing else — the feed
  // resets the sort, the spam toggle and the lens, and keeps the tab because
  // the tab is a filter it was itself running under. A hashtag clicked in a
  // Media feed searches that tag in Media, which is the same rule the chip
  // follows inside a search.
  if (/^\/\?q=/.test(href)) {
    e.preventDefault();
    navigate(currentUrl(new URLSearchParams(href.slice(1)).get("q") || "", true));
    return;
  }
  if (!/^\/(npub|nprofile|note|nevent|naddr)1[a-z0-9]+$/i.test(href)) return;
  e.preventDefault();
  navigate(href);
});

// ---- media that loads when it is nearly on screen -------------------------
//
// A video card renders its url in `data-src`. `preload="metadata"` is a range
// request per card — two for an mp4 whose moov atom is at the end — so a
// search returning sixty short videos opened sixty of them before the reader
// had scrolled past the second. This promotes the url one screen ahead, which
// still leaves the first frame time to paint before the card arrives.
//
// Armed from a MutationObserver rather than from each render because cards are
// inserted from more than one place — renderResults() here, and entity.js for
// a permalink — and a path that forgot to arm would show a video that never
// loads at all. A card renderer cannot know about the page's scroll, and a
// render path cannot forget a call it does not have to make.
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

/** An internal path as a pushState render — the one place both click paths
    (a card's anchors, and the card itself) turn a href into a view. */
function navigate(href) {
  if (location.pathname + location.search !== href) history.pushState(null, "", href);
  applyUrl();
}

// Chips render at boot, not only inside applyUrl's search branch: the entity
// branch returns before that code, so a direct load of /npub1… used to show
// an empty chip row until the first Back into a search view.
// The face from the LAST load, drawn on the first paint — before the extension
// has been asked anything, let alone the relay.
//
// getPublicKey() is a local call but it is still a round trip to the extension,
// and everything after it is network. Assuming the account you were last signed
// in as, when you have not since chosen to be signed out, is what makes the
// picture present at first paint instead of a hole that fills in later.
//
// It is an ASSUMPTION and the field says so: `.pending` renders it faded and
// titled "signing in as", the same as any other unproven state. Switch accounts
// in your extension between loads and it is wrong for the few milliseconds
// prefetchFace() takes to correct it — the honest cost of not showing an empty
// seat to everybody else. Signed out by choice, nothing is assumed at all.
if (wantsSignIn() && meFace) mePending = meFace.pubkey;
renderChips();
renderWhoami();
// The anonymous reference socket, opened at BOOT rather than on first use.
// Every path this page can take needs it within a second or two — the face in
// the field, the names under the results, the score chips, the entity view's
// whole-index fallback — and it used to be opened from cold at the moment one
// of them asked, which put a WebSocket handshake in front of the first answer
// instead of alongside the main socket's. refConn() dedupes its own opening,
// so this is a warm-up rather than a connection anybody has to account for.
refConn().catch(() => {});
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
//
// And never where a caret arrives without a keyboard behind it. `focus()` from
// script cannot raise a soft keyboard — mobile browsers raise one only for a
// focus a finger caused — so on a phone this autofocus landed a blinking caret
// in a field nothing could type into, and left it there: tapping the box did
// nothing either, the element being focused already. softKeyboard() is that
// distinction, and searchfield.js owns it because the field's own rules turn
// on the same question.
if ($results.hidden && !softKeyboard()) $q.focus();
