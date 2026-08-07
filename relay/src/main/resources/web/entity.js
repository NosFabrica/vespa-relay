// The NIP-19 entity view: /npub1…, /nprofile1…, /note1…, /nevent1…, /naddr1…
// each render the thing they name, with the same cards the search uses — in
// permalink depth, so the event is whole rather than previewed.
//
// The fetch order is the relay's own philosophy applied to one link. A
// signed-in reader asks THROUGH THEIR LENS first — the authenticated socket,
// which the store trust-gates — and only then anonymously. An event the lens
// held back but the index holds is OFFERED, not shown: a notice that it
// exists with its author outside the reader's web of trust, and a "show it
// anyway" that renders it under a persistent warning. The choice to step
// outside the lens belongs to the reader, and the page never makes it for
// them in either direction. An anonymous visitor has no lens, so their page
// is simply the index.
//
// When NOBODY here has it and the identifier carries relay hints, those are
// dialed as the last resort (gated: ws only, no private hosts, no mixed
// content) — and a hit is both rendered and handed back to this relay for
// indexing, so the next visit of the same link is served locally.
//
// The identifier decides the QUERY; the fetched event's kind decides the
// CARD. A note1… id can name an article or a live stream — it renders as
// what it is, not as what the URL called it.

import { refConn, relay } from "./shared/conn.js";
import { Relay } from "./shared/relay.js";
import { enrichProfiles } from "./shared/profiles.js";
import { unknownParents, loadParentAuthors } from "./shared/parents.js";
import { watchNip05 } from "./shared/nip05.js";
import { esc, titleOf } from "./shared/format.js";
import { kindLabel } from "./shared/kinds.js";
import { nip19Parse, shortNpub } from "./shared/nip19.js";
import { njumpFor, tagsWhere } from "./cards/base.js";
import { card, namedPubkeys } from "./cards.js";
import { loadRelated, relatedHtml } from "./related.js";

// A token, not a flag: navigating away (or to the next entity) invalidates
// any fetch still in flight, so a slow lookup can never paint over the view
// that replaced it.
let token = 0;
export function cancelEntity() { token++; }

const shortIdent = (v) => (v.length > 24 ? v.slice(0, 14) + "…" + v.slice(-8) : v);

function headHtml(raw) {
  return `
    <div class="entity-head">
      <a href="/" class="back-home">← Search</a>
      <span class="ident" title="${esc(raw)}">${esc(shortIdent(raw))}</span>
      <a href="${njumpFor(raw)}" target="_blank" rel="noopener noreferrer">open on njump ↗</a>
    </div>`;
}

const emptyState = (title, body) => `<div class="empty"><b>${esc(title)}</b>${esc(body)}</div>`;

async function fetchEntity(conn, p, timeoutMs) {
  // Replaceable kinds can still hand back more than one event; newest wins.
  const newest = (evs) => evs.reduce((a, b) => (!a || b.created_at > a.created_at ? b : a), null);
  if (p.type === "npub" || p.type === "nprofile") {
    return newest(await conn.req({ kinds: [0], authors: [p.pubkey], limit: 1 }, timeoutMs));
  }
  if (p.type === "note" || p.type === "nevent") {
    return (await conn.req({ ids: [p.id], limit: 1 }, timeoutMs))[0] || null;
  }
  return newest(await conn.req({ kinds: [p.kind], authors: [p.author], "#d": [p.d], limit: 1 }, timeoutMs));
}

/**
 * A relay hint as written in an identifier, reduced to a url this page will
 * actually dial — or null. Same rules the observer stats page applies to
 * 10040 relay tags, for the same reasons: only ws schemes; never loopback or
 * private ranges (in a hint those mean the MINTER's machine, and from this
 * browser they would mean the reader's); and never plain ws:// from an https
 * page, which the browser refuses before a packet moves.
 */
function normalizeHint(raw) {
  if (!raw) return null;
  const t = String(raw).trim();
  if (!t || /\s/.test(t)) return null;
  let u;
  try { u = new URL(/^wss?:\/\//i.test(t) ? t : "wss://" + t); } catch (e) { return null; }
  if (u.protocol !== "ws:" && u.protocol !== "wss:") return null;
  if (location.protocol === "https:" && u.protocol === "ws:") return null;
  if (/^(localhost|127\.|10\.|192\.168\.|169\.254\.|\[::1\]|0\.0\.0\.0)/i.test(u.hostname) ||
      /^172\.(1[6-9]|2\d|3[01])\./.test(u.hostname)) return null;
  const path = u.pathname === "/" ? "" : u.pathname.replace(/\/+$/, "");
  return `${u.protocol}//${u.host}${path}`;
}

/**
 * The hint fallback. nevent/nprofile/naddr carry the relays their minter
 * believed hold the thing; when THIS relay doesn't, asking them is the
 * difference between a page and a shrug. Our own relay is always asked
 * first — the hints only run on a miss. One at a time (hints are nearly
 * always one or two), a short budget each, first hit wins, and the socket is
 * closed whatever happens.
 */
const HINT_TRIES = 3;
const HINT_TIMEOUT_MS = 6000;
async function fetchFromHints(parsed, stage = () => {}) {
  const urls = [...new Set((parsed.relays || []).map(normalizeHint).filter(Boolean))].slice(0, HINT_TRIES);
  for (const url of urls) {
    stage(`not here — asking its relay hint ${url.replace(/^wss?:\/\//, "")}…`);
    const r = new Relay(url);
    try {
      const ev = await fetchEntity(r, parsed, HINT_TIMEOUT_MS);
      if (ev) return { ev, from: url };
    } catch (e) { /* an unreachable hint is normal; try the next */ }
    finally { try { r.ws && r.ws.close(); } catch (e) {} }
  }
  return { ev: null, from: null };
}

/**
 * Hand a hint-fetched event to OUR relay. Two things at once: the index
 * gains an event it was missing — the whole reason the hint path ran — and
 * the relay's signature verification passes judgement on a payload this page
 * took from a third party it had no reason to trust. The verdict is printed
 * either way; a rejection (bad signature, policy) is a fact worth showing,
 * not a failure to hide.
 */
async function submitForIndexing(ev, host, my) {
  const note = () => document.getElementById("prov");
  try {
    const conn = await refConn();
    await conn.publish(ev);
    if (my === token && note()) note().textContent = `was not in this relay's index — fetched from ${host}, and indexed here now (signature verified by this relay)`;
  } catch (e) {
    if (my === token && note()) note().textContent = `fetched from ${host} — this relay declined to index it: ${e.message || e}`;
  }
}

/** This page's render depth — the permalink is always the whole card. */
const FULL = { full: true };

function titleFor(ev, parsed) {
  const t = ev ? (titleOf(ev) || kindLabel(ev.kind)) : parsed ? parsed.type : "not found";
  return `SearchOverTrust — ${t}`;
}

/**
 * Names and faces for everyone the card will mention. Two sets, because they
 * answer different questions:
 *
 * - every 64-hex tag value, capped: the FACES. A follow set's strip, a
 *   community's moderators. Broad and cheap to be wrong about — a value that
 *   turns out to be an event id costs one lookup that finds nothing.
 * - namedPubkeys: the NAMES, from cards.js, which knows what the renderers
 *   actually write out. Not a subset of the above: a zap receipt's sender
 *   lives inside the stringified request in its `description` tag, so no scan
 *   of the outer event's tags can reach it, and the permalink rendered that
 *   one person as an npub.
 *
 * "Never an npub where a name exists" only holds if the profiles are actually
 * loaded before the card renders.
 *
 * namedPubkeys takes the DEPTH this page will render at, because some cards
 * name more people the deeper they are drawn — a list's people grid fills more
 * cells here than in a result row, and each of those cells is a name.
 */
async function enrichMentions(ev) {
  const faces = [...new Set(tagsWhere(ev, () => true).map((t) => t[1]).filter((v) => /^[0-9a-f]{64}$/.test(v || "")))];
  // The parent lookup runs ALONGSIDE the mentions, not before them: they ask
  // different questions of the same relay, and this page renders once — every
  // round trip it serialises is dead time on a blank card. It cannot be
  // skipped entirely, though: on a reply whose `e` tag names no author, WHO
  // the parent is only becomes answerable once the parent event has been
  // fetched, and namedPubkeys cannot declare a pubkey nothing here knows yet.
  await Promise.all([
    loadParentAuthors(unknownParents([ev])),
    enrichProfiles([...new Set([ev.pubkey, ...faces.slice(0, 50), ...namedPubkeys(ev, FULL)])]),
  ]);
  // Which is what this second pass is for — the parent's own profile, and a
  // no-op when the lookup above learned nobody new.
  await enrichProfiles(namedPubkeys(ev, FULL));
}

/**
 * What a git permalink shows under its card, once the card is already up.
 *
 * Deliberately AFTER the paint and never awaited by it: this is a second REQ,
 * and a repository's issue list is not worth a blank page for. It fails
 * silently for the same reason — the page it decorates is complete without it.
 *
 * The reader's lens asks when there is one, matching the fetch above: a page
 * that trust-gated the event and then listed its answers unfiltered would be
 * showing, under a card, exactly what it just declined to show as one.
 *
 * `setHits` hands the drawn events back to app.js, which is what makes their
 * `json` toggles work — that button looks the event up by id in the page's
 * current results, and on this view there were none.
 */
async function paintRelated(ev, my, { paintScores, setHits }) {
  const conn = relay.authed ? relay : await refConn().catch(() => null);
  if (!conn || my !== token) return;
  const shape = await loadRelated(ev, conn);
  if (!shape || my !== token) return;
  const html = relatedHtml(shape);
  if (!html) return;
  const $results = document.getElementById("results");
  if (!$results) return;
  $results.insertAdjacentHTML("beforeend", html);
  setHits && setHits([ev, ...shape.events]);
  paintScores();
  watchNip05();
}

/**
 * Render the entity named by [seg] (the URL path segment) into #results.
 * paintScores and ensureLogin arrive as hooks because the lens they involve
 * is app state; everything else here owns itself.
 */
export async function showEntity(seg, { paintScores, ensureLogin, setHits }) {
  const my = ++token;
  const $results = document.getElementById("results");
  const parsed = nip19Parse(seg);

  if (!parsed) {
    // The server only routes bech32-SHAPED paths here, so this is a checksum
    // or structure failure: a truncated paste, a typo. Say that, instead of
    // a blank page or a fake "not found" that blames the relay.
    $results.innerHTML = headHtml(seg) +
      emptyState("Not a valid NIP-19 identifier", "The checksum does not match — the link was probably truncated or mistyped.");
    document.title = "SearchOverTrust";
    return;
  }

  // The wait can be real — sign-in, two asks, up to three hint dials — so
  // the skeleton narrates which step it is on rather than shimmering mutely
  // for twenty seconds. The stage line is honest loading UI, same doctrine
  // as the stats pages' status text.
  $results.innerHTML = headHtml(parsed.raw) +
    `<div class="skel-card"><div class="skel-line" style="width:34%"></div><div class="skel-line" style="width:92%"></div><div class="skel-line" style="width:66%"></div></div>` +
    `<div class="entity-stage" id="entity-stage">looking it up…</div>`;
  const stage = (msg) => {
    if (my !== token) return;
    const el = document.getElementById("entity-stage");
    if (el) el.textContent = msg;
  };

  let ev = null, err = null, hint = null, gated = null;
  try {
    // The anonymous socket is opened ALONGSIDE sign-in, not after the lens has
    // already missed. Every path below reaches it — the whole-index fallback,
    // the names, the score chips — and opening a WebSocket is a round trip
    // before a single byte of question moves. A signed-in reader got this for
    // free (login() fetches their own face over it); a signed-out one, or one
    // whose lens holds the event, was paying the handshake in the middle of
    // the lookup. refConn() dedupes its own opening, so this is a warm-up, not
    // a second connection.
    const warming = refConn().catch(() => null);
    // Settle sign-in first: whether there is a lens decides who gets asked.
    stage("signing in…");
    try { await ensureLogin(); } catch (e) {}
    if (my !== token) return;

    if (relay.authed) {
      // Through the reader's web of trust first — the same gate the store
      // applies to their searches applies to their permalinks.
      stage("asking this relay, through your web of trust…");
      ev = await fetchEntity(relay, parsed);
    }
    if (!ev) {
      stage(relay.authed ? "not in your network's view — checking the whole index…" : "asking this relay…");
      const conn = (await warming) || (await refConn());
      const anon = await fetchEntity(conn, parsed);
      // Present in the index but held back by the lens: OFFERED, not shown.
      if (anon && relay.authed) gated = anon;
      else ev = anon;
    }
    if (!ev && !gated && (parsed.relays || []).length) {
      ({ ev, from: hint } = await fetchFromHints(parsed, stage));
      if (my !== token) return;
    }
    if (ev || gated) {
      stage("fetching names…");
      await enrichMentions(ev || gated);
    }
  } catch (e) { err = e; }
  if (my !== token) return;

  if (err) {
    $results.innerHTML = headHtml(parsed.raw) + `<div class="error">${esc(err.message || String(err))}</div>`;
  } else if (gated) {
    // The reader's web of trust said no; the index said it exists. Both
    // facts are shown, and the step outside the lens is the reader's click,
    // never the page's guess — with the warning staying on the revealed
    // card so the choice remains visible after it is made.
    const what = gated.kind === 0 ? "profile" : "event";
    $results.innerHTML = headHtml(parsed.raw) +
      `<div class="empty"><b>Outside your web of trust</b>` +
      `This ${what} is in this relay's index, but its author scores nothing under your lens, so the relay held it back.` +
      `<div><button type="button" id="reveal" class="reveal-btn">Show it anyway</button></div></div>`;
    document.title = titleFor(null, parsed);
    const btn = document.getElementById("reveal");
    if (btn) btn.onclick = () => {
      if (my !== token) return;
      $results.innerHTML = headHtml(parsed.raw) +
        `<div class="prov warn">⚠ shown from outside your web of trust — the author has no score under your lens</div>` +
        card(gated, FULL);
      document.title = titleFor(gated, parsed);
      setHits && setHits([gated]);
      paintScores();
      watchNip05();
      paintRelated(gated, my, { paintScores, setHits });
    };
    return;
  } else if (!ev) {
    // Absence here is a fact about THIS MIRROR, not about the event — say so,
    // and hand the reader to the wider network instead of a dead end.
    const what = parsed.type === "npub" || parsed.type === "nprofile"
      ? `No profile event for ${shortNpub(parsed.pubkey)} in this relay's index.`
      : "This event is not in this relay's index.";
    const also = (parsed.relays || []).length
      ? "Its relay hints did not answer with it either — "
      : "It may exist elsewhere — ";
    $results.innerHTML = headHtml(parsed.raw) + emptyState("Not here", `${what} ${also}try njump above.`);
  } else {
    // A hint-fetched event renders with its provenance on it, then goes to
    // this relay for indexing; submitForIndexing rewrites the note with the
    // relay's verdict.
    const host = hint ? hint.replace(/^wss?:\/\//, "") : null;
    const prov = hint
      ? `<div class="prov" id="prov">not in this relay's index — fetched from its hint <span class="mono">${esc(host)}</span>, submitting here for indexing…</div>`
      : "";
    $results.innerHTML = headHtml(parsed.raw) + prov + card(ev, FULL);
    setHits && setHits([ev]);
    if (hint) submitForIndexing(ev, host, my);
    paintRelated(ev, my, { paintScores, setHits });
  }
  document.title = titleFor(ev, parsed);
  paintScores();
  watchNip05();
}
