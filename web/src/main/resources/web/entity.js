// The NIP-19 entity view: /npub1, /nprofile1, /note1, /nevent1 and /naddr1
// each render the thing they name with the search's cards, at permalink
// depth. A signed-in reader asks through their lens first, then
// anonymously; an event the lens held back but the index holds is offered
// behind a "show it anyway", never shown outright. When nobody here has it
// and the identifier carries relay hints, those are dialed last, and a hit
// is handed back to this relay for indexing. The identifier decides the
// query; the fetched event's kind decides the card.

import { refConn, relay } from "./shared/conn.js";
import { Relay } from "./shared/relay.js";
import { enrichProfiles } from "./shared/profiles.js";
import { unknownParents, loadParentAuthors } from "./shared/parents.js";
import { postedTo } from "./shared/groups.js";
import { enrichGroupNames } from "./shared/groupnames.js";
import { watchNip05 } from "./shared/nip05.js";
import { esc, titleOf } from "./shared/format.js";
import { kindLabel } from "./shared/kinds.js";
import { nip19Parse, shortNpub } from "./shared/nip19.js";
import { njumpFor, tagsWhere } from "./cards/base.js";
import { card, namedPubkeys } from "./cards.js";
import { forgetProvenance } from "./provenance.js";
import { loadRelated, relatedHtml } from "./related.js";

// Navigating away invalidates any fetch still in flight, so a slow lookup
// never paints over the view that replaced it.
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
 * A relay hint reduced to a url this page will dial, or null: ws schemes
 * only, never loopback or private ranges (in a hint those mean the minter's
 * machine), and never plain ws:// from an https page.
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
 * The hint fallback, run only on a miss here: one hint at a time, a short
 * budget each, first hit wins, and the socket is closed whatever happens.
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
 * Hand a hint-fetched event to our relay, whose signature check is the
 * judgement on a payload taken from a third party. The verdict is printed
 * either way; a rejection is a fact worth showing.
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

/** This page's render depth: the permalink is always the whole card. */
const FULL = { full: true };

function titleFor(ev, parsed) {
  const t = ev ? (titleOf(ev) || kindLabel(ev.kind)) : parsed ? parsed.type : "not found";
  return `SearchOverTrust — ${t}`;
}

/**
 * Names and faces for everyone the card will mention: every 64-hex tag
 * value, capped, for the faces, and namedPubkeys at this page's depth for
 * the names (a zap's sender is reachable by no tag scan). Profiles must be
 * loaded before the card renders, or it falls back to an npub.
 */
async function enrichMentions(ev) {
  const faces = [...new Set(tagsWhere(ev, () => true).map((t) => t[1]).filter((v) => /^[0-9a-f]{64}$/.test(v || "")))];
  // The parent lookup and the room name run alongside the mentions: this page
  // renders once, so every serialised round trip is dead time on a blank card.
  const room = postedTo(ev);
  await Promise.all([
    loadParentAuthors(unknownParents([ev])),
    enrichProfiles([...new Set([ev.pubkey, ...faces.slice(0, 50), ...namedPubkeys(ev, FULL)])]),
    room ? enrichGroupNames([room]) : null,
  ]);
  // The parent's own profile, known only once the parent has been fetched.
  await enrichProfiles(namedPubkeys(ev, FULL));
}

/**
 * What a git permalink shows under its card, after the paint and never
 * awaited by it. It asks through the reader's lens when there is one, to
 * match the fetch above. `setHits` hands the drawn events to app.js so their
 * json toggles can find them.
 */
async function paintRelated(ev, my, { paintScores, setHits }) {
  const conn = relay.authed ? relay : await refConn().catch(() => null);
  if (!conn || my !== token) return;
  const shape = await loadRelated(ev, conn);
  if (!shape || my !== token) return;
  const html = relatedHtml(shape);
  if (!html) return;
  const $results = document.getElementById("results");
  // The gated card's "Show it anyway" calls this again under the same token,
  // so "has one already" is the guard the token cannot give.
  if (!$results || $results.querySelector(".related")) return;
  $results.insertAdjacentHTML("beforeend", html);
  setHits && setHits([ev, ...shape.events]);
  paintScores();
  watchNip05();
}

/**
 * The card in a slot that can be redrawn without touching what came after
 * it: paintRelated appends to #results and declines to rebuild its section.
 */
const cardSlot = (ev) => `<div id="entity-card">${card(ev, FULL)}</div>`;

/**
 * Ask for this entity's provenance row and redraw the card if it learned
 * any. [seedRow] is app.js's because the lens the ask is made through is app
 * state; absent means no row here.
 */
function fillRow(ev, my, { seedRow, paintScores }) {
  if (!seedRow) return;
  // The gated read and the open one are separate asks; each redraws on its own.
  let asks = [];
  try { asks = seedRow([ev]) || []; } catch (e) { return; }
  for (const ask of asks) {
    Promise.resolve(ask).then((learned) => {
      const slot = document.getElementById("entity-card");
      if (!learned || my !== token || !slot) return;
      slot.innerHTML = card(ev, FULL);
      paintScores();
      watchNip05();
    }).catch(() => {});
  }
}

/**
 * Render the entity named by [seg] (the URL path segment) into #results.
 * paintScores and ensureLogin arrive as hooks because the lens they involve
 * is app state.
 */
export async function showEntity(seg, { paintScores, ensureLogin, setHits, seedRow }) {
  const my = ++token;
  // The last search's row would make its presence mean "how you got here";
  // [seedRow] asks again for the entity itself once it is drawn.
  forgetProvenance();
  const $results = document.getElementById("results");
  const parsed = nip19Parse(seg);

  if (!parsed) {
    // The server only routes bech32-shaped paths here, so this is a checksum
    // or structure failure: a truncated paste, a typo.
    $results.innerHTML = headHtml(seg) +
      emptyState("Not a valid NIP-19 identifier", "The checksum does not match — the link was probably truncated or mistyped.");
    document.title = "SearchOverTrust";
    return;
  }

  // The wait can be real (sign-in, two asks, up to three hint dials), so the
  // skeleton narrates which step it is on.
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
    // The anonymous socket opens alongside sign-in: every path below reaches
    // it, and refConn() dedupes its own opening.
    const warming = refConn().catch(() => null);
    // Settle sign-in first: whether there is a lens decides who gets asked.
    stage("signing in…");
    try { await ensureLogin(); } catch (e) {}
    if (my !== token) return;

    if (relay.authed) {
      stage("asking this relay, through your web of trust…");
      ev = await fetchEntity(relay, parsed);
    }
    if (!ev) {
      stage(relay.authed ? "not in your network's view — checking the whole index…" : "asking this relay…");
      const conn = (await warming) || (await refConn());
      const anon = await fetchEntity(conn, parsed);
      // Present in the index but held back by the lens: offered, not shown.
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
    // The step outside the lens is the reader's click, and the warning stays
    // on the revealed card.
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
        cardSlot(gated);
      document.title = titleFor(gated, parsed);
      setHits && setHits([gated]);
      paintScores();
      watchNip05();
      fillRow(gated, my, { seedRow, paintScores });
      paintRelated(gated, my, { paintScores, setHits });
    };
    return;
  } else if (!ev) {
    // Absence is a fact about this mirror, not about the event.
    const what = parsed.type === "npub" || parsed.type === "nprofile"
      ? `No profile event for ${shortNpub(parsed.pubkey)} in this relay's index.`
      : "This event is not in this relay's index.";
    const also = (parsed.relays || []).length
      ? "Its relay hints did not answer with it either — "
      : "It may exist elsewhere — ";
    $results.innerHTML = headHtml(parsed.raw) + emptyState("Not here", `${what} ${also}try njump above.`);
  } else {
    // A hint-fetched event renders with its provenance on it, then goes to
    // this relay for indexing; submitForIndexing rewrites the note with the verdict.
    const host = hint ? hint.replace(/^wss?:\/\//, "") : null;
    const prov = hint
      ? `<div class="prov" id="prov">not in this relay's index — fetched from its hint <span class="mono">${esc(host)}</span>, submitting here for indexing…</div>`
      : "";
    $results.innerHTML = headHtml(parsed.raw) + prov + cardSlot(ev);
    setHits && setHits([ev]);
    if (hint) submitForIndexing(ev, host, my);
    fillRow(ev, my, { seedRow, paintScores });
    paintRelated(ev, my, { paintScores, setHits });
  }
  document.title = titleFor(ev, parsed);
  paintScores();
  watchNip05();
}
