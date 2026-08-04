// The NIP-19 entity view: /npub1…, /nprofile1…, /note1…, /nevent1…, /naddr1…
// each render the thing they name, with the same cards the search uses — in
// permalink depth, so the event is whole rather than previewed.
//
// Fetched ANONYMOUSLY, on the reference connection, always. A permalink
// answers "does this relay hold it", and the authenticated socket is
// trust-gated to authors the reader has scored — through it, a perfectly
// stored event by someone outside your web of trust would render as "not
// found", which is a statement about you dressed up as one about the relay.
//
// The identifier decides the QUERY; the fetched event's kind decides the
// CARD. A note1… id can name an article or a live stream — it renders as
// what it is, not as what the URL called it.

import { refConn } from "./shared/conn.js";
import { enrichProfiles } from "./shared/profiles.js";
import { watchNip05 } from "./shared/nip05.js";
import { esc, titleOf } from "./shared/format.js";
import { kindLabel } from "./shared/kinds.js";
import { nip19Parse, shortNpub } from "./shared/nip19.js";
import { njumpFor } from "./cards/base.js";
import { card } from "./cards.js";

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

async function fetchEntity(conn, p) {
  // Replaceable kinds can still hand back more than one event; newest wins.
  const newest = (evs) => evs.reduce((a, b) => (!a || b.created_at > a.created_at ? b : a), null);
  if (p.type === "npub" || p.type === "nprofile") {
    return newest(await conn.req({ kinds: [0], authors: [p.pubkey], limit: 1 }));
  }
  if (p.type === "note" || p.type === "nevent") {
    return (await conn.req({ ids: [p.id], limit: 1 }))[0] || null;
  }
  return newest(await conn.req({ kinds: [p.kind], authors: [p.author], "#d": [p.d], limit: 1 }));
}

function titleFor(ev, parsed) {
  const t = ev ? (titleOf(ev) || kindLabel(ev.kind)) : parsed ? parsed.type : "not found";
  return `SearchOverTrust — ${t}`;
}

/**
 * Render the entity named by [seg] (the URL path segment) into #results.
 * paintScores arrives as a hook because the lens it paints under is app
 * state; everything else here is anonymous and stateless.
 */
export async function showEntity(seg, { paintScores }) {
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

  $results.innerHTML = headHtml(parsed.raw) +
    `<div class="skel-card"><div class="skel-line" style="width:34%"></div><div class="skel-line" style="width:92%"></div><div class="skel-line" style="width:66%"></div></div>`;

  let ev = null, err = null;
  try {
    const conn = await refConn();
    ev = await fetchEntity(conn, parsed);
    if (ev) {
      // Names and faces for the byline and any face strip — the author plus
      // the first handful of p-tags a list kind is about to render.
      const people = (ev.tags || []).filter((t) => t[0] === "p" && /^[0-9a-f]{64}$/.test(t[1] || "")).slice(0, 24).map((t) => t[1]);
      await enrichProfiles([ev.pubkey, ...people]);
    }
  } catch (e) { err = e; }
  if (my !== token) return;

  if (err) {
    $results.innerHTML = headHtml(parsed.raw) + `<div class="error">${esc(err.message || String(err))}</div>`;
  } else if (!ev) {
    // Absence here is a fact about THIS MIRROR, not about the event — say so,
    // and hand the reader to the wider network instead of a dead end.
    const what = parsed.type === "npub" || parsed.type === "nprofile"
      ? `No profile event for ${shortNpub(parsed.pubkey)} in this relay's index.`
      : "This event is not in this relay's index.";
    $results.innerHTML = headHtml(parsed.raw) +
      emptyState("Not here", `${what} It may exist elsewhere — try njump above.`);
  } else {
    $results.innerHTML = headHtml(parsed.raw) + card(ev, { full: true });
  }
  document.title = titleFor(ev, parsed);
  paintScores();
  watchNip05();
}
