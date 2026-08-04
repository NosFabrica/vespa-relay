// The renderers: one card per kind family, dispatched by the event's kind.
// This file is the one expected to grow — a bespoke renderer per kind that
// earns one — which is why it is a module and not a region of index.html.
// Everything here is a pure event -> HTML-string function over the shared
// caches; page state (who is signed in, whose lens ranks) never enters.

import { esc, clip, fullDate, when, titleOf, summaryOf, imageOf } from "./shared/format.js";
import { kindLabel, kindTone } from "./shared/kinds.js";
import { npub, noteId, shortNpub, shortNote } from "./shared/nip19.js";
import { authorOf, displayName, parseProfile } from "./shared/profiles.js";

// njump reads NIP-19 identifiers, so the link and the label agree.
const njumpKey = (hex) => `https://njump.me/${esc(npub(hex))}`;
const njumpNote = (hex) => `https://njump.me/${esc(noteId(hex))}`;

/** A pubkey-derived hue, so a missing picture is still a stable, distinct face. */
const hueOf = (seed) => (parseInt(String(seed || "").slice(0, 4), 16) || 0) % 360;
const BLANK = "data:image/gif;base64,R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==";

/** A broken picture falls back to the same generated face, in place. */
function avatarHtml(pic, seed) {
  const style = `style="--h:${hueOf(seed)}"`;
  const face = pic
    ? `<img class="avatar" ${style} src="${esc(pic)}" alt="" loading="lazy" referrerpolicy="no-referrer"
         onerror="this.classList.add('gen');this.src='${BLANK}'" />`
    : `<div class="avatar gen" ${style}></div>`;
  // The chip is painted after the fact by paintScores(): the score is a second
  // round trip, and a face should not wait on it.
  return `<span class="av-wrap">${face}<span class="score-chip" data-pk="${esc(seed || "")}"></span></span>`;
}

const badgeHtml = (ev) => `<span class="kind-badge" data-tone="${kindTone(ev.kind)}">${esc(kindLabel(ev.kind))}</span>`;

/**
 * The raw event, one click away on every result.
 *
 * Deliberately quiet — a small grey word, not a button. Nobody reading search
 * results wants it, and everybody debugging one does: what the relay actually
 * returned, tags and sig included, without a console or a second client.
 * Filled on demand rather than inlined into every card, so a page of results
 * does not carry a second copy of itself in the DOM.
 */
const jsonHtml = (ev) =>
  `<div class="raw"><button type="button" class="raw-toggle" data-id="${esc(ev.id)}">json</button>` +
  `<pre class="raw-body" hidden></pre></div>`;

/** The shared author line: avatar, name, date, kind badge. */
function bylineHtml(ev) {
  const a = authorOf(ev);
  return `
    <div class="byline">
      ${avatarHtml(a.picture, ev.pubkey)}
      <span class="by-name">${esc(a.name)}</span>
      <span class="dot">·</span>
      <span class="by-date" title="${esc(fullDate(ev))}">${esc(when(ev))}</span>
      <span class="spacer"></span>
      ${badgeHtml(ev)}
    </div>`;
}

/** kind 0 — the profile card (rendered purely from the event, no server-side numbers). */
function profileCard(ev) {
  const p = parseProfile(ev);
  const name = displayName(p) || shortNpub(ev.pubkey);
  const props = [];
  if (p.nip05) {
    props.push(["nip05",
      `<span class="nip05" data-addr="${esc(p.nip05)}" data-pk="${esc(ev.pubkey)}">` +
      `${esc(p.nip05)}<span class="n5chip checking" title="checking with the domain…">…</span></span>`]);
  }
  if (p.website) props.push(["website", `<a href="${esc(p.website)}" target="_blank" rel="noopener noreferrer">${esc(p.website)}</a>`]);
  if (p.lud16) props.push(["lightning", esc(p.lud16)]);
  props.push(["pubkey", `<a class="mono" href="${njumpKey(ev.pubkey)}" target="_blank" rel="noopener noreferrer" title="${esc(npub(ev.pubkey))}">${esc(shortNpub(ev.pubkey))}</a>`]);
  return `
    <article class="result" data-id="${esc(ev.id)}">
      <div class="result-header">
        ${avatarHtml(p.picture, ev.pubkey)}
        <div class="who">
          <h2 class="result-name"><a href="${njumpKey(ev.pubkey)}" target="_blank" rel="noopener noreferrer">${esc(name)}</a></h2>
          ${(p.name || "").trim() && (p.name || "").trim() !== name ? `<div class="result-display">${esc(p.name)}</div>` : ""}
        </div>
        ${badgeHtml(ev)}
      </div>
      ${p.about ? `<div class="result-body clamp">${esc(clip(p.about, 400))}</div>` : ""}
      <dl class="props">${props.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join("")}</dl>
      ${jsonHtml(ev)}
    </article>`;
}

/** kinds 1/11 — the note card: author byline + the text itself. */
function noteCard(ev) {
  return `
    <article class="result" data-id="${esc(ev.id)}">
      ${bylineHtml(ev)}
      <div class="result-body">${esc(clip(ev.content, 500))}</div>
      <dl class="props"><dt>note</dt><dd><a class="mono" href="${njumpNote(ev.id)}" target="_blank" rel="noopener noreferrer" title="${esc(noteId(ev.id))}">${esc(shortNote(ev.id))}</a></dd></dl>
      ${jsonHtml(ev)}
    </article>`;
}

/**
 * Everything else — the generic card that makes "all indexed kinds" render:
 * byline, the event's title-ish tag, its summary-ish tag (else the content),
 * and an image when the tags carry one. Exactly the fields the extractors
 * index, so what matched is what shows.
 */
function genericCard(ev) {
  const title = titleOf(ev);
  const summary = summaryOf(ev);
  const body = summary || clip(ev.content, 400);
  const img = imageOf(ev);
  return `
    <article class="result" data-id="${esc(ev.id)}">
      ${bylineHtml(ev)}
      <div class="result-main">
        <div class="text">
          ${title ? `<h2 class="result-title"><a href="${njumpNote(ev.id)}" target="_blank" rel="noopener noreferrer">${esc(clip(title, 120))}</a></h2>` : ""}
          ${body ? `<div class="result-body clamp${summary ? " muted" : ""}">${esc(clip(body, 400))}</div>` : ""}
        </div>
        ${img ? `<img class="thumb" src="${esc(img)}" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()" />` : ""}
      </div>
      ${!title ? `<dl class="props"><dt>id</dt><dd><a class="mono" href="${njumpNote(ev.id)}" target="_blank" rel="noopener noreferrer" title="${esc(noteId(ev.id))}">${esc(shortNote(ev.id))}</a></dd></dl>` : ""}
      ${jsonHtml(ev)}
    </article>`;
}

export function card(ev) {
  if (ev.kind === 0) return profileCard(ev);
  if (ev.kind === 1 || ev.kind === 11) return noteCard(ev);
  return genericCard(ev);
}

/** The type-ahead row: works for every kind (profiles show their own face). */
export function popupRow(ev, idx) {
  let pic, name, sub;
  if (ev.kind === 0) {
    const p = parseProfile(ev);
    pic = p.picture; name = displayName(p) || shortNpub(ev.pubkey); sub = p.about;
  } else {
    const a = authorOf(ev);
    pic = a.picture; name = titleOf(ev) || clip(ev.content, 60) || a.name; sub = titleOf(ev) ? (summaryOf(ev) || clip(ev.content, 60)) : a.name;
  }
  return `
    <div class="popup-item" data-idx="${idx}" role="option">
      ${avatarHtml(pic, ev.pubkey)}
      <div class="row-main">
        <div class="row-name">${esc(clip(name, 80))}</div>
        <div class="row-about">${esc(clip(sub || "", 90))}</div>
      </div>
      ${badgeHtml(ev)}
    </div>`;
}
