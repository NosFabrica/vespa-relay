// The substrate every renderer stands on: the registry, the byline, avatars,
// badges, props tables, the json toggle, and the two rendering modes. Family
// modules import from here and call register(); they never import each other,
// and dispatch lives in cards.js so registration stays cycle-free.
//
// Every renderer is (ev, opts) -> HTML string. opts.full is the permalink
// mode: a search result is a PREVIEW of the card (clipped text, clamped
// lines, relative dates), the entity page is the WHOLE card (nothing clipped,
// full dates) — one template per kind, two depths, so the two views can
// never drift apart.

import { esc, clip, fullDate, when } from "../shared/format.js";
import { kindLabel, kindTone } from "../shared/kinds.js";
import { npub, noteId } from "../shared/nip19.js";
import { authorOf } from "../shared/profiles.js";

// ---- the registry ---------------------------------------------------------
export const renderers = new Map(); // kind -> (ev, opts) -> html
export function register(kinds, fn) { for (const k of kinds) renderers.set(k, fn); }

// ---- links ----------------------------------------------------------------
// Internal first: this app renders NIP-19 pages itself, so cards link to
// /npub1…, /note1… and app.js intercepts the click into a pushState render —
// no reload, socket and backstack intact. njump is the entity page's escape
// hatch to the wider network, not the default for every click.
export const keyHref = (hex) => `/${esc(npub(hex))}`;
export const noteHref = (hex) => `/${esc(noteId(hex))}`;
export const njumpFor = (bech) => `https://njump.me/${esc(bech)}`;

// ---- tag access -----------------------------------------------------------
export const tagsOf = (ev, name) => (ev.tags || []).filter((t) => t[0] === name);
export const tagOf = (ev, ...names) => {
  for (const name of names) for (const t of ev.tags || []) if (t[0] === name && t[1]) return t[1];
  return null;
};

/** NIP-92/94 media fields: ["imeta", "url https://…", "m video/mp4", …]. */
export function imetaField(ev, field) {
  for (const t of tagsOf(ev, "imeta")) {
    for (const part of t.slice(1)) {
      if (typeof part === "string" && part.startsWith(field + " ")) return part.slice(field.length + 1);
    }
  }
  return null;
}

export const fmtTs = (secs) => {
  const n = Number(secs);
  return Number.isFinite(n) && n > 0
    ? new Date(n * 1000).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" })
    : String(secs || "");
};

/** Guarded JSON content — half the marketplace/app kinds keep their payload there. */
export function jsonContent(ev) {
  try { return JSON.parse(ev.content) || {}; } catch (e) { return {}; }
}

// ---- the two depths -------------------------------------------------------
export const clipIf = (opts, s, n) => (opts && opts.full ? String(s || "").trim() : clip(s, n));
export const clampCls = (opts) => (opts && opts.full ? "" : " clamp");

// ---- shared chrome --------------------------------------------------------
/** A pubkey-derived hue, so a missing picture is still a stable, distinct face. */
export const hueOf = (seed) => (parseInt(String(seed || "").slice(0, 4), 16) || 0) % 360;
const BLANK = "data:image/gif;base64,R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw==";

/** A broken picture falls back to the same generated face, in place. */
export function avatarHtml(pic, seed) {
  const style = `style="--h:${hueOf(seed)}"`;
  const face = pic
    ? `<img class="avatar" ${style} src="${esc(pic)}" alt="" loading="lazy" referrerpolicy="no-referrer"
         onerror="this.classList.add('gen');this.src='${BLANK}'" />`
    : `<div class="avatar gen" ${style}></div>`;
  // The chip is painted after the fact by paintScores(): the score is a second
  // round trip, and a face should not wait on it.
  return `<span class="av-wrap">${face}<span class="score-chip" data-pk="${esc(seed || "")}"></span></span>`;
}

export const badgeHtml = (ev) => `<span class="kind-badge" data-tone="${kindTone(ev.kind)}">${esc(kindLabel(ev.kind))}</span>`;

/**
 * The raw event, one click away on every result.
 *
 * Deliberately quiet — a small grey word, not a button. Nobody reading search
 * results wants it, and everybody debugging one does: what the relay actually
 * returned, tags and sig included, without a console or a second client. On
 * the permalink this doubles as the "complete event" in the strictest sense.
 */
export const jsonHtml = (ev) =>
  `<div class="raw"><button type="button" class="raw-toggle" data-id="${esc(ev.id)}">json</button>` +
  `<pre class="raw-body" hidden></pre></div>`;

/** The shared author line: avatar, name (a link to the author's page), date, badge. */
export function bylineHtml(ev, opts) {
  const a = authorOf(ev);
  return `
    <div class="byline">
      ${avatarHtml(a.picture, ev.pubkey)}
      <a class="by-name" href="${keyHref(ev.pubkey)}">${esc(a.name)}</a>
      <span class="dot">·</span>
      <span class="by-date" title="${esc(fullDate(ev))}">${esc(opts && opts.full ? fullDate(ev) : when(ev))}</span>
      <span class="spacer"></span>
      ${badgeHtml(ev)}
    </div>`;
}

/** A props table, skipping rows whose value came up empty. */
export const propsHtml = (props) => {
  const rows = props.filter(([, v]) => v != null && v !== "");
  return rows.length ? `<dl class="props">${rows.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${v}</dd>`).join("")}</dl>` : "";
};

/** The card frame most kinds share: byline, the kind's body, props, json. */
export function shell(ev, opts, inner, props = []) {
  return `
    <article class="result${opts && opts.full ? " full" : ""}" data-id="${esc(ev.id)}">
      ${bylineHtml(ev, opts)}
      ${inner}
      ${propsHtml(props)}
      ${jsonHtml(ev)}
    </article>`;
}

/** Plain text body under the house rules: escaped, pre-wrap, clamped unless full. */
export const bodyHtml = (opts, text, n = 400, muted = false) => {
  const s = clipIf(opts, text, n);
  return s ? `<div class="result-body${clampCls(opts)}${muted ? " muted" : ""}">${esc(s)}</div>` : "";
};

/** A strip of faces for list kinds — stable generated faces even before any profile loads. */
export function faceStrip(pubkeys, max = 12) {
  const shown = pubkeys.slice(0, max);
  if (!shown.length) return "";
  const more = pubkeys.length - shown.length;
  return `<div class="face-strip">${shown.map((pk) => `<a href="${keyHref(pk)}">${avatarHtml(authorOf({ pubkey: pk }).picture, pk)}</a>`).join("")}${more > 0 ? `<span class="face-more">+${more}</span>` : ""}</div>`;
}
