// The substrate every renderer stands on: the registry, the byline, badges,
// props tables, the json toggle, and the two rendering modes. Family modules
// import from here and call register(); they never import each other, and
// dispatch lives in cards.js so registration stays cycle-free. (Faces live in
// shared/avatar.js — the search field draws them too, and a card module is no
// place for the page's other half to have to import from.)
//
// Every renderer is (ev, opts) -> HTML string. opts.full is the permalink
// mode: a search result is a PREVIEW of the card (clipped text, clamped
// lines, relative dates), the entity page is the WHOLE card (nothing clipped,
// full dates) — one template per kind, two depths, so the two views can
// never drift apart.

import { esc, clip, fullDate, when } from "../shared/format.js";
import { avatarHtml } from "../shared/avatar.js";
import { kindLabel, kindTone } from "../shared/kinds.js";
import { npub, noteId, naddr, shortAddr, shortNote, shortNpub } from "../shared/nip19.js";
import { authorOf, displayName, profiles } from "../shared/profiles.js";

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
/** An `a` tag as a link to its entity page — null when it cannot be encoded. */
export const addrHref = (a) => { const n = naddr(a); return n ? `/${esc(n)}` : null; };

// ---- tag access -----------------------------------------------------------
// `Array.isArray` on every entry, for the same reason format.js's firstTag
// guards `ev.tags`: a hint-fetched event is rendered before anything has
// verified it, and `["title"], null, ["e"]` threw on `t[0]` here.
export const tagsOf = (ev, name) => ((ev && ev.tags) || []).filter((t) => Array.isArray(t) && t[0] === name);
export const tagOf = (ev, ...names) => {
  for (const name of names) {
    for (const t of (ev && ev.tags) || []) if (Array.isArray(t) && t[0] === name && t[1]) return t[1];
  }
  return null;
};

/**
 * Tags matched by a PREDICATE on the name — a 10040's `30382:rank`, a 30618's
 * `refs/heads/…`, a report's flagged p/e. tagsOf takes a literal name, so
 * these five call sites each re-implemented the iteration and each re-made
 * the same `t[0] of null` mistake. One accessor, one guard.
 */
export const tagsWhere = (ev, pred) =>
  ((ev && ev.tags) || []).filter((t) => Array.isArray(t) && pred(String(t[0] ?? ""), t));

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
      ${avatarHtml(a.picture, ev.pubkey, "sm")}
      <a class="by-name" href="${keyHref(ev.pubkey)}">${esc(a.name)}</a>
      <span class="dot">·</span>
      <span class="by-date" title="${esc(fullDate(ev))}">${esc(opts && opts.full ? fullDate(ev) : when(ev))}</span>
      <span class="spacer"></span>
      ${badgeHtml(ev)}
    </div>`;
}

/**
 * A props table, skipping rows whose value came up empty.
 *
 * The VALUE goes in as raw HTML — that is what lets a row be a link — so every
 * value derived from an event must arrive already escaped. This is not a
 * theoretical rule: four cards passed `fmtTs(tagOf(ev, …))` straight in, and
 * fmtTs hands back its argument verbatim when it is not a number, so
 * `["endsAt", "<img src=x onerror=…>"]` on a kind 1068 executed in the page.
 * tools/webtest/cards.test.mjs now renders every registered kind with a payload
 * in every tag and fails if it survives, so the next one is caught here rather
 * than in the wild.
 */
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

/**
 * A person, linked: their name when the store knows one, a short npub only as
 * the fallback, the full npub in the hover — and never, anywhere, hex. Three
 * families name people now (the graph cards, the social cards, the NIP-85
 * assertions), so the rule lives here rather than in whichever one wrote it
 * down first.
 */
export const personLink = (pk) => {
  const nm = displayName(profiles.get(pk));
  return `<a${nm ? "" : ' class="mono"'} href="${keyHref(pk)}" title="${esc(npub(pk))}">${esc(nm || shortNpub(pk))}</a>`;
};

/**
 * A heading, at either depth, optionally linking somewhere. `href` goes in
 * RAW — pass keyHref/noteHref/addrHref, which escape, and never a url taken
 * straight off an event.
 */
export const titleHtml = (opts, text, n = 140, href = null) => {
  const t = text ? clipIf(opts, text, n) : "";
  if (!t) return "";
  return `<h2 class="result-title">${href ? `<a href="${href}">${esc(t)}</a>` : esc(t)}</h2>`;
};

/**
 * A url off an event, reduced to one this page will put in an `href` — or null.
 *
 * `esc()` makes a url safe to SIT in an attribute; it says nothing about what
 * the browser does when the link is clicked, and `javascript:` or
 * `data:text/html` in an href is a script the reader runs on themselves. Every
 * link here carries `target="_blank"`, which current browsers refuse to follow
 * for both schemes — but that is a browser's behaviour, not this page's, and
 * every one of these urls came from a stranger's event.
 *
 * Absolute http/https only. A relative url would resolve against this origin,
 * which is never what an event meant.
 */
export const safeUrl = (u) => {
  const s = String(u || "").trim();
  if (!s) return null;
  try {
    const p = new URL(s).protocol;   // throws on anything not absolute
    return p === "http:" || p === "https:" ? s : null;
  } catch (e) { return null; }
};

/**
 * The one external link. Unlinkable urls render as their own text rather than
 * disappearing: the reader can still see what the event claimed, which is the
 * point of showing the field at all.
 */
export const extLink = (url, label) => {
  if (!url) return null;
  const safe = safeUrl(url);
  return safe
    ? `<a href="${esc(safe)}" target="_blank" rel="noopener noreferrer">${esc(label || safe)}</a>`
    : `<span class="mono">${esc(clip(String(url), 120))}</span>`;
};

/**
 * A list of relay rows; full mode shows all, preview the first few. Lives here
 * rather than in people.js because relay urls are carried by NIP-65 lists,
 * NIP-51 relay sets, DM relay lists, blossom server lists and NIP-66 discovery
 * records alike — five families, one row.
 */
export function relayRows(rows, opts) {
  const shown = opts && opts.full ? rows : rows.slice(0, 6);
  const more = rows.length - shown.length;
  return `<ul class="relay-list">${shown.map((r) => `<li><span class="mono">${esc(r.url)}</span>${r.note ? ` <span class="muted-note">${esc(r.note)}</span>` : ""}</li>`).join("")}${more > 0 ? `<li class="muted-note">…and ${more} more</li>` : ""}</ul>`;
}

/** Hashtags, words, mime types — short values that read as chips, not rows. */
export function chipRow(values, opts) {
  const shown = opts && opts.full ? values : values.slice(0, 12);
  const more = values.length - shown.length;
  if (!shown.length) return "";
  return `<div class="chip-row">${shown.map((v) => `<span class="tag-chip">${esc(clip(v, 40))}</span>`).join("")}${more > 0 ? `<span class="tag-chip more">+${more}</span>` : ""}</div>`;
}

/** The emoji themselves — shared by the 30030 set and the 10030 user list. */
export function emojiGrid(pairs, opts) {
  const shown = opts && opts.full ? pairs : pairs.slice(0, 16);
  if (!shown.length) return "";
  return `<div class="emoji-grid">${shown.map(([name, url]) => `<img src="${esc(url)}" alt=":${esc(name)}:" title=":${esc(name)}:" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()" />`).join("")}</div>`;
}

/**
 * What a list points AT, as links: `e` tags to /note1…, `a` tags to /naddr1….
 * A set that only counts its members is a card that says "12" and shows
 * nothing, which is what 30003 rendered as before these existed.
 */
export function refRows(refs, opts) {
  const shown = opts && opts.full ? refs : refs.slice(0, 8);
  const more = refs.length - shown.length;
  if (!shown.length) return "";
  const row = (r) => {
    if (r.kind === "e") return /^[0-9a-f]{64}$/.test(r.value)
      ? `<a class="mono" href="${noteHref(r.value)}">${esc(shortNote(r.value))}</a>`
      : `<span class="mono">${esc(clip(r.value, 40))}</span>`;
    const href = addrHref(r.value);
    const label = shortAddr(r.value);
    return href ? `<a href="${href}">${esc(clip(label, 60))}</a>` : `<span class="mono">${esc(clip(label, 60))}</span>`;
  };
  return `<ul class="ref-list">${shown.map((r) => `<li>${row(r)}</li>`).join("")}${more > 0 ? `<li class="muted-note">…and ${more} more</li>` : ""}</ul>`;
}

/** A strip of faces for list kinds — stable generated faces even before any profile loads. */
export function faceStrip(pubkeys, max = 12) {
  const shown = pubkeys.slice(0, max);
  if (!shown.length) return "";
  const more = pubkeys.length - shown.length;
  return `<div class="face-strip">${shown.map((pk) => `<a href="${keyHref(pk)}">${avatarHtml(authorOf({ pubkey: pk }).picture, pk, "md")}</a>`).join("")}${more > 0 ? `<span class="face-more">+${more}</span>` : ""}</div>`;
}
