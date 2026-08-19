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
import { npub, noteId, naddr, nevent, shortAddr, shortNote, shortNpub, tinyNpub } from "../shared/nip19.js";
import { authorOf, displayName, profiles } from "../shared/profiles.js";
import { replyTarget, replyAddr, replyAuthor } from "../shared/parents.js";

// ---- the registry ---------------------------------------------------------
export const renderers = new Map(); // kind -> (ev, opts) -> html
export function register(kinds, fn) { for (const k of kinds) renderers.set(k, fn); }

/**
 * The TYPE-AHEAD registry: kind -> (ev) -> `{name, sub, pic, self}`.
 *
 * A card and a popup row are the same knowledge at two sizes, and the row had
 * none of it: one ladder — a `title`-ish tag, else the raw content — stood in
 * for all 118 kinds. So every kind whose payload is JSON led with its own
 * source. A search for "group" returned rows reading
 * `{"about":"","name":"Test group","picture":""}` where the card two hundred
 * pixels away drew the channel properly, and the same went for stalls,
 * products, app handlers and NIP-66 records. The kinds that carry a FRAGMENT
 * fared no better: a patch led with git's `From <sha> Mon Sep 17 00:00:00
 * 2001`, an article with its markdown marks, and everything countable — a
 * follow list, a relay list, a score — with the author's name twice.
 *
 * So a family that knows how to draw a card says here what that card says in
 * one line. Four slots, and only the first is normally set:
 *
 *   name  what the event IS: a channel's name, a patch's subject, "liked a
 *         note". Empty falls back to the AUTHOR — never to raw content, which
 *         is the fallback that printed the JSON.
 *   sub   the second line. Empty falls back to who posted it, which is the
 *         other half of the answer for every kind but a profile.
 *   pic   a face of the row's own, set only by the kinds whose CARD draws one
 *         (a channel's picture, an app's icon) — so the circle goes on meaning
 *         "the author" everywhere else. The score chip on it is the author's
 *         either way; avatarHtml says why.
 *   self  the event is ABOUT its author (a profile, and only a profile), so
 *         the second line must not repeat the name the first one already is.
 *
 * cards.js applies those fallbacks once, and the render test holds this map
 * and `renderers` to the same key set — a new card cannot ship without the row
 * that goes with it.
 */
export const rows = new Map(); // kind -> (ev) -> {name, sub, pic, self}
export function registerRow(kinds, fn) { for (const k of kinds) rows.set(k, fn); }

/**
 * A stranger's value as ONE line of text, or "".
 *
 * Two guards in one, both of them paid for: a JSON payload's field can be any
 * type at all (`{"name":{}}` interpolates as "[object Object]"), and a note's
 * text routinely opens with blank lines — `clip` counts CHARACTERS, so a row
 * whose content began with a paragraph break spent its whole budget on
 * whitespace and rendered empty.
 */
export const oneLine = (v) =>
  (typeof v === "string" || typeof v === "number" ? String(v).replace(/\s+/g, " ").trim() : "");

/** "1 relay" / "2,431 people" — the count a card and its row both have to say. */
export const plural = (n, one, many = `${one}s`) => `${n.toLocaleString()} ${n === 1 ? one : many}`;

/**
 * A NIP-57 amount as sats, or null. The protocol counts MILLISATS and three
 * cards read one, so this is the difference between "1,000 sats" and a number
 * three orders of magnitude wrong.
 */
export const satsOf = (msats) => {
  const n = Number(msats);
  return Number.isFinite(n) && n > 0 ? Math.round(n / 1000).toLocaleString() : null;
};

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

/**
 * An event page, carrying whatever the tag that named the event knew about it.
 *
 * noteHref is the bare form and stays the default; when a hint is at hand this
 * mints an nevent instead, because those hints are what entity.js falls back
 * to when this relay's index misses. A reply whose parent we never mirrored
 * opens anyway — from a note1… it could only ever say "Not here".
 */
export const eventHref = (id, hints = {}) => {
  const n = hints.relay || hints.author
    ? nevent(id, { relays: hints.relay ? [hints.relay] : [], author: hints.author, kind: hints.kind })
    : "";
  return n ? `/${esc(n)}` : noteHref(id);
};

/**
 * The card's OWN page — what the whole card, and its date, link to.
 *
 * By event id for everything, which is what every kind's title already did:
 * the entity page dispatches on the FETCHED event's kind, never on the
 * identifier that led there, so a note1… naming an article renders as an
 * article. A profile is the one exception, because a person's page is their
 * npub — a kind 0's id names one revision of it and stops resolving the
 * moment they edit their bio.
 *
 * Null when the event carries no usable identifier: a card with nowhere to go
 * must not become a card that navigates to "/".
 */
export const selfHref = (ev) => {
  if (ev && ev.kind === 0 && HEX64.test(ev.pubkey || "")) return keyHref(ev.pubkey);
  return ev && HEX64.test(ev.id || "") ? noteHref(ev.id) : null;
};
// Module scope, because a literal inside the function allocates a RegExp on
// every evaluation and this one runs twice per card, per render.
const HEX64 = /^[0-9a-f]{64}$/;

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

/**
 * Every NIP-92/94 imeta on the event, parsed: `["imeta", "url https://…",
 * "dim 1088x1920"]` becomes `{url: "https://…", dim: "1088x1920"}`.
 *
 * Per TAG, because the count matters: a video's imeta is the video, so the
 * first one is the whole story — but NIP-68 gives a picture post one imeta
 * PER PICTURE, and reading only the first turns an album into a single photo.
 *
 * A null-prototype object because the keys are a stranger's: `"constructor …"`
 * is a legal imeta part, and on a `{}` the `in` guard below would read the
 * prototype's and drop it. First occurrence of a key wins.
 */
export const imetas = (ev) => tagsOf(ev, "imeta").map((t) => {
  const m = Object.create(null);
  for (const part of t.slice(1)) {
    if (typeof part !== "string") continue;
    const sp = part.indexOf(" ");
    if (sp > 0 && !(part.slice(0, sp) in m)) m[part.slice(0, sp)] = part.slice(sp + 1);
  }
  return m;
});

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

/**
 * The shared author line: avatar, name (a link to the author's page), date,
 * badge.
 *
 * The DATE is the card's permalink, as it is in every other client — and it is
 * the reason the whole card can be clickable without the page losing anything:
 * this is a real anchor, so middle-click opens a tab, right-click copies the
 * link, and Tab reaches it. A div that navigates on click can do none of the
 * three. On the permalink itself the date stays plain text; a page does not
 * link to itself.
 */
export function bylineHtml(ev, opts) {
  const a = authorOf(ev);
  const href = opts && opts.full ? null : selfHref(ev);
  const date = esc(opts && opts.full ? fullDate(ev) : when(ev));
  return `
    <div class="byline">
      ${avatarHtml(a.picture, ev.pubkey, "sm")}
      <a class="by-name" href="${keyHref(ev.pubkey)}">${esc(a.name)}</a>
      <span class="dot">·</span>
      ${href
        ? `<a class="by-date" href="${href}" title="${esc(fullDate(ev))}">${date}</a>`
        : `<span class="by-date" title="${esc(fullDate(ev))}">${date}</span>`}
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
 * web/src/test/js/cards.test.mjs now renders every registered kind with a payload
 * in every tag and fails if it survives, so the next one is caught here rather
 * than in the wild.
 */
export const propsHtml = (props) => {
  const rows = props.filter(([, v]) => v != null && v !== "");
  return rows.length ? `<dl class="props">${rows.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${v}</dd>`).join("")}</dl>` : "";
};

/**
 * The card frame most kinds share: byline, the kind's body, props, json.
 *
 * `data-href` is where the CARD goes when clicked — app.js reads it off the
 * article. It is an attribute rather than a wrapping `<a>` because a card
 * legitimately contains links (the author, a hashtag, whoever it replies to)
 * and anchors cannot nest; the handler yields to any real control inside, and
 * to a text selection. Preview depth only: on the permalink the card IS the
 * page.
 */
export function shell(ev, opts, inner, props = []) {
  const href = opts && opts.full ? null : selfHref(ev);
  return `
    <article class="result${opts && opts.full ? " full" : ""}" data-id="${esc(ev.id)}"${href ? ` data-href="${href}"` : ""}>
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
 * "↩ in reply to <person>" — the line a reply-shaped card leads with, or ""
 * when the event is not a reply.
 *
 * Two decisions worth stating, because both were the other way round:
 *
 * The LABEL is the person. A reply used to render its parent as `note1qqq…`
 * in the props table, which is a hash: it tells a reader nothing about what
 * they are looking at, and no other client shows one. Who is being answered
 * is the context that makes the text above it read as a conversation.
 *
 * The LINK is the parent EVENT, not the parent's profile. Somebody clicking
 * "in reply to Alice" wants the thing Alice said; her profile is one more
 * click away from the byline of the card that opens. The href therefore
 * disagrees with the label on purpose — and carries the `e` tag's relay hint,
 * so a parent this relay never mirrored still opens.
 *
 * The fallback ladder is name -> npub -> note id, in decreasing usefulness:
 * the last rung is only reached when neither the tag nor the lookup produced
 * an author, which means this relay does not hold the parent either.
 */
export function replyLine(ev) {
  const t = replyTarget(ev);
  if (t) {
    const pk = replyAuthor(ev);
    return replyRow(eventHref(t.id, { relay: t.relay, author: pk }), pk, shortNote(t.id), noteId(t.id));
  }
  // A NIP-22 comment on something addressable — an article, a listing — has no
  // `e` at all, and its `a` carries the author in the address itself.
  const a = replyAddr(ev);
  const href = a && addrHref(a.addr);
  return href ? replyRow(href, a.author, shortAddr(a.addr), a.addr) : "";
}

const replyRow = (href, pk, fallbackLabel, fallbackTitle) => {
  const nm = pk ? displayName(profiles.get(pk)) : "";
  const label = nm || (pk ? shortNpub(pk) : fallbackLabel);
  const title = pk ? npub(pk) : fallbackTitle;
  return `<div class="reply-line">↩ in reply to <a${nm ? "" : ' class="mono"'} href="${href}" title="${esc(title)}">${esc(label)}</a></div>`;
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

/**
 * A search this page can run, as a url — `#scotland` is a query, not a place,
 * so it lands at the root with `q` set exactly as the search field would have
 * tokenized it. app.js's link interceptor turns a left click on one of these
 * into a pushState render, so the socket and its NIP-42 auth survive the trip.
 */
export const searchHref = (q) => `/?${new URLSearchParams({ q })}`;
/** A topic, however it was written: `t` tags carry `scotland`, cards show `#scotland`. */
export const hashtagHref = (t) => searchHref(String(t).startsWith("#") ? t : `#${t}`);
/** A NIP-29 group, as the search that finds what was posted in it. */
export const groupHref = (id) => searchHref(`group:${id}`);

/**
 * Hashtags, words, mime types — short values that read as chips, not rows.
 *
 * `hrefOf` makes them links: a hashtag is a search waiting to happen, and a
 * chip that looks like every other hashtag on the internet but does nothing
 * when clicked is a worse affordance than plain text. Values with no search
 * behind them (a mute word, a mime type) pass no `hrefOf` and stay spans.
 */
export function chipRow(values, opts, hrefOf = null) {
  const shown = opts && opts.full ? values : values.slice(0, 12);
  const more = values.length - shown.length;
  if (!shown.length) return "";
  const chip = (v) => {
    const href = hrefOf && hrefOf(v);
    return href
      ? `<a class="tag-chip" href="${href}">${esc(clip(v, 40))}</a>`
      : `<span class="tag-chip">${esc(clip(v, 40))}</span>`;
  };
  return `<div class="chip-row">${shown.map(chip).join("")}${more > 0 ? `<span class="tag-chip more">+${more}</span>` : ""}</div>`;
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

/** A strip of faces where people are a card's CONTEXT — a poll's winners, a community's moderators. */
export function faceStrip(pubkeys, max = 12) {
  const shown = pubkeys.slice(0, max);
  if (!shown.length) return "";
  const more = pubkeys.length - shown.length;
  return `<div class="face-strip">${shown.map((pk) => `<a href="${keyHref(pk)}">${avatarHtml(authorOf({ pubkey: pk }).picture, pk, "md")}</a>`).join("")}${more > 0 ? `<span class="face-more">+${more}</span>` : ""}</div>`;
}

/**
 * How many CELLS a grid gets, per depth. Not rows: how many columns fit is the
 * width's answer, so a count fixed here is one row on a laptop and two on a
 * phone. A preview is a card among cards and takes about a row; the permalink
 * is the page about this list and takes four.
 *
 * Both numbers are divisible by every column count the stylesheet uses (6 and
 * 3) — that is a pair, not a coincidence. Change one of these and the grid can
 * end in a ragged row, which for this grid means the `+1.2k more` cell alone on
 * a row of its own. index.html's `.people-grid` records the other half.
 *
 * Exported because cards.js reads these to decide whose profile the page owes
 * itself: the number drawn and the number NAMED are the same number, and a
 * second copy of it would drift into npubs in the last row.
 */
export const PEOPLE_GRID = { preview: 6, full: 24 };

/**
 * A list's `p` values as PEOPLE: hex-shaped, and each of them once.
 *
 * Both halves are load-bearing. Every caller filtered the hex itself and one
 * of them would eventually not, and npub() on a value that is not a key is a
 * label for somebody who does not exist. The dedupe is the one that shows:
 * kind-3 lists in the wild repeat entries (clients append without checking),
 * and the grid drew that person twice, in two cells, both linking to the same
 * page — while the count above it said something a reader could not reconcile
 * with what they were looking at.
 */
export const uniquePubkeys = (values) =>
  [...new Set((values || []).filter((pk) => HEX64.test(pk || "")))];

/** The distinct people a list holds — its `p` tags, deduped, hex only. */
export const peopleOf = (ev) => uniquePubkeys(tagsOf(ev, "p").map((t) => t[1]));

/**
 * Who a grid actually draws, and how many it leaves out — the ONE answer both
 * the renderer and the profile loader read, so the faces on the page and the
 * profiles fetched for them cannot be different sets.
 *
 * When the list overruns, the last cell IS the count, so one fewer face fits:
 * a `+794 more` cell keeps the row rectangular and puts the number where the
 * eye already is, instead of a line of text below a ragged row.
 */
export function gridCells(pubkeys, opts) {
  const cap = opts && opts.full ? PEOPLE_GRID.full : PEOPLE_GRID.preview;
  const shown = pubkeys.length > cap ? pubkeys.slice(0, cap - 1) : pubkeys;
  return { shown, more: pubkeys.length - shown.length };
}

/**
 * The people a list HOLDS, as a grid of face + name.
 *
 * A `p` tag on a list kind is not a mention — it IS the list. Rendered as a
 * face strip it was a row of 30px thumbnails with no names at all, so a follow
 * set of twelve people said "12 members" and then showed twelve anonymous
 * circles: everything about WHO was in it had to be recovered by hovering, one
 * at a time. So the people get a cell each, at a size a face is recognisable
 * at, with the name under it.
 *
 * Two depths, one template: the preview is a card among cards and takes the
 * smaller face, the permalink is the page about this list and takes the larger
 * one. The stylesheet is told which by the same `full` class the card frame
 * carries, and decides both the face size and how many columns the width can
 * hold — this function counts cells, never rows.
 *
 * The name is `personLink`'s ladder, minus the link markup — display name, a
 * short npub only when the store knows no profile, never hex — and the npub
 * always sits in the title, which is also how cards.test.mjs finds every
 * person a card names.
 */
export function peopleGrid(pubkeys, opts) {
  const full = !!(opts && opts.full);
  const { shown, more } = gridCells(uniquePubkeys(pubkeys), opts);
  if (!shown.length) return "";
  const size = full ? "xxl" : "xl";
  // One map read per cell. This was authorOf() for the picture and a second
  // profiles.get() for the name — two lookups and a throwaway object per face,
  // 24 of them per card, for one entry that is already in hand.
  const cell = (pk) => {
    const p = profiles.get(pk);
    const nm = displayName(p);
    return `<a class="person-cell" href="${keyHref(pk)}" title="${esc(npub(pk))}">` +
      avatarHtml((p && p.picture) || "", pk, size) +
      `<span class="person-name${nm ? "" : " mono"}">${esc(nm || tinyNpub(pk))}</span></a>`;
  };
  const moreCell = `<div class="person-cell more"><span class="av-wrap av-${size}">` +
    `<span class="avatar more-face">+${esc(compactCount(more))}</span></span>` +
    `<span class="person-name">more</span></div>`;
  return `<div class="people-grid${full ? " full" : ""}">${shown.map(cell).join("")}${more > 0 ? moreCell : ""}</div>`;
}

/** `+8.4k` rather than `+8,432`: it has to read inside a 46px circle. */
const compactCount = (n) =>
  n < 1000 ? String(n) : n < 10000 ? `${(n / 1000).toFixed(1)}k` : `${Math.round(n / 1000)}k`;

/**
 * Which kinds draw that grid — declared by the families that register the
 * renderers, not listed a second time in cards.js.
 *
 * A card that NAMES somebody must have that somebody's profile loaded before
 * it renders, and cards.js is where the page asks who those people are. It
 * used to answer "never a `p` tag on a list kind", which was true while lists
 * drew faces and became silently wrong the moment they drew names — the exact
 * drift its own docstring warns about. Registering the kinds beside the
 * renderer keeps one answer: whoever draws a grid says so here.
 */
export const PEOPLE_GRID_KINDS = new Set();
export const registerPeopleGrid = (kinds) => { for (const k of kinds) PEOPLE_GRID_KINDS.add(k); };

/**
 * The pubkeys [ev]'s grid draws AT THIS DEPTH — the profiles the page owes
 * itself before rendering it.
 *
 * Depth matters here, and getting it wrong is paid for per result: the results
 * list only ever renders previews, so declaring the permalink's cap there
 * fetched three times the profiles any card on the page could show.
 */
export const gridPeople = (ev, opts) =>
  PEOPLE_GRID_KINDS.has(ev.kind) ? gridCells(peopleOf(ev), opts).shown : [];

/**
 * The same declaration, for a card that names people from somewhere no scan of
 * `p` tags reaches — a repository's `["maintainers", <pk>, <pk>, …]`, where the
 * people are the tag's VALUES rather than one tag each.
 *
 * A renderer registers the very function it draws with, so the set named and
 * the set declared are one expression evaluated twice, not two lists to keep
 * in step. Depth is passed through for the same reason gridPeople takes it:
 * a preview names fewer of them than the permalink does.
 */
export const NAMED_PEOPLE = new Map(); // kind -> (ev, opts) -> pubkeys
export const registerNamedPeople = (kinds, fn) => { for (const k of kinds) NAMED_PEOPLE.set(k, fn); };
export const namedPeople = (ev, opts) => (NAMED_PEOPLE.get(ev.kind) || NOBODY)(ev, opts);
const NOBODY = () => [];
