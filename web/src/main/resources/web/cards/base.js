// The substrate every renderer stands on: the registry, the byline, badges,
// props tables, the json toggle, and the two rendering modes. Family modules
// import from here and call register(); they never import each other, and
// dispatch lives in cards.js so registration stays cycle-free.
//
// Every renderer is (ev, opts) -> HTML string. opts.full is the permalink
// mode: a search result is a preview (clipped text, clamped lines, relative
// dates), the entity page is the whole card. One template per kind, two
// depths, so the two views cannot drift apart.

import { esc, clip, fullDate, when } from "../shared/format.js";
import { avatarHtml } from "../shared/avatar.js";
import { kindLabel, kindTone } from "../shared/kinds.js";
import { npub, noteId, naddr, addrOf, nevent, shortAddr, shortNote, shortNpub, tinyNpub } from "../shared/nip19.js";
import { authorOf, displayName, profiles } from "../shared/profiles.js";
import { replyTarget, replyAddr, replyAuthor } from "../shared/parents.js";
import { groupTokenizes } from "../shared/query.js";
import { postedTo } from "../shared/groups.js";
import { groupName } from "../shared/groupnames.js";
import { provenance, attribution, PILL_BUDGET } from "../provenance.js";

// ---- the registry ---------------------------------------------------------
export const renderers = new Map(); // kind -> (ev, opts) -> html
export function register(kinds, fn) { for (const k of kinds) renderers.set(k, fn); }

/**
 * The type-ahead registry: kind -> (ev) -> `{name, sub, pic, self}`, a card's
 * knowledge at one line. cards.js applies the fallbacks: an empty `name` falls
 * to the author, never to raw content (that fallback printed JSON payloads);
 * an empty `sub` falls to who posted it. `pic` is set only by kinds whose card
 * draws a face of its own, so the circle means "the author" everywhere else.
 * `self` marks an event about its author (a profile), whose second line must
 * not repeat the name. The render test holds this map and `renderers` to the
 * same key set.
 */
export const rows = new Map(); // kind -> (ev) -> {name, sub, pic, self}
export function registerRow(kinds, fn) { for (const k of kinds) rows.set(k, fn); }

/**
 * A stranger's value as one line of text, or "". Non-strings become "" (a JSON
 * field can be any type), and whitespace collapses before `clip` counts it.
 */
export const oneLine = (v) =>
  (typeof v === "string" || typeof v === "number" ? String(v).replace(/\s+/g, " ").trim() : "");

/** "1 relay" / "2,431 people". */
export const plural = (n, one, many = `${one}s`) => `${n.toLocaleString()} ${n === 1 ? one : many}`;

/** A NIP-57 amount as sats, or null. The protocol counts millisats. */
export const satsOf = (msats) => {
  const n = Number(msats);
  return Number.isFinite(n) && n > 0 ? Math.round(n / 1000).toLocaleString() : null;
};

// ---- links ----------------------------------------------------------------
// Internal first: this app renders NIP-19 pages itself, and app.js intercepts
// the click into a pushState render. njump is the entity page's escape hatch.
export const keyHref = (hex) => `/${esc(npub(hex))}`;
export const noteHref = (hex) => `/${esc(noteId(hex))}`;
export const njumpFor = (bech) => `https://njump.me/${esc(bech)}`;
/** An `a` tag as a link to its entity page — null when it cannot be encoded. */
export const addrHref = (a) => { const n = naddr(a); return n ? `/${esc(n)}` : null; };

/**
 * An event page. With a relay or author hint this mints an nevent, which is
 * what entity.js falls back to when this relay's index misses the event.
 */
export const eventHref = (id, hints = {}) => {
  const n = hints.relay || hints.author
    ? nevent(id, { relays: hints.relay ? [hints.relay] : [], author: hints.author, kind: hints.kind })
    : "";
  return n ? `/${esc(n)}` : noteHref(id);
};

/**
 * The card's own page: a profile's npub, an addressable event's naddr, else
 * the event id. An id names one revision, so the first two would go stale on
 * the next edit; the naddr is also what a provenance pill opens, and the two
 * must agree. Null when the event carries no usable identifier.
 */
export const selfHref = (ev) => {
  if (ev && ev.kind === 0 && HEX64.test(ev.pubkey || "")) return keyHref(ev.pubkey);
  // Addressable is not the same as encodable: a `d` over 255 bytes has no
  // legal naddr, and such a card still has an id to link to.
  const addr = addrOf(ev);
  const byAddr = addr ? addrHref(addr) : null;
  if (byAddr) return byAddr;
  return ev && HEX64.test(ev.id || "") ? noteHref(ev.id) : null;
};

const HEX64 = /^[0-9a-f]{64}$/;

// ---- tag access -----------------------------------------------------------
// `Array.isArray` on every entry: a hint-fetched event is rendered before
// anything has verified its tags.
export const tagsOf = (ev, name) => ((ev && ev.tags) || []).filter((t) => Array.isArray(t) && t[0] === name);
export const tagOf = (ev, ...names) => {
  for (const name of names) {
    for (const t of (ev && ev.tags) || []) if (Array.isArray(t) && t[0] === name && t[1]) return t[1];
  }
  return null;
};

/** Tags matched by a predicate on the name: a 10040's `30382:rank`, a 30618's `refs/heads/…`. */
export const tagsWhere = (ev, pred) =>
  ((ev && ev.tags) || []).filter((t) => Array.isArray(t) && pred(String(t[0] ?? ""), t));

/**
 * Every NIP-92/94 imeta on the event, parsed: `["imeta", "url https://…",
 * "dim 1088x1920"]` becomes `{url: "https://…", dim: "1088x1920"}`. One entry
 * per tag, because NIP-68 gives a picture post one imeta per picture.
 * A null-prototype object because the keys are a stranger's; first key wins.
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

/** Unix seconds as a local date; a non-number comes back verbatim, so escape the result. */
export const fmtTs = (secs) => {
  const n = Number(secs);
  return Number.isFinite(n) && n > 0
    ? new Date(n * 1000).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" })
    : String(secs || "");
};

/** Guarded JSON content, `{}` when it does not parse. */
export function jsonContent(ev) {
  try { return JSON.parse(ev.content) || {}; } catch (e) { return {}; }
}

// ---- the two depths -------------------------------------------------------
export const clipIf = (opts, s, n) => (opts && opts.full ? String(s || "").trim() : clip(s, n));
export const clampCls = (opts) => (opts && opts.full ? "" : " clamp");

// ---- shared chrome --------------------------------------------------------
export const badgeHtml = (ev) => `<span class="kind-badge" data-tone="${kindTone(ev.kind)}">${esc(kindLabel(ev.kind))}</span>`;

/**
 * The provenance pills, or "" for a card that is here because the search
 * matched it. Most cards draw nothing; the row's presence is itself the
 * signal, so it carries no standing label.
 */
export function provHtml(ev, opts) {
  const pills = provenance.get((ev && ev.id) || "");
  if (!pills || !pills.length) return "";
  const cap = opts && opts.full ? PILL_BUDGET.full : PILL_BUDGET.preview;
  const shown = pills.slice(0, cap);
  const rest = pills.slice(cap);
  return `<div class="prov pills">` +
    shown.map((p) => pillHtml(p)).join("") +
    (rest.length
      ? `<button type="button" class="prov-more" aria-expanded="false" data-label="+${rest.length.toLocaleString()} more">+${rest.length.toLocaleString()} more</button>` +
        rest.map((p) => pillHtml(p, true)).join("")
      : "") +
    `</div>`;
}

/** One pill. `overflow` pills sit hidden in the DOM behind the count, so expanding is no re-render. */
function pillHtml(p, overflow = false) {
  const href = pillHref(p);
  const tone = p.gated ? "vouched" : "open";
  const body =
    facesFor(p) +
    esc(p.text) +
    (p.count > 1 ? ` <span class="n">${p.count.toLocaleString()}</span>` : "");
  const attrs = `class="prov-pill ${tone}${overflow ? " extra" : ""}" title="${esc(pillTitle(p))}"${overflow ? " hidden" : ""}`;
  // A pill with nowhere to go is text, not a dead link.
  return href ? `<a ${attrs} href="${href}">${body}</a>` : `<span ${attrs}>${body}</span>`;
}

/**
 * A pill's destination: a list or assertion opens its own entity page (the
 * same page its card opens), a label searches for itself, a topic opens that
 * topic's screen.
 */
function pillHref(p) {
  if (p.to === "addr") return addrHref(p.value);
  if (p.to === "topic") return hashtagHref(p.value);
  return searchHref(p.value);
}

/** What the hover says: the count, and who is behind it. */
function pillTitle(p) {
  const who = p.authors.map((pk) => displayName(profiles.get(pk)) || shortNpub(pk)).join(", ");
  const n = p.count === 1 ? "1 record" : `${p.count.toLocaleString()} records`;
  return p.gated
    ? `${n} from ${who} — a publisher your provider list names`
    : `${n} from ${who} — a NIP-32 label, which anyone may publish`;
}

/**
 * The author's face where it disambiguates: always on an ungated pill (who is
 * speaking is the whole trust question), and on a gated one only when the page
 * holds more than one delegated publisher.
 */
function facesFor(p) {
  if (p.gated && !attribution.faces) return "";
  const shown = p.authors.slice(0, 3);
  const faces = shown.map((pk) => avatarHtml(authorOf({ pubkey: pk }).picture, pk, "xs")).join("");
  return shown.length > 1 ? `<span class="prov-pile">${faces}</span>` : faces;
}

/** The raw event, one click away on every result; on the permalink it is the complete event. */
export const jsonHtml = (ev) =>
  `<div class="raw"><button type="button" class="raw-toggle" data-id="${esc(ev.id)}">json</button>` +
  `<pre class="raw-body" hidden></pre></div>`;

/**
 * The shared author line: avatar, name, date, badge. The date is the card's
 * permalink and a real anchor, which is what lets the whole card be clickable
 * without losing middle-click, right-click or Tab. On the permalink it stays
 * plain text.
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
      ${groupPillHtml(ev)}
      ${badgeHtml(ev)}
    </div>`;
}

/**
 * The NIP-29 room a chat line was posted to, as a pill beside the badge,
 * linking to that group's search (a group has no event of its own to open).
 * An `h` names no host and this store does not record which relay an event
 * came from, so the name is groupnames.js's "what this id is called where the
 * sources agree", never "which room this is".
 */
export function groupPillHtml(ev) {
  const id = postedTo(ev);
  if (!id) return "";
  const name = groupName(id);
  const said = `the NIP-29 group this was posted to, id ${id}`;
  const title = name ? `“${name}” — ${said}` : said;
  const label = esc(clip(name || id, 28));
  // An id the search language cannot carry keeps its label and loses its link.
  const href = groupHref(id);
  return href
    ? `<a class="group-pill" href="${href}" title="${esc(title)}">${label}</a>`
    : `<span class="group-pill" title="${esc(title)}">${label}</span>`;
}

/**
 * A props table, skipping rows whose value came up empty. The value goes in
 * as raw HTML so a row can be a link, so every value derived from an event
 * must arrive already escaped; cards.test.mjs renders every kind with a
 * payload in every tag to catch the one that does not.
 */
export const propsHtml = (props) => {
  const rows = props.filter(([, v]) => v != null && v !== "");
  return rows.length ? `<dl class="props">${rows.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${v}</dd>`).join("")}</dl>` : "";
};

/**
 * The card frame most kinds share: byline, the kind's body, props, json.
 * `data-href` is where the card goes when clicked; an attribute rather than a
 * wrapping `<a>` because a card contains links and anchors cannot nest.
 * Preview depth only: on the permalink the card is the page.
 */
export function shell(ev, opts, inner, props = []) {
  const href = opts && opts.full ? null : selfHref(ev);
  return `
    <article class="result${opts && opts.full ? " full" : ""}" data-id="${esc(ev.id)}"${href ? ` data-href="${href}"` : ""}>
      ${bylineHtml(ev, opts)}
      ${provHtml(ev, opts)}
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

/** A person, linked: their name when the store knows one, else a short npub, the full npub in the hover. Never hex. */
export const personLink = (pk) => {
  const nm = displayName(profiles.get(pk));
  return `<a${nm ? "" : ' class="mono"'} href="${keyHref(pk)}" title="${esc(npub(pk))}">${esc(nm || shortNpub(pk))}</a>`;
};

/**
 * "↩ in reply to <person>", or "" when the event is not a reply. The label is
 * the person; the link is the parent event, carrying the `e` tag's relay hint
 * so a parent this relay never mirrored still opens.
 */
export function replyLine(ev) {
  const t = replyTarget(ev);
  if (t) {
    const pk = replyAuthor(ev);
    return replyRow(eventHref(t.id, { relay: t.relay, author: pk }), pk, shortNote(t.id), noteId(t.id));
  }
  // A NIP-22 comment on something addressable has no `e`; its `a` carries the author.
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

/** A heading at either depth. `href` goes in raw: pass keyHref/noteHref/addrHref, never a url off an event. */
export const titleHtml = (opts, text, n = 140, href = null) => {
  const t = text ? clipIf(opts, text, n) : "";
  if (!t) return "";
  return `<h2 class="result-title">${href ? `<a href="${href}">${esc(t)}</a>` : esc(t)}</h2>`;
};

/**
 * A url off an event, reduced to one this page will put in an `href`, or null.
 * Absolute http/https only: `esc()` makes a url safe to sit in an attribute
 * and says nothing about `javascript:` or `data:` being followed, and a
 * relative url would resolve against this origin.
 */
export const safeUrl = (u) => {
  const s = String(u || "").trim();
  if (!s) return null;
  try {
    const p = new URL(s).protocol;   // throws on anything not absolute
    return p === "http:" || p === "https:" ? s : null;
  } catch (e) { return null; }
};

/** The one external link. An unlinkable url renders as its own text rather than disappearing. */
export const extLink = (url, label) => {
  if (!url) return null;
  const safe = safeUrl(url);
  return safe
    ? `<a href="${esc(safe)}" target="_blank" rel="noopener noreferrer">${esc(label || safe)}</a>`
    : `<span class="mono">${esc(clip(String(url), 120))}</span>`;
};

/** A list of relay rows; full mode shows all, preview the first few. Five families carry relay urls. */
export function relayRows(rows, opts) {
  const shown = opts && opts.full ? rows : rows.slice(0, 6);
  const more = rows.length - shown.length;
  return `<ul class="relay-list">${shown.map((r) => `<li><span class="mono">${esc(r.url)}</span>${r.note ? ` <span class="muted-note">${esc(r.note)}</span>` : ""}</li>`).join("")}${more > 0 ? `<li class="muted-note">…and ${more} more</li>` : ""}</ul>`;
}

/** A search this page can run, as a url: the root with `q` set as the search field would have tokenized it. */
export const searchHref = (q) => `/?${new URLSearchParams({ q })}`;
/** A topic, however it was written: `t` tags carry `scotland`, cards show `#scotland`. */
export const hashtagHref = (t) => searchHref(String(t).startsWith("#") ? t : `#${t}`);
/**
 * A NIP-29 group as the search that finds what was posted in it, or null when
 * the id would not read back as itself in the token language (whitespace ends
 * a token, trailing punctuation is stripped). A link that searches for a
 * different group is worse than no link.
 */
export const groupHref = (id) => (groupTokenizes(id) ? searchHref(`group:${id}`) : null);

/**
 * Hashtags, words, mime types — short values that read as chips, not rows.
 * `hrefOf` makes them links; values with no search behind them stay spans.
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

/** What a list points at, as links: `e` tags to /note1…, `a` tags to /naddr1…. */
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

/** A strip of faces where people are a card's context — a poll's winners, a community's moderators. */
export function faceStrip(pubkeys, max = 12) {
  const shown = pubkeys.slice(0, max);
  if (!shown.length) return "";
  const more = pubkeys.length - shown.length;
  return `<div class="face-strip">${shown.map((pk) => `<a href="${keyHref(pk)}">${avatarHtml(authorOf({ pubkey: pk }).picture, pk, "md")}</a>`).join("")}${more > 0 ? `<span class="face-more">+${more}</span>` : ""}</div>`;
}

/**
 * How many cells a people grid gets, per depth. Both numbers must stay
 * divisible by every column count index.html's `.people-grid` uses (6 and 3),
 * or the `+N more` cell ends up alone on a ragged row. cards.js reads these to
 * fetch exactly the profiles the grid draws.
 */
export const PEOPLE_GRID = { preview: 6, full: 24 };

/** A list's `p` values as people: hex-shaped, and each of them once (kind-3 lists repeat entries). */
export const uniquePubkeys = (values) =>
  [...new Set((values || []).filter((pk) => HEX64.test(pk || "")))];

/** The distinct people a list holds — its `p` tags, deduped, hex only. */
export const peopleOf = (ev) => uniquePubkeys(tagsOf(ev, "p").map((t) => t[1]));

/**
 * Who a grid draws and how many it leaves out, the one answer the renderer
 * and the profile loader both read. When the list overruns, the last cell is
 * the count, so one fewer face fits.
 */
export function gridCells(pubkeys, opts) {
  const cap = opts && opts.full ? PEOPLE_GRID.full : PEOPLE_GRID.preview;
  const shown = pubkeys.length > cap ? pubkeys.slice(0, cap - 1) : pubkeys;
  return { shown, more: pubkeys.length - shown.length };
}

/**
 * The people a list holds, as a grid of face + name. The stylesheet takes the
 * `full` class to pick the face size and the column count; this function
 * counts cells, never rows. The npub always sits in the title, which is how
 * cards.test.mjs finds every person a card names.
 */
export function peopleGrid(pubkeys, opts) {
  const full = !!(opts && opts.full);
  const { shown, more } = gridCells(uniquePubkeys(pubkeys), opts);
  if (!shown.length) return "";
  const size = full ? "xxl" : "xl";
  // One map read per cell; the profile entry carries both picture and name.
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

/** `+8.4k` rather than `+8,432`: it has to read inside a small circle. */
const compactCount = (n) =>
  n < 1000 ? String(n) : n < 10000 ? `${(n / 1000).toFixed(1)}k` : `${Math.round(n / 1000)}k`;

/**
 * Which kinds draw that grid, declared by the family that registers the
 * renderer. cards.js loads those people's profiles before rendering, so the
 * set must be kept beside the renderer rather than listed a second time.
 */
export const PEOPLE_GRID_KINDS = new Set();
export const registerPeopleGrid = (kinds) => { for (const k of kinds) PEOPLE_GRID_KINDS.add(k); };

/**
 * The pubkeys [ev]'s grid draws at this depth. Depth matters: the results
 * list only ever renders previews, and the permalink's cap would fetch
 * profiles no card on the page can show.
 */
export const gridPeople = (ev, opts) =>
  PEOPLE_GRID_KINDS.has(ev.kind) ? gridCells(peopleOf(ev), opts).shown : [];

/**
 * The same declaration for a card that names people no scan of `p` tags
 * reaches, such as a repository's `["maintainers", <pk>, <pk>, …]`. A renderer
 * registers the very function it draws with, so the set named and the set
 * declared are one expression.
 */
export const NAMED_PEOPLE = new Map(); // kind -> (ev, opts) -> pubkeys
export const registerNamedPeople = (kinds, fn) => { for (const k of kinds) NAMED_PEOPLE.set(k, fn); };
export const namedPeople = (ev, opts) => (NAMED_PEOPLE.get(ev.kind) || NOBODY)(ev, opts);
const NOBODY = () => [];
