// NIP-51 lists and sets — the family the registry kept skipping. Six of them
// had bespoke cards (3, 10002, 30000, 30002, 30004, 30005, 30030, 30063)
// because something else in the relay needed them; the other thirty fell to
// the generic floor, where a bookmark set with twelve saved articles rendered
// as a title, a badge reading "kind 30003", and nothing else. That is the bug
// this module exists to close, and it closes it for the WHOLE NIP-51 table
// rather than for 30003 alone — the next unrendered set kind would otherwise
// arrive the same way.
//
// One card, driven by one table, because every list kind is the same shape:
// an optional title/description, and items in tags whose NAME says what they
// are. So the only per-kind knowledge is which tags to read and what to call
// what is in them. Rendering is by tag, not by kind: `p` is always faces,
// `relay` is always relay rows, `emoji` is always the emoji themselves.

import { esc, titleOf, summaryOf } from "../shared/format.js";
import {
  register, shell, titleHtml, bodyHtml, faceStrip, relayRows, chipRow, hashtagHref,
  emojiGrid, refRows, extLink, tagsOf, tagOf,
} from "./base.js";

// What each tag holds, and how it wants to be shown. A section with no
// renderer here cannot be declared below — the table is the whole vocabulary.
const TAGS = {
  p: { one: "person", many: "people", show: (v, o) => faceStrip(v.filter((pk) => /^[0-9a-f]{64}$/.test(pk)), o && o.full ? 24 : 12) },
  e: { one: "event", many: "events", show: (v, o) => refRows(v.map((x) => ({ kind: "e", value: x })), o) },
  a: { one: "entry", many: "entries", show: (v, o) => refRows(v.map((x) => ({ kind: "a", value: x })), o) },
  // A hashtag is a search wherever it appears — on an interest list as much as
  // on a picture. A mute WORD is not: it is a string this person does not want
  // to read, and linking it to a page full of it would be a joke at their
  // expense.
  t: { one: "hashtag", many: "hashtags", show: (v, o) => chipRow(v, o, hashtagHref) },
  word: { one: "word", many: "words", show: chipRow },
  relay: { one: "relay", many: "relays", show: (v, o) => relayRows(v.map((url) => ({ url })), o) },
  server: { one: "server", many: "servers", show: (v, o) => relayRows(v.map((url) => ({ url })), o) },
  group: { one: "group", many: "groups", show: (v, o) => relayRows(v.map((url) => ({ url })), o) },
  url: { one: "feed", many: "feeds", show: (v, o) => relayRows(v.map((url) => ({ url })), o) },
  r: { one: "link", many: "links", show: (v, o) => relayRows(v.map((url) => ({ url })), o) },
  emoji: { one: "emoji", many: "emoji", show: emojiGrid },
};

/**
 * kind -> the tags it carries, in the order NIP-51 lists them. A bare string
 * takes the tag's default nouns; a triple renames them for this kind, because
 * "12 entries" and "12 communities" are the same `a` tags and only one of
 * them tells the reader what they are looking at.
 */
const LISTS = {
  // ---- standard lists (10000-10099): one per user ------------------------
  10000: ["p", "t", "word", "e"],
  10001: [["e", "note", "notes"]],
  10003: ["e", ["a", "article", "articles"], "t", "r"],
  10004: [["a", "community", "communities"]],
  10005: [["e", "channel", "channels"]],
  10006: ["relay"],
  10007: ["relay"],
  10008: [["a", "badge", "badges"], ["e", "award", "awards"]],
  10009: ["group", "r"],
  10011: [["a", "follow set", "follow sets"]],
  10012: ["relay", ["a", "relay set", "relay sets"]],
  10013: ["relay"],
  10015: ["t", ["a", "interest set", "interest sets"]],
  10017: ["p"],
  10018: [["a", "repository", "repositories"]],
  10020: ["p"],
  10030: ["emoji", ["a", "emoji set", "emoji sets"]],
  10050: ["relay"],
  10054: ["p", "url"],
  10063: ["server"],
  10064: ["p"],
  10096: ["server"],
  10101: ["p"],
  10102: ["relay"],
  // ---- sets (30000-30099): many per user, each with its own `d` ----------
  30001: ["e", "a", "p", "t"],
  30003: ["e", ["a", "article", "articles"], "t", "r"],
  30006: [["e", "picture", "pictures"]],
  30007: ["p"],
  30008: [["a", "badge", "badges"], ["e", "award", "awards"]],
  30015: ["t"],
  30267: [["a", "app", "apps"]],
};

const spec = (entry) => {
  const [tag, one, many] = Array.isArray(entry) ? entry : [entry];
  const d = TAGS[tag];
  return { tag, one: one || d.one, many: many || d.many, show: d.show };
};

/** `emoji` carries a pair per tag (shortcode, url); everything else a value. */
const valuesOf = (ev, tag) =>
  tag === "emoji"
    ? tagsOf(ev, "emoji").filter((t) => t[1] && t[2]).map((t) => [t[1], t[2]])
    : tagsOf(ev, tag).map((t) => t[1]).filter(Boolean);

const countOf = (s, n) => `${n.toLocaleString()} ${n === 1 ? s.one : s.many}`;

function listCard(ev, opts) {
  const full = opts && opts.full;
  const sections = (LISTS[ev.kind] || [])
    .map(spec)
    .map((s) => ({ ...s, values: valuesOf(ev, s.tag) }))
    .filter((s) => s.values.length);

  // 30007's `d` IS the muted kind, not a name — a title tag would be wrong to
  // invent, so the identifier is stated as what it is.
  const title = ev.kind === 30007
    ? (tagOf(ev, "d") ? `kind ${tagOf(ev, "d")}` : "")
    : (tagsOf(ev, "title").length || tagsOf(ev, "name").length ? titleOf(ev) : "");

  // A list whose every item is private is a legal, common NIP-51 event: the
  // items live NIP-44-encrypted in .content and the tags are genuinely empty.
  // "nothing public here" is the true statement; "0 people" reads as a list
  // its author left empty, which is a different thing.
  const body = sections.length
    ? (full
        ? sections.map((s) => `<div class="list-section"><div class="section-head">${esc(countOf(s, s.values.length))}</div>${s.show(s.values, opts)}</div>`).join("")
        : `<div class="result-body">${esc(sections.map((s) => countOf(s, s.values.length)).join(" · "))}</div>` +
          sections[0].show(sections[0].values, opts))
    : `<div class="result-body muted">nothing public here${ev.content ? " — this list keeps its items encrypted" : ""}</div>`;

  const inner = titleHtml(opts, title, 120) + bodyHtml(opts, summaryOf(ev), 300, true) + body;
  return shell(ev, opts, inner);
}

register(Object.keys(LISTS).map(Number), listCard);

// 39701 — NIP-B0 web bookmarks. Not a NIP-51 list at all, but the same
// instinct one file over: the `d` IS the bookmarked url (without its scheme),
// so the card's job is to make that url clickable rather than to count tags.
function webBookmarkCard(ev, opts) {
  const d = tagOf(ev, "d") || "";
  const url = d ? (/^https?:\/\//i.test(d) ? d : `https://${d}`) : null;
  const inner =
    titleHtml(opts, titleOf(ev) || d, 140) +
    bodyHtml(opts, summaryOf(ev) || ev.content, 400);
  return shell(ev, opts, inner, [["url", extLink(url, d)]]);
}

register([39701], webBookmarkCard);
