// NIP-51 lists and sets: one card driven by one table, because every list
// kind is an optional title/description plus items in tags whose name says
// what they are. Rendering is by tag, not by kind: `p` is always faces,
// `relay` always relay rows, `emoji` always the emoji themselves.

import { esc, titleOf, summaryOf } from "../shared/format.js";
import { relayLabel } from "../shared/groups.js";
import {
  register, registerRow, registerPeopleGrid, shell, titleHtml, bodyHtml, peopleGrid, uniquePubkeys, relayRows,
  chipRow, hashtagHref, emojiGrid, refRows, extLink, tagsOf, tagOf, groupHref, plural,
} from "./base.js";

/**
 * NIP-51 `group` tags, `["group", <id>, <relay url>, <name?>]`, as rows. The
 * name links to the `group:` search; the url stays beside it as text, since
 * it is a relay to dial, not a page here.
 */
function groupRows(rows, opts) {
  const shown = opts && opts.full ? rows : rows.slice(0, 6);
  const more = rows.length - shown.length;
  // An id the token language cannot carry keeps its label and loses its link (see base.js groupHref).
  const cells = shown.map(([id, url, name]) => {
    const href = groupHref(id);
    const label = esc(name || id);
    return `<li>${href ? `<a href="${href}">${label}</a>` : label}` +
      (url ? `<span class="muted-note mono"> ${esc(relayLabel(url))}</span>` : "") + `</li>`;
  });
  return `<ul class="relay-list">${cells.join("")}${more > 0 ? `<li class="muted-note">…and ${more} more</li>` : ""}</ul>`;
}

// What each tag holds and how it is shown. The table is the whole vocabulary
// LISTS may declare.
const TAGS = {
  p: { one: "person", many: "people", show: (v, o) => peopleGrid(v, o) },
  e: { one: "event", many: "events", show: (v, o) => refRows(v.map((x) => ({ kind: "e", value: x })), o) },
  a: { one: "entry", many: "entries", show: (v, o) => refRows(v.map((x) => ({ kind: "a", value: x })), o) },
  // A hashtag is a search wherever it appears. A mute word is not: linking it
  // to a page full of it would be a joke at the reader's expense.
  t: { one: "hashtag", many: "hashtags", show: (v, o) => chipRow(v, o, hashtagHref) },
  word: { one: "word", many: "words", show: chipRow },
  relay: { one: "relay", many: "relays", show: (v, o) => relayRows(v.map((url) => ({ url })), o) },
  server: { one: "server", many: "servers", show: (v, o) => relayRows(v.map((url) => ({ url })), o) },
  group: { one: "group", many: "groups", show: groupRows },
  url: { one: "feed", many: "feeds", show: (v, o) => relayRows(v.map((url) => ({ url })), o) },
  r: { one: "link", many: "links", show: (v, o) => relayRows(v.map((url) => ({ url })), o) },
  emoji: { one: "emoji", many: "emoji", show: emojiGrid },
};

/**
 * kind -> the tags it carries, in NIP-51's order. A bare string takes the
 * tag's default nouns; a triple renames them for this kind ("12 communities"
 * rather than "12 entries").
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

/**
 * A tag's values, deduped: a list holds a thing once, and every section both
 * counts its values and draws them. `emoji` carries a (shortcode, url) pair
 * per tag; everything else a value.
 */
const valuesOf = (ev, tag) => {
  if (tag === "emoji") return unique(tagsOf(ev, "emoji").filter((t) => t[1] && t[2]).map((t) => [t[1], t[2]]), (v) => v.join("|"));
  // A group's identity is the pair (id, host relay); the name is not part of
  // it, as in quartz's GroupTag.equals. Only the id is required: a card draws
  // what somebody's list says, and an entry with no host is still an entry.
  if (tag === "group") {
    return unique(
      tagsOf(ev, "group").filter((t) => t[1]).map((t) => [t[1], t[2] || "", t[3] || ""]),
      (v) => `${v[0]}|${v[1]}`,
    );
  }
  // `p` counts and draws the same set: a value that is not a key is not a person.
  if (tag === "p") return uniquePubkeys(tagsOf(ev, "p").map((t) => t[1]));
  return unique(tagsOf(ev, tag).map((t) => t[1]).filter(Boolean), (v) => v);
};

const unique = (values, keyOf) => {
  const seen = new Set();
  return values.filter((v) => { const k = keyOf(v); return seen.has(k) ? false : (seen.add(k), true); });
};

const countOf = (s, n) => plural(n, s.one, s.many);

/** The sections this list has something in, in the NIP's own order. */
const sectionsOf = (ev) => (LISTS[ev.kind] || [])
  .map(spec)
  .map((s) => ({ ...s, values: valuesOf(ev, s.tag) }))
  .filter((s) => s.values.length);

/**
 * What the list is called, or "" for a list with no name. 30007's `d` is the
 * muted kind, not a name, so it is stated as what it is.
 */
const listTitle = (ev) => ev.kind === 30007
  ? (tagOf(ev, "d") ? `kind ${tagOf(ev, "d")}` : "")
  : (tagsOf(ev, "title").length || tagsOf(ev, "name").length ? titleOf(ev) : "");

/**
 * What it holds in one line: "12 people · 3 hashtags". A list whose items are
 * all NIP-44-encrypted in .content has genuinely empty tags, and "nothing
 * public here" is the true statement where "0 people" would read as empty.
 */
const countsLine = (ev, sections = sectionsOf(ev)) => sections.length
  ? sections.map((s) => countOf(s, s.values.length)).join(" · ")
  : `nothing public here${ev.content ? " — this list keeps its items encrypted" : ""}`;

function listCard(ev, opts) {
  const full = opts && opts.full;
  // Read once and passed down: valuesOf dedupes every tag, and a follow list is thousands.
  const sections = sectionsOf(ev);
  const body = sections.length
    ? (full
        ? sections.map((s) => `<div class="list-section"><div class="section-head">${esc(countOf(s, s.values.length))}</div>${s.show(s.values, opts)}</div>`).join("")
        : `<div class="result-body">${esc(countsLine(ev, sections))}</div>` +
          sections[0].show(sections[0].values, opts))
    : `<div class="result-body muted">${esc(countsLine(ev, sections))}</div>`;

  const inner = titleHtml(opts, listTitle(ev), 120) + bodyHtml(opts, summaryOf(ev), 300, true) + body;
  return shell(ev, opts, inner);
}

register(Object.keys(LISTS).map(Number), listCard);
// One row for the whole table: name, then what it holds. The count comes
// before the description so clipping takes words off the sentence, not the number.
registerRow(Object.keys(LISTS).map(Number), (ev) => ({
  name: listTitle(ev),
  sub: [countsLine(ev), summaryOf(ev)].filter(Boolean).join(" · "),
}));
// Derived from the table: a kind draws a people grid exactly when it carries a `p` section.
registerPeopleGrid(
  Object.keys(LISTS)
    .filter((k) => LISTS[k].some((e) => (Array.isArray(e) ? e[0] : e) === "p"))
    .map(Number));

// 39701 — a NIP-B0 web bookmark. The `d` is the bookmarked url without its
// scheme, so the card's job is to make it clickable.
function webBookmarkCard(ev, opts) {
  const d = tagOf(ev, "d") || "";
  const url = d ? (/^https?:\/\//i.test(d) ? d : `https://${d}`) : null;
  const inner =
    titleHtml(opts, titleOf(ev) || d, 140) +
    bodyHtml(opts, summaryOf(ev) || ev.content, 400);
  return shell(ev, opts, inner, [["url", extLink(url, d)]]);
}

register([39701], webBookmarkCard);
registerRow([39701], (ev) => ({ name: titleOf(ev) || tagOf(ev, "d"), sub: summaryOf(ev) || ev.content }));
