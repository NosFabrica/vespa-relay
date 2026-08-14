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
import { relayLabel } from "../shared/groups.js";
import {
  register, registerRow, registerPeopleGrid, shell, titleHtml, bodyHtml, peopleGrid, uniquePubkeys, relayRows,
  chipRow, hashtagHref, emojiGrid, refRows, extLink, tagsOf, tagOf, groupHref, plural,
} from "./base.js";

/**
 * NIP-51's `group` tags — `["group", <id>, <relay url>, <name?>]`.
 *
 * The one tag in this table whose value is NOT its second element, which is
 * exactly how it was drawn wrong: it went through [relayRows] like `relay` and
 * `server` do, over `t[1]` like every other tag, so the card printed group IDS
 * in a list of relay urls and threw the real url and the name away. A NIP-29
 * group is the pair (id, host relay) — neither half is the group — so all
 * three elements are read and the row shows what each of them is.
 *
 * The name is the link, because a group is a search waiting to happen exactly
 * as a hashtag is: `group:<id>` is what this page's search box means by it.
 * The url stays beside it in `mono` rather than becoming a second link — it is
 * a relay to dial, not a page here, and it is OPTIONAL here in a way it is not
 * in the picker: a card DRAWS a stranger's list, so a `group` tag whose host is
 * missing is still an entry they put there and still a searchable id. Dropping
 * it is how a list with entries rendered as "nothing public here". The picker
 * makes the stricter demand (see shared/groups.js) because it is choosing a
 * group to filter by, and a row that cannot say where it lives is not a choice.
 */
function groupRows(rows, opts) {
  const shown = opts && opts.full ? rows : rows.slice(0, 6);
  const more = rows.length - shown.length;
  const cells = shown.map(([id, url, name]) =>
    `<li><a href="${groupHref(id)}">${esc(name || id)}</a>` +
    (url ? `<span class="muted-note mono"> ${esc(relayLabel(url))}</span>` : "") + `</li>`);
  return `<ul class="relay-list">${cells.join("")}${more > 0 ? `<li class="muted-note">…and ${more} more</li>` : ""}</ul>`;
}

// What each tag holds, and how it wants to be shown. A section with no
// renderer here cannot be declared below — the table is the whole vocabulary.
const TAGS = {
  p: { one: "person", many: "people", show: (v, o) => peopleGrid(v, o) },
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
  group: { one: "group", many: "groups", show: groupRows },
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

/**
 * `emoji` carries a pair per tag (shortcode, url); everything else a value.
 *
 * Deduped, because a list holds a THING once: repeated tags are common (a
 * client appends without checking what is already there, two merges of the
 * same list stack up), and every section here both counts its values and
 * draws them. Undeduped, a follow list with the same person twice drew that
 * person in two cells and counted them as two members.
 */
const valuesOf = (ev, tag) => {
  if (tag === "emoji") return unique(tagsOf(ev, "emoji").filter((t) => t[1] && t[2]).map((t) => [t[1], t[2]]), (v) => v.join("|"));
  // A group is the PAIR (id, host relay), so both are part of its identity and
  // the key is the pair rather than the id — the same id on two relays is two
  // groups. The optional name rides along and is deliberately NOT in the key:
  // quartz's own GroupTag.equals excludes it for the same reason, so one group
  // listed once with a cached name and once without stays one row.
  //
  // Only the ID is REQUIRED. A card draws what somebody else's list says, and
  // an entry with no host is still an entry they put there — dropping it left a
  // list of them rendering as "nothing public here", which is a card claiming
  // its author saved nothing. The host is what makes a group choosable, not
  // what makes it real, so the picker demands both and this does not.
  if (tag === "group") {
    return unique(
      tagsOf(ev, "group").filter((t) => t[1]).map((t) => [t[1], t[2] || "", t[3] || ""]),
      (v) => `${v[0]}|${v[1]}`,
    );
  }
  // `p` counts and draws the same set: a value that is not a key is not a
  // person, and "7 people" over six faces is the count answering a different
  // question from the grid.
  if (tag === "p") return uniquePubkeys(tagsOf(ev, "p").map((t) => t[1]));
  return unique(tagsOf(ev, tag).map((t) => t[1]).filter(Boolean), (v) => v);
};

const unique = (values, keyOf) => {
  const seen = new Set();
  return values.filter((v) => { const k = keyOf(v); return seen.has(k) ? false : (seen.add(k), true); });
};

const countOf = (s, n) => plural(n, s.one, s.many);

/** The sections this list actually has something in, in the NIP's own order. */
const sectionsOf = (ev) => (LISTS[ev.kind] || [])
  .map(spec)
  .map((s) => ({ ...s, values: valuesOf(ev, s.tag) }))
  .filter((s) => s.values.length);

/**
 * What the list is CALLED, or "" — a list is entitled to have no name, and the
 * card then leads with what it holds.
 *
 * 30007's `d` IS the muted kind, not a name — a title tag would be wrong to
 * invent, so the identifier is stated as what it is.
 */
const listTitle = (ev) => ev.kind === 30007
  ? (tagOf(ev, "d") ? `kind ${tagOf(ev, "d")}` : "")
  : (tagsOf(ev, "title").length || tagsOf(ev, "name").length ? titleOf(ev) : "");

/**
 * What it HOLDS, in one line: "12 people · 3 hashtags" — the card's preview
 * line, and the type-ahead row's second line, which is the whole of what a
 * list has to say about itself when it has no title.
 *
 * A list whose every item is private is a legal, common NIP-51 event: the items
 * live NIP-44-encrypted in .content and the tags are genuinely empty. "nothing
 * public here" is the true statement; "0 people" reads as a list its author
 * left empty, which is a different thing.
 */
const countsLine = (ev) => {
  const sections = sectionsOf(ev);
  return sections.length
    ? sections.map((s) => countOf(s, s.values.length)).join(" · ")
    : `nothing public here${ev.content ? " — this list keeps its items encrypted" : ""}`;
};

function listCard(ev, opts) {
  const full = opts && opts.full;
  const sections = sectionsOf(ev);
  const body = sections.length
    ? (full
        ? sections.map((s) => `<div class="list-section"><div class="section-head">${esc(countOf(s, s.values.length))}</div>${s.show(s.values, opts)}</div>`).join("")
        : `<div class="result-body">${esc(countsLine(ev))}</div>` +
          sections[0].show(sections[0].values, opts))
    : `<div class="result-body muted">${esc(countsLine(ev))}</div>`;

  const inner = titleHtml(opts, listTitle(ev), 120) + bodyHtml(opts, summaryOf(ev), 300, true) + body;
  return shell(ev, opts, inner);
}

register(Object.keys(LISTS).map(Number), listCard);
// One row for the whole table, for the same reason there is one card: a list's
// name and what it holds are the two lines it has, whichever tags carry the
// items. Unnamed lists — most of the standard ones, which are a person's ONE
// mute list or relay list — lead with their owner and count underneath.
//
// The count comes FIRST and the description after it: the count is short and
// every list has one, so clipping takes words off the end of a sentence rather
// than the number the row was rebuilt to show.
registerRow(Object.keys(LISTS).map(Number), (ev) => ({
  name: listTitle(ev),
  sub: [countsLine(ev), summaryOf(ev)].filter(Boolean).join(" · "),
}));
// Derived from the table rather than listed again: a kind draws a people grid
// exactly when its row carries a `p` section, so adding one to the table above
// is all it takes for those people to be named — and enriched — as well as drawn.
registerPeopleGrid(
  Object.keys(LISTS)
    .filter((k) => LISTS[k].some((e) => (Array.isArray(e) ? e[0] : e) === "p"))
    .map(Number));

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
registerRow([39701], (ev) => ({ name: titleOf(ev) || tagOf(ev, "d"), sub: summaryOf(ev) || ev.content }));
