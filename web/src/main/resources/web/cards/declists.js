// Decentralized lists (Tapestry's pre-NIP draft): lists anyone can add to. The author owns only
// the header; every item is somebody's event of its own, pointing at the list with a `z`.
//
// | kind | | |
// |---|---|---|
// | 9998 / 39998 | a list header | `names` singular/plural, `description`, which tags an item must carry |
// | 9999 / 39999 | a list item | `z` the list(s) it belongs to; `t` / `p` / `e` / `a` the item itself |
//
// The 3999x forms are the editable, addressable twins. An item may also DECLARE a list (the
// spec's "nonstandard method"), so it carries the header vocabulary too; both cards read it.
// Not NIP-51: there is no people grid here, because an item names its person once, as a face.

import { esc, clip } from "../shared/format.js";
import { shortNote, shortAddr } from "../shared/nip19.js";
import {
  register, registerRow, shell, titleHtml, bodyHtml, chipRow, refRows, faceStrip, noteHref, addrHref,
  tagsOf, tagOf, oneLine, plural, uniquePubkeys,
} from "./base.js";

/** A `["names" | "titles", <singular>, <plural>]` pair, or null when either form is missing. */
const pairOf = (ev, name) => {
  const t = tagsOf(ev, name).find((x) => oneLine(x[1]) && oneLine(x[2]));
  return t ? { one: oneLine(t[1]), many: oneLine(t[2]) } : null;
};

/** What a list is called: its titles, else its names, in the plural a header reads in. */
const listName = (ev) => {
  const p = pairOf(ev, "titles") || pairOf(ev, "names");
  return p ? p.many : "";
};

/** The four rule kinds, in the order a contributor needs them. */
const RULES = ["required", "recommended", "allowed", "disallowed"];

/** `required: t, comments` — the tag names a header constrains, per rule. */
const rulesOf = (ev) => RULES
  .map((rule) => [rule, [...new Set(tagsOf(ev, rule).map((t) => oneLine(t[1])).filter(Boolean))]])
  .filter(([, names]) => names.length);

/** 9998 / 39998 — a list header: what the list is of, and what an item must look like. */
function headerCard(ev, opts) {
  const names = pairOf(ev, "names");
  const kinds = [...new Set(tagsOf(ev, "item-kind").map((t) => oneLine(t[1])).filter((k) => /^\d+$/.test(k)))];
  const inner =
    titleHtml(opts, listName(ev) || tagOf(ev, "d"), 140) +
    (names ? `<div class="result-body muted">a list anyone can add ${esc(clip(names.many, 60))} to</div>` : "") +
    bodyHtml(opts, tagOf(ev, "description"), 400);
  return shell(ev, opts, inner, [
    ...rulesOf(ev).map(([rule, tags]) => [rule, esc(clip(tags.join(", "), 120))]),
    ["item kinds", kinds.length ? esc(kinds.join(", ")) : null],
  ]);
}

/** The lists an item belongs to, from its `z` tags: an event id, an address, or a bare name. */
function parentsOf(ev) {
  const seen = new Set(), out = [];
  for (const t of tagsOf(ev, "z")) {
    const v = oneLine(t[1]);
    if (!v || seen.has(v)) continue;
    seen.add(v);
    out.push(v);
  }
  return out;
}

/** One parent, linked when it is an id or an address, named when it is a word. */
const parentLink = (v) => {
  if (/^[0-9a-f]{64}$/.test(v)) return `<a class="mono" href="${noteHref(v)}">${esc(shortNote(v))}</a>`;
  const href = /^\d+:[0-9a-f]{64}:/.test(v) ? addrHref(v) : null;
  return href ? `<a href="${href}">${esc(shortAddr(v))}</a>` : `<b>${esc(clip(v, 60))}</b>`;
};

/** An item's values: its `t` strings as written (they are values, "Fido", not hashtags). */
const stringsOf = (ev) => [...new Set(tagsOf(ev, "t").map((t) => oneLine(t[1])).filter(Boolean))];

/** What an item IS, the way a reader would say it: its name or title, else its first value. */
const itemName = (ev) => oneLine(tagOf(ev, "name")) || oneLine(tagOf(ev, "title")) || stringsOf(ev)[0] || "";

/** 9999 / 39999 — an item: what it adds, to which list, and why. */
function itemCard(ev, opts) {
  const parents = parentsOf(ev);
  const people = uniquePubkeys(tagsOf(ev, "p").map((t) => t[1]));
  const refs = [
    ...tagsOf(ev, "e").map((t) => t[1]).filter((v) => /^[0-9a-f]{64}$/.test(v || "")).map((v) => ({ kind: "e", value: v })),
    ...tagsOf(ev, "a").map((t) => oneLine(t[1])).filter(Boolean).map((v) => ({ kind: "a", value: v })),
  ];
  const declares = pairOf(ev, "names");
  const full = opts && opts.full;
  const title = itemName(ev);
  const inner =
    titleHtml(opts, title || (declares && declares.many), 140) +
    (parents.length
      ? `<div class="result-body">${declares ? "declares a list in" : "adds to"} ${parents.slice(0, full ? 12 : 3).map(parentLink).join(", ")}` +
        `${parents.length > (full ? 12 : 3) ? ` <span class="muted-note">and ${parents.length - (full ? 12 : 3)} more</span>` : ""}</div>`
      : "") +
    bodyHtml(opts, tagOf(ev, "description"), 300) +
    bodyHtml(opts, tagOf(ev, "comments"), 300, true) +
    // The value that already titles the card is not repeated as a chip under it.
    chipRow(stringsOf(ev).filter((v) => v !== title), opts) +
    faceStrip(people, full ? 24 : 12) +
    refRows(refs, opts);
  return shell(ev, opts, inner, declares ? rulesOf(ev).map(([rule, tags]) => [rule, esc(clip(tags.join(", "), 120))]) : []);
}

register([9998, 39998], headerCard);
register([9999, 39999], itemCard);

registerRow([9998, 39998], (ev) => {
  const names = pairOf(ev, "names");
  return {
    name: listName(ev) || tagOf(ev, "d") || "a decentralized list",
    sub: [tagOf(ev, "description"), names ? `a list of ${names.many}` : ""].filter(Boolean).join(" · "),
  };
});
registerRow([9999, 39999], (ev) => {
  const parents = parentsOf(ev).filter((v) => !/^[0-9a-f]{64}$/.test(v) && !/^\d+:/.test(v));
  return {
    name: itemName(ev) || "a list item",
    sub: [parents.length ? `on ${parents.join(", ")}` : "", tagOf(ev, "description") || tagOf(ev, "comments")].filter(Boolean).join(" · "),
  };
});
