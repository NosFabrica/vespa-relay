// Moderation and membership: somebody deciding who is in a room, what stays in it, and what a
// role there means. Each card leads with the act, then the reason given, which is the text a
// search found it by.
//
// | kind | | |
// |---|---|---|
// | 43 / 44 | NIP-28 hide a message / mute a user | content is the reason, plain or `{"reason"}` |
// | 9007 | NIP-29 create a group | `h` the group, `name` / `about` its first metadata |
// | 9021 / 9022 | NIP-29 ask to join / leave | `h` the group; content is why |
// | 39003 | NIP-29 the group's roles | `["role", <name>, <description>]` each |
// | 33534 | NIP-43 a relay role | `d` the id, `label`, `description` |
// | 9035 / 9036 | Buzz ask to archive / restore an identity | `p` whose, `reason`, `replaced-by` |
// | 8002 / 8003 | Buzz an identity archived / restored | the relay's record of it, with `consent` |
// | 42000 | Buzz product feedback | `category`; content is the feedback |
// | 46030 / 46031 | Buzz a workflow approval granted / denied | `d` the approval token's hash |
//
// The group or room is the byline's pill wherever an `h` names it, so no card repeats it.

import { esc, clip } from "../shared/format.js";
import { shortNote } from "../shared/nip19.js";
import {
  register, registerRow, registerNamedPeople, shell, titleHtml, bodyHtml, personLink, noteHref,
  tagsOf, tagOf, oneLine, plural, peopleOf, firstPerson,
} from "./base.js";

const HEX64 = /^[0-9a-f]{64}$/;

/**
 * A NIP-28 moderation reason, read as quartz reads it: plain text as written, or the `reason`
 * of a JSON object; JSON with no reason says nothing rather than printing braces.
 */
export function moderationReason(content) {
  const s = String(content || "").trim();
  if (!s) return "";
  if (!s.startsWith("{") || !s.endsWith("}")) return s;
  try {
    const o = JSON.parse(s);
    return o && typeof o === "object" ? oneLine(o.reason) : "";
  } catch (e) { return s; }
}

const peopleLine = (pks, max) =>
  pks.slice(0, max).map(personLink).join(", ") + (pks.length > max ? ` <span class="muted-note">and ${pks.length - max} more</span>` : "");

/** The messages a 43 hides: its `e` tags other than the channel root it is scoped by. */
const hiddenOf = (ev) => {
  const root = tagsOf(ev, "e").find((t) => t[3] === "root");
  return [...new Set(tagsOf(ev, "e").filter((t) => t !== root).map((t) => t[1]).filter((v) => HEX64.test(v || "")))];
};

/** 43 — a channel moderator hiding messages. */
function hideMessageCard(ev, opts) {
  const ids = hiddenOf(ev);
  const shown = ids.slice(0, opts && opts.full ? 24 : 3);
  const inner =
    `<div class="result-body">hides ${shown.length
      ? shown.map((id) => `<a class="mono" href="${noteHref(id)}">${esc(shortNote(id))}</a>`).join(", ") +
        (ids.length > shown.length ? ` <span class="muted-note">and ${ids.length - shown.length} more</span>` : "")
      : "a message"}</div>` +
    bodyHtml(opts, moderationReason(ev.content), 300, true);
  return shell(ev, opts, inner);
}

/** 44 — a channel moderator muting people. */
function muteUserCard(ev, opts) {
  const pks = peopleOf(ev);
  const inner =
    `<div class="result-body">mutes ${pks.length ? peopleLine(pks, mutedShown(opts)) : "someone"}</div>` +
    bodyHtml(opts, moderationReason(ev.content), 300, true);
  return shell(ev, opts, inner);
}
const mutedShown = (opts) => (opts && opts.full ? 24 : 3);

/** 9007 — a NIP-29 group coming into being, with its first name and description. */
function createGroupCard(ev, opts) {
  const name = tagOf(ev, "name");
  const inner =
    `<div class="result-body">creates ${name ? `the group <b>${esc(clip(name, 80))}</b>` : "a group"}</div>` +
    bodyHtml(opts, tagOf(ev, "about"), 300, true);
  return shell(ev, opts, inner);
}

/** 9021 / 9022 — asking a NIP-29 relay to be let in, or let out. The invite code stays off the page. */
function membershipRequestCard(ev, opts) {
  const joining = ev.kind === 9021;
  const inner =
    `<div class="result-body">${joining ? "asks to join" : "asks to leave"} the group` +
    `${joining && tagsOf(ev, "code").length ? ` <span class="muted-note">with an invite code</span>` : ""}</div>` +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner);
}

/** A 39003's roles, `[name, description]`, each name once. */
const rolesOf = (ev) => {
  const seen = new Set(), out = [];
  for (const t of tagsOf(ev, "role")) {
    const name = oneLine(t[1]);
    if (name && !seen.has(name)) { seen.add(name); out.push([name, oneLine(t[2])]); }
  }
  return out;
};

/** 39003 — the roles a NIP-29 group defines, published by its relay. */
function groupRolesCard(ev, opts) {
  const roles = rolesOf(ev);
  const shown = opts && opts.full ? roles : roles.slice(0, 6);
  const inner =
    `<div class="result-body">${esc(plural(roles.length, "role"))} in this group</div>` +
    (shown.length
      ? `<dl class="props">${shown.map(([n, d]) => `<dt>${esc(clip(n, 40))}</dt><dd>${d ? esc(clip(d, 160)) : "—"}</dd>`).join("")}</dl>`
      : "") +
    (roles.length > shown.length ? `<div class="muted-note">…and ${roles.length - shown.length} more</div>` : "");
  return shell(ev, opts, inner, [["group", tagOf(ev, "d") ? `<span class="mono">${esc(clip(tagOf(ev, "d"), 40))}</span>` : null]]);
}

/** 33534 — a NIP-43 relay role: a label members can hold, and what it means. Its hue is not drawn. */
function relayRoleCard(ev, opts) {
  // An absent `order` is no order: Number(null) would say 0.
  const order = /^-?\d+$/.test(oneLine(tagOf(ev, "order"))) ? Number(oneLine(tagOf(ev, "order"))) : null;
  const inner =
    titleHtml(opts, tagOf(ev, "label") || tagOf(ev, "d"), 120) +
    `<div class="result-body muted">a role on this relay</div>` +
    bodyHtml(opts, tagOf(ev, "description"), 300);
  return shell(ev, opts, inner, [
    ["id", tagOf(ev, "d") ? `<span class="mono">${esc(clip(tagOf(ev, "d"), 40))}</span>` : null],
    ["order", order === null ? null : esc(order)],
  ]);
}

// ---- Buzz identity archival -------------------------------------------------

/** What each archival kind says it does to the identity its `p` names. */
const ARCHIVAL = {
  9035: "asks to archive",
  9036: "asks to restore",
  8002: "archived",
  8003: "restored",
};

/** The identity acted on: the first `p`. */
const archivalTarget = firstPerson;
/** The identity that takes its place, from `replaced-by`. */
const replacedBy = (ev) => (HEX64.test(tagOf(ev, "replaced-by") || "") ? tagOf(ev, "replaced-by") : null);

/** The consent a record cites: `["consent", self|owner|admin, <actor>]`; only the path is said. */
const consentOf = (ev) => {
  const t = tagsOf(ev, "consent").find((x) => oneLine(x[1]));
  return t ? oneLine(t[1]) : "";
};

/** 9035 / 9036 / 8002 / 8003 — an identity archived or restored, asked for or done. */
function archivalCard(ev, opts) {
  const target = archivalTarget(ev);
  const next = replacedBy(ev);
  const request = (ev.kind === 8002 || ev.kind === 8003)
    ? tagsOf(ev, "e").map((t) => t[1]).find((id) => HEX64.test(id || "")) : null;
  const inner =
    `<div class="result-body">${esc(ARCHIVAL[ev.kind])} ${target ? personLink(target) : "an identity"}` +
    `${next ? `, replaced by ${personLink(next)}` : ""}</div>` +
    bodyHtml(opts, tagOf(ev, "reason"), 200, true) +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner, [
    ["consent", consentOf(ev) ? esc(clip(consentOf(ev), 20)) : null],
    ["request", request ? `<a class="mono" href="${noteHref(request)}">${esc(shortNote(request))}</a>` : null],
  ]);
}

/** 42000 — feedback about the product, filed from inside a Buzz workspace. */
function feedbackCard(ev, opts) {
  const category = oneLine(tagOf(ev, "category"));
  const inner =
    (category ? `<div class="pill-row"><span class="status-pill">${esc(clip(category, 32))}</span></div>` : "") +
    bodyHtml(opts, ev.content, 600);
  return shell(ev, opts, inner);
}

/** 46030 / 46031 — an approver's answer to a workflow step that waits on a person. */
function approvalCard(ev, opts) {
  const token = oneLine(tagOf(ev, "d"));
  const inner =
    `<div class="result-body">${ev.kind === 46030 ? "approves" : "denies"} a workflow step</div>` +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner, [["approval", token ? `<span class="mono">${esc(clip(token, 20))}</span>` : null]]);
}

register([43], hideMessageCard);
register([44], muteUserCard);
register([9007], createGroupCard);
register([9021, 9022], membershipRequestCard);
register([39003], groupRolesCard);
register([33534], relayRoleCard);
register(Object.keys(ARCHIVAL).map(Number), archivalCard);
register([42000], feedbackCard);
register([46030, 46031], approvalCard);
// Everyone these cards write by name: the people muted, and the identities archived or named in their place.
registerNamedPeople([44], (ev, opts) => peopleOf(ev).slice(0, mutedShown(opts)));
registerNamedPeople(Object.keys(ARCHIVAL).map(Number), (ev) => [archivalTarget(ev), replacedBy(ev)].filter(Boolean));

registerRow([43], (ev) => ({ name: `hides ${plural(Math.max(1, hiddenOf(ev).length), "message")}`, sub: moderationReason(ev.content) }));
registerRow([44], (ev) => ({ name: `mutes ${plural(Math.max(1, peopleOf(ev).length), "person", "people")}`, sub: moderationReason(ev.content) }));
registerRow([9007], (ev) => ({ name: tagOf(ev, "name") ? `creates ${tagOf(ev, "name")}` : "creates a group", sub: tagOf(ev, "about") }));
registerRow([9021], (ev) => ({ name: "asks to join a group", sub: ev.content }));
registerRow([9022], (ev) => ({ name: "asks to leave a group", sub: ev.content }));
registerRow([39003], (ev) => {
  const roles = rolesOf(ev);
  return { name: `${plural(roles.length, "role")} in a group`, sub: roles.map(([n]) => n).join(", ") };
});
registerRow([33534], (ev) => ({ name: tagOf(ev, "label") || tagOf(ev, "d"), sub: tagOf(ev, "description") }));
registerRow(Object.keys(ARCHIVAL).map(Number), (ev) => ({
  name: `${ARCHIVAL[ev.kind]} an identity`,
  sub: tagOf(ev, "reason") || ev.content,
}));
registerRow([42000], (ev) => ({ name: ev.content || "product feedback", sub: tagOf(ev, "category") }));
registerRow([46030, 46031], (ev) => ({ name: `${ev.kind === 46030 ? "approves" : "denies"} a workflow step`, sub: ev.content }));
