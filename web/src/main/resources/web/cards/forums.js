// Messages and forum threads — the kinds whose content IS the card, scoped to a room rather
// than to a feed. Four vocabularies, one shape:
//
// | kind | | |
// |---|---|---|
// | 14    | a NIP-17 direct message | plaintext here, so it is the rumor inside a seal |
// | 24    | a NIP-A4 public message | addressed to its `p`s, in the open |
// | 3302  | a Concord chat edit | the replacement text for the message its `e` names |
// | 40002 | a Buzz stream message | a line in a channel, maybe a broadcast |
// | 45001 | a Buzz forum post | the root of a thread |
// | 45003 | a Buzz forum comment | a reply inside one |
// | 40100 | a Buzz canvas | the room's shared markdown document |
// | 48106 | Buzz huddle guidelines | the rules the room's agents are steered by |
// | 40003 | a Buzz message edit | the replacement text, like 3302 |
// | 40006 | a Buzz scheduled message | a line queued to post later |
// | 40007 | a Buzz reminder | a nudge to the people it names, about a message |
// | 40099 | a Buzz system message | the room saying what happened in it, as JSON |
// | 40901 | a Buzz channel summary | the room describing itself, as JSON |
// | 45010 | a Buzz artifact | a versioned document whose home is a room |
//
// The room itself needs nothing here: every one of these scopes with NIP-29's `h`, which the
// byline already draws as the group pill.

import { esc, clip, titleOf } from "../shared/format.js";
import { shortNote, shortNpub } from "../shared/nip19.js";
import {
  register, registerRow, registerNamedPeople, shell, titleHtml, bodyHtml, replyLine, faceStrip,
  noteHref, personLink, jsonContent, oneLine, fmtTs, tagOf, tagsOf, plural,
} from "./base.js";
import { codeBlock } from "./code.js";

const HEX64 = /^[0-9a-f]{64}$/;

/** The people a message names, as faces: a DM's recipients, a post's mentions. */
const mentionsOf = (ev) => tagsOf(ev, "p").map((t) => t[1]).filter((pk) => HEX64.test(pk || ""));

/**
 * What every message in this family draws: its subject where it has one, its text, and the
 * faces it names. The room is the byline's pill, and the author is the byline.
 */
function messageBody(ev, opts) {
  const full = opts && opts.full;
  return (
    titleHtml(opts, titleOf(ev), 140) +
    // A Buzz stream message may be a broadcast: the one thing about it a reader cannot infer.
    // The flag is the literal "1" the sdk writes — a `["broadcast", "0"]` is the opposite claim.
    (tagOf(ev, "broadcast") === "1" ? `<div class="pill-row"><span class="status-pill lead">broadcast</span></div>` : "") +
    bodyHtml(opts, ev.content, 400) +
    faceStrip(mentionsOf(ev), full ? 24 : 12)
  );
}

/** 24 / 40002 / 45001 — a message that answers nothing: it opens the thread or the room. */
const messageCard = (ev, opts) => shell(ev, opts, messageBody(ev, opts));

/** 14 / 45003 — the same message, one rung into a conversation. */
const replyingMessageCard = (ev, opts) => shell(ev, opts, replyLine(ev) + messageBody(ev, opts));

/**
 * 3302 — a chat edit: a dedicated kind rather than a deletion and a repost, so the original
 * keeps its id and everything attached to it. The card is the replacement text, over what it
 * replaces.
 */
function chatEditCard(ev, opts) {
  const target = tagsOf(ev, "e").map((t) => t[1]).find((v) => HEX64.test(v || ""));
  const inner =
    `<div class="result-body">edits ${target ? `<a class="mono" href="${noteHref(target)}">${esc(shortNote(target))}</a>` : "a message"}</div>` +
    bodyHtml(opts, ev.content, 400);
  return shell(ev, opts, inner);
}

/** What a room document is, per kind; both are markdown in `content` scoped by an `h`. */
const DOCUMENT_ROLE = {
  40100: "the shared document of this room",
  48106: "the rules this room's agents are steered by",
};

/** 40100 / 48106 — a document, not a line of chat, so it reads as one. */
function roomDocumentCard(ev, opts) {
  const inner =
    `<div class="result-body muted">${esc(DOCUMENT_ROLE[ev.kind] || "a room document")}</div>` +
    bodyHtml(opts, ev.content, 600);
  return shell(ev, opts, inner);
}

/** 40006 — a message written now to post later; the room is the byline's pill. */
function scheduledMessageCard(ev, opts) {
  const inner =
    `<div class="pill-row"><span class="status-pill">scheduled</span></div>` +
    bodyHtml(opts, ev.content, 400) +
    faceStrip(mentionsOf(ev), opts && opts.full ? 24 : 12);
  return shell(ev, opts, inner);
}

/** 40007 — a reminder: who it nudges (faces), about which message, saying what. */
function reminderCard(ev, opts) {
  const target = tagsOf(ev, "e").map((t) => t[1]).find((v) => HEX64.test(v || ""));
  const who = mentionsOf(ev);
  const inner =
    `<div class="result-body">reminds ${esc(who.length ? plural(who.length, "person", "people") : "the room")}` +
    `${target ? ` about <a class="mono" href="${noteHref(target)}">${esc(shortNote(target))}</a>` : ""}</div>` +
    bodyHtml(opts, ev.content, 400) +
    faceStrip(who, opts && opts.full ? 24 : 12);
  return shell(ev, opts, inner);
}

// ---- 40099, the room narrating itself --------------------------------------

/** A 40099's JSON payload; every field is a stranger's, so only text survives. */
function systemPayload(ev) {
  const c = jsonContent(ev);
  const pk = (v) => (HEX64.test(oneLine(v)) ? oneLine(v) : null);
  return {
    type: oneLine(c.type),
    actor: pk(c.actor),
    target: pk(c.target),
    topic: oneLine(c.topic),
    purpose: oneLine(c.purpose),
    visibility: oneLine(c.visibility),
    ttl: Number(c.ttl_seconds),
    deleted: HEX64.test(oneLine(c.target_event_id)) ? oneLine(c.target_event_id) : null,
    reason: oneLine(c.public_reason) || oneLine(c.reason_code),
    participants: Array.isArray(c.participants) ? c.participants.map(oneLine).filter((v) => HEX64.test(v)) : [],
  };
}

/** The people a system message writes a name for: whoever acted, and whoever it was done to. */
const systemPeople = (ev) => {
  const p = systemPayload(ev);
  return [...new Set([p.actor, p.target].filter(Boolean))];
};

/** "2 days", "an hour": a channel's message lifetime in the unit a person would say it in. */
const lifetime = (secs) => {
  if (!Number.isFinite(secs) || secs <= 0) return "";
  for (const [unit, n] of [["day", 86400], ["hour", 3600], ["minute", 60]]) {
    if (secs >= n) return plural(Math.round(secs / n), unit);
  }
  return plural(Math.round(secs), "second");
};

/**
 * One system message as the sentence a room would show, with the person links in HTML and the
 * same sentence as plain text for the type-ahead row. An unknown `type` is said as its code.
 */
function systemSentence(p, html) {
  const who = (pk, fallback) => (pk ? (html ? personLink(pk) : shortNpub(pk)) : fallback);
  const val = (v) => (html ? `<b>${esc(clip(v, 120))}</b>` : clip(v, 120));
  const t = (s) => (html ? esc(s) : s);
  const by = p.actor ? `${who(p.actor, "")} ` : "";
  switch (p.type) {
    case "member_joined": return `${who(p.target || p.actor, t("someone"))} ${t("joined")}`;
    case "member_left": return `${who(p.target || p.actor, t("someone"))} ${t("left")}`;
    case "member_removed":
    case "admin_kick": return `${by}${t("removed")} ${who(p.target, t("a member"))}`;
    case "topic_changed": return p.topic ? `${by}${t("set the topic to")} ${val(p.topic)}` : `${by}${t("cleared the topic")}`;
    case "purpose_changed": return p.purpose ? `${by}${t("set the purpose to")} ${val(p.purpose)}` : `${by}${t("cleared the purpose")}`;
    case "visibility_changed": return `${by}${t("made the channel")} ${val(p.visibility || "private")}`;
    case "ttl_changed": return lifetime(p.ttl) ? `${by}${t("set messages to expire after")} ${val(lifetime(p.ttl))}` : `${by}${t("turned message expiry off")}`;
    case "channel_created": return `${by}${t("created the channel")}`;
    case "channel_deleted": return `${by}${t("deleted the channel")}`;
    case "channel_archived": return `${by}${t("archived the channel")}`;
    case "channel_unarchived": return `${by}${t("unarchived the channel")}`;
    case "channel_auto_archived": return t("the channel was archived for inactivity");
    case "message_deleted": return `${by}${t("deleted a message")}`;
    case "dm_created": return `${by}${t(`opened a conversation with ${plural(Math.max(0, p.participants.length - 1), "person", "people")}`)}`;
    default: return p.type ? `${by}${t(p.type.replace(/_/g, " "))}` : t("a system message");
  }
}

/** 40099 — what happened in the room, said as the sentence the room shows. */
function systemMessageCard(ev, opts) {
  const p = systemPayload(ev);
  const inner =
    `<div class="result-body">${systemSentence(p, true)}</div>` +
    (p.deleted ? `<div class="result-body muted">the message was <a class="mono" href="${noteHref(p.deleted)}">${esc(shortNote(p.deleted))}</a></div>` : "") +
    (p.reason ? `<div class="result-body muted">${esc(clip(p.reason, 300))}</div>` : "") +
    faceStrip(p.participants, opts && opts.full ? 24 : 12);
  return shell(ev, opts, inner);
}

/** 40901 — a channel's own summary: name, about, topic and purpose, and how busy it is. */
function channelSummaryCard(ev, opts) {
  const c = jsonContent(ev);
  const count = (v) => (Number.isFinite(Number(v)) && Number(v) >= 0 && oneLine(v) ? Number(v) : null);
  const members = count(c.member_count);
  const messages = count(c.message_count);
  const visibility = oneLine(c.visibility);
  const pills = [visibility, c.archived === true ? "archived" : ""].filter(Boolean)
    .map((v) => `<span class="status-pill">${esc(clip(v, 20))}</span>`).join("");
  const inner =
    titleHtml(opts, oneLine(c.name), 120) +
    (pills ? `<div class="pill-row">${pills}</div>` : "") +
    bodyHtml(opts, oneLine(c.about), 300) +
    ((members !== null || messages !== null)
      ? `<div class="result-body muted">${esc([members !== null && plural(members, "member"), messages !== null && plural(messages, "message")].filter(Boolean).join(" · "))}</div>`
      : "");
  return shell(ev, opts, inner, [
    ["topic", oneLine(c.topic) ? esc(clip(oneLine(c.topic), 120)) : null],
    ["purpose", oneLine(c.purpose) ? esc(clip(oneLine(c.purpose), 200)) : null],
    ["last active", count(c.last_activity_at) ? esc(fmtTs(c.last_activity_at)) : null],
  ]);
}

/** Whether a body is a JSON document: an artifact may be one, and then it is data, not prose. */
const isJsonDocument = (s) => {
  const t = String(s || "").trim();
  if (!/^[[{]/.test(t)) return false;
  try { JSON.parse(t); return true; } catch (e) { return false; }
};

/**
 * 45010 — a Buzz artifact revision: a document (`type`, `title`) whose identity is its `d`, with
 * an `op` saying what this revision does to it. Prose content is the body; a JSON one is drawn
 * as code, at the permalink only.
 */
function artifactCard(ev, opts) {
  const op = oneLine(tagOf(ev, "op"));
  const type = oneLine(tagOf(ev, "type"));
  const json = isJsonDocument(ev.content);
  const pills = [op, type].filter(Boolean).map((v) => `<span class="status-pill">${esc(clip(v, 24))}</span>`).join("");
  const inner =
    titleHtml(opts, tagOf(ev, "title"), 140) +
    (pills ? `<div class="pill-row">${pills}</div>` : "") +
    (op === "delete" ? `<div class="result-body muted">removes this artifact</div>` : "") +
    (json
      ? (opts && opts.full ? codeBlock(opts, String(ev.content).split(/\r?\n/), { lang: "json" }) : `<div class="result-body muted">a JSON document</div>`)
      : bodyHtml(opts, ev.content, 600));
  const prev = tagOf(ev, "prev");
  return shell(ev, opts, inner, [
    ["revises", HEX64.test(prev || "") ? `<a class="mono" href="${noteHref(prev)}">${esc(shortNote(prev))}</a>` : null],
    ["version", tagOf(ev, "ar") ? esc(clip(tagOf(ev, "ar"), 12)) : null],
  ]);
}

register([24, 40002, 45001], messageCard);
register([14, 45003], replyingMessageCard);
register([3302, 40003], chatEditCard);
register([40006], scheduledMessageCard);
register([40007], reminderCard);
register([40099], systemMessageCard);
register([40901], channelSummaryCard);
register([45010], artifactCard);
registerNamedPeople([40099], systemPeople);
register([40100, 48106], roomDocumentCard);

// A message's line IS its text; a subject, where one exists, leads and the text follows.
const messageRow = (ev) => {
  const subject = titleOf(ev);
  return { name: subject || ev.content, sub: subject ? ev.content : "" };
};
registerRow([14, 24, 40002, 45001, 45003], (ev) => {
  const row = messageRow(ev);
  const named = mentionsOf(ev).length;
  return { ...row, sub: row.sub || (named ? `names ${plural(named, "person", "people")}` : "") };
});
registerRow([3302, 40003], (ev) => ({ name: "edits a message", sub: ev.content }));
registerRow([40006], (ev) => ({ name: ev.content || "a scheduled message", sub: ev.content ? "scheduled" : "" }));
registerRow([40007], (ev) => ({ name: ev.content || "a reminder", sub: `reminds ${plural(mentionsOf(ev).length, "person", "people")}` }));
registerRow([40099], (ev) => ({ name: systemSentence(systemPayload(ev), false) }));
registerRow([40901], (ev) => {
  const c = jsonContent(ev);
  return { name: oneLine(c.name), sub: oneLine(c.about) || oneLine(c.topic) || oneLine(c.purpose) };
});
registerRow([45010], (ev) => ({
  name: tagOf(ev, "title") || [tagOf(ev, "op"), tagOf(ev, "type")].filter(Boolean).join(" ") || "an artifact",
  sub: isJsonDocument(ev.content) ? [tagOf(ev, "type"), "a JSON document"].filter(Boolean).join(" · ") : ev.content,
}));
registerRow([40100], (ev) => ({ name: "room canvas", sub: ev.content }));
registerRow([48106], (ev) => ({ name: "room guidelines", sub: ev.content }));
