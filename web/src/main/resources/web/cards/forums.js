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
//
// The room itself needs nothing here: every one of these scopes with NIP-29's `h`, which the
// byline already draws as the group pill.

import { esc, titleOf } from "../shared/format.js";
import { shortNote } from "../shared/nip19.js";
import {
  register, registerRow, shell, titleHtml, bodyHtml, replyLine, faceStrip, noteHref,
  tagOf, tagsOf, plural,
} from "./base.js";

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

register([24, 40002, 45001], messageCard);
register([14, 45003], replyingMessageCard);
register([3302], chatEditCard);
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
registerRow([3302], (ev) => ({ name: "edits a message", sub: ev.content }));
registerRow([40100], (ev) => ({ name: "room canvas", sub: ev.content }));
registerRow([48106], (ev) => ({ name: "room guidelines", sub: ev.content }));
