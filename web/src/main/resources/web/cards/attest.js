// Attestations: somebody vouching, in public, that a particular event or key is what it claims
// to be — and the market around that. Four kinds, one loop:
//
// | kind | | |
// |---|---|---|
// | 31872 | a request | asks an attestor to check the thing this names, sometimes with payment |
// | 31871 | the attestation | the verdict, which is a status and a window it holds for |
// | 11871 | a proficiency | what an attestor is willing to check, by kind |
// | 31873 | a recommendation | who a reader thinks is worth asking, by kind |
//
// The reference client leads with the verdict and colours it; so does this, with the pill the
// git verdicts already use, because "valid" and "revoked" are the same sort of fact as "merged"
// and "closed".

import { esc, clip } from "../shared/format.js";
import { shortNote, shortAddr } from "../shared/nip19.js";
import { kindLabel } from "../shared/kinds.js";
import {
  register, registerRow, shell, bodyHtml, chipRow, personLink, noteHref, addrHref,
  tagOf, tagsOf, oneLine, fmtTs, plural,
} from "./base.js";

const HEX64 = /^[0-9a-f]{64}$/;

/** The vocabulary an attestation's `s` may carry, and which tint each verdict takes. */
const STATUS_TONE = { valid: "open", verifying: "tentative", invalid: "closed", revoked: "closed" };

/** What the attestation is about: the coordinate it names, else the event id. */
function subjectLink(ev) {
  const addr = tagOf(ev, "a");
  if (addr) {
    const href = addrHref(addr);
    return href ? `<a href="${href}">${esc(clip(shortAddr(addr), 60))}</a>` : `<span class="mono">${esc(clip(shortAddr(addr), 60))}</span>`;
  }
  const id = tagsOf(ev, "e").map((t) => t[1]).find((v) => HEX64.test(v || ""));
  if (id) return `<a class="mono" href="${noteHref(id)}">${esc(shortNote(id))}</a>`;
  const who = tagOf(ev, "p");
  return HEX64.test(who || "") ? personLink(who) : null;
}

/** A unix second, or whatever was published there — never "Invalid Date". */
const dateCell = (v) => (v ? esc(clip(fmtTs(v), 60)) : null);

/** 31871 — the verdict: a status, the window it holds for, and what the attestor wrote. */
function attestationCard(ev, opts) {
  const status = oneLine(tagOf(ev, "s")).toLowerCase();
  const subject = subjectLink(ev);
  const inner =
    (status
      ? `<div class="pill-row"><span class="status-pill lead ${STATUS_TONE[status] || ""}">${esc(clip(status, 24))}</span>` +
        `${subject ? `<span>attests ${subject}</span>` : ""}</div>`
      : `<div class="result-body">attests ${subject || "something it does not name"}</div>`) +
    bodyHtml(opts, ev.content, 400);
  return shell(ev, opts, inner, [
    ["valid from", dateCell(tagOf(ev, "valid_from"))],
    ["valid to", dateCell(tagOf(ev, "valid_to"))],
    // The request this answers, so a reader can see what was actually asked.
    ["answers", requestLink(ev)],
  ]);
}

/** The 31872 an attestation answers, as a link when the `request` tag can be encoded. */
function requestLink(ev) {
  const req = tagOf(ev, "request");
  if (!req) return null;
  if (HEX64.test(req)) return `<a class="mono" href="${noteHref(req)}">${esc(shortNote(req))}</a>`;
  const href = addrHref(req);
  return href ? `<a href="${href}">${esc(clip(shortAddr(req), 60))}</a>` : `<span class="mono">${esc(clip(req, 40))}</span>`;
}

/** 31872 — the ask. A cashu token rides along when the requester paid up front. */
function requestCard(ev, opts) {
  const subject = subjectLink(ev);
  const inner =
    `<div class="result-body">asks for an attestation of ${subject || "something it does not name"}</div>` +
    bodyHtml(opts, ev.content, 400);
  // The token itself is a bearer secret: that one was attached is the fact, never its value.
  return shell(ev, opts, inner, [["payment", tagOf(ev, "cashu_token") ? "attached" : null]]);
}

/** The kinds an attestor deals in, as names rather than numbers. */
const kindsOf = (ev) => tagsOf(ev, "k").map((t) => Number(t[1])).filter(Number.isInteger);
const kindChips = (ev) => kindsOf(ev).map((k) => kindLabel(k));

/** 11871 — an attestor saying what it is willing to check. */
function proficiencyCard(ev, opts) {
  const kinds = kindsOf(ev);
  const inner =
    `<div class="result-body">attests ${esc(plural(kinds.length, "kind"))}</div>` +
    chipRow(kindChips(ev), opts) +
    bodyHtml(opts, ev.content, 400);
  return shell(ev, opts, inner);
}

/** 31873 — a reader recommending an attestor for the kinds it handles. */
function recommendationCard(ev, opts) {
  const who = tagOf(ev, "p");
  const kinds = kindsOf(ev);
  const inner =
    `<div class="result-body">recommends ${HEX64.test(who || "") ? personLink(who) : "an attestor"}` +
    `${kinds.length ? ` for ${esc(plural(kinds.length, "kind"))}` : ""}</div>` +
    chipRow(kindChips(ev), opts) +
    bodyHtml(opts, ev.content, 400);
  return shell(ev, opts, inner);
}

register([31871], attestationCard);
register([31872], requestCard);
register([11871], proficiencyCard);
register([31873], recommendationCard);

// The verdict leads every row too; what was attested is a link the popup cannot draw.
registerRow([31871], (ev) => {
  const status = oneLine(tagOf(ev, "s"));
  return { name: status ? `attests: ${status}` : "attests", sub: ev.content };
});
registerRow([31872], (ev) => ({ name: "asks for an attestation", sub: ev.content }));
registerRow([11871], (ev) => ({
  name: `attests ${plural(kindsOf(ev).length, "kind")}`,
  sub: [kindChips(ev).join(", "), ev.content].filter(Boolean).join(" · "),
}));
registerRow([31873], (ev) => ({
  name: `recommends an attestor${kindsOf(ev).length ? ` for ${plural(kindsOf(ev).length, "kind")}` : ""}`,
  sub: [kindChips(ev).join(", "), ev.content].filter(Boolean).join(" · "),
}));
