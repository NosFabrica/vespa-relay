// Chess, NIP-64: a finished game as PGN, and the live-chess events two players exchange.
//
// | kind | | |
// |---|---|---|
// | 64    | a game | `content` is a PGN: `[Tag "value"]` headers, then the moves |
// | 30066 | a live move | `game_id`, `move_number`, `san`, `fen`; the content is a comment |
// | 30068 | a draw offer | `d` is the game; the content is an optional message |
//
// A game is found by its players, event, site and opening — the PGN headers — and the card
// leads with exactly those. The move text is drawn as text, never replayed on a board.

import { esc, clip } from "../shared/format.js";
import {
  register, registerRow, registerNamedPeople, shell, titleHtml, bodyHtml, personLink, tagOf, tagsOf,
  oneLine,
} from "./base.js";
import { codeBlock } from "./code.js";

const HEX64 = /^[0-9a-f]{64}$/;

/** The PGN's tag pairs, `[Name "value"]`, first of each name. PGN escapes `"` and `\` with `\`. */
export function pgnHeaders(pgn) {
  const out = Object.create(null);
  for (const m of String(pgn || "").matchAll(/^\s*\[([A-Za-z0-9_]+)\s+"((?:[^"\\]|\\.)*)"\s*\]\s*$/gm)) {
    if (!(m[1] in out)) out[m[1]] = m[2].replace(/\\(["\\])/g, "$1");
  }
  return out;
}

/** The movetext: everything after the header block, as written. */
const movetextOf = (pgn) => String(pgn || "").replace(/^\s*\[[^\]\n]*\]\s*$/gm, "").trim();

/** A header value worth showing: PGN writes "?" (and "????.??.??") for unknown. */
const known = (v) => {
  const s = oneLine(v);
  return s && !/^[?.\s-]*$/.test(s) ? s : "";
};

/** "White vs Black", from the headers, or "". */
const playersOf = (h) => {
  const w = known(h.White), b = known(h.Black);
  return w || b ? `${w || "?"} vs ${b || "?"}` : "";
};

/** 64 — a game: who played, where, how it ended, and the moves. */
function gameCard(ev, opts) {
  const h = pgnHeaders(ev.content);
  const moves = movetextOf(ev.content);
  const opening = [known(h.Opening), known(h.Variation)].filter(Boolean).join(", ") || known(h.ECO);
  const inner =
    titleHtml(opts, playersOf(h) || known(h.Event), 140) +
    (known(h.Result) ? `<div class="pill-row"><span class="status-pill">${esc(clip(h.Result, 12))}</span></div>` : "") +
    (moves ? codeBlock(opts, moves.split(/\r?\n/), { lang: "pgn" }) : "");
  return shell(ev, opts, inner, [
    ["event", playersOf(h) && known(h.Event) ? esc(clip(known(h.Event), 80)) : null],
    ["site", known(h.Site) ? esc(clip(known(h.Site), 80)) : null],
    ["date", known(h.Date) ? esc(clip(known(h.Date), 20)) : null],
    ["opening", opening ? esc(clip(opening, 80)) : null],
    ["time control", known(h.TimeControl) ? esc(clip(known(h.TimeControl), 20)) : null],
  ]);
}

/** The opponent a live-chess event names: its one `p`. */
const opponentOf = (ev) => tagsOf(ev, "p").map((t) => t[1]).find((pk) => HEX64.test(pk || "")) || null;

/** "move 12: Nf3", or what of it the tags carry. */
const moveLine = (ev) => {
  const n = Number(tagOf(ev, "move_number"));
  const san = oneLine(tagOf(ev, "san"));
  return [Number.isInteger(n) && n > 0 ? `move ${n}` : "", san].filter(Boolean).join(": ");
};

/** 30066 — one move in a live game, against the opponent it names. */
function moveCard(ev, opts) {
  const vs = opponentOf(ev);
  const inner =
    `<div class="result-body">plays <b>${esc(clip(moveLine(ev) || "a move", 40))}</b>${vs ? ` against ${personLink(vs)}` : ""}</div>` +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner, [
    ["game", tagOf(ev, "game_id") ? `<span class="mono">${esc(clip(tagOf(ev, "game_id"), 40))}</span>` : null],
    ["position", opts && opts.full && tagOf(ev, "fen") ? `<span class="mono">${esc(clip(tagOf(ev, "fen"), 100))}</span>` : null],
  ]);
}

/** 30068 — a draw offer: the game is its `d`, and the opponent it is offered to. */
function drawOfferCard(ev, opts) {
  const vs = opponentOf(ev);
  const inner =
    `<div class="result-body">offers a draw${vs ? ` to ${personLink(vs)}` : ""}</div>` +
    bodyHtml(opts, ev.content, 300);
  return shell(ev, opts, inner, [
    ["game", tagOf(ev, "d") ? `<span class="mono">${esc(clip(tagOf(ev, "d"), 40))}</span>` : null],
  ]);
}

register([64], gameCard);
register([30066], moveCard);
register([30068], drawOfferCard);
// The opponent is written by name, so their profile is loaded with the page.
registerNamedPeople([30066, 30068], (ev) => [opponentOf(ev)].filter(Boolean));

registerRow([64], (ev) => {
  const h = pgnHeaders(ev.content);
  return {
    name: playersOf(h) || known(h.Event) || "a chess game",
    sub: [known(h.Result), playersOf(h) ? known(h.Event) : "", known(h.Date)].filter(Boolean).join(" · "),
  };
});
registerRow([30066], (ev) => ({ name: `plays ${moveLine(ev) || "a move"}`, sub: ev.content }));
registerRow([30068], (ev) => ({ name: "offers a draw", sub: ev.content }));
