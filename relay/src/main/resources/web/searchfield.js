// The search box, as a field that RENDERS what it holds.
//
// Four things live here, and they are one feature:
//
//   1. `from:npub1…` / `to:npub1…` draw as a face and a name instead of 63
//      characters of bech32. The field's VALUE is still the plain text — the
//      URL, the export and the query builder all read `from:npub1…` — so the
//      rendering is a view of the string and never a second source of truth.
//   2. Typing `from:` or `to:` opens a people picker over the network, ranked
//      by your own web of trust because the lookup rides the AUTHENTICATED
//      socket. Picking somebody writes their npub into the text.
//   3. Typing `since:` or `until:` opens a CALENDAR in the same box, and
//      picking a day writes `since:2026-08-06` into the text. Nothing about it
//      goes near the network — a month grid is arithmetic — so unlike the
//      people picker it answers instantly and has nothing to debounce. WHICH
//      days exist is shared/calendar.js's, tested there; what is here is only
//      how they are drawn and what a click on one does.
//      `since:2026-08-06` then draws as a pill reading `since 6 Aug 2026`:
//      the token is the ISO day, because `06/08/2026` means two different days
//      to two readers, and the pill is the reader's own spelling of it.
//   4. `#hashtag` draws as a pill. NOT for the reason a person does — a
//      hashtag is already the word it means, and nothing is being hidden. It
//      is the field admitting what it did: that word LEFT the NIP-50 search
//      string and became a tag filter, which the box had no way of saying. So
//      it is quieter than a person (an outline, no face, no lookup, no
//      repaint) and it appears only once the caret leaves the tag — see
//      query.js's drawable(), and the comment there on why a pill that
//      appeared at `#n` would re-render the field on every keystroke. A date
//      pill settles on the same rule, and needs it more: one more digit after
//      `since:2026-08-06` takes the whole token back to text.
//
// Why a contenteditable rather than the <input> this used to be: an input's
// value is a string of characters and nothing else — there is no way to put a
// picture inside one. The alternative shape (a transparent input over a
// mirrored overlay) only holds while every rendered token is exactly as wide
// as the text it hides, which a name and a face are not. So the field is a
// div, and everything an input gave for free is given back here deliberately:
// `value` (get and set), `select()`, a placeholder, single-line behaviour,
// paste/drop that insert TEXT rather than whatever html was on the clipboard,
// and letting go of the caret when the reader touches the page — which a
// finger does not get from a contenteditable the way a mouse does. app.js
// keeps writing `$q.value` and never learns the difference.
//
// The pickers and the results popup are mutually exclusive on purpose. All
// three occupy the same square of screen, and a keystroke inside `from:ali` is
// not a search for "from:ali" — so while a token is being built this module
// owns the arrows and Enter, and app.js is told to stand down. The two pickers
// are exclusive with each OTHER for free: one caret is inside one token, and
// `from|to` and `since|until` are different prefixes.

import { npub, shortNpub } from "./shared/nip19.js";
import { esc, clip } from "./shared/format.js";
import { profiles, displayName, enrichProfiles } from "./shared/profiles.js";
import { avatarHtml } from "./shared/avatar.js";
import { tokenize, mentionAt, dateAt, drawable, ymd } from "./shared/query.js";
import {
  DOW, dayLabel, midnight, monthGrid, quickPicks, sameMonth, shiftDays, shiftMonths, typedMonth,
} from "./shared/calendar.js";

const PICKER_LIMIT = 8;
const DEBOUNCE_MS = 150;

// The faces here are the CARD's face — avatarHtml, score chip and all. This
// module used to draw its own <img>, which is how the one place that ASKS "who
// do you mean by this person" ended up being the one place that did not say
// what the active lens thinks of them. A picker offering eight strangers is
// exactly where the number earns its keep.
/**
 * The picture a person draws with right now, from whatever profiles knows.
 *
 * The SIZE is the caller's — a chip inside the field is `xs`, a picker row is
 * `lg` — which is why it is an argument rather than a constant.
 *
 * KEEP IT HERE, above mountSearchField and below the constants. It used to sit
 * one line under the calendar's `QUICK` table, and when that table and the rest
 * of the date arithmetic were lifted into shared/calendar.js the deletion
 * overshot by exactly one declaration and took this with it — one `const` line
 * inside a 276-line diff that was otherwise all move. Both call sites went on
 * calling it, and the two symptoms were the two you would not guess from
 * "undefined function":
 *
 *   - rowHtml() threw inside the debounced lookup, whose catch exists to keep
 *     a failed relay read from claiming nobody matches — so the picker sent
 *     its REQ, got its twelve answers, swallowed the error and drew nothing.
 *   - the NEXT keystroke then threw out of updateMention() before it could
 *     reach `++reqId`, so no further lookup was ever sent: the box stopped
 *     asking the server halfway through a name.
 */
const faceHtml = (pubkey, size) => avatarHtml((profiles.get(pubkey) || {}).picture, pubkey, size);

/**
 * Does this reader RAISE a keyboard to type, rather than already having one?
 *
 * The question two rules below both turn on, and app.js's autofocus with them,
 * so it is asked once here. A caret and a keyboard are one thing on a phone
 * and two on a desktop: a soft keyboard is raised only for a focus a finger
 * caused — `focus()` from script cannot do it, on any mobile browser, by
 * design — so anywhere the caret can arrive without one, a caret placed by
 * script is a field that looks ready and cannot be typed into.
 *
 * `pointer: coarse` is the PRIMARY pointer, which is the right cut rather than
 * "has a touchscreen": a laptop with a touch screen still types on the
 * keyboard it has, and would lose its autofocus for nothing.
 *
 * Asked at the moment it matters, not cached at load — a tablet gains and
 * loses a keyboard by being put in a case.
 */
export const softKeyboard = () => window.matchMedia("(pointer: coarse)").matches;

// A line break, which this field has nowhere to put. Module constants because
// the first of them is tested against every keystroke's beforeinput, and a
// regex literal inside the handler builds a new RegExp on each of them.
const NEWLINE = /[\r\n]/;
const NEWLINES = /[\r\n]+/g;

export function mountSearchField(el, list, { lookup, onEdit, onSubmit, paintScores }) {
  let mention = null;   // the from:/to: token being built, from mentionAt()
  let hits = [];        // pubkeys currently offered
  let active = -1;      // which one is highlighted
  let timer = null;
  let reqId = 0;
  let day = null;       // the since:/until: token being built, from dateAt()
  let month = null;     // the month the grid is showing (a Date on its 1st)
  let cursor = null;    // the day the KEYBOARD is on, or null while the mouse leads

  /**
   * Fill the chips on the faces this module just drew.
   *
   * Fire and forget, like every other caller: the score is a second round trip
   * and a face must not wait on it. A list that re-renders before the fill
   * lands leaves it painting detached nodes, which is harmless — the scores it
   * fetched are cached, so the render that replaced them fills from memory.
   */
  const fillScores = () => { if (paintScores) Promise.resolve(paintScores()).catch(() => {}); };

  // ---- the value, and where the caret is inside it ------------------------
  //
  // Every offset this module passes around is an index into the VALUE string,
  // never into the DOM. A chip is worth its whole token, so splicing text at
  // an offset and re-rendering puts the caret back exactly where it was —
  // which is the only reason a chip can appear mid-typing without the caret
  // jumping to the end.

  // A chip is anything DRAWN over a token — a person or a hashtag. Keyed on the
  // token it carries rather than its class, because the two draw differently
  // and everything below cares only that the node stands for `dataset.token`.
  const isChip = (n) => n && n.nodeType === 1 && n.dataset && n.dataset.token != null;
  const lenOf = (n) => (n.nodeType === 3 ? n.data.length : isChip(n) ? n.dataset.token.length : (n.textContent || "").length);

  function readValue() {
    let out = "";
    for (const n of el.childNodes) out += n.nodeType === 3 ? n.data : isChip(n) ? n.dataset.token : n.textContent || "";
    // A contenteditable inserts NBSP to keep trailing spaces from collapsing.
    // Left in, `from:npub1… alice` fails every \s in query.js and the
    // token silently stops being a token.
    return out.replace(/\u00a0/g, " ");
  }

  function indexOf(node, offset) {
    if (node === el) {
      let i = 0;
      for (let k = 0; k < offset && k < el.childNodes.length; k++) i += lenOf(el.childNodes[k]);
      return i;
    }
    let i = 0;
    for (const n of el.childNodes) {
      if (n === node) return i + (n.nodeType === 3 ? offset : lenOf(n));
      if (n.nodeType === 1 && n.contains(node)) return i + lenOf(n);
      i += lenOf(n);
    }
    return i;
  }

  function selectionRange() {
    const sel = document.getSelection();
    const r = sel && sel.rangeCount ? sel.getRangeAt(0) : null;
    const inside = (n) => n === el || el.contains(n);
    // The end-of-value fallback is computed only when it is needed — it walks
    // the whole field, and this runs on every keystroke by way of `mentioning`.
    if (!r || !inside(r.startContainer) || !inside(r.endContainer)) {
      const n = readValue().length;
      return [n, n];
    }
    return [indexOf(r.startContainer, r.startOffset), indexOf(r.endContainer, r.endOffset)];
  }

  /**
   * Where a DROP landed, in value offsets — not where the caret happens to be.
   *
   * A drop does not move the selection first, so inserting at `selectionRange`
   * put the text wherever the caret was last AND replaced anything selected
   * there, which is a destructive edit somewhere the reader was not looking.
   */
  function dropIndex(e) {
    const r = document.caretRangeFromPoint
      ? document.caretRangeFromPoint(e.clientX, e.clientY)
      : document.caretPositionFromPoint
        ? document.caretPositionFromPoint(e.clientX, e.clientY)
        : null;
    if (!r) return null;
    const node = r.startContainer ?? r.offsetNode;
    const offset = r.startOffset ?? r.offset;
    if (!node || !(node === el || el.contains(node))) return null;
    return indexOf(node, offset);
  }

  const caretIndex = () => selectionRange()[1];

  function setCaret(index) {
    const r = document.createRange();
    let i = 0, placed = false;
    for (const n of el.childNodes) {
      const len = lenOf(n);
      if (index <= i + len) {
        // Never INSIDE a chip: it is contenteditable=false, and a caret that
        // lands there has nothing to type into.
        if (n.nodeType === 3) r.setStart(n, Math.max(0, index - i));
        else if (index <= i) r.setStartBefore(n);
        else r.setStartAfter(n);
        placed = true;
        break;
      }
      i += len;
    }
    if (!placed) { r.selectNodeContents(el); r.collapse(false); } else r.collapse(true);
    const sel = document.getSelection();
    if (!sel) return;
    sel.removeAllRanges();
    sel.addRange(r);
  }

  // ---- rendering the value ------------------------------------------------

  /**
   * What a chip currently DRAWS — name and face. Unchanged means no repaint.
   *
   * The separator is written as the ESCAPE `\u0000`, never typed as the byte it
   * stands for. A literal NUL in this file is what made git call the whole
   * module binary, and four commits of it landed as `Bin 23897 -> 23932 bytes`
   * with nothing in them to read — which is how a refactor that deleted
   * faceHtml() while keeping both of its call sites got reviewed and shipped.
   * The character itself is still the right one: a space would join the pair
   * ("Alice B", "") to the same key as ("Alice", "B") and skip a repaint.
   */
  const chipFace = (pk) => {
    const p = profiles.get(pk);
    return `${displayName(p) || shortNpub(pk)}\u0000${(p && p.picture) || ""}`;
  };

  function paintChip(span) {
    const pk = span.dataset.pk;
    const field = span.dataset.field;
    const name = displayName(profiles.get(pk)) || shortNpub(pk);
    span.innerHTML =
      (field ? `<span class="mention-kind">${esc(field)}</span>` : "") +
      faceHtml(pk, "xs") +
      `<span class="mention-name">${esc(clip(name, 32))}</span>`;
    span.dataset.face = chipFace(pk);
    // The hover is the token itself: the field shows a person, and the string
    // it stands for has to stay reachable without deleting it to look.
    span.title = span.dataset.token;
  }

  function chipEl(seg) {
    const span = document.createElement("span");
    span.contentEditable = "false";
    span.dataset.token = seg.raw;
    if (seg.type === "tag") {
      // A hashtag needs no lookup and never repaints: unlike an npub it is
      // already readable, and the pill is not hiding it — it is saying that
      // this word left the search string and became a filter, which is the
      // one thing the box could not tell you before.
      span.className = "hashtag";
      span.textContent = seg.raw;
      // What it actually asks for, when that is not what it says: `#Nostr`
      // filters for `nostr`, and the hover is where a chip puts the truth.
      span.title = seg.raw.slice(1) === seg.tag
        ? `tag filter — this word is a #t/#l/#i filter, not a search term`
        : `tag filter for “${seg.tag}”`;
      return span;
    }
    if (seg.type === "date") {
      // Quieter than a person, like a hashtag, and for the same reason: the
      // token is readable, so the pill is not standing in for it. What it does
      // do is RESPELL it — `2026-08-06` is the one spelling both readers of a
      // slash-separated date agree on, and `6 Aug 2026` is the one they read.
      span.className = "datepill";
      span.innerHTML = `<b>${esc(seg.field)}</b>${esc(dayLabel(new Date(seg.at * 1000)))}`;
      // The hover is where the exactness goes: which second of the day the
      // bound lands on is the whole difference between the two prefixes, and
      // the pill has no room to say it.
      span.title =
        `${seg.raw} — a NIP-01 ${seg.field} filter: ` +
        (seg.field === "since" ? `written from 00:00` : `written up to 23:59`) +
        ` on ${dayLabel(new Date(seg.at * 1000))}, your time`;
      return span;
    }
    span.className = "mention";
    span.dataset.pk = seg.pubkey;
    if (seg.field) span.dataset.field = seg.field;
    paintChip(span);
    return span;
  }

  /**
   * Re-label the chips in place — no node is replaced, so the caret holds.
   *
   * Only the ones whose name or face actually CHANGED. app.js calls this after
   * every search whose profile lookup learned anything, which is usually about
   * somebody in the results and nobody in the field; rewriting every chip's
   * innerHTML for that rebuilt `<img>` elements to draw the same picture, and
   * would collapse a selection the reader had dragged across a chip.
   */
  function repaint() {
    let drew = false;
    for (const c of el.querySelectorAll(".mention")) {
      if (c.dataset.face !== chipFace(c.dataset.pk)) { paintChip(c); drew = true; }
    }
    // A repainted chip is a NEW score chip — paintChip replaces the innerHTML,
    // so whatever number was on the old face went with it.
    if (drew) fillScores();
  }

  function render(text, caret, typingAt = caret) {
    const segs = drawable(text, typingAt);
    el.innerHTML = "";
    const unknown = [];
    let chips = 0;
    for (const seg of segs) {
      if (seg.type === "text") { if (seg.text) el.appendChild(document.createTextNode(seg.text)); continue; }
      el.appendChild(chipEl(seg));
      if (seg.type === "key") {
        chips++;
        if (!profiles.has(seg.pubkey)) unknown.push(seg.pubkey);
      }
    }
    if (caret != null) setCaret(caret);
    if (chips) fillScores();
    // A chip for somebody the page has never met renders as a short npub and
    // then becomes a face. Repainting only when the lookup LEARNED something
    // is the same rule the results list follows: a lookup that found nothing
    // must not cost a render.
    if (unknown.length) enrichProfiles(unknown).then((n) => { if (n) repaint(); }).catch(() => {});
  }

  /**
   * Does the DOM still match what the text tokenizes to?
   *
   * Re-rendering on every keystroke would work and would also fight the
   * browser over the caret, IME composition and undo. So the field is left
   * alone until its STRUCTURE is wrong — a token finished or was broken, or
   * contenteditable invented a node (a `<br>` from a stray Enter, a `<div>`
   * or a `<b>` from a paste this module did not intercept).
   */
  function structureChanged(text, typingAt) {
    const want = drawable(text, typingAt).filter((s) => s.type !== "text").map((s) => s.raw);
    const have = [];
    for (const n of el.childNodes) {
      if (n.nodeType === 3) continue;
      if (!isChip(n)) return true;
      have.push(n.dataset.token);
    }
    return want.length !== have.length || want.some((w, i) => w !== have[i]);
  }

  function replaceRange(from, to, insert, caret, typingAt = caret) {
    const text = readValue();
    render(text.slice(0, from) + insert + text.slice(to), caret, typingAt);
  }

  // ---- the pickers ---------------------------------------------------------

  const listOpen = () => list.classList.contains("open");

  function closeList() {
    clearTimeout(timer);
    mention = null;
    hits = [];
    active = -1;
    day = null;
    month = null;
    cursor = null;
    list.classList.remove("open");
    list.innerHTML = "";
    // Back to the shape the markup declares, so a calendar's dialog role does
    // not outlive the calendar.
    list.setAttribute("role", "listbox");
    // Handed back to the results popup, which owns these the rest of the time.
    el.setAttribute("aria-expanded", "false");
    el.setAttribute("aria-controls", "popup");
    el.removeAttribute("aria-activedescendant");
    el.removeAttribute("aria-haspopup");
  }

  /**
   * Raise the box, whichever picker filled it, and say what is in it.
   *
   * The ROLE changes with the contents, and has to. A list of people is a
   * listbox; a calendar is a nav pair, a grid and four shortcuts, and calling
   * that a listbox puts five interactive non-options inside one — which is not
   * a listbox a screen reader can read. So the calendar is a dialog with the
   * listbox narrowed to the days themselves, and the combobox's aria-haspopup
   * says which of the two it currently offers.
   */
  function openList(role, label) {
    list.setAttribute("role", role);
    list.setAttribute("aria-label", label);
    list.classList.add("open");
    el.setAttribute("aria-expanded", "true");
    el.setAttribute("aria-controls", "mentions");
    el.setAttribute("aria-haspopup", role);
  }

  /**
   * Splice a finished token over the partial the caret is in.
   *
   * Both pickers end here, because both are answering the same question — the
   * reader typed a prefix, the box worked out what they meant, and the VALUE
   * is what has to change. One trailing space, so the next word is a word and
   * not more of the token; and the caret lands past it, which is outside the
   * token and therefore the moment the pill draws.
   */
  function replaceToken(ctx, token) {
    const text = readValue();
    const tail = text.slice(ctx.end).startsWith(" ") ? "" : " ";
    const { start, end } = ctx;
    closeList();
    replaceRange(start, end, token + tail, start + token.length + tail.length);
    el.focus();
    onEdit && onEdit();
  }

  const ROW_ID = (i) => `mention-opt-${i}`;

  function rowHtml(pk, i) {
    const p = profiles.get(pk) || {};
    const name = displayName(p) || shortNpub(pk);
    const sub = (p.nip05 || "").trim() || clip(p.about || "", 90) || npub(pk);
    return `
      <div class="popup-item" id="${ROW_ID(i)}" data-i="${i}" role="option" aria-selected="false">
        ${faceHtml(pk, "lg")}
        <div class="row-main">
          <div class="row-name">${esc(clip(name, 80))}</div>
          <div class="row-about">${esc(sub)}</div>
        </div>
      </div>`;
  }

  /**
   * Which row is highlighted, as a class flip on rows that already exist.
   *
   * This used to re-render the whole list per arrow press, which threw away
   * and rebuilt every row's `<img>` to change one class — and told a screen
   * reader nothing, because a combobox announces its highlight through
   * aria-activedescendant and there was none.
   */
  function markActive(scroll) {
    const rows = list.querySelectorAll(".popup-item");
    rows.forEach((r, i) => {
      const on = i === active;
      r.classList.toggle("active", on);
      r.setAttribute("aria-selected", String(on));
    });
    const row = rows[active];
    if (!row) { el.removeAttribute("aria-activedescendant"); return; }
    el.setAttribute("aria-activedescendant", row.id);
    // Only when the KEYBOARD moved the highlight. scrollIntoView forces layout
    // and scrolls every scrollable ancestor including the page, and a fresh
    // list always highlights its first row — so calling it per keystroke was
    // paying for a scroll nobody asked for.
    if (scroll) row.scrollIntoView({ block: "nearest" });
  }

  /** `rows` null means "still asking" — an empty array means "asked, none". */
  function renderList(rows) {
    if (!mention) return;
    const head =
      `<div class="popup-head"><span>${mention.field === "from" ? "Written by" : "Mentioning"}</span>` +
      `<span class="timing">${esc(mention.field)}:</span></div>`;
    let body;
    if (!mention.partial) body = `<div class="popup-note">Type a name, or paste an npub</div>`;
    else if (rows == null) body = `<div class="popup-note">Searching people…</div>`;
    else if (!rows.length) body = `<div class="popup-note">Nobody matches “${esc(clip(mention.partial, 40))}”</div>`;
    else body = rows.map(rowHtml).join("");
    list.innerHTML = head + body;
    openList("listbox", "People");
    markActive();
    // Asked of the DOM rather than of `rows`, because a note ("type a name",
    // "nobody matches") is also a body built from a non-empty argument.
    if (list.querySelector(".popup-item")) fillScores();
  }

  function move(delta) {
    if (!hits.length) return;
    active = (active + delta + hits.length) % hits.length;
    markActive(true);
  }

  /**
   * The UNFINISHED `from:`/`to:` token the caret sits in, or null.
   *
   * Derived from the text every time rather than remembered, because the
   * picker being shut is not the same thing as the token being done — blur
   * closes the list, and the half-written `from:al` is still sitting there
   * waiting to be finished. With `mentioning` keyed on the open list, coming
   * back to the field opened the RESULTS popup over that token: the one thing
   * this feature promises will not happen.
   *
   * With no selection in the field (nothing focused, or focus elsewhere) the
   * caret reads as the end of the value, which is the right answer for the
   * question app.js asks: a box ENDING in a half-written token is not a box
   * to show text results for.
   */
  function pendingMention() {
    const m = mentionAt(readValue(), caretIndex());
    return m && !m.complete ? m : null;
  }

  /**
   * Re-read the token under the caret and keep the picker in step.
   *
   * Idempotent by design: an unchanged token returns early rather than
   * re-querying and resetting the highlight, so this is safe to call from
   * every edit AND every caret move.
   */
  function updateMention() {
    const next = pendingMention();
    if (!next) { if (mention) closeList(); return; }
    const sameToken = !!mention && mention.field === next.field && mention.start === next.start;
    if (sameToken && mention.partial === next.partial) return;
    mention = next;
    // The rows from the previous keystroke STAY UP while the next answer is
    // fetched. Blanking them to "Searching people…" between every character
    // made the list flicker rows → note → rows at typing speed, and moved the
    // row out from under whatever the cursor was over.
    //
    // They are dropped when the caret moves to a DIFFERENT token, and when the
    // partial empties: the list then reads "type a name" while `hits` would
    // still have answered Enter with somebody it is no longer offering.
    if (!sameToken || !next.partial) { hits = []; active = -1; }
    renderList(hits.length ? hits : null);
    clearTimeout(timer);
    if (!next.partial) return;
    const id = ++reqId;
    timer = setTimeout(async () => {
      let found;
      try {
        found = await lookup(next.partial);
      } catch (e) {
        // A failed lookup leaves the last rows (or "searching") up rather than
        // claiming nobody matches — the relay not answering is not evidence
        // that the person does not exist.
        return;
      }
      // OUTSIDE the catch, and that is the point. This used to be inside it,
      // where the forgiveness meant for a relay that did not answer was also
      // applied to a bug in the drawing: rowHtml() threw on every row, the
      // catch ate it, and the picker sat on "Searching people…" having been
      // handed twelve perfectly good answers. A lookup can fail; drawing what
      // it returned cannot, and if it does the console is where that belongs.
      if (id !== reqId || !mention) return;
      hits = Array.isArray(found) ? found.slice(0, PICKER_LIMIT) : [];
      active = hits.length ? 0 : -1;
      renderList(hits);
    }, DEBOUNCE_MS);
  }

  function pick(pubkey) {
    if (!mention || !pubkey) return;
    replaceToken(mention, `${mention.field}:${npub(pubkey)}`);
  }

  // ---- the calendar --------------------------------------------------------
  //
  // Which days exist is shared/calendar.js's business and is tested there; what
  // is here is only how they are drawn and what a click on one does. No lookup,
  // no debounce, no request id — a month grid is not a question anyone has to
  // answer, which is the whole difference from the people picker beside it.

  const DAY_ID = (value) => `cal-day-${value}`;
  const HEAD = { since: "Written on or after", until: "Written on or before" };

  // Nothing here draws the CURSOR: markDay does, on the cells this built, for
  // the same reason markActive does it for the people rows. A grid rebuilt per
  // arrow press throws away fifty nodes to move one highlight — the lesson the
  // people list already paid for once.
  //
  // tabindex="-1" on every control, as the people rows are not focusable
  // either: the caret has to stay in the field, because it is what a pick
  // splices into, and thirty-one tab stops between the box and the next control
  // is not a keyboard path anybody wants. The keyboard reaches all of this
  // through the field instead — arrows for days, Page keys for months.
  function calendarHtml() {
    const today = midnight(new Date());
    const grid = monthGrid(month || shiftMonths(today, 0), today);
    // The blanks before the 1st are real cells, not a margin: the grid is seven
    // columns and the 1st has to land under its own weekday. aria-hidden, so
    // the listbox they sit in holds nothing but days.
    const pads = Array.from({ length: grid.lead }, () => `<span class="cal-pad" aria-hidden="true"></span>`);
    const cells = grid.days.map((d) =>
      `<button type="button" tabindex="-1" id="${DAY_ID(d.value)}" data-day="${d.value}"` +
      ` class="cal-day${d.today ? " today" : ""}${d.ahead ? " ahead" : ""}"` +
      ` role="option" aria-selected="false" aria-label="${esc(dayLabel(d.at))}">${d.at.getDate()}</button>`);
    const dow = DOW.map((w) => `<span class="cal-dow" aria-hidden="true" title="${esc(w.long)}">${esc(w.narrow)}</span>`).join("");
    const quick = quickPicks(day.field, today)
      .map((p) => `<button type="button" tabindex="-1" class="cal-pick" data-day="${p.value}">${esc(p.label)}</button>`)
      .join("");
    return (
      `<div class="cal">` +
      `<div class="cal-nav">` +
      `<button type="button" tabindex="-1" class="cal-step" data-step="-1" aria-label="Previous month">&lsaquo;</button>` +
      `<div class="cal-month">${esc(grid.label)}</div>` +
      `<button type="button" tabindex="-1" class="cal-step" data-step="1" aria-label="Next month">&rsaquo;</button>` +
      `</div>` +
      `<div class="cal-grid" role="listbox" aria-label="Days">${dow}${pads.join("")}${cells.join("")}</div>` +
      `<div class="cal-quick">${quick}</div>` +
      `</div>`
    );
  }

  /**
   * Which day the keyboard is on, as a class flip on cells that already exist.
   *
   * The people list's markActive, for the grid: moving the highlight must not
   * cost a render. It is also the only writer of aria-activedescendant on this
   * side, so the announced day and the filled square cannot disagree.
   */
  function markDay() {
    const on = cursor ? ymd(cursor) : null;
    let found = null;
    for (const cell of list.querySelectorAll(".cal-day")) {
      const is = cell.dataset.day === on;
      cell.classList.toggle("active", is);
      cell.setAttribute("aria-selected", String(is));
      if (is) found = cell;
    }
    if (found) el.setAttribute("aria-activedescendant", found.id);
    else el.removeAttribute("aria-activedescendant");
  }

  function renderCalendar() {
    if (!day) return;
    list.innerHTML =
      `<div class="popup-head"><span>${HEAD[day.field]}</span><span class="timing">${esc(day.field)}:</span></div>` +
      calendarHtml();
    openList("dialog", HEAD[day.field]);
    markDay();
  }

  /**
   * Point the grid at a month, dropping a cursor that would fall off it.
   *
   * The one invariant the three callers below all need: the highlighted day is
   * either on screen or gone. A cursor left behind in a month nobody is looking
   * at is a day Enter would pick sight unseen.
   */
  function setMonth(next) {
    month = next;
    if (!sameMonth(cursor, month)) cursor = null;
  }

  /** Re-read the `since:`/`until:` token under the caret and keep the grid in step. */
  function showCalendar(next) {
    // Idempotent for the same reason updateMention() is: this runs on every
    // caret move as well as every edit, and a grid that rebuilt itself for an
    // unchanged token would throw away the month the reader had stepped to.
    const same = !!day && day.field === next.field && day.start === next.start && listOpen();
    if (same && day.partial === next.partial) return;
    day = next;
    if (mention) { clearTimeout(timer); mention = null; hits = []; active = -1; }
    // The typed month wins whenever the partial names one, so the grid follows
    // what the box says. Failing that, the same token keeps wherever the reader
    // had stepped it, and a calendar only just opened starts on this month.
    setMonth(typedMonth(next.partial) || (same ? month : null) || shiftMonths(midnight(new Date()), 0));
    // Always a full draw, even for an unchanged month: the head and the
    // shortcuts belong to the TOKEN, and `since`→`until` changes both.
    renderCalendar();
  }

  function stepMonth(by) {
    if (!day) return;
    setMonth(shiftMonths(month || midnight(new Date()), by));
    renderCalendar();
  }

  /**
   * Move the keyboard cursor by `by` days, opening the month it lands in.
   *
   * The first press starts from the day the grid is already about: today when
   * it is in view, the 1st of the shown month otherwise. Starting from "today"
   * unconditionally would put the cursor off-screen the moment somebody
   * arrowed after stepping back a year.
   *
   * The month is set from the day rather than the other way round, so setMonth
   * is not in a position to throw away the cursor it was just handed.
   */
  function moveDay(by) {
    if (!day) return;
    const today = midnight(new Date());
    const shown = month || today;
    cursor = cursor ? shiftDays(cursor, by)
      : sameMonth(today, shown) ? today
        : new Date(shown.getFullYear(), shown.getMonth(), 1);
    const crossed = !sameMonth(month, cursor);
    month = shiftMonths(cursor, 0);
    // A step inside the month is a class flip, not a grid: the rule the people
    // list already follows, and the reason an arrow press costs what it does.
    if (crossed) renderCalendar();
    else markDay();
  }

  function pickDay(value) {
    if (!day || !value) return;
    replaceToken(day, `${day.field}:${value}`);
  }

  // ---- what the caret is inside, and which picker that opens ---------------

  /**
   * The UNFINISHED `since:`/`until:` token the caret sits in, or null.
   *
   * Read from the text every time, exactly as [pendingMention] is and for the
   * same reason: the box being shut is not the same thing as the token being
   * done, and `since:2026-08-` left behind by a blur is still waiting.
   */
  function pendingDate() {
    const d = dateAt(readValue(), caretIndex());
    return d && !d.complete ? d : null;
  }

  /**
   * Re-read the token under the caret and put the right picker under it.
   *
   * The date half goes first because it is the cheap one — no network, no
   * debounce — and because only one of the two can match: a caret is inside one
   * token, and `from|to` and `since|until` are different prefixes. Whichever
   * does not match has to be shut, or stepping from `since:` straight into a
   * `from:` would leave a calendar over a people search.
   */
  function updateToken() {
    const next = pendingDate();
    if (next) { showCalendar(next); return; }
    if (day) closeList();
    updateMention();
  }

  /**
   * Enter, however it arrived — true when a picker consumed it.
   *
   * Shared by the two doors Enter comes through, because a phone's action key
   * is not reliably a keydown (see the beforeinput handler below) and a
   * highlighted person or day has to be picked either way. Tab lands here too:
   * completing the token is what Tab means over an open picker.
   *
   * Nothing highlighted is deliberately NOT consumed. The picker is a
   * suggestion over the text, not a gate in front of it, so a partial date
   * with no day chosen falls through to the page's own Enter — the search.
   */
  function takeEnter() {
    if (!listOpen()) return false;
    if (day) { if (!cursor) return false; pickDay(ymd(cursor)); return true; }
    if (active < 0 || !hits[active]) return false;
    pick(hits[active]);
    return true;
  }

  el.addEventListener("input", (e) => {
    const text = readValue();
    // Empty means EMPTY: the browser leaves a `<br>` behind when the last
    // character goes, and `:empty` is what draws the placeholder.
    if (!text) { if (el.innerHTML) el.innerHTML = ""; }
    // NEVER re-render mid-composition: rebuilding the nodes under an IME tears
    // down the text it is still composing, which is a whole class of users
    // typing a whole class of scripts. Nothing can finish a token during a
    // composition anyway — an npub is 63 bech32 characters.
    else if (!e.isComposing) { const at = caretIndex(); if (structureChanged(text, at)) render(text, at); }
    updateToken();
    onEdit && onEdit();
  });

  // Paste and drop insert TEXT. Left to the browser they insert markup — a
  // pasted link arrives as an <a>, a pasted paragraph as a <div> — and this
  // field's value is the concatenation of its nodes.
  function insertPlain(e, raw, at) {
    e.preventDefault();
    const text = String(raw || "").replace(/\s+/g, " ");
    const [from, to] = at == null ? selectionRange() : [at, at];
    // A paste is not somebody midway through typing a word, so a pasted tag
    // pills immediately: `typingAt` null draws every token it finds.
    replaceRange(from, to, text, from + text.length, null);
    updateToken();
    onEdit && onEdit();
  }
  el.addEventListener("paste", (e) => insertPlain(e, (e.clipboardData || window.clipboardData).getData("text/plain")));
  el.addEventListener("drop", (e) => insertPlain(e, e.dataTransfer && e.dataTransfer.getData("text/plain"), dropIndex(e)));

  /**
   * The other door Enter comes through, and on a phone the only one.
   *
   * A soft keyboard's action key is not a key press. Gboard and the iOS
   * keyboard report an IME edit, so the keydown that reaches the page carries
   * `Unidentified` (keyCode 229) or does not arrive at all — `e.key ===
   * "Enter"` never matched, app.js never got its preventDefault in, and the
   * div did what a div does with a newline: grew a second line, which is why
   * the box and the faces in it jumped. An <input> had no such failure mode
   * because it has nowhere to put a line break.
   *
   * So the newline is caught here as what it actually IS — an input of a line
   * break — whichever keyboard produced it, and refused rather than merely
   * redirected: the field is one line by construction (`white-space: pre`,
   * `aria-multiline="false"`), so a break has no meaning in it even when the
   * Enter behind it is not a submit.
   *
   * Three things this had wrong when it was written, all of them the same
   * mistake — treating "there is a newline in here" as "this event IS the
   * newline":
   *
   *   - An IME can commit the composing word and the action key as ONE
   *     insertion, `hello\n`. Refusing that outright dropped `hello` with the
   *     break, which is the worst failure available: text the reader typed,
   *     gone, with a search running to distract from it. The break is stripped
   *     and the rest inserted, so only the newline is refused.
   *   - insertCompositionText is deliberately absent. A pick re-renders the
   *     field, and doing that under a live composition is exactly what the
   *     input handler above refuses to do for the same reason.
   *   - The regex is hoisted, because this runs on every keystroke.
   *
   * Desktop cannot double-submit through this: there Enter IS a keydown, and
   * app.js's preventDefault stops the input from ever being attempted.
   */
  el.addEventListener("beforeinput", (e) => {
    const t = e.inputType;
    const typed = t === "insertText" && e.data && NEWLINE.test(e.data) ? e.data : null;
    if (!typed && t !== "insertLineBreak" && t !== "insertParagraph") return;
    const rest = typed ? typed.replace(NEWLINES, "") : "";
    // insertPlain preventDefaults, splices and reports the edit; a bare
    // newline leaves nothing to insert and is only the submit.
    if (rest) insertPlain(e, rest); else e.preventDefault();
    if (takeEnter()) return;
    onSubmit && onSubmit();
  });

  /**
   * A caret MOVE finishes a hashtag or a date without editing anything.
   *
   * `#nos|` is text while the caret is in it and a pill the moment the caret
   * leaves — so clicking away, arrowing past it or tabbing out has to re-read
   * the structure, exactly as those same moves re-read the people picker.
   * Nothing happens unless the drawing would actually change, so this stays a
   * comparison of two short lists on a keyup, not a render.
   */
  function syncPills(typingAt = caretIndex()) {
    const text = readValue();
    if (text && structureChanged(text, typingAt)) render(text, typingAt, typingAt);
  }

  // A caret MOVE can enter or leave a token without editing anything. Arrows
  // that the picker is using are excluded — they move the highlight, not the
  // caret, and re-reading here would reset it on every press.
  el.addEventListener("click", () => { updateToken(); syncPills(); });
  // Coming BACK to a half-written token has to pick up where it left off. Blur
  // shut the picker, and without this the only way to see it again was to type
  // another character — so tabbing away and back, or leaving the window and
  // returning, stranded `from:al` with no way to finish it but by hand.
  el.addEventListener("focus", () => { updateToken(); syncPills(); });
  el.addEventListener("keyup", (e) => {
    if (e.key === "ArrowLeft" || e.key === "ArrowRight" || e.key === "Home" || e.key === "End") { updateToken(); syncPills(); }
  });
  el.addEventListener("blur", () => {
    // Nobody is typing a word in a field they have left: every tag pills, and
    // the caret is not placed at all — restoring one into an unfocused field
    // would scroll it back into view for no reader.
    syncPills(null);
    // The SELECTION outlives the focus, and a caret is drawn from the
    // selection. Losing focus leaves the document's only range sitting inside
    // this div, which is a blinking caret in a field the reader has left —
    // visible on a phone, where the compositor keeps painting it after the
    // keyboard has gone. Dropped only when the range is ours, so a selection
    // the same gesture made somewhere else survives. It is also what
    // pendingMention() already documents as the unfocused state: no range in
    // here means the caret reads as the end of the value.
    const sel = document.getSelection();
    if (sel && sel.rangeCount && el.contains(sel.getRangeAt(0).startContainer)) sel.removeAllRanges();
    // A click ON the picker must land before the picker disappears; mousedown
    // there has already fired by the time blur does, so the delay only has to
    // outlast the same tick.
    setTimeout(() => { if (!list.contains(document.activeElement)) closeList(); }, 120);
  });

  // preventDefault throughout: the caret in the field is what a pick splices
  // into, and a mousedown in here would move it out of the token first. The
  // month arrows need it for a second reason — they are not a pick at all, and
  // a field that lost focus to a `‹` would close the calendar being paged.
  list.addEventListener("mousedown", (e) => {
    const step = e.target.closest(".cal-step");
    if (step) { e.preventDefault(); stepMonth(Number(step.dataset.step)); return; }
    const cell = e.target.closest(".cal-day, .cal-pick");
    if (cell) { e.preventDefault(); pickDay(cell.dataset.day); return; }
    const row = e.target.closest(".popup-item");
    if (!row) return;
    e.preventDefault();
    pick(hits[Number(row.dataset.i)]);
  });

  /**
   * A touch anywhere else on the page LEAVES the field — said out loud, one
   * more thing an <input> gave for free.
   *
   * An input loses focus the moment a pointer goes down outside it, on every
   * device. A contenteditable div gets that from a desktop mouse and not from
   * a finger: with nothing focusable under the tap there is nowhere for focus
   * to go, so the editing session is torn down when the browser is finished
   * with the soft keyboard rather than when the reader touched the page —
   * seconds later, with the caret still blinking in the box the whole time.
   * (It is invisible to a desktop browser in mobile-emulation, which
   * synthesises the tap but has no keyboard to put away, so this is one to
   * check on a phone.)
   *
   * pointerdown, because that is the moment the desktop already does it, and
   * it is ahead of the click that closeList() waits for below. Capture, so
   * nothing between the tap and the document can strand the caret by
   * swallowing the event. Passive, because unlike the picker's own mousedown
   * below — which preventDefaults to keep the caret in the token a row splices
   * into — this one never cancels anything; it is the opposite handler, and
   * the flag says so to the browser as well as to the reader.
   *
   * Never inside .search-wrap: that box holds the field, both pickers and the
   * results popup, and a row is picked with the caret still in the token.
   * The activeElement test is first because it is the cheap one and it is
   * false for almost every tap on the page — this runs on all of them.
   */
  document.addEventListener("pointerdown", (e) => {
    if (document.activeElement !== el) return;
    const t = e.target;
    if (t && t.closest && t.closest(".search-wrap")) return;
    el.blur();
  }, { capture: true, passive: true });

  /**
   * Leaving the page takes the keyboard with it, so the field lets the caret
   * go too.
   *
   * Coming BACK is where this was felt. The page freezes with the field
   * focused, the keyboard is gone by the time it thaws, and a caret is still
   * blinking in a box that cannot be typed into — and tapping that box does
   * not fix it, because the element is already focused, so there is no focus
   * change for the browser to raise a keyboard for. The only way back in was
   * to tap somewhere else first and then return, which is a thing no reader
   * should have to know.
   *
   * Three events for one rule, because no one of them fires everywhere:
   * pagehide is the back/forward cache freeze (conn.js closes the sockets on
   * the same signal), visibilitychange is switching apps without a navigation,
   * and pageshow is the belt to their braces — if the page did come back with
   * the caret still in here, this is where it goes.
   *
   * Only where a keyboard has to be raised at all. On a desktop the caret and
   * the keyboard are not the same thing, and dropping a caret because the
   * reader looked at another tab would be taking something away for nothing.
   */
  const releaseOnHide = () => { if (document.activeElement === el && softKeyboard()) el.blur(); };
  window.addEventListener("pagehide", releaseOnHide);
  window.addEventListener("pageshow", releaseOnHide);
  document.addEventListener("visibilitychange", releaseOnHide);

  document.addEventListener("click", (e) => { if (!e.target.closest(".search-wrap")) closeList(); });

  // ---- what app.js needs ---------------------------------------------------

  Object.defineProperty(el, "value", {
    get: readValue,
    // A programmatic set is a RESTORE (the URL, the clear button), never a
    // keystroke: it does not call onEdit, exactly as assigning to an input's
    // value fires no input event, and it dismisses the picker because the
    // token that was being built no longer exists.
    set(v) {
      // The last door a line break could come through. The keyboard's is shut
      // on beforeinput and the clipboard's by insertPlain, but this one takes
      // whatever `?q=` carried — and a text node holding a `\n` under
      // `white-space: pre` is the same two-line box, arrived at from the URL.
      const text = String(v ?? "").replace(NEWLINES, " ");
      closeList();
      // typingAt null: a URL restore or a clear is not typing, so a tag the
      // caret happens to land after still draws as the filter it already is.
      render(text, document.activeElement === el ? text.length : null, null);
    },
  });

  // The "/" shortcut selects the field's contents; a div has no select().
  el.select = () => {
    const sel = document.getSelection();
    if (!sel) return;
    const r = document.createRange();
    r.selectNodeContents(el);
    sel.removeAllRanges();
    sel.addRange(r);
  };

  return {
    /**
     * Is a `from:`/`to:`/`since:`/`until:` token being built right now?
     *
     * An open picker OR an unfinished token under the caret — the second half
     * matters because the box closes on blur and on Escape while the token
     * stays half-written, and app.js uses this to decide whether the results
     * popup may open. Answering "no" in that window is how a stale result
     * list ended up over `from:al`.
     */
    get picking() { return listOpen() || !!pendingMention() || !!pendingDate(); },
    /**
     * Is a picker itself on screen? Narrower than `picking`, and the difference
     * matters to exactly one caller: while one is up it owns the field's
     * aria-expanded / aria-controls / aria-activedescendant, and closePopup()
     * must not lower them under it.
     */
    get pickerOpen() { return listOpen(); },
    /**
     * The keys a picker owns while it is open, consumed here so app.js's own
     * handler can return. Called BY app.js rather than racing it in the
     * capture phase: which of two listeners on one element runs first is
     * registration order, and that is not a thing to depend on.
     *
     * The calendar takes the same four keys and reads them as a grid rather
     * than a list — sideways is a day, up and down are a week, which is what
     * the shape on screen promises. Two more are its own: Page keys are how a
     * reader gets to last March without forty presses.
     *
     * That does cost Left and Right, which the people list leaves to the caret;
     * Escape is what hands them back, and it hands them back for good — one
     * press moves the caret INTO the token, where dateAt stops reporting a
     * partial and nothing reopens.
     */
    handleKey(e) {
      if (!listOpen()) return false;
      if (e.key === "Escape") { e.preventDefault(); closeList(); return true; }
      if (day) {
        const by = { ArrowLeft: -1, ArrowRight: 1, ArrowUp: -7, ArrowDown: 7 }[e.key];
        if (by) { e.preventDefault(); moveDay(by); return true; }
        if (e.key === "PageUp" || e.key === "PageDown") {
          e.preventDefault();
          stepMonth(e.key === "PageUp" ? -1 : 1);
          return true;
        }
        // Enter on nothing highlighted stays the page's Enter, exactly as it
        // does over the people list: the reader typed a partial date and hit
        // Enter, which is a search for what is in the box, not a pick of
        // whatever day the grid happens to be showing. takeEnter() is that
        // rule, for both lists and for the phones that never send this key.
        if (e.key === "Enter" || e.key === "Tab") {
          if (!takeEnter()) return false;
          e.preventDefault();
          return true;
        }
        return false;
      }
      if (e.key === "ArrowDown") { e.preventDefault(); move(1); return true; }
      if (e.key === "ArrowUp") { e.preventDefault(); move(-1); return true; }
      if (e.key === "Enter" || e.key === "Tab") {
        if (!takeEnter()) return false;
        e.preventDefault();
        return true;
      }
      return false;
    },
    /** Re-label the chips — for when profiles land after a render. */
    repaint,
    close: closeList,
  };
}
