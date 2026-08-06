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
//      people picker it answers instantly and has nothing to debounce.
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
// and paste/drop that insert TEXT rather than whatever html was on the
// clipboard. app.js keeps writing `$q.value` and never learns the difference.
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

const PICKER_LIMIT = 8;
const DEBOUNCE_MS = 150;

// ---- days, as the calendar counts them ------------------------------------
//
// All local, all midnight, because that is what the language means by a day
// (query.js's dayBound says why) and because a grid that drifted by a timezone
// would highlight the wrong square for "today".

const midnight = (d) => new Date(d.getFullYear(), d.getMonth(), d.getDate());
const shiftDays = (d, n) => new Date(d.getFullYear(), d.getMonth(), d.getDate() + n);
const shiftMonths = (d, n) => new Date(d.getFullYear(), d.getMonth() + n, 1);
const sameDay = (a, b) => !!a && !!b && ymd(a) === ymd(b);

/**
 * Which weekday a week starts on, 0 = Sunday.
 *
 * Asked of the reader's own locale where the browser will say (Chrome, Safari
 * carry Intl.Locale.weekInfo) and ISO Monday where it will not (Firefox). A
 * calendar that starts the week on the wrong day is not wrong so much as
 * unreadable — the columns stop being where the eye expects them.
 */
function weekStart() {
  try {
    // weekInfo counts 1..7 from Monday; JS dates count 0..6 from Sunday.
    const first = new Intl.Locale(navigator.language).weekInfo?.firstDay;
    if (first) return first % 7;
  } catch (e) { /* no weekInfo here — ISO it is */ }
  return 1;
}

/** The column headings, in the order this locale's week runs. 4 Jan 2026 is a Sunday. */
const dowNames = (start) =>
  Array.from({ length: 7 }, (_, i) => new Date(2026, 0, 4 + start + i)).map((d) => ({
    narrow: d.toLocaleDateString(undefined, { weekday: "narrow" }),
    long: d.toLocaleDateString(undefined, { weekday: "long" }),
  }));

/** A day in the reader's own spelling — what a pill and a calendar say out loud. */
const dayLabel = (d) => d.toLocaleDateString(undefined, { day: "numeric", month: "short", year: "numeric" });

/**
 * The quick picks under the grid, as offsets in DAYS from today.
 *
 * Different for the two prefixes because they answer different questions: a
 * `since` is the start of a window ("the last week"), an `until` is a cutoff
 * ("before last week"). Each resolves to an absolute day and writes that, so
 * the language stays one token shape — a saved URL reading `since:7d` would
 * mean a different search every day it was opened.
 */
const QUICK = {
  since: [["Today", 0], ["Last 7 days", -6], ["Last 30 days", -29], ["Last 90 days", -89]],
  until: [["Today", 0], ["Yesterday", -1], ["A week ago", -7], ["A month ago", -30]],
};

// The faces here are the page's face — generated fallback, score chip and all.
// This module used to draw its own <img>, which is how the one place that ASKS
// "who do you mean by this person" ended up being the one place that did not
// say what the active lens thinks of them. A picker offering eight strangers
// is exactly where the number earns its keep.
/** The picture a person draws with right now, from whatever profiles knows. */
const faceHtml = (pubkey, size) => avatarHtml((profiles.get(pubkey) || {}).picture, pubkey, size);

/**
 * Take over `el` as the rendered search field, with `list` as its picker.
 *
 * `lookup(partial)` resolves to hex pubkeys, best first — app.js supplies it,
 * because whose socket the question goes down is app.js's business, not this
 * module's. `onEdit()` is called after every human edit, once this module's
 * own state is already current: app.js reads `mentioning` inside it and would
 * otherwise be reacting to the keystroke before last.
 *
 * `paintScores()` fills the score chips on whatever faces are on screen. It is
 * a hook for the same reason it is one on the entity page: the lens a score is
 * read under is app state, and this module only draws the faces.
 */
export function mountSearchField(el, list, { lookup, onEdit, paintScores }) {
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

  /** What a chip currently DRAWS — name and face. Unchanged means no repaint. */
  const chipFace = (pk) => {
    const p = profiles.get(pk);
    return `${displayName(p) || shortNpub(pk)} ${(p && p.picture) || ""}`;
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
    // Handed back to the results popup, which owns them the rest of the time.
    el.setAttribute("aria-expanded", "false");
    el.setAttribute("aria-controls", "popup");
    el.removeAttribute("aria-activedescendant");
  }

  /** Raise the box, whichever picker filled it, and say so to a screen reader. */
  function openList(label) {
    list.setAttribute("aria-label", label);
    list.classList.add("open");
    el.setAttribute("aria-expanded", "true");
    el.setAttribute("aria-controls", "mentions");
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
    openList("People");
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
      try {
        const found = await lookup(next.partial);
        if (id !== reqId || !mention) return;
        hits = Array.isArray(found) ? found.slice(0, PICKER_LIMIT) : [];
        active = hits.length ? 0 : -1;
        renderList(hits);
      } catch (e) {
        // A failed lookup leaves the last rows (or "searching") up rather than
        // claiming nobody matches — the relay not answering is not evidence
        // that the person does not exist.
      }
    }, DEBOUNCE_MS);
  }

  function pick(pubkey) {
    if (!mention || !pubkey) return;
    replaceToken(mention, `${mention.field}:${npub(pubkey)}`);
  }

  // ---- the calendar --------------------------------------------------------
  //
  // Everything here is arithmetic on local dates: there is no lookup, no
  // debounce and no request id, because a month grid is not a question anyone
  // has to answer. That is the whole difference from the people picker beside
  // it — same box, same keys, same splice at the end, no network in the middle.

  const DAY_ID = (d) => `cal-day-${ymd(d)}`;

  /**
   * The month a half-typed date names, or null while it names none.
   *
   * `since:2026-01` should not leave the grid sitting on this month: the reader
   * has already said which one they mean, and making them arrow back to it is
   * the calendar ignoring what is in the box. The day half is deliberately not
   * read — `2026-01-3` is not a day yet, and jumping the grid at the third
   * digit of a year would make it lurch through 2 AD on the way to 2026.
   */
  function typedMonth(partial) {
    const m = /^(\d{4})-(\d{2})/.exec(String(partial || ""));
    if (!m) return null;
    const [y, mo] = m.slice(1).map(Number);
    if (mo < 1 || mo > 12) return null;
    const at = new Date(y, mo - 1, 1);
    return at.getFullYear() === y ? at : null;
  }

  function calendarHtml() {
    const today = midnight(new Date());
    const start = weekStart();
    const shown = month || shiftMonths(today, 0);
    const lead = (new Date(shown.getFullYear(), shown.getMonth(), 1).getDay() - start + 7) % 7;
    const days = new Date(shown.getFullYear(), shown.getMonth() + 1, 0).getDate();
    const cells = [];
    // The blanks before the 1st are real cells, not a margin: the grid is seven
    // columns and the 1st has to land under its own weekday.
    for (let i = 0; i < lead; i++) cells.push(`<span class="cal-pad" aria-hidden="true"></span>`);
    for (let n = 1; n <= days; n++) {
      const d = new Date(shown.getFullYear(), shown.getMonth(), n);
      const cls = ["cal-day"];
      if (sameDay(d, today)) cls.push("today");
      if (sameDay(d, cursor)) cls.push("active");
      // A future day is a legal bound — events carry the created_at their
      // author claimed — so it is dimmed rather than disabled: rare, not wrong.
      if (d > today) cls.push("ahead");
      cells.push(
        `<button type="button" class="${cls.join(" ")}" id="${DAY_ID(d)}" data-day="${ymd(d)}"` +
        ` role="option" aria-selected="${sameDay(d, cursor)}" aria-label="${esc(dayLabel(d))}">${n}</button>`,
      );
    }
    const dow = dowNames(start)
      .map((w) => `<span class="cal-dow" title="${esc(w.long)}">${esc(w.narrow)}</span>`)
      .join("");
    const quick = QUICK[day.field]
      .map(([label, off]) => `<button type="button" class="cal-pick" data-day="${ymd(shiftDays(today, off))}">${esc(label)}</button>`)
      .join("");
    return (
      `<div class="cal">` +
      `<div class="cal-nav">` +
      `<button type="button" class="cal-step" data-step="-1" aria-label="Previous month">&lsaquo;</button>` +
      `<div class="cal-month">${esc(shown.toLocaleDateString(undefined, { month: "long", year: "numeric" }))}</div>` +
      `<button type="button" class="cal-step" data-step="1" aria-label="Next month">&rsaquo;</button>` +
      `</div>` +
      `<div class="cal-grid">${dow}${cells.join("")}</div>` +
      `<div class="cal-quick">${quick}</div>` +
      `</div>`
    );
  }

  function renderCalendar() {
    if (!day) return;
    const head =
      `<div class="popup-head"><span>${day.field === "since" ? "Written on or after" : "Written on or before"}</span>` +
      `<span class="timing">${esc(day.field)}:</span></div>`;
    list.innerHTML = head + calendarHtml();
    // A listbox of days rather than a grid: the field is a combobox, the two
    // pickers share its aria-activedescendant, and one honest pattern that both
    // fit beats a second one that only this half implements properly.
    openList("Dates");
    const on = cursor && list.querySelector(".cal-day.active");
    if (on) el.setAttribute("aria-activedescendant", on.id);
    else el.removeAttribute("aria-activedescendant");
  }

  /** Re-read the `since:`/`until:` token under the caret and keep the grid in step. */
  function showCalendar(next) {
    const same = !!day && day.field === next.field && day.start === next.start;
    // Idempotent for the same reason updateMention() is: this runs on every
    // caret move as well as every edit, and a grid that rebuilt itself for an
    // unchanged token would throw away the month the reader had stepped to.
    if (same && day.partial === next.partial && listOpen()) return;
    day = next;
    if (mention) { clearTimeout(timer); mention = null; hits = []; active = -1; }
    // The typed month wins whenever the partial names one, so the grid follows
    // the box; otherwise it holds where the reader last stepped it, and opens
    // on this month.
    month = typedMonth(next.partial) || (same && month) || shiftMonths(midnight(new Date()), 0);
    if (!same) cursor = null;
    // A cursor the reader has stepped off the shown month is not on screen and
    // must not stay highlighted in the dark; Enter would pick a day nobody saw.
    if (cursor && (cursor.getFullYear() !== month.getFullYear() || cursor.getMonth() !== month.getMonth())) cursor = null;
    renderCalendar();
  }

  function stepMonth(by) {
    if (!day) return;
    month = shiftMonths(month || midnight(new Date()), by);
    if (cursor) cursor = null;
    renderCalendar();
  }

  /**
   * Move the keyboard cursor by `by` days, opening the month it lands in.
   *
   * The first press starts from the day the grid is already about: today when
   * it is in view, the 1st of the shown month otherwise. Starting from "today"
   * unconditionally would put the cursor off-screen the moment somebody
   * arrowed after stepping back a year.
   */
  function moveDay(by) {
    if (!day) return;
    const today = midnight(new Date());
    const shown = month || today;
    const inShown = today.getFullYear() === shown.getFullYear() && today.getMonth() === shown.getMonth();
    cursor = cursor ? shiftDays(cursor, by) : inShown ? today : new Date(shown.getFullYear(), shown.getMonth(), 1);
    month = shiftMonths(cursor, 0);
    renderCalendar();
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

  document.addEventListener("click", (e) => { if (!e.target.closest(".search-wrap")) closeList(); });

  // ---- what app.js needs ---------------------------------------------------

  Object.defineProperty(el, "value", {
    get: readValue,
    // A programmatic set is a RESTORE (the URL, the clear button), never a
    // keystroke: it does not call onEdit, exactly as assigning to an input's
    // value fires no input event, and it dismisses the picker because the
    // token that was being built no longer exists.
    set(v) {
      const text = String(v ?? "");
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
        // whatever day the grid happens to be showing.
        if ((e.key === "Enter" || e.key === "Tab") && cursor) { e.preventDefault(); pickDay(ymd(cursor)); return true; }
        return false;
      }
      if (e.key === "ArrowDown") { e.preventDefault(); move(1); return true; }
      if (e.key === "ArrowUp") { e.preventDefault(); move(-1); return true; }
      if (e.key === "Enter" || e.key === "Tab") {
        // Enter with nothing highlighted stays the page's Enter — the picker
        // is a suggestion over the text, not a gate in front of it.
        if (active < 0 || !hits[active]) return false;
        e.preventDefault();
        pick(hits[active]);
        return true;
      }
      return false;
    },
    /** Re-label the chips — for when profiles land after a render. */
    repaint,
    close: closeList,
  };
}
