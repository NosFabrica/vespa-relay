// The search box, as a field that renders what it holds: `from:`/`to:` as a
// face and a name, `since:`/`until:` as a date pill, `#tag` and NIP-73 scopes
// as outline pills, `group:` as a name over its id. The value stays the plain
// text and every rendering is a view of it. `from:`, `to:` and `group:` open
// pickers over the network and `since:`/`until:` a calendar; all share one
// square of screen with the results popup, so while a token is being built
// this module owns the arrows and Enter and app.js stands down. It is a
// contenteditable, since an <input> cannot hold a picture, with `value`,
// `select()` and single-line behaviour given back so app.js never notices.

import { npub, shortNpub } from "./shared/nip19.js";
import { esc, clip } from "./shared/format.js";
import { profiles, displayName, enrichProfiles } from "./shared/profiles.js";
import { avatarHtml } from "./shared/avatar.js";
import { tokenize, mentionAt, dateAt, groupAt, drawable, ymd, scopeIds } from "./shared/query.js";
import { where as groupWhere } from "./shared/groups.js";
import { groupName, knowsGroup, enrichGroupNames } from "./shared/groupnames.js";
import {
  DOW, dayLabel, midnight, monthGrid, quickPicks, sameMonth, shiftDays, shiftMonths, typedMonth,
} from "./shared/calendar.js";

const PICKER_LIMIT = 8;
const DEBOUNCE_MS = 150;

/** The card's own face for a person, score chip and all; the size is the caller's. */
const faceHtml = (pubkey, size) => avatarHtml((profiles.get(pubkey) || {}).picture, pubkey, size);

/**
 * Does this reader raise a keyboard to type? A soft keyboard rises only for
 * a focus a finger caused, so a caret placed by script is a field that cannot
 * be typed into. `pointer: coarse` is the primary pointer, asked when it
 * matters rather than cached: a tablet gains a keyboard by being docked.
 */
export const softKeyboard = () => window.matchMedia("(pointer: coarse)").matches;

// Hoisted: the first is tested against every keystroke's beforeinput.
const NEWLINE = /[\r\n]/;
const NEWLINES = /[\r\n]+/g;

export function mountSearchField(el, list, { lookup, lookupGroup, unlockGroups, onEdit, onSubmit, paintScores }) {
  let mention = null;   // the from:/to: token being built, from mentionAt()
  let hits = [];        // pubkeys currently offered
  let active = -1;      // which one is highlighted
  let timer = null;
  let reqId = 0;
  let day = null;       // the since:/until: token being built, from dateAt()
  let month = null;     // the month the grid is showing (a Date on its 1st)
  let cursor = null;    // the day the keyboard is on, or null while the mouse leads
  let group = null;     // the group: token being built, from groupAt()
  let groups = [];      // the candidates currently offered, from lookupGroup()
  let groupLock = null; // what lookupGroup() says about the reader's locked groups
  let regroup = false;  // re-ask the group lookup even though the token is unchanged

  // The one non-group row: an action with no id, which [takeEnter]
  // dispatches on rather than picks.
  const UNLOCK_ROW = { unlock: true };
  // Which list the arrows and Enter walk. The two network pickers share
  // `active`, since one caret is inside one token. The unlock action is last
  // so its index matches its place among the rendered `.popup-item`s.
  const groupRows = () => (groupLock && groupLock.state === "denied" ? [...groups, UNLOCK_ROW] : groups);
  const liveRows = () => (group ? groupRows() : hits);

  /** Fill the chips on the faces just drawn. Fire and forget: a face must not wait on its score. */
  const fillScores = () => { if (paintScores) Promise.resolve(paintScores()).catch(() => {}); };

  // ---- the value, and where the caret is inside it ------------------------
  //
  // Every offset here is an index into the value string, never the DOM. A
  // chip is worth its whole token, so splicing at an offset and re-rendering
  // puts the caret back where it was.

  // A chip is any node drawn over a token, keyed on `dataset.token`.
  const isChip = (n) => n && n.nodeType === 1 && n.dataset && n.dataset.token != null;
  const lenOf = (n) => (n.nodeType === 3 ? n.data.length : isChip(n) ? n.dataset.token.length : (n.textContent || "").length);

  function readValue() {
    let out = "";
    for (const n of el.childNodes) out += n.nodeType === 3 ? n.data : isChip(n) ? n.dataset.token : n.textContent || "";
    // A contenteditable inserts NBSP to keep trailing spaces; left in, the
    // token fails every \s in query.js.
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
    // The fallback walks the whole field, and this runs on every keystroke.
    if (!r || !inside(r.startContainer) || !inside(r.endContainer)) {
      const n = readValue().length;
      return [n, n];
    }
    return [indexOf(r.startContainer, r.startOffset), indexOf(r.endContainer, r.endOffset)];
  }

  /** Where a drop landed, in value offsets: a drop does not move the selection first. */
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
        // Never inside a chip: it is contenteditable=false.
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
   * What a chip currently draws; unchanged means no repaint. The separator
   * is written as the escape `\u0000`: a literal NUL makes git treat the
   * file as binary. A space would not do, since ("Alice B", "") and
   * ("Alice", "B") must not share a key.
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
    // The hover is the token itself, so the string stays reachable.
    span.title = span.dataset.token;
  }

  /**
   * A group pill, drawn from whatever groupnames knows now and repainted
   * when a name arrives. `dataset.face` holds what the cache said, not the
   * clipped drawing; an empty face means "asked, no one name", not "not
   * asked". The hover carries the id whether or not a name replaced it.
   */
  function paintGroupChip(span) {
    const id = span.dataset.gid;
    const known = groupName(id);
    // Clipped, where the id is not: a name is a stranger's string, and the
    // pill's `max-width` bounds pixels without bounding the DOM.
    const name = clip(known, 48);
    span.innerHTML = `<b>group:</b><span class="scope-id">${esc(name || id)}</span>`;
    span.dataset.face = known;
    span.title = `${span.dataset.token} — a NIP-29 group filter: events posted to ` +
      (name ? `“${name}”, the group whose id is ${id}` : `the group whose id is ${id}`);
  }

  function chipEl(seg) {
    const span = document.createElement("span");
    span.contentEditable = "false";
    span.dataset.token = seg.raw;
    if (seg.type === "tag") {
      // No lookup and no repaint: the pill says this word left the search
      // string and became a filter, not that anything is hidden.
      span.className = "hashtag";
      span.textContent = seg.raw;
      // The hover says what is asked when that differs: `#Nostr` filters for `nostr`.
      span.title = seg.raw.slice(1) === seg.tag
        ? `tag filter — this word is a #t/#l/#i filter, not a search term`
        : `tag filter for “${seg.tag}”`;
      return span;
    }
    if (seg.type === "scope") {
      // The value draws as typed; what is asked may be spelled differently
      // (isbn: drops hyphens, doi: lowers), and the hover carries that.
      span.className = "scopepill";
      span.innerHTML = `<b>${esc(seg.field)}:</b><span class="scope-id">${esc(seg.value)}</span>`;
      const asks = scopeIds(seg.field, seg.value);
      span.title = `${seg.raw} — a NIP-73 scope filter: comments written on ${asks[0] || seg.value}`;
      return span;
    }
    if (seg.type === "group") {
      // A name drawn over an id nobody can recognise, even the reader who
      // picked it. A view of the token only: the value still reads
      // `group:0fe5…`, the hover carries the id, and the name is re-derived
      // from the id on every render. Which name is shared/groupnames.js's.
      span.className = "scopepill grouppill";
      span.dataset.gid = seg.id;
      paintGroupChip(span);
      return span;
    }
    if (seg.type === "date") {
      // Quiet like a hashtag, since the token is readable; the pill only
      // respells `2026-08-06` as `6 Aug 2026`.
      span.className = "datepill";
      span.innerHTML = `<b>${esc(seg.field)}</b>${esc(dayLabel(new Date(seg.at * 1000)))}`;
      // The hover carries which second of the day the bound lands on.
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
   * Re-label in place the chips whose name or face changed; no node is
   * replaced, so the caret and any selection across a chip hold.
   */
  function repaint() {
    let drew = false;
    for (const c of el.querySelectorAll(".mention")) {
      if (c.dataset.face !== chipFace(c.dataset.pk)) { paintChip(c); drew = true; }
    }
    // A repainted chip is a new score chip.
    if (drew) fillScores();
    // Group pills follow the same rule with nothing to fill: a repaint only
    // turns a hex id into a name.
    for (const c of el.querySelectorAll(".grouppill")) {
      if (c.dataset.face !== groupName(c.dataset.gid)) paintGroupChip(c);
    }
  }

  function render(text, caret, typingAt = caret) {
    const segs = drawable(text, typingAt);
    el.innerHTML = "";
    const unknown = [];
    const strangeGroups = [];
    let chips = 0;
    for (const seg of segs) {
      if (seg.type === "text") { if (seg.text) el.appendChild(document.createTextNode(seg.text)); continue; }
      el.appendChild(chipEl(seg));
      if (seg.type === "group" && !knowsGroup(seg.id)) strangeGroups.push(seg.id);
      if (seg.type === "key") {
        chips++;
        if (!profiles.has(seg.pubkey)) unknown.push(seg.pubkey);
      }
    }
    if (caret != null) setCaret(caret);
    if (chips) fillScores();
    // Repaint only when the lookup learned something, as the results list does.
    if (unknown.length) enrichProfiles(unknown).then((n) => { if (n) repaint(); }).catch(() => {});
    // A `group:` token from a URL or a paste was never offered by the
    // picker, so this is the path that names it.
    if (strangeGroups.length) {
      enrichGroupNames(strangeGroups).then((n) => { if (n) repaint(); }).catch(() => {});
    }
  }

  /**
   * Does the DOM still match what the text tokenizes to? The field is left
   * alone (caret, IME, undo) until a token finished or broke, or the browser
   * invented a node: a `<br>` from Enter, a `<div>` from a paste.
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
    group = null;
    groups = [];
    groupLock = null;
    list.classList.remove("open");
    list.innerHTML = "";
    // Back to the markup's shape, so a calendar's dialog role does not outlive it.
    list.setAttribute("role", "listbox");
    // Handed back to the results popup, which owns these the rest of the time.
    el.setAttribute("aria-expanded", "false");
    el.setAttribute("aria-controls", "popup");
    el.removeAttribute("aria-activedescendant");
    el.removeAttribute("aria-haspopup");
  }

  /**
   * Raise the box and say what is in it. A list of people is a listbox; a
   * calendar has nav, a grid and shortcuts, so it is a dialog with the
   * listbox narrowed to the days, and aria-haspopup says which.
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
   * Splice a finished token over the partial the caret is in. One trailing
   * space, and the caret lands past it, outside the token, where the pill
   * draws.
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

  /** Which row is highlighted, as a class flip on existing rows, announced through aria-activedescendant. */
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
    // Only when the keyboard moved it: scrollIntoView forces layout and
    // scrolls every scrollable ancestor, and a fresh list highlights row 0.
    if (scroll) row.scrollIntoView({ block: "nearest" });
  }

  /** `rows` null means "still asking"; an empty array means "asked, none". */
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
    // Asked of the DOM: a note is also a body built from a non-empty argument.
    if (list.querySelector(".popup-item")) fillScores();
  }

  function move(delta) {
    const rows = liveRows();
    if (!rows.length) return;
    active = (active + delta + rows.length) % rows.length;
    markActive(true);
  }

  /**
   * The unfinished `from:`/`to:` token the caret sits in, or null. Derived
   * from the text, not the open list: blur closes the list while the token
   * stays half-written. With no selection in the field the caret reads as
   * the end of the value.
   */
  function pendingMention() {
    const m = mentionAt(readValue(), caretIndex());
    return m && !m.complete ? m : null;
  }

  /**
   * Re-read the token under the caret and keep the picker in step.
   * Idempotent: an unchanged token returns early, so this is safe from every
   * edit and every caret move.
   */
  function updateMention() {
    const next = pendingMention();
    if (!next) { if (mention) closeList(); return; }
    const sameToken = !!mention && mention.field === next.field && mention.start === next.start;
    if (sameToken && mention.partial === next.partial) return;
    mention = next;
    // The last rows stay up while the next answer is fetched, and are
    // dropped only for a different token or an emptied partial.
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
        // A relay not answering is not evidence that nobody matches.
        return;
      }
      // Outside the catch: a lookup may fail, drawing what it returned may not.
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

  // ---- the group picker ----------------------------------------------------
  //
  // The people picker's shape over NIP-29 groups: the rows show a name and a
  // place, and a pick writes the id. No face on a row, since the only key a
  // group row has is the host relay's, and a score chip on it would answer a
  // question about the relay under a list of channels.

  const GROUP_ROW_ID = (i) => `group-opt-${i}`;

  /**
   * One candidate: what it is called and where it is. A url out of the
   * reader's own `group` tag and a name a signing key claims for itself are
   * drawn differently; `ambiguous` warns that a pick filters on the bare id.
   */
  function groupRowHtml(cand, i) {
    const place = groupWhere(cand, displayName(profiles.get(cand.host)));
    const name = (cand.name || "").trim() || cand.id;
    const sub = clip((cand.about || "").trim(), 90);
    return `
      <div class="popup-item" id="${GROUP_ROW_ID(i)}" data-g="${i}" role="option" aria-selected="false">
        <div class="row-main">
          <div class="row-name">${esc(clip(name, 80))}${cand.mine ? `<span class="group-mine">${cand.secret ? "private" : "yours"}</span>` : ""}</div>
          <div class="row-about">${sub ? `${esc(sub)} · ` : ""}<span class="${place.exact ? "mono" : "group-host"}">${esc(place.text)}</span></div>
        </div>
        ${cand.ambiguous ? `<span class="group-warn" title="More than one group here carries the id “${esc(cand.id)}”, and a search filters on the id alone — so the results include all of them.">shared id</span>` : ""}
      </div>`;
  }

  /**
   * What the list says about the reader's locked groups. Two states are
   * notes; the third is an action and so a real `.popup-item`, last, so the
   * arrows reach it. [takeEnter] tells it by its `unlock` flag, never by
   * position.
   */
  function lockHtml(at) {
    if (!groupLock) return "";
    if (groupLock.state === "asking") {
      return `<div class="popup-note group-lock">Waiting for your extension to unlock your private groups…</div>`;
    }
    if (groupLock.state === "unsupported") {
      return `<div class="popup-note group-lock">Some of your groups are encrypted (${esc(groupLock.scheme)}), and this extension cannot decrypt them.</div>`;
    }
    return `
      <div class="popup-item group-lock group-unlock" id="${GROUP_ROW_ID(at)}" data-unlock="1" role="option" aria-selected="false">
        <div class="row-main"><div class="row-name">Unlock your private groups</div>
        <div class="row-about">Some of your groups are encrypted; your extension will ask before opening them.</div></div>
      </div>`;
  }

  /** `rows` null means "still asking"; an empty array means "asked, none". */
  function renderGroupList(rows) {
    if (!group) return;
    const head = `<div class="popup-head"><span>Posted in</span><span class="timing">group:</span></div>`;
    let body;
    if (rows == null) body = `<div class="popup-note">Finding groups…</div>`;
    else if (rows.length) body = rows.map(groupRowHtml).join("");
    // The empty states are different facts: with nothing typed, an empty
    // list means the reader's kind 10009 was not found; a locked list is
    // unknown rather than empty, and the footer carries it.
    else if (groupLock) body = "";
    else if (!group.partial) body = `<div class="popup-note">No groups of yours here yet — type a name, or paste a group id</div>`;
    else body = `<div class="popup-note">No group matches “${esc(clip(group.partial, 40))}”</div>`;
    // The action's index is the count of group rows above it.
    list.innerHTML = head + body + lockHtml(rows ? rows.length : 0);
    openList("listbox", "Groups");
    markActive();
  }

  /**
   * Re-read the `group:` token under the caret and keep the picker in step:
   * updateMention()'s twin, except that an empty partial is a question with
   * an answer (the reader's own groups) and asks like any other.
   */
  function updateGroups() {
    // `regroup` is the one thing past the unchanged-token guard: an unlock
    // changes what the same token answers. Read and cleared above every
    // return, or it is spent on the next `group:` typed.
    const forced = regroup;
    regroup = false;
    const next = pendingGroup();
    if (!next) { if (group) closeList(); return; }
    const sameToken = !!group && group.start === next.start;
    if (!forced && sameToken && group.partial === next.partial) return;

    group = next;
    if (!sameToken) { groups = []; groupLock = null; active = -1; }
    renderGroupList(groups.length ? groups : null);
    clearTimeout(timer);
    const id = ++reqId;
    timer = setTimeout(async () => {
      let found;
      try {
        found = await lookupGroup(next.partial);
      } catch (e) {
        // The last rows stay up: updateMention's rule.
        return;
      }
      // Outside the catch, as there.
      if (id !== reqId || !group) return;
      const rows = (found && found.rows) || [];
      groupLock = (found && found.lock) || null;
      groups = rows.slice(0, PICKER_LIMIT);
      // Off the walkable list, not `groups`: with no groups and a refused
      // unlock the only row is the action, and it must start highlighted.
      active = groupRows().length ? 0 : -1;
      renderGroupList(groups);
    }, DEBOUNCE_MS);
  }

  /**
   * The reader asking for a dismissed permission dialog back; nothing else
   * reopens one. The notice flips to `asking` now, since the dialog is up
   * from this moment.
   */
  async function unlock() {
    if (!group || !unlockGroups) return;
    groupLock = { state: "asking" };
    renderGroupList(groups);
    // Not awaited: the answer is a human's, on their own schedule, and
    // refreshGroups() puts it on screen when it lands.
    try { unlockGroups(); } catch (e) { /* refreshGroups reports whatever state resulted */ }
  }

  function pickGroup(cand) {
    if (!group || !cand) return;
    replaceToken(group, `group:${cand.id}`);
  }

  // ---- the calendar --------------------------------------------------------
  //
  // Which days exist is shared/calendar.js's; here is only how they are
  // drawn and what a click does. No lookup, no debounce, no request id.

  const DAY_ID = (value) => `cal-day-${value}`;
  const HEAD = { since: "Written on or after", until: "Written on or before" };

  // markDay draws the cursor on the cells this built, as markActive does for
  // the people rows. tabindex="-1" on every control: the caret has to stay in
  // the field, which a pick splices into, and the keyboard reaches all of
  // this through the field.
  function calendarHtml() {
    const today = midnight(new Date());
    const grid = monthGrid(month || shiftMonths(today, 0), today);
    // The blanks before the 1st are cells so the 1st lands under its own
    // weekday; aria-hidden, so the listbox holds nothing but days.
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
   * Which day the keyboard is on, as a class flip on existing cells, and the
   * only writer of aria-activedescendant on this side.
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
   * Point the grid at a month, dropping a cursor that would fall off it: the
   * highlighted day is on screen or gone, never picked sight unseen.
   */
  function setMonth(next) {
    month = next;
    if (!sameMonth(cursor, month)) cursor = null;
  }

  /** Re-read the `since:`/`until:` token under the caret and keep the grid in step. */
  function showCalendar(next) {
    // Idempotent, as updateMention() is: a grid rebuilt for an unchanged
    // token would lose the month the reader had stepped to.
    const same = !!day && day.field === next.field && day.start === next.start && listOpen();
    if (same && day.partial === next.partial) return;
    day = next;
    // Both network pickers stand down with `timer`/`reqId`, or a late reply
    // lands in a list no longer on screen.
    if (mention || group) { clearTimeout(timer); mention = null; hits = []; group = null; groups = []; active = -1; }
    // The typed month wins; failing that the same token keeps its month, and
    // a fresh calendar starts on this one.
    setMonth(typedMonth(next.partial) || (same ? month : null) || shiftMonths(midnight(new Date()), 0));
    // Always a full draw: the head and the shortcuts belong to the token.
    renderCalendar();
  }

  function stepMonth(by) {
    if (!day) return;
    setMonth(shiftMonths(month || midnight(new Date()), by));
    renderCalendar();
  }

  /**
   * Move the keyboard cursor by `by` days, opening the month it lands in.
   * The first press starts from today if in view, else the shown month's 1st.
   * The month is set from the day, so setMonth cannot drop the cursor.
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
    // A step inside the month is a class flip, not a grid.
    if (crossed) renderCalendar();
    else markDay();
  }

  function pickDay(value) {
    if (!day || !value) return;
    replaceToken(day, `${day.field}:${value}`);
  }

  // ---- what the caret is inside, and which picker that opens ---------------

  /**
   * The unfinished `since:`/`until:` token the caret sits in, or null. Read
   * from the text every time, as [pendingMention] is and for the same reason.
   */
  function pendingDate() {
    const d = dateAt(readValue(), caretIndex());
    return d && !d.complete ? d : null;
  }

  /**
   * The `group:` token the caret sits in, or null. No `complete` test:
   * [groupAt] never reports one, and the space a pick writes ends the token.
   */
  const pendingGroup = () => groupAt(readValue(), caretIndex());

  /**
   * Re-read the token under the caret and put the right picker under it.
   * The date half goes first, being the cheap one; at most one prefix
   * matches, and whichever does not match is shut. The two network pickers
   * shut each other explicitly because they share `timer` and `reqId`.
   */
  function updateToken() {
    const next = pendingDate();
    if (next) { showCalendar(next); return; }
    if (day) closeList();
    if (pendingGroup()) {
      if (mention) closeList();
      updateGroups();
      return;
    }
    if (group) closeList();
    updateMention();
  }

  /**
   * Enter, however it arrived: true when a picker consumed it. Shared by
   * keydown and beforeinput (a phone's action key is not reliably a key), and
   * Tab lands here too. Nothing highlighted is not consumed: the picker is a
   * suggestion over the text, not a gate, so Enter falls through to the search.
   */
  function takeEnter() {
    if (!listOpen()) return false;
    if (day) { if (!cursor) return false; pickDay(ymd(cursor)); return true; }
    const rows = liveRows();
    if (active < 0 || !rows[active]) return false;
    // For a group the fall-through is what lets Enter search for a pasted id
    // this relay has never seen. The action is told by its flag, never its
    // position, or a row could splice `group:undefined` into the box.
    if (rows[active].unlock) { unlock(); return true; }
    if (group) pickGroup(rows[active]);
    else pick(rows[active]);
    return true;
  }

  el.addEventListener("input", (e) => {
    const text = readValue();
    // Empty means empty: the browser leaves a `<br>` behind when the last
    // character goes, and `:empty` draws the placeholder.
    if (!text) { if (el.innerHTML) el.innerHTML = ""; }
    // Never re-render mid-composition: rebuilding the nodes under an IME
    // tears down the text it is composing. No token can finish inside one.
    else if (!e.isComposing) { const at = caretIndex(); if (structureChanged(text, at)) render(text, at); }
    updateToken();
    onEdit && onEdit();
  });

  // Paste and drop insert text: left to the browser they insert markup, and
  // this field's value is the concatenation of its nodes.
  function insertPlain(e, raw, at) {
    e.preventDefault();
    const text = String(raw || "").replace(/\s+/g, " ");
    const [from, to] = at == null ? selectionRange() : [at, at];
    // A paste is not somebody midway through a word: `typingAt` null draws
    // every token it finds.
    replaceRange(from, to, text, from + text.length, null);
    updateToken();
    onEdit && onEdit();
  }
  el.addEventListener("paste", (e) => insertPlain(e, (e.clipboardData || window.clipboardData).getData("text/plain")));
  el.addEventListener("drop", (e) => insertPlain(e, e.dataTransfer && e.dataTransfer.getData("text/plain"), dropIndex(e)));

  /**
   * The other door Enter comes through, and on a phone the only one: a soft
   * keyboard's action key arrives as an inserted line break, not a keydown.
   * The break is refused, the field being one line by construction, and any
   * text committed with it (`hello\n` from an IME) is kept.
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
   * A caret move finishes a hashtag, a date or a scope without editing:
   * `#nos|` is text while the caret is in it and a pill once it leaves. A
   * comparison of two short lists unless the drawing would change.
   */
  function syncPills(typingAt = caretIndex()) {
    const text = readValue();
    if (text && structureChanged(text, typingAt)) render(text, typingAt, typingAt);
  }

  // A caret move can enter or leave a token. The arrows a picker is using
  // move the highlight, not the caret, and are left out.
  el.addEventListener("click", () => { updateToken(); syncPills(); });
  // Coming back to a half-written token picks up where it left off: blur
  // shut the picker, and typing another character is not the only way back.
  el.addEventListener("focus", () => { updateToken(); syncPills(); });
  el.addEventListener("keyup", (e) => {
    if (e.key === "ArrowLeft" || e.key === "ArrowRight" || e.key === "Home" || e.key === "End") { updateToken(); syncPills(); }
  });
  el.addEventListener("blur", () => {
    // Nobody is typing a word in a field they have left: every tag pills,
    // and no caret is placed, which would scroll an unfocused field into view.
    syncPills(null);
    // The selection outlives the focus, and a caret is drawn from it: a
    // range left in this div is a blinking caret in a field the reader left.
    // Dropped only when the range is ours.
    const sel = document.getSelection();
    if (sel && sel.rangeCount && el.contains(sel.getRangeAt(0).startContainer)) sel.removeAllRanges();
    // A click on the picker must land before the picker disappears; its
    // mousedown has already fired by the time blur does.
    setTimeout(() => { if (!list.contains(document.activeElement)) closeList(); }, 120);
  });

  // preventDefault throughout: the caret in the field is what a pick splices
  // into. The month arrows need it too, or a field losing focus to `‹` would
  // close the calendar being paged.
  list.addEventListener("mousedown", (e) => {
    const step = e.target.closest(".cal-step");
    if (step) { e.preventDefault(); stepMonth(Number(step.dataset.step)); return; }
    const cell = e.target.closest(".cal-day, .cal-pick");
    if (cell) { e.preventDefault(); pickDay(cell.dataset.day); return; }
    // Not a pick, and needs the preventDefault for the month arrows' reason.
    if (e.target.closest("[data-unlock]")) { e.preventDefault(); unlock(); return; }
    const row = e.target.closest(".popup-item");
    if (!row) return;
    e.preventDefault();
    // Which list a row belongs to is on the row: both pickers draw the same
    // `.popup-item`.
    if (row.dataset.g != null) pickGroup(groups[Number(row.dataset.g)]);
    else pick(hits[Number(row.dataset.i)]);
  });

  /**
   * A touch anywhere else on the page leaves the field, as it would an
   * <input>: a contenteditable loses focus to a mouse but not to a finger.
   * Capture, so nothing can swallow the event; passive, since it cancels
   * nothing. Never inside .search-wrap, where a pick needs the caret.
   */
  document.addEventListener("pointerdown", (e) => {
    if (document.activeElement !== el) return;
    const t = e.target;
    if (t && t.closest && t.closest(".search-wrap")) return;
    el.blur();
  }, { capture: true, passive: true });

  /**
   * Leaving the page takes the keyboard, so the field lets the caret go: a
   * field still focused when the page thaws cannot raise a keyboard by being
   * tapped. Three events, since no one of them fires everywhere; only where
   * a keyboard has to be raised at all.
   */
  const releaseOnHide = () => { if (document.activeElement === el && softKeyboard()) el.blur(); };
  window.addEventListener("pagehide", releaseOnHide);
  window.addEventListener("pageshow", releaseOnHide);
  document.addEventListener("visibilitychange", releaseOnHide);

  document.addEventListener("click", (e) => { if (!e.target.closest(".search-wrap")) closeList(); });

  // ---- what app.js needs ---------------------------------------------------

  Object.defineProperty(el, "value", {
    get: readValue,
    // A programmatic set is a restore (the URL, the clear button), never a
    // keystroke: no onEdit, as an input's value fires no input event.
    set(v) {
      // The last door a line break can come through: `?q=` carries whatever
      // it carries, and a `\n` under `white-space: pre` is a two-line box.
      const text = String(v ?? "").replace(NEWLINES, " ");
      closeList();
      // typingAt null: a restore or a clear is not typing, so a tag the
      // caret lands after still draws as the filter it is.
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
     * Is a token being built now: an open picker, or an unfinished token
     * under the caret? The second half matters because the box closes on
     * blur and Escape while the token stays; app.js gates the results popup
     * on it.
     */
    get picking() { return listOpen() || !!pendingMention() || !!pendingDate() || !!pendingGroup(); },
    /**
     * Is a picker on screen? Narrower than `picking`: while one is up it
     * owns the field's aria attributes, and closePopup() must not lower them.
     */
    get pickerOpen() { return listOpen(); },
    /**
     * The keys a picker owns while open, called by app.js rather than racing
     * it in the capture phase. The calendar reads the arrows as a grid (a
     * day sideways, a week up and down) and takes the Page keys for months;
     * Escape hands Left and Right back to the caret.
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
        // Enter on nothing highlighted stays the page's Enter, as over the
        // people list: a search for the partial date, not a pick.
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
    /**
     * Ask the group lookup again, for an unlock that landed after the draw.
     * A no-op unless a `group:` token is still under the caret.
     */
    refreshGroups() {
      if (!group) return;
      regroup = true;
      updateGroups();
    },
    /** Re-label the chips — for when profiles land after a render. */
    repaint,
    close: closeList,
  };
}
