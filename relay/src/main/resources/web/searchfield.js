// The search box, as a field that RENDERS what it holds.
//
// Two things live here, and they are one feature:
//
//   1. `from:npub1…` / `to:npub1…` draw as a face and a name instead of 63
//      characters of bech32. The field's VALUE is still the plain text — the
//      URL, the export and the query builder all read `from:npub1…` — so the
//      rendering is a view of the string and never a second source of truth.
//   2. Typing `from:` or `to:` opens a people picker over the network, ranked
//      by your own web of trust because the lookup rides the AUTHENTICATED
//      socket. Picking somebody writes their npub into the text.
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
// The picker and the results popup are mutually exclusive on purpose. They
// occupy the same square of screen, and a keystroke inside `from:ali` is not
// a search for "from:ali" — so while a mention is being built this module
// owns the arrows and Enter, and app.js is told to stand down.

import { npub, shortNpub } from "./shared/nip19.js";
import { esc, clip } from "./shared/format.js";
import { profiles, displayName, enrichProfiles } from "./shared/profiles.js";
import { BLANK, hueOf } from "./cards/base.js";
import { tokenize, mentionAt } from "./shared/query.js";

const PICKER_LIMIT = 8;
const DEBOUNCE_MS = 150;

/** The same face a card draws, at whatever size the caller's CSS asks for. */
function faceHtml(pubkey, cls) {
  const p = profiles.get(pubkey);
  const style = `style="--h:${hueOf(pubkey)}"`;
  return p && p.picture
    ? `<img class="${cls}" ${style} src="${esc(p.picture)}" alt="" loading="lazy" referrerpolicy="no-referrer"
         onerror="this.classList.add('gen');this.src='${BLANK}'" />`
    : `<span class="${cls} gen" ${style}></span>`;
}

/**
 * Take over `el` as the rendered search field, with `list` as its picker.
 *
 * `lookup(partial)` resolves to hex pubkeys, best first — app.js supplies it,
 * because whose socket the question goes down is app.js's business, not this
 * module's. `onEdit()` is called after every human edit, once this module's
 * own state is already current: app.js reads `mentioning` inside it and would
 * otherwise be reacting to the keystroke before last.
 */
export function mountSearchField(el, list, { lookup, onEdit }) {
  let mention = null;   // the token being built, from mentionAt()
  let hits = [];        // pubkeys currently offered
  let active = -1;      // which one is highlighted
  let timer = null;
  let reqId = 0;

  // ---- the value, and where the caret is inside it ------------------------
  //
  // Every offset this module passes around is an index into the VALUE string,
  // never into the DOM. A chip is worth its whole token, so splicing text at
  // an offset and re-rendering puts the caret back exactly where it was —
  // which is the only reason a chip can appear mid-typing without the caret
  // jumping to the end.

  const isChip = (n) => n && n.nodeType === 1 && n.classList.contains("mention");
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
    const end = readValue().length;
    if (!sel || !sel.rangeCount) return [end, end];
    const r = sel.getRangeAt(0);
    const inside = (n) => n === el || el.contains(n);
    if (!inside(r.startContainer) || !inside(r.endContainer)) return [end, end];
    return [indexOf(r.startContainer, r.startOffset), indexOf(r.endContainer, r.endOffset)];
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
    sel.removeAllRanges();
    sel.addRange(r);
  }

  // ---- rendering the value ------------------------------------------------

  function paintChip(span) {
    const pk = span.dataset.pk;
    const field = span.dataset.field;
    const name = displayName(profiles.get(pk)) || shortNpub(pk);
    span.innerHTML =
      (field ? `<span class="mention-kind">${esc(field)}</span>` : "") +
      faceHtml(pk, "avatar") +
      `<span class="mention-name">${esc(clip(name, 32))}</span>`;
    // The hover is the token itself: the field shows a person, and the string
    // it stands for has to stay reachable without deleting it to look.
    span.title = span.dataset.token;
  }

  function chipEl(seg) {
    const span = document.createElement("span");
    span.className = "mention";
    span.contentEditable = "false";
    span.dataset.token = seg.raw;
    span.dataset.pk = seg.pubkey;
    if (seg.field) span.dataset.field = seg.field;
    paintChip(span);
    return span;
  }

  /** Re-label the chips in place — no node is replaced, so the caret holds. */
  function repaint() {
    for (const c of el.querySelectorAll(".mention")) paintChip(c);
  }

  function render(text, caret) {
    const segs = tokenize(text);
    el.innerHTML = "";
    const unknown = [];
    for (const seg of segs) {
      if (seg.type === "text") { if (seg.text) el.appendChild(document.createTextNode(seg.text)); continue; }
      el.appendChild(chipEl(seg));
      if (!profiles.has(seg.pubkey)) unknown.push(seg.pubkey);
    }
    if (caret != null) setCaret(caret);
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
  function structureChanged(text) {
    const want = tokenize(text).filter((s) => s.type === "key").map((s) => s.raw);
    const have = [];
    for (const n of el.childNodes) {
      if (n.nodeType === 3) continue;
      if (!isChip(n)) return true;
      have.push(n.dataset.token);
    }
    return want.length !== have.length || want.some((w, i) => w !== have[i]);
  }

  function replaceRange(from, to, insert, caret) {
    const text = readValue();
    render(text.slice(0, from) + insert + text.slice(to), caret);
  }

  // ---- the people picker ---------------------------------------------------

  const listOpen = () => list.classList.contains("open");

  function closeList() {
    clearTimeout(timer);
    mention = null;
    hits = [];
    active = -1;
    list.classList.remove("open");
    list.innerHTML = "";
    el.setAttribute("aria-expanded", "false");
  }

  function rowHtml(pk, i) {
    const p = profiles.get(pk) || {};
    const name = displayName(p) || shortNpub(pk);
    const sub = (p.nip05 || "").trim() || clip(p.about || "", 90) || npub(pk);
    return `
      <div class="popup-item${i === active ? " active" : ""}" data-i="${i}" role="option" aria-selected="${i === active}">
        ${faceHtml(pk, "avatar")}
        <div class="row-main">
          <div class="row-name">${esc(clip(name, 80))}</div>
          <div class="row-about">${esc(sub)}</div>
        </div>
      </div>`;
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
    list.classList.add("open");
    el.setAttribute("aria-expanded", "true");
  }

  function move(delta) {
    if (!hits.length) return;
    active = (active + delta + hits.length) % hits.length;
    renderList(hits);
    const row = list.querySelector(".popup-item.active");
    if (row) row.scrollIntoView({ block: "nearest" });
  }

  /**
   * Re-read the token under the caret and keep the picker in step.
   *
   * Idempotent by design: an unchanged token returns early rather than
   * re-querying and resetting the highlight, so this is safe to call from
   * every edit AND every caret move.
   */
  function updateMention() {
    const found = mentionAt(readValue(), caretIndex());
    const next = found && !found.complete ? found : null;
    if (!next) { if (mention) closeList(); return; }
    if (mention && mention.field === next.field && mention.start === next.start && mention.partial === next.partial) return;
    mention = next;
    hits = [];
    active = -1;
    renderList(null);
    clearTimeout(timer);
    if (!next.partial) return;
    const id = ++reqId;
    timer = setTimeout(async () => {
      let found2 = [];
      // A failed lookup leaves the picker saying "searching" rather than
      // claiming nobody matches — the relay not answering is not evidence
      // that the person does not exist.
      try { found2 = await lookup(next.partial); } catch (e) { return; }
      if (id !== reqId || !mention) return;
      hits = found2.slice(0, PICKER_LIMIT);
      active = hits.length ? 0 : -1;
      renderList(hits);
    }, DEBOUNCE_MS);
  }

  function pick(pubkey) {
    if (!mention || !pubkey) return;
    const token = `${mention.field}:${npub(pubkey)}`;
    const text = readValue();
    // One trailing space, so the next word is a word and not more of the
    // token — unless the caret already sits in front of one.
    const tail = text.slice(mention.end).startsWith(" ") ? "" : " ";
    const { start, end } = mention;
    closeList();
    replaceRange(start, end, token + tail, start + token.length + tail.length);
    el.focus();
    onEdit && onEdit();
  }

  // ---- wiring --------------------------------------------------------------

  el.addEventListener("input", () => {
    const text = readValue();
    // Empty means EMPTY: the browser leaves a `<br>` behind when the last
    // character goes, and `:empty` is what draws the placeholder.
    if (!text) { if (el.innerHTML) el.innerHTML = ""; }
    else if (structureChanged(text)) render(text, caretIndex());
    updateMention();
    onEdit && onEdit();
  });

  // Paste and drop insert TEXT. Left to the browser they insert markup — a
  // pasted link arrives as an <a>, a pasted paragraph as a <div> — and this
  // field's value is the concatenation of its nodes.
  function insertPlain(e, raw) {
    e.preventDefault();
    const text = String(raw || "").replace(/\s+/g, " ");
    const [from, to] = selectionRange();
    replaceRange(from, to, text, from + text.length);
    updateMention();
    onEdit && onEdit();
  }
  el.addEventListener("paste", (e) => insertPlain(e, (e.clipboardData || window.clipboardData).getData("text/plain")));
  el.addEventListener("drop", (e) => insertPlain(e, e.dataTransfer && e.dataTransfer.getData("text/plain")));

  // A caret MOVE can enter or leave a token without editing anything. Arrows
  // that the picker is using are excluded — they move the highlight, not the
  // caret, and re-reading here would reset it on every press.
  el.addEventListener("click", updateMention);
  el.addEventListener("keyup", (e) => {
    if (e.key === "ArrowLeft" || e.key === "ArrowRight" || e.key === "Home" || e.key === "End") updateMention();
  });
  el.addEventListener("blur", () => {
    // A click ON the picker must land before the picker disappears; mousedown
    // there has already fired by the time blur does, so the delay only has to
    // outlast the same tick.
    setTimeout(() => { if (!list.contains(document.activeElement)) closeList(); }, 120);
  });

  list.addEventListener("mousedown", (e) => {
    const row = e.target.closest(".popup-item");
    if (!row) return;
    e.preventDefault();   // keep the caret, which pick() is about to splice into
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
      render(text, document.activeElement === el ? text.length : null);
    },
  });

  // The "/" shortcut selects the field's contents; a div has no select().
  el.select = () => {
    const r = document.createRange();
    r.selectNodeContents(el);
    const sel = document.getSelection();
    sel.removeAllRanges();
    sel.addRange(r);
  };

  return {
    /** Is a from:/to: token being built right now? */
    get mentioning() { return listOpen(); },
    /**
     * The keys the picker owns while it is open, consumed here so app.js's
     * own handler can return. Called BY app.js rather than racing it in the
     * capture phase: which of two listeners on one element runs first is
     * registration order, and that is not a thing to depend on.
     */
    handleKey(e) {
      if (!listOpen()) return false;
      if (e.key === "ArrowDown") { e.preventDefault(); move(1); return true; }
      if (e.key === "ArrowUp") { e.preventDefault(); move(-1); return true; }
      if (e.key === "Escape") { e.preventDefault(); closeList(); return true; }
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
