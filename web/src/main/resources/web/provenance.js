// WHY AN EVENT IS IN THIS PAGE — the pills a card draws under its byline.
//
// The relay's search expansion splices events the search terms never matched:
// a profile rides in behind the Trusted List that names it, a note behind the
// label that describes it. Without a word from the card, those arrive as
// results that do not contain what was searched for, which reads as the relay
// being wrong. The pills are that word.
//
// PURE, OVER WHATEVER ARRAY IT IS HANDED. This file makes no ask of its own and
// holds no socket; it indexes an array and reads pointers out of it. Who fills
// that array has changed once and may change again, which is exactly why the
// rules live behind that seam.
//
// It used to be filled by the relay alone: the search expansion sent a pointer
// and everything it named on one subscription, so the answer the page was
// going to render anyway already held both, and the row cost no round trip. It
// no longer does — a search that asks for `kinds:[0]` is answered with the
// profiles the expansion found THROUGH those lists, labels and assertions and
// not with the pointers themselves — so app.js now seeds twice: once off the
// answer, and again once shared/pointers.js has fetched what the answer left
// out. Both seeds go through here unchanged.
//
// What that costs is the trust gate. "It arrived, so the reader asked for it"
// was a fact about the relay's expansion and is NOT a fact about a client
// fetch: the reference socket narrows nothing. [DECLARATION_KINDS] and the
// `gated` tone still mean what they say only because pointers.js re-imposes
// the rule as an `authors` filter off the reader's own kind 10040. If a third
// filler ever appears, it owes the row the same.
//
// NOT BY ADJACENCY, and that distinction is now load-bearing. A subject used to
// arrive directly behind its pointer, and reading the pair off neighbouring
// positions would have worked. It no longer does: the store places a spliced
// member by the confidence its list expressed about it, so a doubted member
// sinks past the organic hits between them and can land anywhere in the page.
// Both passes below are order-independent — index the whole page, then walk the
// pointers — which is why that change cost this file nothing. Keep it that way.
//
// That makes the row deliberately PARTIAL, in three ways worth knowing before
// reading a pill as a complete account:
//
//   - the expansion's budgets (100 per event, 1,000 per request) cap what the
//     ANSWER carries, so a truncated page draws fewer pills than a whole one;
//   - the follow-up read is bounded too — one batch of 100 targets per filter,
//     and an ungated label read stops at pointers.js's LABEL_LIMIT;
//   - the trust gate drops declarations from signers this reader never
//     delegated, so the row is observer-relative by design.
//
// A fourth used to head that list and no longer holds, and the change is worth
// knowing before reading a row: a label that did not match the SEARCH never
// arrived, so the row could only ever say why a card was HERE. Asked by target
// instead, every label the relay holds about it comes back — which is nearer
// to "what has been said about this", the question that belongs on the entity
// page. That drift is not a decision this file made; it is what is left when
// the relay stops telling the client which labels matched, and the client
// cannot re-derive it. Bounding it is pointers.js's LABEL_LIMIT and the
// collapse below, and neither restores the old meaning.
//
// No DOM and one import — nip19's addrOf, because the address a pill links to
// must be the same string the card's own permalink uses, and a second spelling
// is how those two come to disagree. The rules are otherwise pure so
// web/src/test/js can hold them,
// and a pill names its DESTINATION rather than spelling a url — cards/base.js
// owns every href on this page, and two spellings of one route is the bug that
// rule exists to prevent.

/** The label namespaces that are METADATA, not provenance. */
//
// `ISO-639-1` is 87% of the labels on staging — a pill reading "en" on every
// card is furniture, and it would crowd out the one pill that says something.
// A named constant rather than a scatter of conditions, because this is a
// judgement about the corpus and the next reader is entitled to argue with it.
import { addrOf } from "./shared/nip19.js";

export const QUIET_NAMESPACES = new Set(["ISO-639-1", "ISO-639-2", "ISO-3166-1", "ISO-3166-2"]);

/** NIP-32. Anyone may publish one about anything, so a label pill is never gated. */
export const LABEL_KIND = 1985;

/**
 * The kinds whose presence in a page IS the trust gate's verdict.
 *
 * The relay admits a declaration only for a reader whose own kind-10040
 * delegated its signer FOR THAT KIND. So the client does not re-derive the
 * gate and cannot disagree with it: if one of these arrived as a pointer, the
 * reader asked for it, and that is what the gated tone says.
 */
export const DECLARATION_KINDS = new Set([30382, 30383, 30384, 30385, 30392, 30393, 30394, 30395]);

/** Which tag holds a Trusted List's membership, by kind — 30395's `i` names no event. */
const MEMBER_TAG = { 30392: "p", 30393: "e", 30394: "a" };

/** How many pills a preview draws before the rest go behind a count. */
export const PILL_BUDGET = { preview: 4, full: 40 };

const HEX64 = /^[0-9a-f]{64}$/;
const tagsOf = (ev, name) => ((ev && ev.tags) || []).filter((t) => Array.isArray(t) && t[0] === name && t[1]);
const tagOf = (ev, name) => (tagsOf(ev, name)[0] || [])[1] || "";

/**
 * The pills for every event in [events], keyed by the id of the event they go on.
 *
 * One pass to index what the page holds, one to read the pointers. A pointer
 * whose target is NOT in this page contributes nothing: the row is about the
 * cards on screen, and a pill over an absent card is a claim about nothing.
 */
export function provenanceOf(events) {
  const byId = new Map();
  const byAddr = new Map();
  const profileOf = new Map(); // pubkey -> the kind 0 in THIS page
  for (const e of events || []) {
    if (!e || !HEX64.test(e.id || "")) continue;
    byId.set(e.id, e);
    const a = addrOf(e);
    if (a) byAddr.set(a, e);
    if (e.kind === 0) profileOf.set(e.pubkey, e);
  }

  // (target id) -> (pill key) -> pill
  const found = new Map();
  // ONE POINTER MAY NAME ONE TARGET TWICE, and lists in the wild do: clients
  // append without checking, which is the same reason peopleOf dedupes its
  // grid. Counted naively, a single list repeating a member reads "Verified
  // Human 2" — the count claiming two lists where there is one, which is
  // exactly the fact the count exists to state honestly. Deduped PER POINTER,
  // never globally: two different lists sharing a title must still count 2.
  const seenHere = new Set();
  const add = (target, pill) => {
    if (!target || target.id === pill.from) return;
    const once = `${target.id} ${pill.key}`;
    if (seenHere.has(once)) return;
    seenHere.add(once);
    let pills = found.get(target.id);
    if (!pills) found.set(target.id, (pills = new Map()));
    const seen = pills.get(pill.key);
    if (!seen) {
      pills.set(pill.key, { ...pill, count: 1, authors: [pill.author] });
      return;
    }
    // COLLAPSE BY VALUE, NEVER BY EVENT. Two lists titled "Verified Human" are
    // one pill with a 2; 66 labels saying "zapped" are one pill with a 66 —
    // which is the rule that takes the worst real card from 139 pills to 2.
    seen.count++;
    if (!seen.authors.includes(pill.author)) seen.authors.push(pill.author);
  };

  // ONE EVENT WALKED ONCE, however many times the array holds it — and the
  // array does hold it twice now. The page's own answer and the follow-up read
  // that fetches what the answer no longer splices (shared/pointers.js) are
  // seeded TOGETHER, on purpose: a pointer that arrives both ways must not
  // vanish because the caller guessed wrong about which read carried it. But
  // `seenHere` dedupes WITHIN a pointer and is cleared between them, so the
  // second copy walked as a second pointer — and it landed on `count`, which
  // is the number a reader reads as corroboration. "Verified Human 2" over one
  // list is the exact false claim the count exists to avoid making.
  const walked = new Set();
  for (const ev of events || []) {
    if (!ev) continue;
    if (ev.id) {
      if (walked.has(ev.id)) continue;
      walked.add(ev.id);
    }
    seenHere.clear();
    contributionsOf(ev, { byId, byAddr, profileOf }, add);
  }

  const out = new Map();
  for (const [id, pills] of found) out.set(id, order([...pills.values()]));
  return out;
}

/**
 * Hands every (target, pill) one pointer contributes to [emit].
 *
 * A CALLBACK RATHER THAN A RETURNED ARRAY, and it is the difference between
 * this being free and being felt. A Trusted List carries every member it has —
 * thousands — while a page holds at most a hundred results, so building an
 * array per list meant allocating five thousand entries to find five matches,
 * three times over (filter, map, filter). Emitting as we go walks the tags once
 * and allocates only for the members actually on screen.
 */
function contributionsOf(ev, page, emit) {
  if (ev.kind === LABEL_KIND) return labelContributions(ev, page, emit);
  if (!DECLARATION_KINDS.has(ev.kind)) return;
  if (MEMBER_TAG[ev.kind]) return listContributions(ev, page, emit);
  return assertionContributions(ev, page, emit);
}

/**
 * NIP-32: one pill per LABEL VALUE, on every record the label names.
 *
 * `r` and `t` targets are deliberately not read — they name a url and a topic,
 * neither of which is an event this page could be drawing. The same rule the
 * relay's own SearchReferences follows, and for the same reason.
 */
function labelContributions(ev, page, emit) {
  const ns = tagOf(ev, "L");
  const targets = [];
  for (const t of (ev.tags || [])) {
    if (!Array.isArray(t) || !t[1]) continue;
    const target =
      t[0] === "e" ? page.byId.get(t[1])
      : t[0] === "p" ? page.profileOf.get(t[1])
      : t[0] === "a" ? page.byAddr.get(t[1])
      : null;
    if (target) targets.push(target);
  }
  if (!targets.length) return;
  for (const tag of (ev.tags || [])) {
    if (!Array.isArray(tag) || tag[0] !== "l" || !tag[1]) continue;
    // The mark's namespace is its own third element where it has one, and the
    // event's `L` otherwise — a label may carry several, and the one that
    // decides whether this VALUE is metadata is the one written beside it.
    if (QUIET_NAMESPACES.has(tag[2] || ns)) continue;
    const pill = { key: `label:${tag[1]}`, text: tag[1], to: "search", value: tag[1], gated: false, author: ev.pubkey, from: ev.id };
    for (const target of targets) emit(target, pill);
  }
}

/**
 * A Trusted List: one pill, named by the list, on every member this page holds.
 *
 * The TITLE is the pill, because it is the only part of a list this relay
 * indexes and so the only part a reader can have arrived by. An untitled list
 * falls back to its `d` rather than drawing a blank chip.
 */
function listContributions(ev, page, emit) {
  const addr = addrOf(ev);
  if (!addr) return;
  const text = tagOf(ev, "title") || tagOf(ev, "d");
  if (!text) return;
  const name = MEMBER_TAG[ev.kind];
  const pill = { key: `list:${text}`, text, to: "addr", value: addr, gated: true, author: ev.pubkey, from: ev.id };
  for (const t of (ev.tags || [])) {
    if (!Array.isArray(t) || t[0] !== name || !t[1]) continue;
    const target = name === "p" ? page.profileOf.get(t[1]) : name === "e" ? page.byId.get(t[1]) : page.byAddr.get(t[1]);
    if (target) emit(target, pill);
  }
}

/**
 * A NIP-85 assertion, whose subject is its `d` — read BY KIND, since only the
 * kind can say whether that string is a pubkey, an event id or an address.
 *
 * TWO SHAPES, and a contact card can be either. Matched on a `t` tag, the
 * interesting fact is the topic, and the pill goes to that topic's own screen:
 * the reader searched a subject and the answer is the other people under it.
 * With no topic, the card is here because a service the reader delegated
 * scored this person, and the metric is the fact.
 */
function assertionContributions(ev, page, emit) {
  const addr = addrOf(ev);
  const subject = tagOf(ev, "d");
  if (!addr || !subject) return;
  const target =
    ev.kind === 30382 ? page.profileOf.get(subject)
    : ev.kind === 30383 ? page.byId.get(subject)
    : ev.kind === 30384 ? page.byAddr.get(subject)
    : null; // 30385's subject is a NIP-73 identifier — not an event this page draws.
  if (!target) return;

  let topics = 0;
  if (ev.kind === 30382) {
    for (const t of (ev.tags || [])) {
      if (!Array.isArray(t) || t[0] !== "t" || !t[1]) continue;
      topics++;
      emit(target, { key: `topic:${t[1]}`, text: t[1], to: "topic", value: t[1], gated: true, author: ev.pubkey, from: ev.id });
    }
  }
  if (topics) return;
  const rank = tagOf(ev, "rank");
  const text = rank ? `rank ${rank}` : "scored";
  emit(target, { key: `score:${text}`, text, to: "addr", value: addr, gated: true, author: ev.pubkey, from: ev.id });
}

/**
 * Delegated first, then by weight, then alphabetically.
 *
 * The last clause is the one that matters: a card must not reshuffle its own
 * pills between two renders of the same page, and `count` alone leaves ties to
 * whatever order the events happened to arrive in.
 */
function order(pills) {
  return pills.sort((a, b) =>
    (b.gated - a.gated) || (b.count - a.count) || a.text.localeCompare(b.text) || a.key.localeCompare(b.key));
}

/**
 * WHICH PILLS CARRY A FACE — drawn where it disambiguates, nowhere else.
 *
 * An UNGATED pill always carries one: nothing gated it, so who is speaking is
 * the entire trust question, and the page cannot answer it any other way.
 *
 * A GATED pill carries one only when the page holds more than one delegated
 * publisher. Measured on staging, this reader's Map names exactly one
 * publisher for lists and one for scores, so a face on every gated pill would
 * be the same face forty times down a results list — restating what the tone
 * already says, which is that the reader asked for it.
 */
export function facesNeeded(pillsByTarget) {
  const publishers = new Set();
  for (const pills of pillsByTarget.values()) {
    for (const p of pills) if (p.gated) for (const a of p.authors) publishers.add(a);
  }
  return publishers.size > 1;
}

/** The page's own answer, filled by app.js before it renders and read by cards/base.js. */
export const provenance = new Map();
/** True when a gated pill should be attributed — see [facesNeeded]. */
export const attribution = { faces: false };

/**
 * Drop what the page knows, for a view that is not a page of results.
 *
 * The entity page is the one that needs it: it renders a card without going
 * through hydrate(), so it would otherwise inherit whatever the last SEARCH
 * left behind — a row appearing on a permalink reached by clicking a result
 * and not on the same permalink typed into the bar. Worse than either
 * behaviour is the two of them together, since it makes the row's presence
 * mean "how you got here". "Why is this in this page" is not a question a
 * permalink has, and what IS said about an event belongs in a section of its
 * own there, on a bounded ask, the way related.js already does it.
 */
export function forgetProvenance() {
  provenance.clear();
  attribution.faces = false;
}

/** Replace what the page knows with this page's answer. Returns how many cards gained a row. */
export function seedProvenance(events) {
  provenance.clear();
  const built = provenanceOf(events);
  for (const [id, pills] of built) provenance.set(id, pills);
  attribution.faces = facesNeeded(built);
  return built.size;
}
