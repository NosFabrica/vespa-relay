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
// What that costs is the trust gate, and it costs more than the fetch. "It
// arrived, so the reader asked for it" was never quite a fact even about the
// relay: the expansion's COMPANION is gated, but plain recall is not, and the
// store says so outright — "an explicit `kinds:[30392]` is a NIP-01 ask and
// serves strangers' lists as plain hits, gate or no gate". A search that names
// no kinds recalls every kind, so a stranger's list whose TITLE matches lands
// in the answer beside the delegated publisher's. Read as gated, the two
// collapsed by value into one pill with a count of 2 — anyone could inflate a
// publisher's corroboration number by signing a list with the same title.
//
// So the gate is applied HERE, over whatever filled the array, from the
// `trusted` map [provenanceOf] takes. That is one rule for both fillers and
// for any third.
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
//
// `pub.ditto.trends` is the same judgement about the other half of the corpus,
// and it is the one that shows up on PEOPLE. Ditto publishes a trending feed
// as NIP-32: `["L","pub.ditto.trends"]`, `["l","#p","pub.ditto.trends"]`, and
// forty `p` tags carrying a pubkey, a relay and two counts. Two things make it
// furniture. The value is `#p` — a NIP-01 TAG NAME, saying "this trend list is
// about pubkeys" rather than anything about any of them — so the pill reads
// `#p` and means nothing to a reader. And one event names forty people, so a
// single trends post puts that pill on forty cards at once.
//
// Measured against staging on 2026-09-01, asking for the labels on one page of
// 40 profiles: 500 events (the whole budget), 2.4 MB of JSON, 100% this one
// namespace from this one publisher, and exactly one distinct pill out of it,
// reading `#p`. A general sample of the relay's 1985s is 100% `ISO-639-1`, and
// the same 40 people have ZERO labels in any other namespace. So both entries
// here are the same finding twice: the label half of this row is, on this
// corpus, entirely furniture, and what it costs is in [LABEL_LIMIT].
import { addrOf } from "./shared/nip19.js";

export const QUIET_NAMESPACES = new Set(["ISO-639-1", "ISO-639-2", "ISO-3166-1", "ISO-3166-2", "pub.ditto.trends"]);

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
 *
 * [trusted] is `kind -> Set(signer)` — who this reader delegated for each
 * declaration kind, plus the reader themselves, built by pointers.js's
 * trustedSigners off their kind 10040. A declaration from anyone else
 * contributes NOTHING: not a quieter pill, nothing. The row says why a card is
 * in this page, and a stranger's list did not put it there — the relay would
 * not unpack it for this reader, so whatever brought the card, it was not
 * that. Labels are unaffected; NIP-32 is open by construction and its tone
 * already says so.
 *
 * ABSENT MEANS NOBODY, deliberately. An anonymous reader delegates no one and
 * gets label pills only, which is exactly what the relay serves them. A
 * signed-in reader whose Map has not landed yet is in that state for one paint
 * and gains the pills on the next — the row understating itself for a moment
 * is the right way round for a claim about who vouched for whom.
 */
export function provenanceOf(events, trusted) {
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
    contributionsOf(ev, { byId, byAddr, profileOf, trusted }, add);
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
  // WHOSE WORD, not just which kind — and this is the check that used to be
  // the relay's. See [provenanceOf]'s `trusted`.
  if (!delegated(page.trusted, ev.kind, ev.pubkey)) return;
  if (MEMBER_TAG[ev.kind]) return listContributions(ev, page, emit);
  return assertionContributions(ev, page, emit);
}

/** Did this reader name [pubkey] for [kind]? Nothing is delegated by an absent map. */
const delegated = (trusted, kind, pubkey) => {
  const keys = trusted && trusted.get(kind);
  return !!keys && keys.has(pubkey);
};

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
 * ITS TOPICS, AND NOTHING ELSE. Every `t` tag becomes a pill pointing at that
 * topic's own screen: the reader searched a subject, and the answer worth
 * giving is the other people under it. An assertion with no `t` contributes
 * NOTHING — not a quieter pill, nothing.
 *
 * WHY A SCORE IS NOT A REASON. This used to fall back to `rank 92`, or the
 * word "scored" where there was no rank, on the grounds that a delegated
 * service having scored somebody is why their card is here. Two things are
 * wrong with that. A number out of its scale says nothing a reader can act on
 * — `rank 2` beside `rank 98` invites a comparison the pill cannot support,
 * since the scale is the service's and is nowhere on the card. And the page
 * already answers it properly: shared/avatar.js puts a score chip on every
 * face and app.js fills it from the reader's own `30382:rank` service, so the
 * ranking has a place that carries its lens with it. A second, worse spelling
 * of the same fact was crowding the row that explains the OTHER reason a card
 * is here — the list that vouched for it.
 *
 * ALL THREE KINDS, not just the contact card. A topic is a topic whether the
 * subject is a person, an event or an article, and the `t` tag means the same
 * thing in each; reading it only off 30382 was the old shape's assumption, not
 * a rule about the data.
 */
function assertionContributions(ev, page, emit) {
  const subject = tagOf(ev, "d");
  if (!subject) return;
  const target =
    ev.kind === 30382 ? page.profileOf.get(subject)
    : ev.kind === 30383 ? page.byId.get(subject)
    : ev.kind === 30384 ? page.byAddr.get(subject)
    : null; // 30385's subject is a NIP-73 identifier — not an event this page draws.
  if (!target) return;

  // EVERY `t`, not the first. A service that files somebody under three topics
  // has said three things, and the collapse below folds a repeat into a count
  // rather than a second chip, so there is nothing to protect against here.
  for (const t of (ev.tags || [])) {
    if (!Array.isArray(t) || t[0] !== "t" || !t[1]) continue;
    emit(target, { key: `topic:${t[1]}`, text: t[1], to: "topic", value: t[1], gated: true, author: ev.pubkey, from: ev.id });
  }
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

/**
 * WHICH PAGE THIS ANSWER BELONGS TO — bumped by every write, including a clear.
 *
 * The row is now filled in two passes (see the header), and the second lands
 * after an await. Between them the reader can start another search, or click a
 * result and leave the results view entirely — and both of those replace what
 * this map is about. The late pass therefore has to check that it is still
 * writing into the page it read for, and it needs a counter to check against
 * because the events alone cannot tell it: re-running the SAME search under a
 * new observer is a different answer over an identical array.
 *
 * It lives here, with the two writers, rather than in the caller that happens
 * to await. It was a counter in app.js first, and [forgetProvenance] — which
 * is called from entity.js, not from app.js — could not reach it. A permalink
 * opened while a search's second pass was in flight cleared the row on the way
 * in and then had it written straight back, which is precisely the "how you
 * got here" reading forgetProvenance exists to prevent.
 */
let epoch = 0;

/** The current [epoch]. Capture before an await, compare after, drop if it moved. */
export const provenanceEpoch = () => epoch;
/** True when a gated pill should be attributed — see [facesNeeded]. */
export const attribution = { faces: false };

/**
 * Drop what the page knows, so the next view starts from nothing.
 *
 * Two callers, for two different reasons. The FEED clears and stops: it draws
 * full cards, so a row left over from the last search would sit under them
 * explaining a list nobody searched, and a plain NIP-01 read expands nothing,
 * so it has no row of its own to put there.
 *
 * The ENTITY PAGE clears on the way in and then asks again. The clear is the
 * old reason and it still holds — a row inherited from the last SEARCH would
 * appear on a permalink reached by clicking a result and not on the same
 * permalink typed into the bar, which makes its presence mean "how you got
 * here". What has changed is that a permalink now HAS an answer: since the
 * page asks by target rather than reading what the relay happened to splice,
 * "which of the providers you named vouch for this person" is a question a
 * permalink asks more sharply than a results list does. entity.js seeds it
 * from the entity itself once that is known.
 */
export function forgetProvenance() {
  epoch++;
  provenance.clear();
  attribution.faces = false;
}

/** Replace what the page knows with this page's answer. Returns how many cards gained a row. */
export function seedProvenance(events, trusted) {
  epoch++;
  provenance.clear();
  const built = provenanceOf(events, trusted);
  for (const [id, pills] of built) provenance.set(id, pills);
  attribution.faces = facesNeeded(built);
  return built.size;
}
