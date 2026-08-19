// The reply-parent lookup: which `e` tag, whose pubkey, and what may be
// cached as an answer.
//
// The rule under test on the cache side is the page's oldest one, restated
// here because this is a THIRD place to get it wrong: "this relay does not
// hold the parent" may be recorded only when the relay finished answering. A
// null cached off a timed-out read is permanent for the session, and every
// reply to that parent then renders "in reply to note1qqq…" forever — the
// exact shape this module exists to remove.
//
// The selection side is tested here rather than only through the cards because
// it is NIP-10's rule, not a rendering choice: markers first, position last.
import assert from 'assert';

globalThis.location = { protocol: "http:", host: "localhost:7787" };
globalThis.window = { addEventListener: () => {} };

let answer = () => {};
class FakeWS {
  static OPEN = 1;
  constructor() { this.readyState = 0; queueMicrotask(() => { this.readyState = 1; this.onopen && this.onopen(); }); }
  send(raw) { answer(JSON.parse(raw), this); }
  close() { this.readyState = 3; this.onclose && this.onclose(); }
  deliver(msg) { this.onmessage && this.onmessage({ data: JSON.stringify(msg) }); }
}
globalThis.WebSocket = FakeWS;

const { replyTarget, replyAuthor, replyPerson, seedParentAuthors, unknownParents, loadParentAuthors } =
  await import(new URL("../../web/src/main/resources/web/shared/parents.js", import.meta.url));

const hex = (c) => c.repeat(64);
const alice = hex("a"), bob = hex("b");
const root = hex("1"), parent = hex("2");
const ev = (kind, tags) => ({ id: hex("f"), pubkey: alice, kind, created_at: 1, tags, content: "" });

// ---- which `e` tag is the parent -----------------------------------------
const idOf = (e) => (replyTarget(e) || {}).id;
assert.strictEqual(idOf(ev(1, [["e", root, "", "root"], ["e", parent, "", "reply"]])), parent, "`reply` wins");
assert.strictEqual(idOf(ev(1, [["e", root, "", "root"]])), root, "a lone `root` is what is being answered");
assert.strictEqual(idOf(ev(1, [["e", root], ["e", parent]])), parent, "positional: the last one");
assert.strictEqual(idOf(ev(1, [["e", parent], ["e", root, "", "mention"]])), parent, "a quote is not a parent");
assert.strictEqual(idOf(ev(1, [["e", "nope"], ["e", parent]])), parent, "a malformed id is skipped, not rendered");
assert.strictEqual(replyTarget(ev(1, [])), null, "no `e` tags, no parent");
assert.strictEqual(replyTarget(ev(1, null)), null, "an event with no tags array is not a crash");
assert.strictEqual(replyTarget(ev(7, [["e", parent]])), null, "a reaction is not a reply");
assert.strictEqual(replyTarget(ev(42, [["e", root, "", "root"]])), null, "a channel message's room is not its parent");
assert.strictEqual(idOf(ev(42, [["e", root, "", "root"], ["e", parent]])), parent, "…the second one is");

// ---- and whose it is, from the event alone --------------------------------
assert.strictEqual(replyAuthor(ev(1, [["e", parent, "wss://r.x", "reply", bob]])), bob, "NIP-10's 5th element");
assert.strictEqual(replyAuthor(ev(1111, [["e", parent, "wss://r.x", bob]])), bob, "NIP-22's 4th");
assert.strictEqual(replyAuthor(ev(1111, [["e", parent], ["p", bob]])), bob, "a NIP-22 comment's required `p`");
assert.strictEqual(replyAuthor(ev(1, [["e", parent], ["p", bob]])), null,
  "a kind 1's `p` tags are everyone in the thread — guessing the parent's author from them is not an answer");
assert.strictEqual(replyPerson(ev(1111, [["a", `30023:${bob}:art`]])), bob, "an addressable parent carries its author");
assert.strictEqual(replyTarget(ev(1, [["e", parent, "http://not-a-relay", "reply"]])).relay, null,
  "only ws(s) hints are worth minting into a link");

// ---- the lookup: only a finished answer is an answer ----------------------
const wanted = ev(1, [["e", parent]]);
assert.deepStrictEqual(unknownParents([wanted]), [parent], "an unhinted parent is a question");
assert.deepStrictEqual(unknownParents([ev(1, [["e", parent, "", "reply", bob]])]), [],
  "a hinted one is not — the answer is already on the event");

// An event in hand IS the answer for its own id. A thread in a result page
// carries its own parents, so seeding from what already arrived removes those
// ids from the ask entirely — a round trip for a fact already in memory.
const inHand = hex("3");
seedParentAuthors([{ id: inHand, pubkey: alice, kind: 1, created_at: 1, tags: [] }]);
assert.deepStrictEqual(unknownParents([ev(1, [["e", inHand]])]), [], "a parent already on the page is not a question");
assert.strictEqual(replyAuthor(ev(1, [["e", inHand]])), alice, "…and it names its author with no lookup at all");
seedParentAuthors([null, {}, { id: "junk", pubkey: alice }]);   // must not throw or record rubbish
assert.strictEqual(replyAuthor(ev(1, [["e", hex("4")]])), null);

answer = () => {};                                  // the relay never answers
assert.strictEqual(await loadParentAuthors([parent]), 0);
assert.deepStrictEqual(unknownParents([wanted]), [parent],
  "a timed-out read leaves the parent UNKNOWN — caching the gap here is the poisoning bug");

answer = ([type, id], ws) => {
  if (type !== "REQ") return;
  ws.deliver(["EVENT", id, { id: parent, pubkey: bob, kind: 1, created_at: 1, tags: [], content: "the parent" }]);
  ws.deliver(["EOSE", id]);
};
assert.strictEqual(await loadParentAuthors([parent, root]), 1, "reports how many NEW authors it learned");
assert.strictEqual(replyAuthor(wanted), bob, "…and the card now names a person");
assert.deepStrictEqual(unknownParents([wanted]), [], "an answered parent is not asked twice");
assert.strictEqual(await loadParentAuthors([parent, root]), 0, "nor is a stated absence");
assert.strictEqual(replyAuthor(ev(1, [["e", root]])), null,
  "the relay answered without it: nobody, which is not the same as not asked");

console.log("parents: NIP-10 selection, and absence cached only when the relay answered");
