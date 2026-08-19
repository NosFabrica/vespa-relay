// What the `group:<id>` pill is allowed to draw over an id — the cache behind
// it, driven through the real client over a fake socket, the shape
// profiles.test.mjs uses.
//
// Two rules, and the first is this module's own. A group is the pair (id, host
// relay) while the pill, the `#h` filter and the url all carry the bare id, so
// "what is this id called" can have more than one answer — and a name drawn
// over an id the corpus disagrees about would tell the reader the results are
// one group when they are two. The second rule is the page's oldest: an
// absence may be cached only when the relay finished answering, or a dropped
// lookup means that group has no name for the rest of the session.
import assert from "assert";
import { readFileSync } from "node:fs";

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

const { groupName, knowsGroup, seedGroupNames, seedGroupEvents, enrichGroupNames, forgetPrivateGroupNames } =
  await import(new URL("../../relay/src/main/resources/web/shared/groupnames.js", import.meta.url));

const HOST_A = "a".repeat(64);
const HOST_B = "b".repeat(64);
const meta = (id, pubkey, name) => ({
  id: "e".repeat(64), kind: 39000, pubkey, created_at: 1, content: "",
  tags: [["d", id], ["name", name]],
});

// ---- seeding: rows and events both teach the same cache -------------------

assert.strictEqual(
  seedGroupEvents([meta("nos", HOST_A, "nos engineers"), { kind: 1, tags: [["d", "nope"]] }]), 1,
  "reports how many ids DRAW differently now, so a seed that changed nothing costs no repaint",
);
assert.strictEqual(groupName("nos"), "nos engineers", "a 39000's `name` is what the pill draws");
assert.strictEqual(groupName("nope"), "", "…and only a 39000 is a group's metadata");
assert.strictEqual(groupName("never-heard-of-it"), "", "an unknown id has no name, and no exception either");

// A row with no name teaches nothing. Recording it as a blank would be a
// second "name" for the id to disagree with — and the disagreement rule below
// would then hide the real name behind it.
assert.strictEqual(seedGroupNames([{ id: "nos", name: "   ", host: HOST_B }]), 0, "a nameless row teaches nothing");
assert.strictEqual(groupName("nos"), "nos engineers", "…so it is not a name to disagree with, either");

// ---- two hosts, one id: the case this module exists to notice -------------

seedGroupEvents([meta("general", HOST_A, "General")]);
assert.strictEqual(groupName("general"), "General", "one host is one name");
assert.strictEqual(seedGroupEvents([meta("general", HOST_B, "Generalists")]), 1,
  "a second host CONTRADICTING the first changes the drawing, which is a change to report");
assert.strictEqual(groupName("general"), "",
  "two relays signing one id under different names leaves the ID on the pill: the search returns both");

// Agreeing is a different fact and draws.
seedGroupEvents([meta("agreed", HOST_A, "Bitcoin")]);
assert.strictEqual(seedGroupEvents([meta("agreed", HOST_B, "Bitcoin")]), 0,
  "a second host CONFIRMING the name changes no drawing, and must not cost a repaint");
assert.strictEqual(groupName("agreed"), "Bitcoin", "…where they agree there is one honest name to draw");

// YOUR list wins outright, which is what keeps the common case from tripping
// the rule above: a 10009 tag names its host as a url and a 39000 names it as
// a signing key, so your row and the corpus's row are always two hosts —
// they would disagree over a difference in spelling alone.
seedGroupNames([
  { id: "chachi", name: "Chachi", relayUrl: "wss://relay.groups.nip29.com/", mine: true },
  { id: "chachi", name: "chachi (public)", host: HOST_A },
]);
assert.strictEqual(groupName("chachi"), "Chachi", "the name in your own list is the name you know it by");

// ---- what must not outlive a sign-out -------------------------------------
//
// One cache, two kinds of name in it. A 39000 is signed by a relay for anyone
// to read and a `group` tag in the clear is in the clear — but a name that
// arrived NIP-44-encrypted got here because the reader unlocked their own list
// to pick from it, and a label somebody gave a group in private must not still
// be drawn on a pill for whoever uses the tab next.
seedGroupNames([
  { id: "hidden", name: "Family", relayUrl: "wss://groups.example", mine: true, secret: true },
  { id: "shared-with-public", name: "Book Club", relayUrl: "wss://groups.example", mine: true, secret: true },
  { id: "shared-with-public", name: "Book Club", host: HOST_A },
]);
assert.strictEqual(groupName("hidden"), "Family", "an unlocked list names its groups like any other source");

forgetPrivateGroupNames();
assert.strictEqual(groupName("hidden"), "", "…and that name goes when the reader does");
assert.strictEqual(knowsGroup("hidden"), false, "…leaving the id askable rather than remembered as nameless");
assert.strictEqual(groupName("shared-with-public"), "Book Club",
  "the PUBLIC half of the same id stays: dropping it would cost a round trip to learn what the network can already see");
assert.strictEqual(groupName("chachi"), "Chachi", "…as does a name from a `group` tag that was never encrypted");

// ---- the lookup, and the complete-read rule -------------------------------

answer = ([type, id, filter], ws) => {
  if (type !== "REQ") return;
  assert.deepStrictEqual(filter.kinds, [39000], "a name lookup asks for group metadata");
  assert(Array.isArray(filter["#d"]), "…keyed by the group id, which is a 39000's `d`");
  for (const want of filter["#d"]) if (want === "found") ws.deliver(["EVENT", id, meta("found", HOST_A, "Found")]);
  ws.deliver(["EOSE", id]);
};
let learned = await enrichGroupNames(["found", "absent"]);
assert.strictEqual(learned, 1, "reports how many ids ended up with a name, so a caller repaints only when it would change something");
assert.strictEqual(groupName("found"), "Found");
assert.strictEqual(knowsGroup("absent"), true, "EOSE without an event IS the relay stating it holds no such group");
assert.strictEqual(await enrichGroupNames(["found", "absent"]), 0, "…and neither is asked again");

// An id already named by a row is not asked for at all — the picker just told
// us, and a REQ per render for a group the reader picked is a round trip for
// nothing.
answer = () => { throw new Error("asked for an id the cache already knows"); };
assert.strictEqual(await enrichGroupNames(["chachi"]), 0, "a seeded id needs no lookup");

// A read that never finishes states nothing: the id stays askable, or one
// dropped lookup would leave that group nameless for the rest of the session.
answer = () => {};
const slow = "slow-group";
assert.strictEqual(await enrichGroupNames([slow]), 0);
assert.strictEqual(knowsGroup(slow), false, "a timed-out read is not the relay saying the group is nameless");

answer = ([type, id], ws) => {
  if (type !== "REQ") return;
  ws.deliver(["EVENT", id, meta(slow, HOST_A, "Slow")]);
  ws.deliver(["EOSE", id]);
};
assert.strictEqual(await enrichGroupNames([slow]), 1, "…so the next render can still ask");
assert.strictEqual(groupName(slow), "Slow");

// A read that THROWS is not an answer either.
answer = (msg, ws) => { if (msg[0] === "REQ") ws.close(); };
await enrichGroupNames(["dropped"]);
assert.strictEqual(knowsGroup("dropped"), false, "a dropped connection states nothing");

// ---- WHICH CONNECTION this one asks on ------------------------------------
//
// A source assertion, because the rule is a DIVERGENCE from the one
// groups.test.mjs pins next door and the two are a line apart in intent. The
// picker asks two questions about the reader — which groups are yours, which
// to offer first — on the authenticated socket, where the observer gate
// belongs. This asks what a group the reader has ALREADY named is called,
// which is a fact about a subject, exactly like the kind 0 behind a face: the
// store applies the observer as a FILTER, so asking on that socket would leave
// a reader with no scores mirrored here looking at the hex id the pill is
// there to replace.
// ---- forgetting has a DRAWN half, and only app.js can show it -------------
//
// The cache half is asserted above: a private name goes when the reader does.
// What that cannot see is the pill already on the screen. Two of them read
// this cache now — the search box's, and a chat card's — and neither repaints
// on its own: both repaint call sites on that page fire only when a lookup
// LEARNED something, and forgetting is the opposite of learning. So a chat
// permalink would sit there with a label out of somebody's decrypted list on
// it for whoever uses the tab next, which is the exact thing this module's
// forget exists to prevent.
const app = readFileSync(new URL("../../relay/src/main/resources/web/app.js", import.meta.url), "utf8");
const forgetAt = app.indexOf("forgetPrivateGroupNames();");
assert(forgetAt > 0, "app.js forgets the private group names when the reader signs out");
assert(/field\.repaint\(\)/.test(app.slice(forgetAt, forgetAt + 900)),
  "…and repaints the search field's pill there, which nothing else on that path does");
const rerunAt = app.indexOf("function rerun()");
assert(rerunAt > 0, "app.js re-runs the current view after a sign-out");
const rerunSrc = app.slice(rerunAt, rerunAt + 1200);
assert(/entitySeg\(\)/.test(rerunSrc) && /openEntity\(/.test(rerunSrc),
  "…including the ENTITY view, the one view with no query to re-run — so a permalink's card is redrawn too");

const src = readFileSync(new URL("../../relay/src/main/resources/web/shared/groupnames.js", import.meta.url), "utf8");
assert(/await refConn\(\)/.test(src),
  "the name lookup asks on the ANONYMOUS reference connection, like every other fact about a subject");
assert(!/\brelay\.req\(/.test(src), "…and never on the trust-gated socket, which would narrow it to nothing");

console.log("groupnames: a name is drawn over an id only where the sources agree on one");
