// Whose word a reader took: the kind-10040 parse, and the caching rule.
//
// Both delegation shapes are asserted against the Map measured on staging on
// 2026-09-01 — `30382:rank` and `30382:followers` naming one service, a BARE
// `30392` naming another. The bare one is the case: it carries no `:`, so
// quartz's ServiceProviderTag has never parsed it, and a reader that takes
// only the NIP-85 shape resolves this reader's list delegations to the empty
// set and draws no list pill at all, silently. relay's ObserverTrustListIT
// exists for the same shape on the serving side.
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

const { delegationsOf, publishersOf, providersFor, seedProviders, forgetProviders } =
  await import(new URL("../../main/resources/web/shared/providers.js", import.meta.url));

let reqs = 0;
const origSend = FakeWS.prototype.send;
FakeWS.prototype.send = function (raw) { if (JSON.parse(raw)[0] === "REQ") reqs++; return origSend.call(this, raw); };

const RANKER = "d6e47f060bed6fd8c3ec272edc56aacb9eef853d024cd5087cfbd30c329a9cb1";
const LISTER = "8e901369d45081cf05fe17ba802441dd731f73e000149c333daf4880a58e5fb1";
const READER = "f8ff11c7a7d3478355d3b4d174e5a473797a906ea4aa61aa9b6bc0652c1ea17a";

// The real Map, tag for tag.
const MAP = {
  id: "a".repeat(64), pubkey: READER, kind: 10040, created_at: 1, content: "",
  tags: [
    ["30382:rank", RANKER, "wss://nip85-staging.nosfabrica.com"],
    ["30382:followers", RANKER, "wss://nip85-staging.nosfabrica.com"],
    ["30392", LISTER, "wss://nip85.brainstorm.world"],
  ],
};

// ---- both shapes, and the dimension kept ---------------------------------
{
  const d = delegationsOf(MAP);
  assert.deepStrictEqual(d.get("30392"), [LISTER], "a BARE kind is a delegation — the shape NIP-85's parser misses");
  assert.deepStrictEqual(d.get("30382:rank"), [RANKER]);
  assert.deepStrictEqual(d.get("30382:followers"), [RANKER]);
  // Kept apart, not folded. A followers service orders a set and cannot rank
  // one, so a score chip filled off `30382:followers` would be a number the
  // service never claimed to have measured.
  assert.strictEqual(d.has("30382"), false, "the dimension is part of the entry, not a label on it");
}

// ---- the union, where the union is the right question --------------------
{
  const d = delegationsOf(MAP);
  assert.deepStrictEqual(publishersOf(d, 30382), [RANKER],
    "one service under two dimensions is ONE author — a duplicated `authors` entry can be answered twice, and pills are counted");
  assert.deepStrictEqual(publishersOf(d, 30392), [LISTER]);
  assert.deepStrictEqual(publishersOf(d, 30393), [], "a kind the Map never names delegates nobody");
}

// Two different services for one kind is the case TrustNotice.kt says was got
// wrong on the serving side: reading only the first told a reader whose SECOND
// provider is mirrored that their scores were missing, on every login.
{
  const second = "b".repeat(64);
  const d = delegationsOf({ tags: [["30382:rank", RANKER, ""], ["30382:rank", second, ""]] });
  assert.deepStrictEqual(publishersOf(d, 30382), [RANKER, second], "every publisher for a kind, in the order the Map wrote them");
}

// ---- what is NOT a delegation --------------------------------------------
{
  const d = delegationsOf({ tags: [
    ["30392", "not-a-key", "wss://x"],       // an authors entry the relay cannot match
    ["30392", RANKER.toUpperCase(), ""],     // …nor this one: `authors` is lowercase hex
    ["alt", "a note about this map"],        // not a kind, not a key
    ["client", "c".repeat(64)],              // a key, but its tag names no kind
    ["30392"],                               // truncated
  ] });
  assert.strictEqual(d.size, 0, "a malformed entry is dropped, so 'my Map is broken' cannot read as 'this relay has no lists'");
  assert.deepStrictEqual(delegationsOf(null).size, 0);
  assert.deepStrictEqual(delegationsOf({}).size, 0);
}

// ---- the caching rule, over a socket -------------------------------------
//
// The page's oldest hard-won rule, and the third cache to need it: an absence
// may be cached only when the relay ANSWERED. Cached off a dropped read, "this
// reader delegates nobody" is permanent for the session and every gated pill
// is gone with nothing on screen to say so.
answer = ([type, id], ws) => {
  if (type !== "REQ") return;
  ws.deliver(["EVENT", id, MAP]);
  ws.deliver(["EOSE", id]);
};
{
  const d = await providersFor(READER);
  assert.deepStrictEqual(publishersOf(d, 30392), [LISTER], "the Map is read off the wire");
}
// Cached: the relay now says nothing at all, and the answer is unchanged.
answer = () => {};
{
  const d = await providersFor(READER);
  assert.deepStrictEqual(publishersOf(d, 30392), [LISTER], "a complete read is cached");
}

forgetProviders();
{
  // A read that never completes resolves EMPTY — and must not be remembered.
  const dropped = await providersFor(READER);
  assert.strictEqual(dropped.size, 0, "a timed-out read has nothing to say");
  answer = ([type, id], ws) => {
    if (type !== "REQ") return;
    ws.deliver(["EVENT", id, MAP]);
    ws.deliver(["EOSE", id]);
  };
  const d = await providersFor(READER);
  assert.deepStrictEqual(publishersOf(d, 30392), [LISTER],
    "…so the next attempt still asks — caching the gap is the poisoning bug");
}

// ---- ONE READ, NOT ONE PER CALLER ---------------------------------------
//
// The same distinction refConn() draws about the socket: dedupe the READ, not
// just the answer. Both callers fire off one render — `hydrate` starts the
// provenance lookup and the render after it paints the score chips — so the
// second arrives while the first is still on the wire and, caching the value
// alone, found nothing and asked again. Two REQs for one replaceable event, on
// every search.
forgetProviders();
{
  let deliver = null;
  answer = ([type, id], ws) => { if (type === "REQ") deliver = () => { ws.deliver(["EVENT", id, MAP]); ws.deliver(["EOSE", id]); }; };
  reqs = 0;
  const both = Promise.all([providersFor(READER), providersFor(READER)]);
  await new Promise((r) => setTimeout(r, 5));   // both callers are now in
  deliver();
  const [a, b] = await both;
  assert.strictEqual(reqs, 1, "two callers racing on one Map is ONE read");
  assert.deepStrictEqual(publishersOf(a, 30392), [LISTER]);
  assert.strictEqual(a, b, "…and they are handed the same answer");
}

// An in-flight entry must not outlive the read, or it would stand in for the
// cache a dropped read was deliberately kept out of.
forgetProviders();
{
  answer = () => {};
  reqs = 0;
  assert.strictEqual((await providersFor(READER)).size, 0, "a dropped read still has nothing to say");
  answer = ([type, id], ws) => {
    if (type !== "REQ") return;
    ws.deliver(["EVENT", id, MAP]);
    ws.deliver(["EOSE", id]);
  };
  assert.deepStrictEqual(publishersOf(await providersFor(READER), 30392), [LISTER],
    "and the next caller asks again rather than being handed the dropped read");
  assert.strictEqual(reqs, 2, "which is two reads, on purpose");
}

// ---- the preload: a Map somebody else already read ------------------------
//
// readiness.js asks this reader's kind 0, 10002 and 10040 in one REQ the
// moment a sign-in lands, long before the first search. Shared, this module
// never asks at all.
forgetProviders();
{
  reqs = 0;
  answer = () => { throw new Error("providersFor must not ask for a Map it was handed"); };
  seedProviders(READER, MAP, true);
  assert.deepStrictEqual(publishersOf(await providersFor(READER), 30392), [LISTER]);
  assert.strictEqual(reqs, 0, "a seeded Map costs no round trip");
}
// An absence off a FINISHED read is the relay stating it, and is cacheable.
forgetProviders();
{
  reqs = 0;
  seedProviders(READER, null, true);
  assert.strictEqual((await providersFor(READER)).size, 0);
  assert.strictEqual(reqs, 0, "…and 'you delegate nobody' is an answer, not a gap");
}
// A read the caller cannot vouch for is not an answer, and must not seed.
forgetProviders();
{
  answer = ([type, id], ws) => {
    if (type !== "REQ") return;
    ws.deliver(["EVENT", id, MAP]);
    ws.deliver(["EOSE", id]);
  };
  seedProviders(READER, null, false);
  assert.deepStrictEqual(publishersOf(await providersFor(READER), 30392), [LISTER],
    "an unvouched seed is ignored — it is the poisoning bug in a different hat");
}

// A reader who is not a key is answered without a round trip: `authors`
// takes 64 hex and nothing else, and an anonymous page has no Map at all.
{
  let asked = false;
  answer = () => { asked = true; };
  assert.strictEqual((await providersFor(null)).size, 0);
  assert.strictEqual((await providersFor("npub1lrl3r3a86drcx4wnknghfedywduh4yrw5j4xr25md0qx2tq759aq8stqua")).size, 0);
  assert.strictEqual(asked, false, "a bech32 key would pass a laxer test here and delegate nothing there");
}

console.log("providers: both delegation shapes, the dimension kept, and an absence cached only off a complete read");
process.exit(0);
