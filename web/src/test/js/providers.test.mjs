// Whose word a reader took: the kind-10040 parse and the caching rule. A bare `30392` tag (no
// `:` dimension) is a delegation too.
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

// A Map in both shapes, tag for tag.
const MAP = {
  id: "a".repeat(64), pubkey: READER, kind: 10040, created_at: 1, content: "",
  tags: [
    ["30382:rank", RANKER, "wss://nip85-staging.nosfabrica.com"],
    ["30382:followers", RANKER, "wss://nip85-staging.nosfabrica.com"],
    ["30392", LISTER, "wss://nip85.brainstorm.world"],
  ],
};

{
  const d = delegationsOf(MAP);
  assert.deepStrictEqual(d.get("30392"), [LISTER], "a BARE kind is a delegation — the shape NIP-85's parser misses");
  assert.deepStrictEqual(d.get("30382:rank"), [RANKER]);
  assert.deepStrictEqual(d.get("30382:followers"), [RANKER]);
  // A followers service orders a set and cannot rank one.
  assert.strictEqual(d.has("30382"), false, "the dimension is part of the entry, not a label on it");
}

{
  const d = delegationsOf(MAP);
  assert.deepStrictEqual(publishersOf(d, 30382), [RANKER],
    "one service under two dimensions is ONE author — a duplicated `authors` entry can be answered twice, and pills are counted");
  assert.deepStrictEqual(publishersOf(d, 30392), [LISTER]);
  assert.deepStrictEqual(publishersOf(d, 30393), [], "a kind the Map never names delegates nobody");
}

// Every publisher, not the first.
{
  const second = "b".repeat(64);
  const d = delegationsOf({ tags: [["30382:rank", RANKER, ""], ["30382:rank", second, ""]] });
  assert.deepStrictEqual(publishersOf(d, 30382), [RANKER, second], "every publisher for a kind, in the order the Map wrote them");
}

{
  const d = delegationsOf({ tags: [
    ["30392", "not-a-key", "wss://x"],       // an authors entry the relay cannot match
    ["30392", RANKER.toUpperCase(), ""],     // `authors` is lowercase hex
    ["alt", "a note about this map"],        // not a kind, not a key
    ["client", "c".repeat(64)],              // a key, but its tag names no kind
    ["30392"],                               // truncated
  ] });
  assert.strictEqual(d.size, 0, "a malformed entry is dropped, so 'my Map is broken' cannot read as 'this relay has no lists'");
  assert.deepStrictEqual(delegationsOf(null).size, 0);
  assert.deepStrictEqual(delegationsOf({}).size, 0);
}

// An absence may be cached only when the relay answered.
answer = ([type, id], ws) => {
  if (type !== "REQ") return;
  ws.deliver(["EVENT", id, MAP]);
  ws.deliver(["EOSE", id]);
};
{
  const d = await providersFor(READER);
  assert.deepStrictEqual(publishersOf(d, 30392), [LISTER], "the Map is read off the wire");
}
answer = () => {};
{
  const d = await providersFor(READER);
  assert.deepStrictEqual(publishersOf(d, 30392), [LISTER], "a complete read is cached");
}

forgetProviders();
{
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

// Dedupe the read, not just the answer: two callers ask off one render.
forgetProviders();
{
  let deliver = null;
  answer = ([type, id], ws) => { if (type === "REQ") deliver = () => { ws.deliver(["EVENT", id, MAP]); ws.deliver(["EOSE", id]); }; };
  reqs = 0;
  const both = Promise.all([providersFor(READER), providersFor(READER)]);
  await new Promise((r) => setTimeout(r, 5));
  deliver();
  const [a, b] = await both;
  assert.strictEqual(reqs, 1, "two callers racing on one Map is ONE read");
  assert.deepStrictEqual(publishersOf(a, 30392), [LISTER]);
  assert.strictEqual(a, b, "…and they are handed the same answer");
}

// An in-flight entry must not outlive the read, or it stands in for the cache a dropped read was kept out of.
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

// readiness.js reads the kind 0, 10002 and 10040 in one REQ at sign-in and shares the Map.
forgetProviders();
{
  reqs = 0;
  answer = () => { throw new Error("providersFor must not ask for a Map it was handed"); };
  seedProviders(READER, MAP, true);
  assert.deepStrictEqual(publishersOf(await providersFor(READER), 30392), [LISTER]);
  assert.strictEqual(reqs, 0, "a seeded Map costs no round trip");
}
forgetProviders();
{
  reqs = 0;
  seedProviders(READER, null, true);
  assert.strictEqual((await providersFor(READER)).size, 0);
  assert.strictEqual(reqs, 0, "…and 'you delegate nobody' is an answer, not a gap");
}
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

// `authors` takes 64 hex and nothing else.
{
  let asked = false;
  answer = () => { asked = true; };
  assert.strictEqual((await providersFor(null)).size, 0);
  assert.strictEqual((await providersFor("npub1lrl3r3a86drcx4wnknghfedywduh4yrw5j4xr25md0qx2tq759aq8stqua")).size, 0);
  assert.strictEqual(asked, false, "a bech32 key would pass a laxer test here and delegate nothing there");
}

console.log("providers: both delegation shapes, the dimension kept, and an absence cached only off a complete read");
process.exit(0);
