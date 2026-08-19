// enrichProfiles' caching contract, driven through the real client over a
// fake socket — the same shape relay.test.mjs uses, one layer up.
//
// The rule under test is the page's oldest hard-won one: a lookup may cache
// "there is no profile here" ONLY when the relay finished answering. Caching
// it off a timed-out read records a fact the relay never stated, and the
// cache is consulted before every render, so the absence is permanent. It has
// been got wrong twice — once in profiles.js (a dropped lookup poisoned your
// own key and signing in appeared to need a refresh) and once in app.js's
// score chips, which cached a miss off an incomplete read until this pass.
import assert from 'assert';

globalThis.location = { protocol: "http:", host: "localhost:7787" };
globalThis.window = { addEventListener: () => {} };

// A WebSocket the test drives: whatever `answer` is set to decides what the
// relay says back, including saying nothing at all.
let answer = () => {};
class FakeWS {
  static OPEN = 1;
  constructor() { this.readyState = 0; queueMicrotask(() => { this.readyState = 1; this.onopen && this.onopen(); }); }
  send(raw) { answer(JSON.parse(raw), this); }
  close() { this.readyState = 3; this.onclose && this.onclose(); }
  deliver(msg) { this.onmessage && this.onmessage({ data: JSON.stringify(msg) }); }
}
globalThis.WebSocket = FakeWS;

const { profiles, enrichProfiles } = await import(new URL("../../web/src/main/resources/web/shared/profiles.js", import.meta.url));

const pk = (c) => c.repeat(64);
const profileEvent = (pubkey, name) => ({ id: pk("e"), pubkey, kind: 0, created_at: 1, tags: [], content: JSON.stringify({ name }) });

// ---- a complete read: names learned, and the misses cached as absent ------
answer = ([type, id], ws) => {
  if (type !== "REQ") return;
  ws.deliver(["EVENT", id, profileEvent(pk("a"), "alice")]);
  ws.deliver(["EOSE", id]);
};
let learned = await enrichProfiles([pk("a"), pk("b")]);
assert.strictEqual(learned, 1, "reports how many NEW profiles it learned");
assert.strictEqual(profiles.get(pk("a")).name, "alice");
assert.strictEqual(profiles.get(pk("b")), null, "EOSE without an event IS the relay stating absence");

// Nothing new to learn is 0, not 1 — a caller that re-renders on a non-zero
// answer must not re-render for profiles it already had.
assert.strictEqual(await enrichProfiles([pk("a")]), 0, "an all-cached ask learns nothing");

// ---- an INCOMPLETE read: absence must not be cached -----------------------
answer = () => {};                              // the relay never answers
const slow = pk("c");
learned = await enrichProfiles([slow]);
assert.strictEqual(learned, 0);
assert.strictEqual(profiles.has(slow), false,
  "a timed-out read must leave the pubkey UNKNOWN — caching null here is the poisoning bug");

// …and the next attempt, once the relay answers, still finds it.
answer = ([type, id], ws) => {
  if (type !== "REQ") return;
  ws.deliver(["EVENT", id, profileEvent(slow, "carol")]);
  ws.deliver(["EOSE", id]);
};
assert.strictEqual(await enrichProfiles([slow]), 1, "an unpoisoned pubkey is still askable");
assert.strictEqual(profiles.get(slow).name, "carol");

// ---- a read that THROWS is not an answer either --------------------------
answer = (msg, ws) => { if (msg[0] === "REQ") ws.close(); };
const dropped = pk("d");
await enrichProfiles([dropped]);
assert.strictEqual(profiles.has(dropped), false, "a dropped connection states nothing");

console.log("profiles: absence is cached only when the relay answered");
