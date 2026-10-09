// enrichProfiles' caching contract over a fake socket: "no profile here" may
// be cached only when the relay finished answering. The cache is consulted
// before every render, so a miss cached off a timed-out read is permanent.
import assert from 'assert';

globalThis.location = { protocol: "http:", host: "localhost:7787" };
globalThis.window = { addEventListener: () => {} };

// `answer` decides what the relay says back, including nothing at all.
let answer = () => {};
class FakeWS {
  static OPEN = 1;
  constructor() { this.readyState = 0; queueMicrotask(() => { this.readyState = 1; this.onopen && this.onopen(); }); }
  send(raw) { answer(JSON.parse(raw), this); }
  close() { this.readyState = 3; this.onclose && this.onclose(); }
  deliver(msg) { this.onmessage && this.onmessage({ data: JSON.stringify(msg) }); }
}
globalThis.WebSocket = FakeWS;

const { profiles, enrichProfiles, parseProfile, displayName, authorOf } = await import(new URL("../../main/resources/web/shared/profiles.js", import.meta.url));

const pk = (c) => c.repeat(64);
const profileEvent = (pubkey, name) => ({ id: pk("e"), pubkey, kind: 0, created_at: 1, tags: [], content: JSON.stringify({ name }) });

answer = ([type, id], ws) => {
  if (type !== "REQ") return;
  ws.deliver(["EVENT", id, profileEvent(pk("a"), "alice")]);
  ws.deliver(["EOSE", id]);
};
let learned = await enrichProfiles([pk("a"), pk("b")]);
assert.strictEqual(learned, 1, "reports how many NEW profiles it learned");
assert.strictEqual(profiles.get(pk("a")).name, "alice");
assert.strictEqual(profiles.get(pk("b")), null, "EOSE without an event IS the relay stating absence");

assert.strictEqual(await enrichProfiles([pk("a")]), 0, "an all-cached ask learns nothing");

answer = () => {};
const slow = pk("c");
learned = await enrichProfiles([slow]);
assert.strictEqual(learned, 0);
assert.strictEqual(profiles.has(slow), false,
  "a timed-out read must leave the pubkey UNKNOWN — caching null here is the poisoning bug");

answer = ([type, id], ws) => {
  if (type !== "REQ") return;
  ws.deliver(["EVENT", id, profileEvent(slow, "carol")]);
  ws.deliver(["EOSE", id]);
};
assert.strictEqual(await enrichProfiles([slow]), 1, "an unpoisoned pubkey is still askable");
assert.strictEqual(profiles.get(slow).name, "carol");

answer = (msg, ws) => { if (msg[0] === "REQ") ws.close(); };
const dropped = pk("d");
await enrichProfiles([dropped]);
assert.strictEqual(profiles.has(dropped), false, "a dropped connection states nothing");

// Kind-0 content is anyone's JSON: a field of the wrong type reads as absent
// instead of throwing out of every render that shows this author.
const odd = parseProfile({ pubkey: pk("f"), kind: 0, created_at: 1,
  content: JSON.stringify({ display_name: 42, name: ["x"], picture: {}, nip05: true, about: null, website: 7, lud16: [] }) });
for (const f of ["name", "display_name", "picture", "nip05", "about", "website", "lud16"]) {
  assert.strictEqual(odd[f], "", `a non-string ${f} becomes ""`);
}
assert.strictEqual(displayName(odd), "");
assert.strictEqual(displayName({ display_name: 42, name: " bob " }), "bob", "a cached non-string falls through too");
assert.strictEqual(parseProfile({ content: JSON.stringify({ displayName: "Zed", username: 3 }) }).display_name, "Zed");
for (const content of ["42", "null", "[1]", "\"str\""]) {
  assert.strictEqual(displayName(parseProfile({ content })), "", `content ${content} names no one`);
}
profiles.set(pk("f"), odd);
assert.ok(authorOf({ pubkey: pk("f") }).name.startsWith("npub1"), "an unnamed author falls back to the npub");

console.log("profiles: absence is cached only when the relay answered; odd fields read as absent");
