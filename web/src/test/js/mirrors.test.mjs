// The kind bound a count against this relay has to carry, read off the document the relay
// publishes and refused rather than guessed.
import assert from "node:assert/strict";
import {
  mirrorScope, scopedTo, readMirrorScope,
} from "../../main/resources/web/shared/mirrors.js";

const ok = (name) => console.log(`  ✓ ${name}`);

// The shape `/stats.json` serves: every section wrapped in the `{status, generatedAt, tookMs, data}`
// envelope.
const doc = (mirrors) => ({
  schema: 3,
  relay: "wss://relay.example.com",
  sync: { status: "ok", generatedAt: "2026-08-11T10:00:00Z", tookMs: 3, data: { mirrors } },
});

/** The same document with the envelope off, as a hand-built one; still accepted. */
const bare = (mirrors) => ({ schema: 3, relay: "wss://relay.example.com", sync: { mirrors } });

{
  const scope = mirrorScope(doc({ writtenAt: 1754900000, kinds: [0, 1, 30023] }));
  assert.deepEqual(scope, { kinds: [0, 1, 30023] });
  assert.deepEqual(scopedTo({ authors: ["abc"] }, scope), { authors: ["abc"], kinds: [0, 1, 30023] });
  assert.deepEqual(
    scopedTo({ authors: ["abc"], limit: 1 }, scope),
    { authors: ["abc"], limit: 1, kinds: [0, 1, 30023] },
    "the newest-event read carries it too — a reaction we never mirror is not this relay being behind",
  );
  ok("a published kind list becomes the bound on both counts");
}

{
  // `allKinds`: some stream names no kinds, so the relay asks upstream for everything.
  const scope = mirrorScope(doc({ allKinds: true }));
  assert.deepEqual(scope, { kinds: null });
  assert.deepEqual(scopedTo({ authors: ["abc"] }, scope), { authors: ["abc"] });
  assert.equal("kinds" in scopedTo({ authors: ["abc"] }, scope), false, "no bound means no `kinds` member");

  // A list beside the flag is only the union over the streams that named kinds.
  assert.deepEqual(mirrorScope(doc({ allKinds: true, kinds: [0, 1] })), { kinds: null });
  ok("`allKinds` is a scope of its own, and it beats a partial list");
}

{
  for (const [what, stats] of [
    ["no document at all", null],
    ["a 503 body", { error: "no statistics computed yet" }],
    ["a relay that mirrors nothing", { schema: 3 }],
    ["a sync section with no mirrors", { sync: { status: "ok", data: { streams: [] } } }],
    ["a sync section that failed outright", { sync: { status: "failed", data: {}, errors: { sync: "boom" } } }],
    ["a manifest naming streams but no kinds", doc({ writtenAt: 1, streams: [{ name: "content", dir: "up" }] })],
    ["kinds present and unreadable", doc({ kinds: "0,1,30023" })],
    ["an empty list", doc({ kinds: [] })],
    ["a list of nothing usable", doc({ kinds: ["1", null, -3, 1.5] })],
    ["allKinds as a string", doc({ allKinds: "true" })],
  ]) {
    assert.equal(mirrorScope(stats), null, what);
  }
  ok("an unreadable, absent or empty bound is null — never a bound of nothing");
}

{
  assert.deepEqual(mirrorScope(doc({ kinds: [0, 1] })), { kinds: [0, 1] }, "the served shape is the one that has to work");
  assert.deepEqual(mirrorScope(bare({ kinds: [0, 1] })), { kinds: [0, 1] }, "and an unwrapped document still reads");
  assert.deepEqual(
    mirrorScope({ sync: { data: { mirrors: { kinds: [7] } }, mirrors: { kinds: [0, 1] } } }),
    { kinds: [7] },
    "`data` is the payload; anything beside it is not what the relay publishes",
  );
  ok("the bound is read out of the section's `data` envelope, which is what the relay serves");
}

{
  assert.throws(() => scopedTo({ authors: ["abc"] }, null), /must not be taken/);
  assert.throws(() => scopedTo({ authors: ["abc"] }, undefined), /must not be taken/);
  ok("no scope refuses to build a filter rather than building the unscoped one");
}

{
  const served = (body, { ok: good = true } = {}) => async () => ({ ok: good, json: async () => body });

  assert.deepEqual(await readMirrorScope(served(doc({ kinds: [0, 1] }))), { kinds: [0, 1] });

  assert.equal(await readMirrorScope(served({ error: "no statistics computed yet" }, { ok: false })), null);
  assert.equal(await readMirrorScope(async () => ({ ok: true, json: async () => { throw new SyntaxError("<html>"); } })), null);
  assert.equal(await readMirrorScope(async () => { throw new TypeError("Failed to fetch"); }), null);
  assert.equal(await readMirrorScope(async () => null), null);

  // Document-relative, so a page mounted behind a path prefix reads this service's document.
  let asked = null;
  await readMirrorScope(async (url) => { asked = url; return { ok: true, json: async () => doc({ kinds: [1] }) }; });
  assert.equal(asked, "stats.json");
  ok("the scope is read from this relay, and every failure to read it is null");
}

console.log("mirrors: ok");
