// The kind bound a count against this relay has to carry — read off the
// document the relay already publishes, and refused rather than guessed.
//
// The bug it exists for is one number: "35% mirroring", drawn from 31,118 here
// of 89,485 on the author's own relay, on a mirror missing nothing it had ever
// been asked to hold. Our side was narrowed by the router, theirs was not. So
// the properties asserted here are all about which way this fails: an unknown
// scope must never become an unscoped count, a partial union must never become
// a bound, and an empty list must never read as "everything".
import assert from "node:assert/strict";
import {
  mirrorScope, scopedTo, readMirrorScope,
} from "../../relay/src/main/resources/web/shared/mirrors.js";

const ok = (name) => console.log(`  ✓ ${name}`);

/** A `/stats.json` with whatever `sync.mirrors` the case is about. */
const doc = (mirrors) => ({ schema: 3, relay: "wss://relay.example.com", sync: { mirrors } });

// ---- the bound, when the relay states one ---------------------------------
{
  const scope = mirrorScope(doc({ writtenAt: 1754900000, kinds: [0, 1, 30023] }));
  assert.deepEqual(scope, { kinds: [0, 1, 30023] });
  // Same object on both sides of the comparison, which is the whole fix.
  assert.deepEqual(scopedTo({ authors: ["abc"] }, scope), { authors: ["abc"], kinds: [0, 1, 30023] });
  assert.deepEqual(
    scopedTo({ authors: ["abc"], limit: 1 }, scope),
    { authors: ["abc"], limit: 1, kinds: [0, 1, 30023] },
    "the newest-event read carries it too — a reaction we never mirror is not this relay being behind",
  );
  ok("a published kind list becomes the bound on both counts");
}

// ---- a mirror with no bound at all ----------------------------------------
{
  // `allKinds` is the writer saying some stream names no kinds, so this relay
  // asks its upstreams for everything they serve. There is nothing to narrow
  // to, and an unscoped comparison is already like-for-like.
  const scope = mirrorScope(doc({ allKinds: true }));
  assert.deepEqual(scope, { kinds: null });
  assert.deepEqual(scopedTo({ authors: ["abc"] }, scope), { authors: ["abc"] });
  assert.equal("kinds" in scopedTo({ authors: ["abc"] }, scope), false, "no bound means no `kinds` member");

  // Both present is the writer contradicting itself, and only one direction is
  // safe: a list beside the flag can only be the union over the streams that
  // DID name kinds, which is smaller than the truth and would under-count the
  // denominator this whole mechanism exists to fix.
  assert.deepEqual(mirrorScope(doc({ allKinds: true, kinds: [0, 1] })), { kinds: null });
  ok("`allKinds` is a scope of its own, and it beats a partial list");
}

// ---- everything that means "we cannot say" --------------------------------
{
  for (const [what, stats] of [
    ["no document at all", null],
    ["a 503 body", { error: "no statistics computed yet" }],
    ["a relay that mirrors nothing", { schema: 3 }],
    ["a sync section with no mirrors", { sync: { coverage: {} } }],
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

// ---- and null must never quietly become the old comparison ----------------
{
  // The one that matters. Passing the filter straight through on a null scope
  // is the 35%, and at the call site it looks exactly like working code.
  assert.throws(() => scopedTo({ authors: ["abc"] }, null), /must not be taken/);
  assert.throws(() => scopedTo({ authors: ["abc"] }, undefined), /must not be taken/);
  ok("no scope refuses to build a filter rather than building the unscoped one");
}

// ---- reading it off the relay ---------------------------------------------
{
  const served = (body, { ok: good = true } = {}) => async () => ({ ok: good, json: async () => body });

  assert.deepEqual(await readMirrorScope(served(doc({ kinds: [0, 1] }))), { kinds: [0, 1] });

  // Every way the ask can fail to answer, and all of them are the same answer
  // to the caller: 503 until the first rollup lands (minutes, after a deploy),
  // a relay serving no statistics route at all, a body that is not JSON, a
  // network that is not there.
  assert.equal(await readMirrorScope(served({ error: "no statistics computed yet" }, { ok: false })), null);
  assert.equal(await readMirrorScope(async () => ({ ok: true, json: async () => { throw new SyntaxError("<html>"); } })), null);
  assert.equal(await readMirrorScope(async () => { throw new TypeError("Failed to fetch"); }), null);
  assert.equal(await readMirrorScope(async () => null), null);

  // It asks this relay, not the reader's write relay — the kind list is a fact
  // about the mirror and nobody else can state it.
  let asked = null;
  await readMirrorScope(async (url) => { asked = url; return { ok: true, json: async () => doc({ kinds: [1] }) }; });
  assert.equal(asked, "/stats.json");
  ok("the scope is read from this relay, and every failure to read it is null");
}

console.log("mirrors: ok");
