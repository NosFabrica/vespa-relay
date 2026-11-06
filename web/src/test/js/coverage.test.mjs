// What the sync says about one reader's own posts, read off its /coverage route and refused
// rather than guessed: no sync named, no answer, or an answer this page cannot read all skip the stage.
import assert from "node:assert/strict";
import { coverageOf, progressOf, readCoverage, syncUrl } from "../../main/resources/web/shared/coverage.js";

const ok = (name) => console.log(`  ✓ ${name}`);

const me = "a1".repeat(32);

/** The document the sync serves, trimmed to what a case needs. */
const doc = (relays, followed = relays.length > 0) => ({ pubkey: me, generatedAt: 1754900000, followed, relays });

{
  const page = (content) => ({ querySelector: (sel) => (sel === 'meta[name="sync-url"]' && content != null ? { getAttribute: () => content } : null) });
  assert.equal(syncUrl(page("https://sync.example.com/")), "https://sync.example.com", "a trailing slash is the relay's spelling, not a path");
  assert.equal(syncUrl(page(null)), null, "no meta: this relay names no sync");
  assert.equal(syncUrl(page("  ")), null);
  assert.equal(syncUrl(page("javascript:alert(1)")), null, "only an http(s) address is asked");
  ok("the sync's address comes from the relay's meta, or there is none");
}

{
  const c = coverageOf(doc([{ relay: "wss://a.example/", stream: "content", syncStatus: "complete", coveredTo: 1754899000, tailed: true }]));
  assert.deepEqual(c, {
    followed: true,
    relays: [{ relay: "wss://a.example/", syncStatus: "complete" }],
    unread: [],
  });
  assert.deepEqual(
    coverageOf({ ...doc([], false), unread: ["wss://b.example/", 7, ""] }),
    { followed: false, relays: [], unread: ["wss://b.example/"] },
    "nobody reads this reader's relays, and the sync names the ones it does not read",
  );
  assert.equal(coverageOf({ error: "too many coverage requests" }), null);
  assert.equal(coverageOf(doc([{ stream: "content" }], true)), null, "followed with no readable row is a document we cannot read");
  assert.equal(coverageOf(null), null);
  ok("an answer is read as {followed, relays}, and anything else as no answer");
}

{
  const p = progressOf(
    coverageOf(
      doc([
        { relay: "wss://a.example/", stream: "content", syncStatus: "paging" },
        { relay: "wss://a.example/", stream: "notes", syncStatus: "complete" },
        { relay: "wss://b.example/", stream: "content", syncStatus: "complete" },
        { relay: "wss://c.example/", stream: "content", syncStatus: "refused" },
      ]),
    ),
  );
  assert.equal(p.total, 3, "relays, not pairs");
  assert.equal(p.done, 1, "a relay two streams read is done only when both are");
  assert.deepEqual(p.relays.map((r) => [r.relay, r.status]), [
    ["wss://c.example/", "refused"],
    ["wss://a.example/", "paging"],
    ["wss://b.example/", "complete"],
  ], "least finished first, each relay at its least finished pair");
  ok("progress counts relays, each at the status of its least finished pair");
}

{
  const asked = [];
  const fetcher = async (url) => {
    asked.push(url);
    return { ok: true, json: async () => doc([{ relay: "wss://a.example/", syncStatus: "complete" }]) };
  };
  const c = await readCoverage(me, { base: "https://sync.example.com", fetcher });
  assert.equal(c.followed, true);
  assert.deepEqual(asked, [`https://sync.example.com/coverage/${me}?kind=1`], "asked about posts, kind 1");
  await readCoverage(me, { base: "https://sync.example.com", fetcher, relays: ["wss://a.example/", "wss://b.example/x?y"] });
  assert.equal(
    asked.pop(),
    `https://sync.example.com/coverage/${me}?kind=1&relay=wss%3A%2F%2Fa.example%2F&relay=wss%3A%2F%2Fb.example%2Fx%3Fy`,
    "the reader's write relays ride along, each escaped",
  );
  const many = Array.from({ length: 30 }, (_, i) => `wss://r${i}.example/`);
  await readCoverage(me, { base: "https://s", fetcher, relays: many });
  assert.equal((asked.pop().match(/relay=/g) || []).length, 20, "no more relays than the sync reads");

  assert.equal(await readCoverage(me, { base: null, fetcher }), null, "no sync named, nothing asked");
  assert.equal(await readCoverage("not-hex", { base: "https://sync.example.com", fetcher }), null);
  assert.equal(asked.length, 1, "neither of those asked anything");
  assert.equal(await readCoverage(me, { base: "https://s", fetcher: async () => ({ ok: false, status: 429 }) }), null, "a refusal is no answer");
  assert.equal(await readCoverage(me, { base: "https://s", fetcher: async () => { throw new TypeError("CORS"); } }), null);
  ok("the sync is asked once, at /coverage/<pubkey>, and every failure is no answer");
}
