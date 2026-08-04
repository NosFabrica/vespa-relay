import assert from 'assert';
globalThis.location = { protocol: "http:", host: "localhost:7787" };
globalThis.window = { addEventListener: () => {} };

const { card } = await import(new URL("../../relay/src/main/resources/web/cards.js", import.meta.url));
const { renderers } = await import(new URL("../../relay/src/main/resources/web/cards/base.js", import.meta.url));
const { seedProfiles } = await import(new URL("../../relay/src/main/resources/web/shared/profiles.js", import.meta.url));

const pk = "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2";
const pk2 = "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d";
const eid = "0123456789abcdef".repeat(4);
const now = Math.floor(Date.now() / 1000);
const ev = (kind, tags = [], content = "") => ({ id: eid, pubkey: pk, kind, created_at: now - 3600, tags, content });

// One fixture per kind, each asserting the renderer's distinctive output.
// [kind, event, substring the FULL render must contain]
const FIXTURES = [
  [0,     ev(0, [], JSON.stringify({ name: "alice", about: "hi", picture: "https://x/p.png", nip05: "a@b.co", website: "https://a.co" })), "nip05"],
  [1,     ev(1, [], "hello world"), "hello world"],
  [11,    ev(11, [], "thread post"), "thread post"],
  [9802,  ev(9802, [["r", "https://src.example/page"]], "the quoted passage"), "quote"],
  [40,    ev(40, [], JSON.stringify({ name: "my channel", about: "chat", picture: "https://x/c.png" })), "my channel"],
  [41,    ev(41, [], JSON.stringify({ name: "renamed channel" })), "renamed channel"],
  [3,     ev(3, [["p", pk2], ["p", pk]]), "follows <b>2</b>"],
  [30000, ev(30000, [["d", "friends"], ["title", "Friends"], ["p", pk2]]), "Friends"],
  [10002, ev(10002, [["r", "wss://relay.a.com"], ["r", "wss://relay.b.com", "read"]]), "relay.a.com"],
  [30002, ev(30002, [["title", "My relays"], ["relay", "wss://relay.c.com"]]), "relay.c.com"],
  [10040, ev(10040, [["30382:rank", pk2, "wss://nip85.example.com"]]), "30382:rank"],
  [30382, ev(30382, [["d", pk2], ["rank", "87"], ["followers", "1234"]]), "rank-big"],
  [30023, ev(30023, [["title", "My Article"], ["summary", "sum"], ["image", "https://x/i.jpg"], ["published_at", String(now)]], "full body text"), "full body text"],
  [30024, ev(30024, [["title", "Draft"]], "draft body"), "Draft"],
  [30818, ev(30818, [["title", "Wiki Page"], ["d", "wiki-page"]], "wiki text"), "Wiki Page"],
  [30004, ev(30004, [["title", "Best of"], ["a", "30023:x:y"], ["e", eid]]), "2 items"],
  [20,    ev(20, [["imeta", "url https://x/photo.jpg", "m image/jpeg"], ["title", "a photo"]]), "https://x/photo.jpg"],
  [21,    ev(21, [["imeta", "url https://x/v.mp4", "image https://x/poster.jpg"], ["title", "a video"]]), "<video"],
  [22,    ev(22, [["imeta", "url https://x/s.mp4"], ["title", "short"]]), "<video"],
  [34235, ev(34235, [["url", "https://x/h.mp4"], ["title", "h video"]]), "<video"],
  [34236, ev(34236, [["url", "https://x/v2.mp4"], ["title", "v short"]]), "<video"],
  [1063,  ev(1063, [["url", "https://x/f.pdf"], ["m", "application/pdf"], ["size", "123456"]], "a file"), "120.6 KB"],
  [1986,  ev(1986, [["url", "https://x/a.mp3"]], "an audio thing"), "<audio"],
  [30005, ev(30005, [["title", "Video set"], ["a", "34235:x:y"]]), "1 video"],
  [30030, ev(30030, [["title", "Pack"], ["emoji", "wave", "https://x/wave.png"]]), "emoji-grid"],
  [1337,  ev(1337, [["l", "kotlin"], ["name", "Snippet.kt"]], "fun main() {}"), "fun main() {}"],
  [1617,  ev(1617, [["subject", "Fix the thing"]], "--- a/f\n+++ b/f"), "codeblock"],
  [1621,  ev(1621, [["subject", "It breaks"]], "steps to reproduce"), "It breaks"],
  [30617, ev(30617, [["name", "my-repo"], ["description", "a repo"], ["web", "https://x/repo"], ["clone", "https://x/repo.git"]]), "my-repo"],
  [30063, ev(30063, [["title", "v1.2.0"], ["url", "https://x/rel.tgz"]], "release notes"), "v1.2.0"],
  [30311, ev(30311, [["title", "Live show"], ["status", "live"], ["streaming", "https://x/hls.m3u8"], ["current_participants", "42"]]), "status-pill live"],
  [31922, ev(31922, [["title", "Conference"], ["start", "2026-09-01"], ["end", "2026-09-03"], ["location", "Lisbon"]]), "Lisbon"],
  [31923, ev(31923, [["title", "Meetup"], ["start", String(now + 86400)]]), "Meetup"],
  [31924, ev(31924, [["title", "My calendar"], ["a", "31923:x:y"]]), "1 event"],
  [30402, ev(30402, [["title", "Bike for sale"], ["price", "250", "USD"], ["location", "Berlin"], ["image", "https://x/bike.jpg"]], "good bike"), "250 USD"],
  [30018, ev(30018, [], JSON.stringify({ name: "Widget", description: "a widget", price: 10, currency: "EUR", images: ["https://x/w.jpg"] })), "10 EUR"],
  [30017, ev(30017, [], JSON.stringify({ name: "My stall", description: "shop", currency: "USD" })), "My stall"],
  [9041,  ev(9041, [["amount", "2100000000"]], "help the cause"), "2,100,000 sats"],
  [30009, ev(30009, [["name", "Helper"], ["description", "helps"], ["image", "https://x/badge.png"]]), "Helper"],
  [31990, ev(31990, [["web", "https://app.example"]], JSON.stringify({ name: "CoolApp", about: "does things", picture: "https://x/app.png" })), "CoolApp"],
  [32267, ev(32267, [["name", "NativeApp"], ["icon", "https://x/icon.png"], ["description", "native"]]), "NativeApp"],
  [31989, ev(31989, [["d", "30023"], ["a", "31990:x:y"], ["a", "31990:z:w"]]), "2 handlers for kind 30023"],
  [31890, ev(31890, [["title", "My feed"], ["description", "stuff"]]), "My feed"],
];

// THE COVERAGE CLAIM, enforced: every registered kind must have a fixture,
// and every fixture must target a registered kind.
const registered = new Set(renderers.keys());
const covered = new Set(FIXTURES.map(([k]) => k));
const missing = [...registered].filter((k) => !covered.has(k)).sort((a, b) => a - b);
const stale = [...covered].filter((k) => !registered.has(k)).sort((a, b) => a - b);
assert.deepStrictEqual(missing, [], `registered kinds without a fixture: ${missing}`);
assert.deepStrictEqual(stale, [], `fixtures for unregistered kinds: ${stale}`);

for (const [kind, fixture, expect] of FIXTURES) {
  for (const opts of [undefined, { full: true }]) {
    const html = card(fixture, opts);
    assert(html.includes("<article"), `kind ${kind}: no card frame (${opts ? "full" : "preview"})`);
    assert(!html.includes("undefined"), `kind ${kind}: leaked "undefined" (${opts ? "full" : "preview"})`);
  }
  assert(card(fixture, { full: true }).includes(expect), `kind ${kind}: full render missing "${expect}"`);
}

// The generic floor: unknown kind renders, labelled honestly.
const unknown = card(ev(12345, [["title", "Mystery"], ["summary", "odd"]], "???"), { full: true });
assert(unknown.includes("kind 12345") && unknown.includes("Mystery"), "generic floor");

// Permalink depth is real: long content clamps in preview, not in full.
const long = "x".repeat(2000);
const preview = card(ev(1, [], long));
const full = card(ev(1, [], long), { full: true });
assert(preview.includes("clamp") && preview.includes("…"), "preview clips and clamps");
assert(!full.includes("clamp") && full.includes(long), "full mode renders every character");

// Media corner cases: a full-depth video with no url renders its text ONCE
// (the embed-vs-body composition double-rendered it), and a preview picture
// with no url still shows its text (it used to show nothing).
const noUrlVideo = card(ev(21, [["title", "the only body line"]]), { full: true });
assert.strictEqual((noUrlVideo.match(/the only body line/g) || []).length, 1, "video full/no-url: body once");
assert(card(ev(20, [], "picture text, no url")).includes("picture text, no url"), "picture preview keeps text without a url");

// Escaping holds in every renderer that touches content.
const hostile = card(ev(1, [], `<img src=x onerror=alert(1)>`), { full: true });
assert(!hostile.includes("<img src=x"), "content is escaped");

// Cards link internally now, not to njump.
const note = card(ev(1, [], "hi"));
assert(note.includes('href="/note1') && note.includes('href="/npub1'), "note links internal");
assert(!note.includes("njump.me"), "search cards no longer link out");

// Names over npubs, npubs over nothing, hex never.
// With a profile in the cache, the score and observer cards name the person;
// the npub survives only in the hover title and the href.
seedProfiles([
  ev(0, [], JSON.stringify({ name: "bob", display_name: "Bob Score" }), now),            // pk
  { ...ev(0, [], JSON.stringify({ name: "olga" }), now), pubkey: pk2 },                  // pk2, the author
]);
const scored = card({ ...ev(30382, [["d", pk], ["rank", "42"]]), pubkey: pk2 }, { full: true });
assert(scored.includes(">Bob Score</a>"), "score subject shows the name");
assert(!scored.includes(">npub1"), "score subject shows no npub once named");
const observer = card({ ...ev(10040, [["30382:rank", pk, "wss://x"]]), pubkey: pk2 }, { full: true });
assert(observer.includes(">Bob Score</a>"), "observer service shows the name");
// A named profile drops its pubkey row; a nameless one keeps it, as npub.
assert(!card(ev(0, [], JSON.stringify({ name: "carol" })), { full: true }).includes("<dt>pubkey</dt>"), "named profile: no pubkey row");
const nameless = card(ev(0, [], "{}"), { full: true });
assert(nameless.includes("<dt>pubkey</dt>") && nameless.includes(">npub1"), "nameless profile: npub row remains");
// Rendered TEXT never contains a bare hex pubkey (attributes like data-pk may).
for (const html of [scored, observer, nameless]) {
  const text = html.replace(/<[^>]*>/g, " ");
  assert(!text.includes(pk) && !text.includes(pk2), "hex is a storage format, not display text");
}

console.log(`all kinds: ${FIXTURES.length} bespoke renderers + generic floor, all assertions passed`);
